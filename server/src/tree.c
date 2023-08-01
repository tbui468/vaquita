#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "util.h"
#include "tree.h"

static struct VdbPtr vdbtree_intern_read_right_ptr(struct VdbTree* tree, uint32_t idx);
static struct VdbPtr vdbtree_intern_read_ptr(struct VdbTree* tree, uint32_t idx, uint32_t ptr_idx);
static uint32_t vdbtree_data_init(struct VdbTree* tree, uint32_t parent_idx);
static uint32_t vdbtree_intern_read_ptr_count(struct VdbTree* tree, uint32_t idx);
static enum VdbNodeType vdbtree_node_type(struct VdbTree* tree, uint32_t idx);
void vdbtree_serialize_to_data_block_if_varlen(struct VdbTree* tree, struct VdbValue* v);
void vdbtree_deserialize_from_data_block_if_varlen(struct VdbTree* tree, struct VdbValue* v);
uint32_t vdbtree_get_data_block(struct VdbTree* tree, uint32_t datacell_size);

void vdbtree_serialize_value(struct VdbTree* tree, uint8_t* buf, struct VdbValue* v) {
    vdbtree_serialize_to_data_block_if_varlen(tree, v);
    vdbvalue_serialize(buf, *v);
}

void vdbtree_deserialize_value(struct VdbTree* tree, struct VdbValue* v, uint8_t* buf) {
    vdbvalue_deserialize(v, buf);
    vdbtree_deserialize_from_data_block_if_varlen(tree, v);
}

void vdbtree_serialize_recptr(struct VdbTree* tree, uint8_t* buf, struct VdbRecPtr* p) {
    *((uint32_t*)buf) = p->block_idx;
    *((uint32_t*)(buf + sizeof(uint32_t))) = p->idxcell_idx;
    vdbtree_serialize_value(tree, buf + sizeof(uint32_t) * 2, &p->key);
}

struct VdbRecPtr vdbtree_deserialize_recptr(struct VdbTree* tree, uint8_t* buf) {
    struct VdbRecPtr p;

    p.block_idx = *((uint32_t*)buf);
    p.idxcell_idx = *((uint32_t*)(buf + sizeof(uint32_t)));
    vdbtree_deserialize_value(tree, &p.key, buf + sizeof(uint32_t) * 2);

    return p;
}

struct VdbRecPtr vdbtree_append_record_to_datablock(struct VdbTree* tree, struct VdbRecord* r) {
    uint32_t data_idx = vdbtree_get_data_block(tree, vdbrecord_fixedlen_size(r));
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->f, data_idx);

    uint32_t idxcell_idx = vdbnode_new_idxcell(page->buf, vdbrecord_fixedlen_size(r));

    for (uint32_t i = 0; i < r->count; i++) {
        vdbtree_serialize_to_data_block_if_varlen(tree, &r->data[i]);
    }
    vdbrecord_serialize(vdbnode_datacell(page->buf, idxcell_idx), r);

    vdbpager_unpin_page(page, true);

    struct VdbRecPtr p;
    p.block_idx = data_idx;
    p.idxcell_idx = idxcell_idx;
    p.key = r->data[tree->schema->key_idx];

    return p;
}

void vdbtree_write_record_to_datablock(struct VdbTree* tree, struct VdbRecord* r, uint32_t leaf_idx, uint32_t leaf_idxcell_idx) {
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->f, leaf_idx);
    //TODO: need to free value if its heap allocated
    struct VdbRecPtr p = vdbtree_deserialize_recptr(tree, vdbnode_datacell(page->buf, leaf_idxcell_idx));
    vdbpager_unpin_page(page, false);

    struct VdbPage* data_page = vdbpager_pin_page(tree->pager, tree->f, p.block_idx);
    for (uint32_t i = 0; i < r->count; i++) {
        vdbtree_serialize_to_data_block_if_varlen(tree, &r->data[i]);
    }
    vdbrecord_serialize(vdbnode_datacell(data_page->buf, p.idxcell_idx), r);
    vdbpager_unpin_page(data_page, true);
}

struct VdbRecord* vdbtree_read_record_from_datablock(struct VdbTree* tree, struct VdbRecPtr* p) {
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->f, p->block_idx);

    struct VdbRecord* r = vdbrecord_deserialize(vdbnode_datacell(page->buf, p->idxcell_idx), tree->schema);
    for (uint32_t i = 0; i < r->count; i++) {
        vdbtree_deserialize_from_data_block_if_varlen(tree, &r->data[i]);
    }

    vdbpager_unpin_page(page, false);
    return r;
}

/*
 * Tree wrapper for generic node
 */

static enum VdbNodeType vdbtree_node_type(struct VdbTree* tree, uint32_t idx) {
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->f, idx);
    enum VdbNodeType type = *vdbnode_type(page->buf);
    vdbpager_unpin_page(page, false);
    return type;
}

/*
 * Tree wrappers for meta node
 */

static uint32_t vdbtree_meta_init(struct VdbTree* tree, struct VdbSchema* schema) {
    uint32_t idx = vdbpager_fresh_page(tree->f);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->f, idx);

    *vdbnode_type(page->buf) = VDBN_META;
    *vdbnode_parent(page->buf) = 0;
    *vdbmeta_auto_counter_ptr(page->buf) = 0;
    *vdbmeta_root_ptr(page->buf) = 0;
    *vdbmeta_data_block_ptr(page->buf) = vdbtree_data_init(tree, idx);
    vdbmeta_allocate_schema_ptr(page->buf, vdbschema_serialized_size(schema));
    vdbschema_serialize(vdbmeta_schema_ptr(page->buf), schema);

    vdbpager_unpin_page(page, true);

    return idx;
}

uint32_t vdbtree_meta_read_primary_key_counter(struct VdbTree* tree) {
    assert(vdbtree_node_type(tree, 0) == VDBN_META);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->f, 0);
    uint32_t pk_counter = *vdbmeta_auto_counter_ptr(page->buf);
    vdbpager_unpin_page(page, false);
    return pk_counter;
}

uint32_t vdbtree_meta_read_root(struct VdbTree* tree) {
    assert(vdbtree_node_type(tree, 0) == VDBN_META);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->f, 0);
    uint32_t root_idx = *vdbmeta_root_ptr(page->buf);
    vdbpager_unpin_page(page, false);
    return root_idx;
}

static void vdbtree_meta_write_root(struct VdbTree* tree, uint32_t root_idx) {
    assert(vdbtree_node_type(tree, 0) == VDBN_META);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->f, 0);
    *vdbmeta_root_ptr(page->buf) = root_idx;
    vdbpager_unpin_page(page, true);
}

uint32_t vdbtree_meta_increment_primary_key_counter(struct VdbTree* tree) {
    assert(vdbtree_node_type(tree, 0) == VDBN_META);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->f, 0);
    *vdbmeta_auto_counter_ptr(page->buf) += 1;
    uint32_t pk_counter = *vdbmeta_auto_counter_ptr(page->buf);
    vdbpager_unpin_page(page, true);

    return pk_counter;
}

/*
 * Tree wrappers for internal node
 */

static uint32_t vdbtree_intern_init(struct VdbTree* tree, uint32_t parent_idx) {
    uint32_t idx = vdbpager_fresh_page(tree->f);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->f, idx);

    *vdbnode_type(page->buf) = VDBN_INTERN;
    *vdbnode_parent(page->buf) = parent_idx;
    *vdbintern_rightptr_block(page->buf) = 0;
    *vdbnode_idxcell_count(page->buf) = 0;
    *vdbnode_datacells_size(page->buf) = 0;
    *vdbnode_idxcells_freelist(page->buf) = 0;
    *vdbnode_datacells_freelist(page->buf) = 0;

    vdbpager_unpin_page(page, true);

    return idx;
}

static void vdbtree_intern_write_right_ptr(struct VdbTree* tree, uint32_t idx, struct VdbPtr ptr) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->f, idx);
    *vdbintern_rightptr_block(page->buf) = ptr.block_idx;
    vdbtree_serialize_value(tree, vdbintern_rightptr_key(page->buf), &ptr.key);
    vdbpager_unpin_page(page, true);
}

static void vdbtree_intern_write_new_ptr(struct VdbTree* tree, uint32_t idx, struct VdbPtr ptr) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->f, idx);

    uint32_t block_idx_size = sizeof(uint32_t);
    uint32_t key_size = vdbvalue_serialized_size(ptr.key);
    uint32_t idxcell_idx = vdbnode_new_idxcell(page->buf, block_idx_size + key_size);

    uint8_t* buf = vdbnode_datacell(page->buf, idxcell_idx);
    *((uint32_t*)buf) = ptr.block_idx;
    vdbtree_serialize_value(tree, buf + sizeof(uint32_t), &ptr.key);

    vdbpager_unpin_page(page, true);
}

static uint32_t vdbtree_intern_read_ptr_count(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->f, idx);
    uint32_t count = *vdbnode_idxcell_count(page->buf);
    vdbpager_unpin_page(page, false);
    return count;
}

static struct VdbPtr vdbtree_intern_read_ptr(struct VdbTree* tree, uint32_t idx, uint32_t ptr_idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->f, idx);

    struct VdbPtr ptr;
    uint8_t* buf = vdbnode_datacell(page->buf, ptr_idx);
    ptr.block_idx = *((uint32_t*)buf);
    vdbtree_deserialize_value(tree, &ptr.key, buf + sizeof(uint32_t));

    vdbpager_unpin_page(page, false);
    return ptr;
}

static struct VdbPtr vdbtree_intern_read_right_ptr(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->f, idx);

    struct VdbPtr right;
    right.block_idx = *vdbintern_rightptr_block(page->buf);
    vdbtree_deserialize_value(tree, &right.key, vdbintern_rightptr_key(page->buf));

    vdbpager_unpin_page(page, false);
    return right;
}

static uint32_t vdbtree_intern_read_parent(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->f, idx);
    uint32_t parent = *vdbnode_parent(page->buf);
    vdbpager_unpin_page(page, false);
    return parent;
}

static void vdbtree_intern_write_parent(struct VdbTree* tree, uint32_t idx, uint32_t parent) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->f, idx);
    *vdbnode_parent(page->buf) = parent;
    vdbpager_unpin_page(page, true);
}

static bool vdbtree_intern_can_fit_ptr(struct VdbTree* tree, uint32_t idx, struct VdbValue key) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->f, idx);
    uint32_t block_idx_size = sizeof(uint32_t);
    bool can_fit = vdbnode_can_fit(page->buf, vdbvalue_serialized_size(key) + block_idx_size);
    vdbpager_unpin_page(page, false);
    return can_fit;
}

static uint32_t vdbtree_intern_split(struct VdbTree* tree, uint32_t idx, struct VdbValue new_right_key) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    uint32_t parent = vdbtree_intern_read_parent(tree, idx);

    if (parent == 0) { //split full root
        uint32_t old_root = vdbtree_meta_read_root(tree);
        struct VdbValue old_key = vdbtree_intern_read_right_ptr(tree, old_root).key;
        uint32_t new_root = vdbtree_intern_init(tree, 0);
        struct VdbPtr new_left_ptr = {old_root, old_key};
        vdbtree_intern_write_new_ptr(tree, new_root, new_left_ptr);
        vdbtree_intern_write_parent(tree, old_root, new_root);

        uint32_t new_right = vdbtree_intern_init(tree, new_root);
        struct VdbPtr new_right_ptr = {new_right, new_right_key};
        vdbtree_intern_write_right_ptr(tree, new_root, new_right_ptr);
        
        vdbtree_meta_write_root(tree, new_root);
        return new_right;
    } else if (vdbtree_intern_can_fit_ptr(tree, parent, new_right_key)) {
        struct VdbPtr old_right = vdbtree_intern_read_right_ptr(tree, parent);
        vdbtree_intern_write_new_ptr(tree, parent, old_right);

        uint32_t new_intern = vdbtree_intern_init(tree, parent);
        struct VdbPtr new_right = {new_intern, new_right_key};
        vdbtree_intern_write_right_ptr(tree, parent, new_right);
        return new_intern;
    } else { 
        uint32_t new_parent = vdbtree_intern_split(tree, parent, new_right_key);
        uint32_t new_intern = vdbtree_intern_init(tree, new_parent);
        struct VdbPtr ptr = {new_intern, new_right_key};
        vdbtree_intern_write_right_ptr(tree, new_parent, ptr);
        return new_intern;
    }

}


/*
 * Tree wrappers for leaf node
 */

static uint32_t vdbtree_leaf_init(struct VdbTree* tree, uint32_t parent_idx) {
    uint32_t idx = vdbpager_fresh_page(tree->f);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->f, idx);

    *vdbnode_type(page->buf) = VDBN_LEAF;
    *vdbnode_parent(page->buf) = parent_idx;
    *vdbnode_next(page->buf) = 0;
    *vdbnode_idxcell_count(page->buf) = 0;
    *vdbnode_datacells_size(page->buf) = 0;
    *vdbnode_idxcells_freelist(page->buf) = 0;
    *vdbnode_datacells_freelist(page->buf) = 0;

    vdbpager_unpin_page(page, true);

    return idx;
}

static uint32_t vdbtree_leaf_read_parent(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->f, idx);
    uint32_t parent_idx = *vdbnode_parent(page->buf);
    vdbpager_unpin_page(page, false);
    return parent_idx;
}

uint32_t vdbtree_leaf_read_record_count(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->f, idx);
    uint32_t count = *vdbnode_idxcell_count(page->buf);
    vdbpager_unpin_page(page, false);
    return count;
}

struct VdbValue vdbtree_leaf_read_record_key(struct VdbTree* tree, uint32_t idx, uint32_t rec_idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->f, idx);

    struct VdbRecord* rec = vdbtree_leaf_read_record(tree, idx, rec_idx);
    if (!rec) printf("rec is null\n"); //TODO: this should never happend...?

    struct VdbValue v = rec->data[tree->schema->key_idx];
    if (v.type == VDBT_TYPE_TEXT) {
        v = vdbstring(v.as.Str.start, v.as.Str.len);
    }

    vdbrecord_free(rec);

    vdbpager_unpin_page(page, false);
    return v;
}

void vdbtree_free_datablock_string(struct VdbTree* tree, struct VdbValue* v) {
    struct VdbPage* data_page = vdbpager_pin_page(tree->pager, tree->f, v->as.Str.block_idx);

    vdbnode_free_cell_and_defrag_datacells_only(data_page->buf, v->as.Str.idxcell_idx);

    vdbpager_unpin_page(data_page, true);
}

struct VdbRecord* vdbtree_leaf_read_record(struct VdbTree* tree, uint32_t idx, uint32_t rec_idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->f, idx);

    struct VdbRecPtr p = vdbtree_deserialize_recptr(tree, vdbnode_datacell(page->buf, rec_idx));
    struct VdbRecord* r = vdbtree_read_record_from_datablock(tree, &p);

    vdbpager_unpin_page(page, false);
    return r;
}

uint32_t vdbtree_leaf_split(struct VdbTree* tree, uint32_t idx, struct VdbValue new_right_key) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);

    uint32_t parent = vdbtree_leaf_read_parent(tree, idx);

    if (!vdbtree_intern_can_fit_ptr(tree, parent, new_right_key)) {
        parent = vdbtree_intern_split(tree, parent, new_right_key);
    } else {
        //copy right pointer into ptr list to make room for new right ptr
        uint32_t count = vdbtree_leaf_read_record_count(tree, idx);
        struct VdbValue last_rec_key = vdbtree_leaf_read_record_key(tree, idx, count - 1);

        struct VdbPtr ptr = {idx, last_rec_key};
        vdbtree_intern_write_new_ptr(tree, parent, ptr);
    }

    uint32_t new_leaf = vdbtree_leaf_init(tree, parent);
    struct VdbPtr new_right = {new_leaf, new_right_key};
    vdbtree_intern_write_right_ptr(tree, parent, new_right);

    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->f, idx);
    *vdbnode_next(page->buf) = new_leaf;
    vdbpager_unpin_page(page, true);

    return new_leaf;
}

uint32_t vdbtree_leaf_read_next_leaf(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->f, idx);
    uint32_t next_leaf_idx = *vdbnode_next(page->buf);

    vdbpager_unpin_page(page, false);

    return next_leaf_idx;
}

/*
 * Tree wrappers for data node
 */

static uint32_t vdbtree_data_init(struct VdbTree* tree, uint32_t parent_idx) {
    uint32_t idx = vdbpager_fresh_page(tree->f);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->f, idx);

    *vdbnode_type(page->buf) = VDBN_DATA;
    *vdbnode_parent(page->buf) = parent_idx;
    *vdbnode_next(page->buf) = 0;
    *vdbnode_idxcell_count(page->buf) = 0;
    *vdbnode_datacells_size(page->buf) = 0;
    *vdbnode_idxcells_freelist(page->buf) = 0;
    *vdbnode_datacells_freelist(page->buf) = 0;

    vdbpager_unpin_page(page, true);

    return idx;
}

/*
 * VdbTree API
 */

struct VdbTree* vdb_tree_init(const char* name, struct VdbSchema* schema, struct VdbPager* pager, FILE* f) {
    struct VdbTree* tree = malloc_w(sizeof(struct VdbTree));
    tree->name = strdup_w(name);
    tree->pager = pager;
    tree->f = f;
    tree->schema = vdb_schema_copy(schema);

    uint32_t meta_idx = vdbtree_meta_init(tree, schema);
    tree->meta_idx = meta_idx;
    uint32_t root_idx = vdbtree_intern_init(tree, meta_idx);
    vdbtree_meta_write_root(tree, root_idx);
    uint32_t leaf_idx = vdbtree_leaf_init(tree, root_idx);
    struct VdbPtr right_ptr = {leaf_idx, vdbint(0)};
    vdbtree_intern_write_right_ptr(tree, root_idx, right_ptr);

    return tree;
}

struct VdbTree* vdb_tree_open(const char* name, FILE* f, struct VdbPager* pager) {
    struct VdbTree* tree = malloc_w(sizeof(struct VdbTree));
    tree->name = strdup(name);
    tree->f = f;
    tree->pager = pager;
    tree->meta_idx = 0;

    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->f, 0);
    tree->schema = vdbschema_deserialize(vdbmeta_schema_ptr(page->buf));
    vdbpager_unpin_page(page, false);

    return tree;
}

void vdb_tree_close(struct VdbTree* tree) {
    fclose_w(tree->f);
    free_w(tree->name, sizeof(char) * (strlen(tree->name) + 1));
    free_w(tree, sizeof(struct VdbTree));
    vdb_schema_free(tree->schema);
}

uint32_t vdbtree_traverse_to_first_leaf(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);

    if (vdbtree_intern_read_ptr_count(tree, idx) > 0) {
        struct VdbPtr p = vdbtree_intern_read_ptr(tree, idx, 0);
        if (vdbtree_node_type(tree, p.block_idx) == VDBN_LEAF) {
            return p.block_idx;
        } else {
            return vdbtree_traverse_to_first_leaf(tree, p.block_idx);
        }
    } else {
        struct VdbPtr p = vdbtree_intern_read_right_ptr(tree, idx);

        if (vdbtree_node_type(tree, p.block_idx) == VDBN_LEAF) {
            return p.block_idx;
        } else {
            return vdbtree_traverse_to_first_leaf(tree, p.block_idx);
        }
    }

}

uint32_t vdb_tree_traverse_to(struct VdbTree* tree, uint32_t idx, struct VdbValue key) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);

    //Check to see if any ptrs have correct key
    //TODO: switch to binary search
    for (uint32_t i = 0; i < vdbtree_intern_read_ptr_count(tree, idx); i++) {
        struct VdbPtr p = vdbtree_intern_read_ptr(tree, idx, i);
        if (vdbtree_node_type(tree, p.block_idx) == VDBN_LEAF && vdbtree_leaf_read_record_count(tree, p.block_idx) > 0) {
            struct VdbValue last_rec_key = vdbtree_leaf_read_record_key(tree, p.block_idx, vdbtree_leaf_read_record_count(tree, p.block_idx) - 1);
            if (vdbvalue_compare(key, last_rec_key) <= 0) {
                return p.block_idx;
            }
        } else if (vdbtree_node_type(tree, p.block_idx) == VDBN_INTERN) {
            //get largest key for a given internal node, and compare to key
            struct VdbPtr right = vdbtree_intern_read_right_ptr(tree, p.block_idx);
            while (vdbtree_node_type(tree, right.block_idx) != VDBN_LEAF) {
                right = vdbtree_intern_read_right_ptr(tree, right.block_idx);
            }
            struct VdbValue last_rec_key = vdbtree_leaf_read_record_key(tree, right.block_idx, vdbtree_leaf_read_record_count(tree, right.block_idx) - 1);
            if (vdbvalue_compare(key, last_rec_key) <= 0) {
                return vdb_tree_traverse_to(tree, p.block_idx, key);
            }
        }
    }

    //if not found, check right pointer
    struct VdbPtr p = vdbtree_intern_read_right_ptr(tree, idx);

    if (vdbtree_node_type(tree, p.block_idx) == VDBN_LEAF) {
        return p.block_idx;
    } else {
        return vdb_tree_traverse_to(tree, p.block_idx, key);
    }

}

uint32_t vdbtree_get_data_block(struct VdbTree* tree, uint32_t datacell_size) {
    struct VdbPage* meta_page = vdbpager_pin_page(tree->pager, tree->f, 0);
    uint32_t data_idx = *vdbmeta_data_block_ptr(meta_page->buf);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->f, data_idx);

    if (!vdbnode_can_fit(page->buf, datacell_size)) {
        uint32_t old_data_idx = data_idx;
        data_idx = vdbtree_data_init(tree, 0);
        *vdbmeta_data_block_ptr(meta_page->buf) = data_idx;
        vdbpager_unpin_page(page, false);
        page = vdbpager_pin_page(tree->pager, tree->f, data_idx);
        *vdbnode_next(page->buf) = old_data_idx;
        vdbpager_unpin_page(page, true);
    } else {
        vdbpager_unpin_page(page, false);
    }

    vdbpager_unpin_page(meta_page, true);

    return data_idx;
}

void vdbtree_serialize_to_data_block_if_varlen(struct VdbTree* tree, struct VdbValue* v) {
    if (v->type != VDBT_TYPE_TEXT) {
        return;
    }

    uint32_t datacell_size = sizeof(uint32_t) + v->as.Str.len + sizeof(uint8_t);
    uint32_t data_idx = vdbtree_get_data_block(tree, datacell_size);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->f, data_idx);

    uint32_t idxcell_idx = vdbnode_new_idxcell(page->buf, datacell_size);
    vdbvalue_serialize_string(vdbnode_datacell(page->buf, idxcell_idx), v);

    v->as.Str.block_idx = data_idx;
    v->as.Str.idxcell_idx = idxcell_idx;

    vdbpager_unpin_page(page, true);
}

void vdbtree_deserialize_from_data_block_if_varlen(struct VdbTree* tree, struct VdbValue* v) {
    if (v->type != VDBT_TYPE_TEXT) {
        return;
    }

    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->f, v->as.Str.block_idx);
    void* ptr = vdbnode_datacell(page->buf, v->as.Str.idxcell_idx);
    vdbvalue_deserialize_string(v, ptr);

    vdbpager_unpin_page(page, false);
}

struct VdbTreeList* vdb_treelist_init() {
    struct VdbTreeList* tl = malloc_w(sizeof(struct VdbTreeList));
    tl->count = 0;
    tl->capacity = 8;
    tl->trees = malloc_w(sizeof(struct VdbTree*) * tl->capacity);

    return tl;
}

void vdb_treelist_append_tree(struct VdbTreeList* tl, struct VdbTree* tree) {
    if (tl->count == tl->capacity) {
        uint32_t old_cap = tl->capacity;
        tl->capacity *= 2;
        tl->trees = realloc_w(tl->trees, sizeof(struct VdbTree*) * tl->capacity, sizeof(struct VdbTree*) * old_cap);
    }

    tl->trees[tl->count++] = tree;
}

struct VdbTree* vdb_treelist_get_tree(struct VdbTreeList* tl, const char* name) {
    struct VdbTree* t;
    for (int i = 0; i < tl->count; i++) {
        t = tl->trees[i];
        if (strncmp(name, t->name, strlen(name)) == 0)
            return t;
    }

    return NULL;
}

struct VdbTree* vdb_treelist_remove_tree(struct VdbTreeList* tl, const char* name) {
    struct VdbTree* t = NULL;
    int i;
    for (i = 0; i < tl->count; i++) {
        t = tl->trees[i];
        if (strncmp(name, t->name, strlen(name)) == 0)
            break;
    }

    if (t) {
        tl->trees[i] = tl->trees[--tl->count];
    }

    return t;
}

void vdb_treelist_free(struct VdbTreeList* tl) {
    for (int i = 0; i < tl->count; i++) {
        struct VdbTree* t = tl->trees[i];
        vdb_tree_close(t);
    }

    free_w(tl->trees, sizeof(struct VdbTree*) * tl->capacity);
    free_w(tl, sizeof(struct VdbTreeList));
}


static void vdbtree_print_node(struct VdbTree* tree, uint32_t idx, uint32_t depth) {
    char spaces[depth * 4 + 1];
    memset(spaces, ' ', sizeof(spaces) - 1);
    spaces[depth * 4] = '\0';
    printf("%s%d", spaces, idx);

    if (vdbtree_node_type(tree, idx) == VDBN_INTERN) {
        printf("\n");
        //TODO: make not printing leaves an option later
        /*
        if (vdbtree_node_type(tree, vdbtree_intern_read_right_ptr(tree, idx).block_idx) == VDBN_LEAF) {
            return;
        }*/
        for (uint32_t i = 0; i < vdbtree_intern_read_ptr_count(tree, idx); i++) {
            vdbtree_print_node(tree, vdbtree_intern_read_ptr(tree, idx, i).block_idx, depth + 1);
        }
        vdbtree_print_node(tree, vdbtree_intern_read_right_ptr(tree, idx).block_idx, depth + 1);
    } else {
        printf(": ");
        for (uint32_t i = 0; i < vdbtree_leaf_read_record_count(tree, idx); i++) {

            struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->f, idx);
            struct VdbRecPtr p = vdbtree_deserialize_recptr(tree, vdbnode_datacell(page->buf, i));
            vdbpager_unpin_page(page, false);
            printf("%d", p.block_idx);

            if (i < vdbtree_leaf_read_record_count(tree, idx) - 1) {
                printf(", ");
            }
        }
        printf("\n");
    }

}

void vdbtree_print(struct VdbTree* tree) {
    uint32_t root = vdbtree_meta_read_root(tree);
    vdbtree_print_node(tree, root, 0);
}
