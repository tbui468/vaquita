#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "util.h"
#include "tree.h"

static struct VdbPtr vdbtree_intern_read_right_ptr(struct VdbTree* tree, uint32_t idx);
static struct VdbPtr vdbtree_intern_read_ptr(struct VdbTree* tree, uint32_t idx, uint32_t ptr_idx);
static uint32_t vdbtree_intern_read_ptr_count(struct VdbTree* tree, uint32_t idx);
static enum VdbNodeType vdbtree_node_type(struct VdbTree* tree, uint32_t idx);

/*
 * Tree wrapper for generic node
 */

static enum VdbNodeType vdbtree_node_type(struct VdbTree* tree, uint32_t idx) {
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->name, tree->f, idx);
    enum VdbNodeType type = *vdbnode_type(page->buf);
    vdbpager_unpin_page(page);
    return type;
}

/*
 * Tree wrappers for meta node
 */

static uint32_t vdbtree_meta_init(struct VdbTree* tree, struct VdbSchema* schema) {
    uint32_t idx = vdbpager_fresh_page(tree->pager, tree->f);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;

    *vdbnode_type(page->buf) = VDBN_META;
    *vdbnode_parent(page->buf) = 0;
    *vdbmeta_auto_counter_ptr(page->buf) = 0;
    *vdbmeta_root_ptr(page->buf) = 0;
    vdbschema_serialize(vdbmeta_schema_ptr(page->buf), schema);

    vdbpager_unpin_page(page);

    return idx;
}

uint32_t vdbtree_meta_read_primary_key_counter(struct VdbTree* tree) {
    assert(vdbtree_node_type(tree, 0) == VDBN_META);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->name, tree->f, 0);
    uint32_t pk_counter = *vdbmeta_auto_counter_ptr(page->buf);
    vdbpager_unpin_page(page);
    return pk_counter;
}

uint32_t vdbtree_meta_read_root(struct VdbTree* tree) {
    assert(vdbtree_node_type(tree, 0) == VDBN_META);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->name, tree->f, 0);
    uint32_t root_idx = *vdbmeta_root_ptr(page->buf);
    vdbpager_unpin_page(page);
    return root_idx;
}

static void vdbtree_meta_write_root(struct VdbTree* tree, uint32_t root_idx) {
    assert(vdbtree_node_type(tree, 0) == VDBN_META);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->name, tree->f, 0);
    page->dirty = true;
    *vdbmeta_root_ptr(page->buf) = root_idx;
    vdbpager_unpin_page(page);
}

uint32_t vdbtree_meta_increment_primary_key_counter(struct VdbTree* tree) {
    assert(vdbtree_node_type(tree, 0) == VDBN_META);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->name, tree->f, 0);
    page->dirty = true;
    *vdbmeta_auto_counter_ptr(page->buf) += 1;
    uint32_t pk_counter = *vdbmeta_auto_counter_ptr(page->buf);
    vdbpager_unpin_page(page);

    return pk_counter;
}

/*
 * Tree wrappers for internal node
 */

static uint32_t vdbtree_intern_init(struct VdbTree* tree, uint32_t parent_idx) {
    uint32_t idx = vdbpager_fresh_page(tree->pager, tree->f);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;

    *vdbnode_type(page->buf) = VDBN_INTERN;
    *vdbnode_parent(page->buf) = parent_idx;
    vdbintern_rightptr_ptr(page->buf)->block_idx = 0;
    vdbintern_rightptr_ptr(page->buf)->key = vdbint(0);
    *vdbintern_nodeptr_count_ptr(page->buf) = 0;
    *vdbintern_datacells_size_ptr(page->buf) = 0;

    vdbpager_unpin_page(page);

    return idx;
}

static void vdbtree_intern_write_right_ptr(struct VdbTree* tree, uint32_t idx, struct VdbPtr ptr) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;
    vdbintern_rightptr_ptr(page->buf)->block_idx = ptr.block_idx;
    vdbintern_rightptr_ptr(page->buf)->key = ptr.key;
    vdbpager_unpin_page(page);
}

static void vdbtree_intern_write_new_ptr(struct VdbTree* tree, uint32_t idx, struct VdbPtr ptr) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;
    vdbintern_append_nodeptr(page->buf, ptr);
    vdbpager_unpin_page(page);
}

static uint32_t vdbtree_intern_read_ptr_count(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->name, tree->f, idx);
    uint32_t count = *vdbintern_nodeptr_count_ptr(page->buf);
    vdbpager_unpin_page(page);
    return count;
}

static struct VdbPtr vdbtree_intern_read_ptr(struct VdbTree* tree, uint32_t idx, uint32_t ptr_idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->name, tree->f, idx);
    struct VdbPtr ptr;
    ptr.block_idx = vdbintern_nodeptr_ptr(page->buf, ptr_idx)->block_idx;
    ptr.key = vdbintern_nodeptr_ptr(page->buf, ptr_idx)->key;
    vdbpager_unpin_page(page);
    return ptr;
}

static struct VdbPtr vdbtree_intern_read_right_ptr(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->name, tree->f, idx);
    struct VdbPtr right;
    right.block_idx = vdbintern_rightptr_ptr(page->buf)->block_idx;
    right.key = vdbintern_rightptr_ptr(page->buf)->key;
    vdbpager_unpin_page(page);
    return right;
}

static uint32_t vdbtree_intern_read_parent(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->name, tree->f, idx);
    uint32_t parent = *vdbnode_parent(page->buf);
    vdbpager_unpin_page(page);
    return parent;
}

static void vdbtree_intern_write_parent(struct VdbTree* tree, uint32_t idx, uint32_t parent) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;
    *vdbnode_parent(page->buf) = parent;
    vdbpager_unpin_page(page);
}

static bool vdbtree_intern_can_fit_ptr(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->name, tree->f, idx);
    bool can_fit = vdbintern_can_fit_nodeptr(page->buf);
    vdbpager_unpin_page(page);
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
    } else if (vdbtree_intern_can_fit_ptr(tree, parent)) {
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
    uint32_t idx = vdbpager_fresh_page(tree->pager, tree->f);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;

    *vdbnode_type(page->buf) = VDBN_LEAF;
    *vdbnode_parent(page->buf) = parent_idx;
    *vdbleaf_record_count_ptr(page->buf) = 0;
    *vdbleaf_datacells_size_ptr(page->buf) = 0;
    *vdbleaf_next_leaf_ptr(page->buf) = 0;

    vdbpager_unpin_page(page);

    return idx;
}

static uint32_t vdbtree_leaf_read_parent(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->name, tree->f, idx);
    uint32_t parent_idx = *vdbnode_parent(page->buf);
    vdbpager_unpin_page(page);
    return parent_idx;
}

static void vdbtree_leaf_write_record(struct VdbTree* tree, uint32_t idx, uint32_t rec_idx, struct VdbRecord* rec) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;
    
    vdbrecord_write(vdbleaf_record_ptr(page->buf, rec_idx), rec, tree->schema);

    vdbpager_unpin_page(page);
}

uint32_t vdbtree_leaf_read_record_count(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->name, tree->f, idx);
    uint32_t count = *vdbleaf_record_count_ptr(page->buf);
    vdbpager_unpin_page(page);
    return count;
}

struct VdbValue vdbtree_leaf_read_record_key(struct VdbTree* tree, uint32_t idx, uint32_t rec_idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->name, tree->f, idx);

    struct VdbRecord* rec = vdbtree_leaf_read_record(tree, idx, rec_idx);
    if (!rec) printf("rec is null\n");
    struct VdbValue v = rec->data[tree->schema->key_idx];

    vdbrecord_free(rec);

    vdbpager_unpin_page(page);
    return v;
}

void vdbtree_leaf_delete_record(struct VdbTree* tree, uint32_t idx, uint32_t rec_idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;

    vdbleaf_delete_idxcell(page->buf, rec_idx);

    vdbpager_unpin_page(page);
}

void vdbtree_leaf_update_record(struct VdbTree* tree, uint32_t idx, uint32_t rec_idx, struct VdbTokenList* attrs, struct VdbValueList* vl) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->name, tree->f, idx);

    page->dirty = true;
    struct VdbRecord* rec = vdbtree_leaf_read_record(tree, idx, rec_idx);

    for (int i = 0; i < attrs->count; i++) {
        for (uint32_t j = 0; j < tree->schema->count; j++) {
            if (strncmp(attrs->tokens[i].lexeme, tree->schema->names[j], attrs->tokens[i].len) == 0) {
                vdbvalue_free(rec->data[j]);
                rec->data[j] = vdbvalue_copy(vl->values[i]);
            }
        }
    }

    vdbtree_leaf_write_record(tree, idx, rec_idx, rec);

    vdbrecord_free(rec);
    vdbpager_unpin_page(page);
}

struct VdbRecord* vdbtree_leaf_read_record(struct VdbTree* tree, uint32_t idx, uint32_t rec_idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->name, tree->f, idx);

    //check if record idx is occupied (may be a freed record)
    if (!(*vdbleaf_record_occupied_ptr(page->buf, rec_idx))) {
        printf("occupied: %d\n", *vdbleaf_record_occupied_ptr(page->buf, rec_idx));
        vdbpager_unpin_page(page);
        return NULL;
    }

    struct VdbRecord* rec = vdbrecord_read(vdbleaf_record_ptr(page->buf, rec_idx), tree->schema);

    vdbpager_unpin_page(page);
    return rec;
}

static bool vdbtree_leaf_can_fit_record(struct VdbTree* tree, uint32_t idx, struct VdbRecord* rec) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->name, tree->f, idx);

    uint32_t idx_cells_size = *vdbleaf_record_count_ptr(page->buf) * sizeof(uint32_t);
    uint32_t data_cells_size = *vdbleaf_datacells_size_ptr(page->buf);

    uint32_t serialized_rec_size = vdbrecord_serialized_size(rec, tree->schema);
    uint32_t idxcell_size = sizeof(uint32_t);
    uint32_t datacell_hdr_size = sizeof(uint32_t) * 2; //next + occupied
    uint32_t total_size = serialized_rec_size + idxcell_size + datacell_hdr_size;

    vdbpager_unpin_page(page);
    return idx_cells_size + data_cells_size + total_size <= VDB_PAGE_SIZE - VDB_PAGE_HDR_SIZE;
}

static uint32_t vdbtree_leaf_split(struct VdbTree* tree, uint32_t idx, struct VdbValue new_right_key) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);

    uint32_t parent = vdbtree_leaf_read_parent(tree, idx);

    if (!vdbtree_intern_can_fit_ptr(tree, parent)) {
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

    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->name, tree->f, idx);
    *vdbleaf_next_leaf_ptr(page->buf) = new_leaf;
    page->dirty = true;
    vdbpager_unpin_page(page);

    return new_leaf;
}

uint32_t vdbtree_leaf_read_next_leaf(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->name, tree->f, idx);
    uint32_t next_leaf_idx = *vdbleaf_next_leaf_ptr(page->buf);

    vdbpager_unpin_page(page);

    return next_leaf_idx;
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

    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->name, tree->f, 0);
    tree->schema = vdbschema_deserialize(vdbmeta_schema_ptr(page->buf));
    vdbpager_unpin_page(page);

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

void vdb_tree_insert_record(struct VdbTree* tree, struct VdbRecord* rec) {
    struct VdbValue key = rec->data[tree->schema->key_idx];
   
    struct VdbPage* meta_page = vdbpager_pin_page(tree->pager, tree->name, tree->f, 0);
    uint32_t leaf_idx;
    if (*vdbmeta_last_leaf(meta_page->buf) == 0) {
        uint32_t root_idx = vdbtree_meta_read_root(tree);
        leaf_idx = vdb_tree_traverse_to(tree, root_idx, key);
    } else {
        leaf_idx = *vdbmeta_last_leaf(meta_page->buf);
    }

    if (!vdbtree_leaf_can_fit_record(tree, leaf_idx, rec)) {
        leaf_idx = vdbtree_leaf_split(tree, leaf_idx, key);
    }

    *vdbmeta_last_leaf(meta_page->buf) = leaf_idx;
    meta_page->dirty = true;
    vdbpager_unpin_page(meta_page);

    /*
    //not right append
    uint32_t leaf_idx = vdb_tree_traverse_to(tree, vdbtree_meta_read_root(tree), key);
    if (!vdbtree_leaf_can_fit_record(tree, leaf_idx, rec)) {
        leaf_idx = vdbtree_leaf_split(tree, leaf_idx, key);
    }*/
   
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->name, tree->f, leaf_idx);
    page->dirty = true;

    struct VdbValue rec_key = rec->data[tree->schema->key_idx];

    /*
    //binary search doesn't really give much speedup (if any) and is so much more complex
    uint32_t i = 0;
    uint32_t left = 0;
    uint32_t right = vdbtree_leaf_read_record_count(tree, leaf_idx);
    while (right > 0) {
        i = (right - left) / 2 + left;
        struct VdbValue key = vdbtree_leaf_read_record_key(tree, leaf_idx, i);

        //record key is smaller than key at i
        int result = vdbvalue_compare(rec_key, key);
        if (result == -1) {
            if (right - left <= 1) {
                break;
            }
            right = i;
        //record key is larger than key at i
        } else if (result == 1) {
            if (right - left <= 1) {
                i++;
                break;
            }
            left = i;
        } else {
            assert(false && "duplicate keys not allowed");
        }

    }*/

    uint32_t i;
    for (i = 0; i < vdbtree_leaf_read_record_count(tree, leaf_idx); i++) {
        struct VdbValue key = vdbtree_leaf_read_record_key(tree, leaf_idx, i);
        if (vdbvalue_compare(rec_key, key) == -1) {
            break;
        }
    }

    vdbleaf_insert_record_cell(page->buf, i, vdbrecord_serialized_size(rec, tree->schema));
    vdbrecord_write(vdbleaf_record_ptr(page->buf, i), rec, tree->schema);

    vdbpager_unpin_page(page);
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
    uint32_t i;
    for (i = 0; i < tl->count; i++) {
        t = tl->trees[i];
        if (strncmp(name, t->name, strlen(name)) == 0)
            return t;
    }

    return NULL;
}

struct VdbTree* vdb_treelist_remove_tree(struct VdbTreeList* tl, const char* name) {
    struct VdbTree* t = NULL;
    uint32_t i;
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
    for (uint32_t i = 0; i < tl->count; i++) {
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
            //uint32_t key = vdbtree_leaf_read_record_key(tree, idx, i).as.Int;
            //printf("%d", key);
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
