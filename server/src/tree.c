#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "util.h"
#include "tree.h"

static uint32_t vdbtree_data_init(struct VdbTree* tree, int32_t parent_idx);
static void vdbtree_data_write_next(struct VdbTree* tree, uint32_t idx, uint32_t next);
static uint32_t vdbtree_data_get_free_space(struct VdbTree* tree, uint32_t idx);
static struct VdbPtr vdbtree_intern_read_right_ptr(struct VdbTree* tree, uint32_t idx);
static struct VdbPtr vdbtree_intern_read_ptr(struct VdbTree* tree, uint32_t idx, uint32_t ptr_idx);
static uint32_t vdbtree_intern_read_ptr_count(struct VdbTree* tree, uint32_t idx);
static enum VdbNodeType vdbtree_node_type(struct VdbTree* tree, uint32_t idx);
void vdbtree_leaf_free_varlen_data(struct VdbTree* tree, struct VdbRecord* rec);

/*
 * Tree wrapper for generic node
 */

static enum VdbNodeType vdbtree_node_type(struct VdbTree* tree, uint32_t idx) {
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    enum VdbNodeType type = *vdbnode_type(page->buf);
    vdb_pager_unpin_page(page);
    return type;
}

/*
 * Tree wrappers for meta node
 */

static uint32_t vdbtree_meta_init(struct VdbTree* tree, struct VdbSchema* schema) {
    uint32_t idx = vdb_pager_fresh_page(tree->f);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;

    *vdbnode_type(page->buf) = VDBN_META;
    *vdbnode_parent(page->buf) = 0;
    *vdbmeta_auto_counter_ptr(page->buf) = 0;
    *vdbmeta_root_ptr(page->buf) = 0;
    vdbschema_serialize(vdbmeta_schema_ptr(page->buf), schema);

    vdb_pager_unpin_page(page);

    return idx;
}

uint32_t vdbtree_meta_read_primary_key_counter(struct VdbTree* tree) {
    assert(vdbtree_node_type(tree, 0) == VDBN_META);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, 0);
    uint32_t pk_counter = *vdbmeta_auto_counter_ptr(page->buf);
    vdb_pager_unpin_page(page);
    return pk_counter;
}

uint32_t vdbtree_meta_read_root(struct VdbTree* tree) {
    assert(vdbtree_node_type(tree, 0) == VDBN_META);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, 0);
    uint32_t root_idx = *vdbmeta_root_ptr(page->buf);
    vdb_pager_unpin_page(page);
    return root_idx;
}

static void vdbtree_meta_write_root(struct VdbTree* tree, uint32_t root_idx) {
    assert(vdbtree_node_type(tree, 0) == VDBN_META);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, 0);
    page->dirty = true;
    *vdbmeta_root_ptr(page->buf) = root_idx;
    vdb_pager_unpin_page(page);
}

struct VdbSchema* vdbtree_meta_read_schema(struct VdbTree* tree) {
    assert(vdbtree_node_type(tree, 0) == VDBN_META);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, 0);

    struct VdbSchema* schema = vdbschema_deserialize(vdbmeta_schema_ptr(page->buf));
    vdb_pager_unpin_page(page);
    return schema;
}

uint32_t vdbtree_meta_increment_primary_key_counter(struct VdbTree* tree) {
    assert(vdbtree_node_type(tree, 0) == VDBN_META);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, 0);
    page->dirty = true;
    *vdbmeta_auto_counter_ptr(page->buf) += 1;
    uint32_t pk_counter = *vdbmeta_auto_counter_ptr(page->buf);
    vdb_pager_unpin_page(page);

    return pk_counter;
}

/*
 * Tree wrappers for internal node
 */

static uint32_t vdbtree_intern_init(struct VdbTree* tree, uint32_t parent_idx) {
    uint32_t idx = vdb_pager_fresh_page(tree->f);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;

    *vdbnode_type(page->buf) = VDBN_INTERN;
    *vdbnode_parent(page->buf) = parent_idx;
    struct VdbPtr right_ptr = {0, 0};
    *vdbintern_rightptr_ptr(page->buf) = right_ptr;
    *vdbintern_nodeptr_count_ptr(page->buf) = 0;
    *vdbintern_datacells_size_ptr(page->buf) = 0;

    vdb_pager_unpin_page(page);

    return idx;
}

static void vdbtree_intern_write_right_ptr(struct VdbTree* tree, uint32_t idx, struct VdbPtr ptr) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;
    *vdbintern_rightptr_ptr(page->buf) = ptr;
    vdb_pager_unpin_page(page);
}

static void vdbtree_intern_write_new_ptr(struct VdbTree* tree, uint32_t idx, struct VdbPtr ptr) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;
    vdbintern_append_nodeptr(page->buf, ptr);
    vdb_pager_unpin_page(page);
}

static uint32_t vdbtree_intern_read_ptr_count(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    uint32_t count = *vdbintern_nodeptr_count_ptr(page->buf);
    vdb_pager_unpin_page(page);
    return count;
}

static struct VdbPtr vdbtree_intern_read_ptr(struct VdbTree* tree, uint32_t idx, uint32_t ptr_idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    struct VdbPtr ptr = *vdbintern_nodeptr_ptr(page->buf, ptr_idx);
    vdb_pager_unpin_page(page);
    return ptr;
}

static struct VdbPtr vdbtree_intern_read_right_ptr(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    struct VdbPtr right = *vdbintern_rightptr_ptr(page->buf);
    vdb_pager_unpin_page(page);
    return right;
}

static uint32_t vdbtree_intern_read_parent(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    uint32_t parent = *vdbnode_parent(page->buf);
    vdb_pager_unpin_page(page);
    return parent;
}

static void vdbtree_intern_write_parent(struct VdbTree* tree, uint32_t idx, uint32_t parent) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;
    *vdbnode_parent(page->buf) = parent;
    vdb_pager_unpin_page(page);
}

static bool vdbtree_intern_can_fit_ptr(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    bool can_fit = vdbintern_can_fit_nodeptr(page->buf);
    vdb_pager_unpin_page(page);
    return can_fit;
}

static uint32_t vdbtree_intern_split(struct VdbTree* tree, uint32_t idx, uint32_t new_right_key) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    uint32_t parent = vdbtree_intern_read_parent(tree, idx);

    if (parent == 0) { //split full root
        uint32_t old_root = vdbtree_meta_read_root(tree);
        uint32_t old_key = vdbtree_intern_read_right_ptr(tree, old_root).key;
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
    uint32_t idx = vdb_pager_fresh_page(tree->f);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;

    *vdbnode_type(page->buf) = VDBN_LEAF;
    *vdbnode_parent(page->buf) = parent_idx;
    *vdbleaf_data_block_ptr(page->buf) = 0;
    *vdbleaf_record_count_ptr(page->buf) = 0;
    *vdbleaf_datacells_size_ptr(page->buf) = 0;
    *vdbleaf_next_leaf_ptr(page->buf) = 0;

    vdb_pager_unpin_page(page);

    return idx;
}

static uint32_t vdbtree_leaf_read_parent(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    uint32_t parent_idx = *vdbnode_parent(page->buf);
    vdb_pager_unpin_page(page);
    return parent_idx;
}

static uint32_t vdbtree_leaf_read_data_block(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    uint32_t data_block_idx = *vdbleaf_data_block_ptr(page->buf);
    vdb_pager_unpin_page(page);
    return data_block_idx;
}

static void vdbtree_leaf_write_data_block(struct VdbTree* tree, uint32_t idx, uint32_t data_block_idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;
    *vdbleaf_data_block_ptr(page->buf) = data_block_idx;
    vdb_pager_unpin_page(page);
}


static void vdbtree_data_patch_overflow(struct VdbTree* tree, uint32_t idx, uint32_t idxcell_idx, uint32_t of_block_idx, uint32_t of_idxcell_idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_DATA);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;
    vdbdata_data_write_overflow(page->buf, idxcell_idx, of_block_idx, of_idxcell_idx);

    vdb_pager_unpin_page(page);
}

static void vdbtree_leaf_write_varlen_data(struct VdbTree* tree, uint32_t idx, struct VdbRecord* rec) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    if (!vdbtree_leaf_read_data_block(tree, idx)) {
        uint32_t data_idx = vdbtree_data_init(tree, idx);
        vdbtree_leaf_write_data_block(tree, idx, data_idx);
    }

    uint32_t data_block_idx = vdbtree_leaf_read_data_block(tree, idx);

    for (uint32_t i = 0; i < rec->count; i++) {
        if (rec->data[i].type != VDBT_TYPE_STR)
            continue;

        //must have enough free space to fit idxcell and at least 1 character
        if (vdbtree_data_get_free_space(tree, data_block_idx) < 2) {
            uint32_t new_data_idx = vdbtree_data_init(tree, data_block_idx);
            vdbtree_data_write_next(tree, data_block_idx, new_data_idx);
            data_block_idx = new_data_idx;
        }

        //append string to data block
        struct VdbPage* data_page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, data_block_idx);

        uint32_t allocated_size;
        uint32_t idxcell_idx = vdbdata_allocate_new_string_space(data_page->buf, rec->data[i].as.Str.len, &allocated_size);
        uint8_t* ptr = vdbdata_string_ptr(data_page->buf, idxcell_idx);

        rec->data[i].block_idx = data_block_idx;
        rec->data[i].idxcell_idx = idxcell_idx;

        uint32_t len_written = 0;
        memcpy(ptr, rec->data[i].as.Str.start, allocated_size);
        len_written += allocated_size;

        vdb_pager_unpin_page(data_page);

        //this only runs if an overflow block is needed
        //keep adding new data blocks and appending string if necessary
        while (len_written < rec->data[i].as.Str.len) {
            uint32_t new_data_idx = vdbtree_data_init(tree, data_block_idx);
            vdbtree_data_write_next(tree, data_block_idx, new_data_idx); 
            
            struct VdbPage* data_page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, new_data_idx);

            uint32_t new_idxcell_idx = vdbdata_allocate_new_string_space(data_page->buf, rec->data[i].as.Str.len - len_written, &allocated_size);
            uint8_t* ptr = vdbdata_string_ptr(data_page->buf, new_idxcell_idx);
            memcpy(ptr, rec->data[i].as.Str.start + len_written, allocated_size);
            len_written += allocated_size;

            vdbtree_data_patch_overflow(tree, data_block_idx, idxcell_idx, new_data_idx, new_idxcell_idx);

            data_block_idx = new_data_idx;
            idxcell_idx = new_idxcell_idx;

            vdb_pager_unpin_page(data_page);
        }

    }
}

static void vdbtree_leaf_append_record(struct VdbTree* tree, uint32_t idx, struct VdbRecord* rec) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;

    if (vdbrecord_has_varlen_data(rec)) {
        vdbtree_leaf_write_varlen_data(tree, idx, rec);
    }

    struct VdbSchema* schema = vdbtree_meta_read_schema(tree);

    uint32_t rec_idx = vdbleaf_append_record_cell(page->buf, vdbschema_fixedlen_record_size(schema));
    vdbrecord_write(vdbleaf_record_ptr(page->buf, rec_idx), rec, schema);

    vdb_schema_free(schema);

    vdb_pager_unpin_page(page);
}

static void vdbtree_leaf_write_record(struct VdbTree* tree, uint32_t idx, uint32_t rec_idx, struct VdbRecord* rec) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;

    if (vdbrecord_has_varlen_data(rec)) {
        vdbtree_leaf_write_varlen_data(tree, idx, rec);
    }

    struct VdbSchema* schema = vdbtree_meta_read_schema(tree);
    
    vdbrecord_write(vdbleaf_record_ptr(page->buf, rec_idx), rec, schema);

    vdb_schema_free(schema);

    vdb_pager_unpin_page(page);
}

uint32_t vdbtree_leaf_read_record_count(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    uint32_t count = *vdbleaf_record_count_ptr(page->buf);
    vdb_pager_unpin_page(page);
    return count;
}

uint32_t vdbtree_leaf_read_record_key(struct VdbTree* tree, uint32_t idx, uint32_t rec_idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);

    struct VdbSchema* schema = vdbtree_meta_read_schema(tree);

    struct VdbValue v = vdbrecord_read_value_at_idx(vdbleaf_record_ptr(page->buf, rec_idx), schema, schema->key_idx);
    assert(v.type == VDBT_TYPE_INT);
    uint32_t key = v.as.Int;

    vdb_schema_free(schema);

    vdb_pager_unpin_page(page);
    return key;
}

void vdbtree_leaf_delete_record(struct VdbTree* tree, uint32_t idx, uint32_t rec_idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    struct VdbSchema* schema = vdbtree_meta_read_schema(tree);

    struct VdbRecord* rec = vdbtree_leaf_read_record(tree, idx, rec_idx);
    vdbtree_leaf_free_varlen_data(tree, rec);
    *vdbleaf_record_occupied_ptr(page->buf, rec_idx) = false;

    vdb_schema_free(schema);
    vdb_record_free(rec);
    vdb_pager_unpin_page(page);
}

void vdbtree_leaf_update_record(struct VdbTree* tree, uint32_t idx, uint32_t rec_idx, struct VdbTokenList* attrs, struct VdbValueList* vl) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    struct VdbSchema* schema = vdbtree_meta_read_schema(tree);

    struct VdbRecord* rec = vdbtree_leaf_read_record(tree, idx, rec_idx);
    vdbtree_leaf_free_varlen_data(tree, rec);

    for (int i = 0; i < attrs->count; i++) {
        for (uint32_t j = 0; j < schema->count; j++) {
            if (strncmp(attrs->tokens[i].lexeme, schema->names[j], attrs->tokens[i].len) == 0) {
                vdbvalue_free(rec->data[j]);
                rec->data[j] = vdbvalue_copy(vl->values[i]);
            }
        }
    }

    vdbtree_leaf_write_record(tree, idx, rec_idx, rec);

    vdb_schema_free(schema);
    vdb_record_free(rec);
    vdb_pager_unpin_page(page);
}

struct VdbRecord* vdbtree_leaf_read_record(struct VdbTree* tree, uint32_t idx, uint32_t rec_idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);

    //check if record idx is occupied (may be a freed record)
    if (!(*vdbleaf_record_occupied_ptr(page->buf, rec_idx))) {
        vdb_pager_unpin_page(page);
        return NULL;
    }

    struct VdbSchema* schema = vdbtree_meta_read_schema(tree);

    struct VdbRecord* rec = vdbrecord_read(vdbleaf_record_ptr(page->buf, rec_idx), schema);

    vdb_schema_free(schema);

    for (uint32_t i = 0; i < rec->count; i++) {
        struct VdbValue* d = &rec->data[i];
        if (d->type != VDBT_TYPE_STR)
            continue;

        uint32_t block_idx = d->block_idx;
        uint32_t idxcell_idx = d->idxcell_idx;
        d->as.Str.start = NULL;
        d->as.Str.len = 0;

        while (block_idx) {
            struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, block_idx);
            uint8_t* ptr = vdbdata_datacell_ptr(page->buf, idxcell_idx);
            struct VdbValue datum = vdbvalue_deserialize_string(ptr);

            d->as.Str.start = realloc_w(d->as.Str.start, sizeof(char) * (d->as.Str.len + datum.as.Str.len), sizeof(char) * d->as.Str.len);
            memcpy(d->as.Str.start + d->as.Str.len, datum.as.Str.start, datum.as.Str.len);
            d->as.Str.len += datum.as.Str.len;

            block_idx = datum.block_idx;
            idxcell_idx = datum.idxcell_idx;
            free_w(datum.as.Str.start, sizeof(char) * datum.as.Str.len);

            vdb_pager_unpin_page(page);
        }

    }
    vdb_pager_unpin_page(page);
    return rec;
}

static bool vdbtree_leaf_can_fit_record(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);

    uint32_t idx_cells_size = *vdbleaf_record_count_ptr(page->buf) * sizeof(uint32_t);
    uint32_t data_cells_size = *vdbleaf_datacells_size_ptr(page->buf);

    struct VdbSchema* schema = vdbtree_meta_read_schema(tree);
    uint32_t record_entry_size = vdbschema_fixedlen_record_size(schema) + sizeof(uint32_t) * 3; //3 for: index cell + next + size fields
    vdb_schema_free(schema);

    vdb_pager_unpin_page(page);
    return idx_cells_size + data_cells_size + record_entry_size <= VDB_PAGE_SIZE - VDB_PAGE_HDR_SIZE;
}

static uint32_t vdbtree_leaf_split(struct VdbTree* tree, uint32_t idx, uint32_t new_right_key) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);

    uint32_t parent = vdbtree_leaf_read_parent(tree, idx);

    if (!vdbtree_intern_can_fit_ptr(tree, parent)) {
        parent = vdbtree_intern_split(tree, parent, new_right_key);
    } else {
        //copy right pointer into ptr list to make room for new right ptr
        uint32_t count = vdbtree_leaf_read_record_count(tree, idx);
        uint32_t last_rec_key = vdbtree_leaf_read_record_key(tree, idx, count - 1);

        struct VdbPtr ptr = {idx, last_rec_key};
        vdbtree_intern_write_new_ptr(tree, parent, ptr);
    }

    uint32_t new_leaf = vdbtree_leaf_init(tree, parent);
    struct VdbPtr new_right = {new_leaf, new_right_key};
    vdbtree_intern_write_right_ptr(tree, parent, new_right);

    *vdbleaf_next_leaf_ptr(page->buf) = new_leaf;
    page->dirty = true;

    vdb_pager_unpin_page(page);

    return new_leaf;
}

uint32_t vdbtree_leaf_read_next_leaf(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    uint32_t next_leaf_idx = *vdbleaf_next_leaf_ptr(page->buf);

    vdb_pager_unpin_page(page);

    return next_leaf_idx;
}

void vdbtree_leaf_write_record_key(struct VdbTree* tree, uint32_t idx, uint32_t rec_idx, uint32_t key) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);

    struct VdbSchema* schema = vdbtree_meta_read_schema(tree);

    struct VdbValue v;
    v.type = VDBT_TYPE_INT;
    v.as.Int = key;

    vdbrecord_write_value_at_idx(vdbleaf_record_ptr(page->buf, rec_idx), schema, 0, v);

    vdb_schema_free(schema);

    vdb_pager_unpin_page(page);
}

/*
 * Tree wrappers for data node
 */

static uint32_t vdbtree_data_init(struct VdbTree* tree, int32_t parent_idx) {
    uint32_t idx = vdb_pager_fresh_page(tree->f);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;

    vdbdata_init(page->buf, parent_idx);

    vdb_pager_unpin_page(page);

    return idx;
}

static void vdbtree_data_write_next(struct VdbTree* tree, uint32_t idx, uint32_t next) {
    assert(vdbtree_node_type(tree, idx) == VDBN_DATA);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;
    *vdbdata_next_ptr(page->buf) = next;
    vdb_pager_unpin_page(page);
}

static uint32_t vdbtree_data_get_free_space(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_DATA);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);

    uint32_t free_size = vdbdata_get_free_space(page->buf);

    vdb_pager_unpin_page(page);

    return free_size;
}



/*
 * VdbTree API
 */

struct VdbTree* vdb_tree_init(const char* name, struct VdbSchema* schema, struct VdbPager* pager, FILE* f) {
    struct VdbTree* tree = malloc_w(sizeof(struct VdbTree));
    tree->name = strdup_w(name);
    tree->pager = pager;
    tree->f = f;

    uint32_t meta_idx = vdbtree_meta_init(tree, schema);
    tree->meta_idx = meta_idx;
    uint32_t root_idx = vdbtree_intern_init(tree, meta_idx);
    vdbtree_meta_write_root(tree, root_idx);
    uint32_t leaf_idx = vdbtree_leaf_init(tree, root_idx);
    struct VdbPtr right_ptr = {leaf_idx, 0};
    vdbtree_intern_write_right_ptr(tree, root_idx, right_ptr);

    return tree;
}

struct VdbTree* vdb_tree_open(const char* name, FILE* f, struct VdbPager* pager) {
    struct VdbTree* tree = malloc_w(sizeof(struct VdbTree));
    tree->name = strdup(name);
    tree->f = f;
    tree->pager = pager;
    tree->meta_idx = 0;
    return tree;
}

void vdb_tree_close(struct VdbTree* tree) {
    fclose_w(tree->f);
    free_w(tree->name, sizeof(char) * (strlen(tree->name) + 1));
    free_w(tree, sizeof(struct VdbTree));
}

uint32_t vdbtree_traverse_to_first_leaf(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);

    if (vdbtree_intern_read_ptr_count(tree, idx) > 0) {
        struct VdbPtr p = vdbtree_intern_read_ptr(tree, idx, 0);
        if (vdbtree_node_type(tree, p.idx) == VDBN_LEAF) {
            return p.idx;
        } else {
            return vdbtree_traverse_to_first_leaf(tree, p.idx);
        }
    } else {
        struct VdbPtr p = vdbtree_intern_read_right_ptr(tree, idx);

        if (vdbtree_node_type(tree, p.idx) == VDBN_LEAF) {
            return p.idx;
        } else {
            return vdbtree_traverse_to_first_leaf(tree, p.idx);
        }
    }

}

uint32_t vdb_tree_traverse_to(struct VdbTree* tree, uint32_t idx, uint32_t key) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);

    //Check to see if any ptrs have correct key
    //TODO: switch to binary search
    for (uint32_t i = 0; i < vdbtree_intern_read_ptr_count(tree, idx); i++) {
        struct VdbPtr p = vdbtree_intern_read_ptr(tree, idx, i);
        if (vdbtree_node_type(tree, p.idx) == VDBN_LEAF && vdbtree_leaf_read_record_count(tree, p.idx) > 0) {
            uint32_t last_rec_key = vdbtree_leaf_read_record_key(tree, p.idx, vdbtree_leaf_read_record_count(tree, p.idx) - 1);
            if (key <= last_rec_key) {
                return p.idx;
            }
        } else if (vdbtree_node_type(tree, p.idx) == VDBN_INTERN) {
            //get largest key for a given internal node, and compare to key
            struct VdbPtr right = vdbtree_intern_read_right_ptr(tree, p.idx);
            while (vdbtree_node_type(tree, right.idx) != VDBN_LEAF) {
                right = vdbtree_intern_read_right_ptr(tree, right.idx);
            }
            uint32_t last_rec_key = vdbtree_leaf_read_record_key(tree, right.idx, vdbtree_leaf_read_record_count(tree, right.idx) - 1);
            if (key <= last_rec_key) {
                return vdb_tree_traverse_to(tree, p.idx, key);
            }
        }
    }

    //if not found, check right pointer
    struct VdbPtr p = vdbtree_intern_read_right_ptr(tree, idx);

    if (vdbtree_node_type(tree, p.idx) == VDBN_LEAF) {
        return p.idx;
    } else {
        return vdb_tree_traverse_to(tree, p.idx, key);
    }

}

void vdb_tree_insert_record(struct VdbTree* tree, struct VdbRecord* rec) {
    uint32_t root_idx = vdbtree_meta_read_root(tree);
    assert(rec->data[0].type == VDBT_TYPE_INT);
    struct VdbSchema* schema = vdbtree_meta_read_schema(tree);
    uint32_t key = rec->data[schema->key_idx].as.Int;
    uint32_t leaf_idx = vdb_tree_traverse_to(tree, root_idx, key);

    if (!vdbtree_leaf_can_fit_record(tree, leaf_idx)) {
        leaf_idx = vdbtree_leaf_split(tree, leaf_idx, key);
    }

    vdbtree_leaf_append_record(tree, leaf_idx, rec);
    vdb_schema_free(schema);
}


void vdbtree_leaf_free_varlen_data(struct VdbTree* tree, struct VdbRecord* rec) {
    for (uint32_t i = 0; i < rec->count; i++) {
        struct VdbValue d = rec->data[i]; 
        if (d.type != VDBT_TYPE_STR)
            continue;
       
        uint32_t block_idx = d.block_idx;
        uint32_t idxcell_idx = d.idxcell_idx;

        while (block_idx != 0) {
            struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, block_idx);
            page->dirty = true;

            vdbdata_free_cells(page->buf, idxcell_idx, &block_idx, &idxcell_idx);

            vdb_pager_unpin_page(page);
        }
    }
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
        if (vdbtree_node_type(tree, vdbtree_intern_read_right_ptr(tree, idx).idx) == VDBN_LEAF) {
            //skip printing leaves
            return;
        }
        for (uint32_t i = 0; i < vdbtree_intern_read_ptr_count(tree, idx); i++) {
            vdbtree_print_node(tree, vdbtree_intern_read_ptr(tree, idx, i).idx, depth + 1);
        }
        vdbtree_print_node(tree, vdbtree_intern_read_right_ptr(tree, idx).idx, depth + 1);
    } else {
        printf(": ");
        for (uint32_t i = 0; i < vdbtree_leaf_read_record_count(tree, idx); i++) {
            uint32_t key = vdbtree_leaf_read_record_key(tree, idx, i);
            printf("%d", key);
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