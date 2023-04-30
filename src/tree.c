#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "util.h"
#include "tree.h"

//TODO: how could we use macros to simplify code a bit? Would it just make things more confusing?
//This macro is currenlty not used
#define vdbintern_write(fn, idx, ...) \
    assert(vdbtree_node_type(tree, (idx)) == VDBN_INTERN);\
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, (idx));\
    page->dirty = true;\
    (fn)(page->buf, __VA_ARGS__);\
    vdb_pager_unpin_page(page)

static uint32_t vdbtree_data_init(struct VdbTree* tree, int32_t parent_idx);
static uint32_t vdbtree_data_write_datum(struct VdbTree* tree, uint32_t idx, struct VdbDatum* datum, uint32_t* len_written);
static void vdbtree_data_write_next(struct VdbTree* tree, uint32_t idx, uint32_t next);
static uint32_t vdbtree_data_read_free_size(struct VdbTree* tree, uint32_t idx);
static void vdbtree_data_write_datum_overflow(struct VdbTree* tree, uint32_t idx, uint32_t datum_off, uint32_t overflow_block, uint32_t overflow_off);
static struct VdbPtr vdbtree_intern_read_right_ptr(struct VdbTree* tree, uint32_t idx);
static struct VdbPtr vdbtree_intern_read_ptr(struct VdbTree* tree, uint32_t idx, uint32_t ptr_idx);
static uint32_t vdbtree_intern_read_ptr_count(struct VdbTree* tree, uint32_t idx);
static enum VdbNodeType vdbtree_node_type(struct VdbTree* tree, uint32_t idx);
static uint32_t vdbtree_leaf_read_record_key(struct VdbTree* tree, uint32_t leaf_idx, uint32_t rec_idx);

/*
 * Tree wrapper for generic node
 */

static enum VdbNodeType vdbtree_node_type(struct VdbTree* tree, uint32_t idx) {
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    enum VdbNodeType type = vdbnode_read_type(page->buf);
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

    vdbnode_write_type(page->buf, VDBN_META);
    vdbnode_write_parent(page->buf, 0);
    vdbnode_meta_write_primary_key_counter(page->buf, 0);
    vdbnode_meta_write_root(page->buf, 0);
    vdbnode_meta_write_schema(page->buf, schema);

    vdb_pager_unpin_page(page);

    return idx;
}

static uint32_t vdbtree_meta_read_primary_key_counter(struct VdbTree* tree) {
    assert(vdbtree_node_type(tree, 0) == VDBN_META);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, 0);
    uint32_t pk_counter = vdbnode_meta_read_primary_key_counter(page->buf);
    vdb_pager_unpin_page(page);
    return pk_counter;
}

static uint32_t vdbtree_meta_read_root(struct VdbTree* tree) {
    assert(vdbtree_node_type(tree, 0) == VDBN_META);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, 0);
    uint32_t root_idx = vdb_node_meta_read_root(page->buf);
    vdb_pager_unpin_page(page);
    return root_idx;
}

static void vdbtree_meta_write_root(struct VdbTree* tree, uint32_t root_idx) {
    assert(vdbtree_node_type(tree, 0) == VDBN_META);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, 0);
    page->dirty = true;
    vdbnode_meta_write_root(page->buf, root_idx);
    vdb_pager_unpin_page(page);
}

struct VdbSchema* vdbtree_meta_read_schema(struct VdbTree* tree) {
    assert(vdbtree_node_type(tree, 0) == VDBN_META);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, 0);
    struct VdbSchema* schema = vdbnode_meta_read_schema(page->buf);
    vdb_pager_unpin_page(page);
    return schema;
}

uint32_t vdbtree_meta_increment_primary_key_counter(struct VdbTree* tree) {
    assert(vdbtree_node_type(tree, 0) == VDBN_META);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, 0);
    page->dirty = true;
    uint32_t pk_counter = vdbnode_meta_read_primary_key_counter(page->buf);
    pk_counter++;
    vdbnode_meta_write_primary_key_counter(page->buf, pk_counter);
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

    vdbnode_write_type(page->buf, VDBN_INTERN);
    vdbnode_write_parent(page->buf, parent_idx);
    struct VdbPtr right_ptr = {0, 0};
    vdbnode_intern_write_right_ptr(page->buf, right_ptr);
    vdbnode_intern_write_ptr_count(page->buf, 0);
    vdbnode_intern_write_datacells_size(page->buf, 0);

    vdb_pager_unpin_page(page);

    return idx;
}

static void vdbtree_intern_write_right_ptr(struct VdbTree* tree, uint32_t idx, struct VdbPtr ptr) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;
    vdbnode_intern_write_right_ptr(page->buf, ptr);
    vdb_pager_unpin_page(page);
}

static void vdbtree_intern_write_new_ptr(struct VdbTree* tree, uint32_t idx, struct VdbPtr ptr) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;
    vdbnode_intern_write_new_ptr(page->buf, ptr);
    vdb_pager_unpin_page(page);
}

static uint32_t vdbtree_intern_read_ptr_count(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    uint32_t count = vdbnode_intern_read_ptr_count(page->buf);
    vdb_pager_unpin_page(page);
    return count;
}

static struct VdbPtr vdbtree_intern_read_ptr(struct VdbTree* tree, uint32_t idx, uint32_t ptr_idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    struct VdbPtr ptr = vdbnode_intern_read_ptr(page->buf, ptr_idx);
    vdb_pager_unpin_page(page);
    return ptr;
}

static struct VdbPtr vdbtree_intern_read_right_ptr(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    struct VdbPtr right = vdbnode_intern_read_right_ptr(page->buf);
    vdb_pager_unpin_page(page);
    return right;
}

static uint32_t vdbtree_intern_read_parent(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    uint32_t parent = vdbnode_read_parent(page->buf);
    vdb_pager_unpin_page(page);
    return parent;
}

static void vdbtree_intern_write_parent(struct VdbTree* tree, uint32_t idx, uint32_t parent) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;
    vdbnode_write_parent(page->buf, parent);
    vdb_pager_unpin_page(page);
}

static bool vdbtree_intern_can_fit_ptr(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    bool can_fit = vdbnode_intern_can_fit_ptr(page->buf);
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

    vdbnode_write_type(page->buf, VDBN_LEAF);
    vdbnode_write_parent(page->buf, parent_idx);
    vdbnode_leaf_write_data_block(page->buf, 0);
    vdbnode_leaf_write_record_count(page->buf, 0);
    vdbnode_leaf_write_datacells_size(page->buf, 0);

    vdb_pager_unpin_page(page);

    return idx;
}

static uint32_t vdbtree_leaf_read_parent(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    uint32_t parent_idx = vdbnode_read_parent(page->buf);
    vdb_pager_unpin_page(page);
    return parent_idx;
}

static uint32_t vdbtree_leaf_read_data_block(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    uint32_t data_block_idx = vdbnode_leaf_read_data_block(page->buf);
    vdb_pager_unpin_page(page);
    return data_block_idx;
}

static void vdbtree_leaf_write_data_block(struct VdbTree* tree, uint32_t idx, uint32_t data_block_idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;
    vdbnode_leaf_write_data_block(page->buf, data_block_idx);
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
        if (rec->data[i].type != VDBF_STR)
            continue;

        //must have enough free space to fit header + at least 1 char
        if (vdbtree_data_read_free_size(tree, data_block_idx) < vdbnode_data_datacell_header_size() + sizeof(char)) {
            uint32_t new_data_idx = vdbtree_data_init(tree, data_block_idx);
            vdbtree_data_write_next(tree, data_block_idx, new_data_idx);
            data_block_idx = new_data_idx;
        }

        uint32_t len_written = 0;
        uint32_t off = vdbtree_data_write_datum(tree, data_block_idx, &rec->data[i], &len_written);

        //record block/offset of written varlen data so that it can be written to fixedlen data later
        rec->data[i].block_idx = data_block_idx;
        rec->data[i].offset_idx = off;

        while (len_written < rec->data[i].as.Str->len) {
            uint32_t new_data_idx = vdbtree_data_init(tree, data_block_idx);
            vdbtree_data_write_next(tree, data_block_idx, new_data_idx); 
            uint32_t new_off = vdbtree_data_write_datum(tree, new_data_idx, &rec->data[i], &len_written);
            vdbtree_data_write_datum_overflow(tree, data_block_idx, off, new_data_idx, new_off);
            data_block_idx = new_data_idx;
            off = new_off;
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

    vdbnode_leaf_append_record(page->buf, rec);

    vdb_pager_unpin_page(page);
}

static void vdbtree_leaf_update_record(struct VdbTree* tree, uint32_t idx, uint32_t rec_idx, struct VdbRecord* rec) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;

    if (vdbrecord_has_varlen_data(rec)) {
        vdbtree_leaf_write_varlen_data(tree, idx, rec);
    }

    vdbnode_leaf_write_record(page->buf, rec_idx, rec);

    vdb_pager_unpin_page(page);
}

static uint32_t vdbtree_leaf_read_record_count(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    uint32_t count = vdbnode_leaf_read_record_count(page->buf);
    vdb_pager_unpin_page(page);
    return count;
}

static uint32_t vdbtree_leaf_read_record_key(struct VdbTree* tree, uint32_t idx, uint32_t rec_idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    uint32_t key = vdbnode_leaf_read_record_key(page->buf, rec_idx);
    vdb_pager_unpin_page(page);
    return key;
}

static struct VdbRecord* vdbtree_leaf_read_record(struct VdbTree* tree, uint32_t idx, uint32_t rec_idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    struct VdbSchema* schema = vdbtree_meta_read_schema(tree);
    struct VdbRecord* rec = vdbnode_leaf_read_fixedlen_record(page->buf, schema, rec_idx);
    vdb_schema_free(schema);

    for (uint32_t i = 0; i < rec->count; i++) {
        struct VdbDatum* d = &rec->data[i];
        if (d->type != VDBF_STR)
            continue;

        uint32_t block_idx = d->block_idx;
        uint32_t offset_idx = d->offset_idx;
        d->as.Str = malloc_w(sizeof(struct VdbString));
        d->as.Str->start = NULL;
        d->as.Str->len = 0;

        while (true) {
            struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, block_idx);
            struct VdbDatum datum = vdbnode_leaf_read_varlen_datum(page->buf, offset_idx);

            d->as.Str->start = realloc_w(d->as.Str->start, sizeof(char) * (d->as.Str->len + datum.as.Str->len));
            memcpy(d->as.Str->start + d->as.Str->len, datum.as.Str->start, datum.as.Str->len);
            d->as.Str->len += datum.as.Str->len;

            block_idx = datum.block_idx;
            offset_idx = datum.offset_idx;
            free(datum.as.Str->start);
            free(datum.as.Str);

            vdb_pager_unpin_page(page);

            if (!datum.block_idx)
                break;
        }

    }

    vdb_pager_unpin_page(page);
    return rec;
}

static bool vdbtree_leaf_can_fit_record(struct VdbTree* tree, uint32_t idx, struct VdbRecord* rec) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);

    uint32_t idx_cells_size = vdbnode_leaf_read_record_count(page->buf) * sizeof(uint32_t);
    uint32_t data_cells_size = vdbnode_leaf_read_datacells_size(page->buf);
    uint32_t record_entry_size = vdbrecord_fixedlen_size(rec) + sizeof(uint32_t) * 3; //3 for: index cell + next + size fields

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

    vdb_pager_unpin_page(page);

    return new_leaf;
}

/*
 * Tree wrappers for data node
 */

static uint32_t vdbtree_data_init(struct VdbTree* tree, int32_t parent_idx) {
    uint32_t idx = vdb_pager_fresh_page(tree->f);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;

    vdbnode_write_type(page->buf, VDBN_DATA);
    vdbnode_write_parent(page->buf, parent_idx);
    vdbnode_data_write_next(page->buf, 0);
    vdbnode_data_write_free_size(page->buf, VDB_PAGE_SIZE - VDB_PAGE_HDR_SIZE);

    vdb_pager_unpin_page(page);

    return idx;
}

static uint32_t vdbtree_data_write_datum(struct VdbTree* tree, uint32_t idx, struct VdbDatum* datum, uint32_t* len_written) {
    assert(vdbtree_node_type(tree, idx) == VDBN_DATA);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;
    uint32_t off = vdbnode_data_write_datum(page->buf, datum, len_written);
    vdb_pager_unpin_page(page);
    return off;
}

static void vdbtree_data_write_datum_overflow(struct VdbTree* tree, uint32_t idx, uint32_t datum_off, uint32_t overflow_block, uint32_t overflow_off) {
    assert(vdbtree_node_type(tree, idx) == VDBN_DATA);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;
    vdbnode_data_write_datum_overflow(page->buf, datum_off, overflow_block, overflow_off);
    vdb_pager_unpin_page(page);
}

static void vdbtree_data_write_next(struct VdbTree* tree, uint32_t idx, uint32_t next) {
    assert(vdbtree_node_type(tree, idx) == VDBN_DATA);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;
    vdbnode_data_write_next(page->buf, next);
    vdb_pager_unpin_page(page);
}

static uint32_t vdbtree_data_read_free_size(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_DATA);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    uint32_t free_size = vdbnode_data_read_free_size(page->buf);
    vdb_pager_unpin_page(page);
    return free_size;
}

uint32_t vdbtree_data_read_datacell_next(struct VdbTree* tree, uint32_t idx, uint32_t off) {
    assert(vdbtree_node_type(tree, idx) == VDBN_DATA);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    uint32_t datacell_next = vdbnode_data_read_datacell_next(page->buf, off);
    vdb_pager_unpin_page(page);
    return datacell_next;
}

void vdbtree_data_write_datacell_next(struct VdbTree* tree, uint32_t idx, uint32_t off, uint32_t next) {
    assert(vdbtree_node_type(tree, idx) == VDBN_DATA);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;
    vdbnode_data_write_datacell_next(page->buf, off, next);
    vdb_pager_unpin_page(page);
}


void vdbtree_data_insert_into_freelist(struct VdbTree* tree, uint32_t block_idx, uint32_t offset_idx) {
    assert(vdbtree_node_type(tree, block_idx) == VDBN_DATA);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, block_idx);
    page->dirty = true;

    uint32_t freelist_head = vdbnode_data_read_freelist_head(page->buf);
    vdbnode_data_write_datacell_next(page->buf, offset_idx, freelist_head);
    vdbnode_data_write_freelist_head(page->buf, offset_idx);

    vdb_pager_unpin_page(page);
}

void vdbtree_data_read_datacell_overflow(struct VdbTree* tree, uint32_t block_idx, uint32_t offset_idx, uint32_t* overflow_block_idx, uint32_t* overflow_offset_idx) {
    assert(vdbtree_node_type(tree, block_idx) == VDBN_DATA);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, block_idx);
    page->dirty = true;

    *overflow_block_idx = vdbnode_data_read_datacell_overflow_block(page->buf, offset_idx);
    *overflow_offset_idx = vdbnode_data_read_datacell_overflow_offset(page->buf, offset_idx);

    vdb_pager_unpin_page(page);
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

struct VdbTree* vdb_tree_catch(const char* name, FILE* f, struct VdbPager* pager) {
    struct VdbTree* tree = malloc_w(sizeof(struct VdbTree));
    tree->name = strdup(name);
    tree->f = f;
    tree->pager = pager;
    tree->meta_idx = 0;
    return tree;
}

void vdb_tree_release(struct VdbTree* tree) {
    free(tree->name);
    free(tree);
}

static uint32_t vdb_tree_traverse_to(struct VdbTree* tree, uint32_t idx, uint32_t key) {
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
    uint32_t leaf_idx = vdb_tree_traverse_to(tree, root_idx, rec->key);
    
    if (!vdbtree_leaf_can_fit_record(tree, leaf_idx, rec)) {
        leaf_idx = vdbtree_leaf_split(tree, leaf_idx, rec->key);
    }

    vdbtree_leaf_append_record(tree, leaf_idx, rec);
}

struct VdbRecord* vdb_tree_fetch_record(struct VdbTree* tree, uint32_t key) {
    if (key > vdbtree_meta_read_primary_key_counter(tree)) {
        return NULL;
    }

    uint32_t root_idx = vdbtree_meta_read_root(tree);
    uint32_t leaf_idx = vdb_tree_traverse_to(tree, root_idx, key);

    //TODO: switch from linear to binary search
    for (uint32_t i = 0; i < vdbtree_leaf_read_record_count(tree, leaf_idx); i++) {
        uint32_t cur_key = vdbtree_leaf_read_record_key(tree, leaf_idx, i);
        if (cur_key == key) {
            return vdbtree_leaf_read_record(tree, leaf_idx, i);
        }
    }

    return NULL;
}

void vdbtree_leaf_free_varlen_data(struct VdbTree* tree, struct VdbRecord* rec) {
    for (uint32_t i = 0; i < rec->count; i++) {
        struct VdbDatum d = rec->data[i]; 
        if (d.type != VDBF_STR)
            continue;
       
        uint32_t block_idx = d.block_idx;
        uint32_t offset_idx = d.offset_idx;

        while (block_idx != 0) {
            vdbtree_data_insert_into_freelist(tree, block_idx, offset_idx); 
            vdbtree_data_read_datacell_overflow(tree, block_idx, offset_idx, &block_idx, &offset_idx);
        }
    }
}

bool vdbtree_delete_record(struct VdbTree* tree, uint32_t key) {
    if (key > vdbtree_meta_read_primary_key_counter(tree)) {
        return false;
    }

    uint32_t root_idx = vdbtree_meta_read_root(tree);
    uint32_t leaf_idx = vdb_tree_traverse_to(tree, root_idx, key);

    //TODO: switch from linear to binary search
    for (uint32_t i = 0; i < vdbtree_leaf_read_record_count(tree, leaf_idx); i++) {
        uint32_t cur_key = vdbtree_leaf_read_record_key(tree, leaf_idx, i);
        if (cur_key == key) {
            struct VdbRecord* rec = vdbtree_leaf_read_record(tree, leaf_idx, i);
            rec->key = 0;
            vdbtree_leaf_free_varlen_data(tree, rec);
            vdbtree_leaf_update_record(tree, leaf_idx, i, rec);
            vdb_record_free(rec);
            return true;
        }
    }

    return false;
}


bool vdbtree_update_record(struct VdbTree* tree, struct VdbRecord* rec) {
    if (rec->key > vdbtree_meta_read_primary_key_counter(tree)) {
        return false;
    }

    uint32_t root_idx = vdbtree_meta_read_root(tree);
    uint32_t leaf_idx = vdb_tree_traverse_to(tree, root_idx, rec->key);

    //TODO: switch from linear to binary search
    for (uint32_t i = 0; i < vdbtree_leaf_read_record_count(tree, leaf_idx); i++) {
        uint32_t cur_key = vdbtree_leaf_read_record_key(tree, leaf_idx, i);
        if (cur_key == rec->key) {
            struct VdbRecord* old_rec = vdbtree_leaf_read_record(tree, leaf_idx, i);
            vdbtree_leaf_free_varlen_data(tree, old_rec);
            vdb_record_free(old_rec);
            vdbtree_leaf_update_record(tree, leaf_idx, i, rec);
            return true;
        }
    }

    return false;
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
        tl->capacity *= 2;
        tl->trees = realloc_w(tl->trees, sizeof(struct VdbTree*) * tl->capacity);
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
        vdb_tree_release(t);
    }

    free(tl->trees);
    free(tl);
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
