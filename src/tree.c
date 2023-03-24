#include <string.h>
#include <stdlib.h>
#include <assert.h>

#include "tree.h"
#include "util.h"
#include "pager.h"

void il_init(struct IndexList* il) {
    il->capacity = 8;
    il->count = 0;
    il->indices = malloc_w(sizeof(uint32_t) * il->capacity);
}

void il_append(struct IndexList* il, uint32_t idx) {
    if (il->count >= il->capacity) {
        il->capacity *= 2;
        il->indices = realloc_w(il->indices, il->capacity * sizeof(uint32_t));
    }

    il->indices[il->count] = idx;
    il->count++;
}

void il_free(struct IndexList* il) {
    free(il->indices);
}


void _tree_read_uint32_t(uint32_t* dst, uint8_t* buf, int* off) {
    *dst = *((uint32_t*)(buf + *off));
    *off += sizeof(uint32_t);
}

void _tree_write_uint32_t(uint8_t* dst, uint32_t v, int* off) {
    *((uint32_t*)(dst + *off)) = v;
    *off += sizeof(uint32_t);
}

struct InternMeta _tree_deserialize_intern_meta(uint8_t* buf) {
    struct InternMeta m;
    int off = 0;

    _tree_read_uint32_t(&m.type, buf, &off);
    _tree_read_uint32_t(&m.offsets_size, buf, &off);
    _tree_read_uint32_t(&m.cells_size, buf, &off);
    _tree_read_uint32_t(&m.freelist, buf, &off);
    _tree_read_uint32_t(&m.right_ptr.next, buf, &off);
    _tree_read_uint32_t(&m.right_ptr.key, buf, &off);
    _tree_read_uint32_t(&m.right_ptr.block_idx, buf, &off);

    return m;
}

int _tree_serialize_intern_meta(uint8_t* buf, struct InternMeta* m) {
    int off = 0;
    _tree_write_uint32_t(buf, (uint32_t)m->type, &off);
    _tree_write_uint32_t(buf, (uint32_t)m->offsets_size, &off);
    _tree_write_uint32_t(buf, (uint32_t)m->cells_size, &off);
    _tree_write_uint32_t(buf, (uint32_t)m->freelist, &off);
    _tree_write_uint32_t(buf, (uint32_t)m->right_ptr.next, &off);
    _tree_write_uint32_t(buf, (uint32_t)m->right_ptr.key, &off);
    _tree_write_uint32_t(buf, (uint32_t)m->right_ptr.block_idx, &off);

    return off;
}

struct LeafMeta _tree_deserialize_leaf_meta(uint8_t* buf) {
    struct LeafMeta m;
    int off = 0;

    _tree_read_uint32_t(&m.type, buf, &off);
    _tree_read_uint32_t(&m.offsets_size, buf, &off);
    _tree_read_uint32_t(&m.cells_size, buf, &off);
    _tree_read_uint32_t(&m.freelist, buf, &off);

    return m;
}

int _tree_serialize_leaf_meta(uint8_t* buf, struct LeafMeta* m) {
    int off = 0;
    _tree_write_uint32_t(buf, (uint32_t)m->type, &off);
    _tree_write_uint32_t(buf, (uint32_t)m->offsets_size, &off);
    _tree_write_uint32_t(buf, (uint32_t)m->cells_size, &off);
    _tree_write_uint32_t(buf, (uint32_t)m->freelist, &off);

    return off;
}

struct NodeCell _tree_deserialize_nodecell(uint8_t* buf) {
    struct NodeCell nc;

    int off = 0;
    _tree_read_uint32_t(&nc.next, buf, &off);
    _tree_read_uint32_t(&nc.key, buf, &off);
    _tree_read_uint32_t(&nc.block_idx, buf, &off);

    return nc;
}

void _tree_serialize_nodecell(uint8_t* buf, struct NodeCell* nc) {
    int off = 0;
    _tree_write_uint32_t(buf, nc->next, &off); 
    _tree_write_uint32_t(buf, nc->key, &off); 
    _tree_write_uint32_t(buf, nc->block_idx, &off); 
}

int _tree_serialize_datacell(uint8_t* buf, struct DataCell* dc) {
    struct VdbData* d = dc->data;
    int off = 0;
    _tree_write_uint32_t(buf, dc->next, &off);
    _tree_write_uint32_t(buf, dc->key, &off);
    _tree_write_uint32_t(buf, dc->size, &off);
    for (uint32_t i = 0; i < d->count; i++) {
        struct VdbDatum* f = &d->data[i];
        switch (f->type) {
        case VDBF_INT: {
            memcpy(buf + off, &f->as.Int, sizeof(uint64_t));
            off += sizeof(uint64_t);
            break;
        }
        case VDBF_STR: {
            memcpy(buf + off, &f->as.Str->len, sizeof(uint32_t));
            off += sizeof(uint32_t);
            memcpy(buf + off, f->as.Str->start, f->as.Str->len);
            off += f->as.Str->len; 
            break;
        }
        case VDBF_BOOL: {
            bool b = f->as.Bool;
            memcpy(buf + off, &b, sizeof(bool));
            off += sizeof(bool);
            break;
        }
        }
    }

    return off;
}

struct DataCell* _tree_deserialize_datacell(uint8_t* buf, struct VdbSchema* schema) {
    struct DataCell* dc = malloc_w(sizeof(struct DataCell));
    int off = 0;
    _tree_read_uint32_t(&dc->next, buf, &off);
    _tree_read_uint32_t(&dc->key, buf, &off);
    _tree_read_uint32_t(&dc->size, buf, &off);
    dc->data = malloc_w(sizeof(struct VdbData));
    dc->data->count = schema->count;
    dc->data->data = malloc_w(sizeof(struct VdbDatum) * schema->count);

    for (uint32_t i = 0; i < schema->count; i++) {
        enum VdbField f = schema->fields[i];
        switch (f) {
        case VDBF_INT:
            dc->data->data[i].type = VDBF_INT;
            dc->data->data[i].as.Int = *((uint64_t*)(buf + off));
            off += sizeof(uint64_t);
            break;
        case VDBF_STR:
            dc->data->data[i].type = VDBF_STR;
            dc->data->data[i].as.Str = malloc_w(sizeof(struct VdbString));
            _tree_read_uint32_t(&dc->data->data[i].as.Str->len, buf, &off);
            dc->data->data[i].as.Str->start = malloc_w(sizeof(char) * dc->data->data[i].as.Str->len);
            memcpy(dc->data->data[i].as.Str->start, buf + off, dc->data->data[i].as.Str->len);
            off += dc->data->data[i].as.Str->len;
            break;
        case VDBF_BOOL:
            dc->data->data[i].type = VDBF_BOOL;
            dc->data->data[i].as.Bool = *((bool*)(buf + off));
            off += sizeof(bool);
            break;
        }
    }

    return dc;
}

void _tree_free_datacell(struct DataCell* dc) {
    vdb_free_data(dc->data);
    free(dc);
}

struct TreeMeta* _tree_deserialize_tree_meta(uint8_t* buf) {
    struct TreeMeta* tm = malloc_w(sizeof(struct TreeMeta));
    int off = 0;
    _tree_read_uint32_t(&tm->node_size, buf, &off);
    _tree_read_uint32_t(&tm->pk_counter, buf, &off);

    tm->schema = malloc_w(sizeof(struct VdbSchema));
    _tree_read_uint32_t(&tm->schema->count, buf, &off);
    tm->schema->fields = malloc_w(sizeof(enum VdbField) * tm->schema->count);
    for (uint32_t i = 0; i < tm->schema->count; i++) {
        uint32_t f;
        _tree_read_uint32_t(&f, buf, &off);
        tm->schema->fields[i] = f;
    }

    return tm;
}

void _tree_serialize_tree_meta(uint8_t* buf, struct TreeMeta* tm) {
    int off = 0;
    _tree_write_uint32_t(buf, (uint32_t)tm->node_size, &off);
    _tree_write_uint32_t(buf, (uint32_t)tm->pk_counter, &off);
    _tree_write_uint32_t(buf, (uint32_t)tm->schema->count, &off);
    for (uint32_t i = 0; i < tm->schema->count; i++) {
        uint32_t field = tm->schema->fields[i];
        _tree_write_uint32_t(buf, field, &off);
    }
}

void _tree_free_tree_meta(struct TreeMeta* tm) {
    free(tm->schema->fields);
    free(tm->schema);
    free(tm);
}

void _tree_init_tree_meta(struct TreeMeta* m, struct VdbSchema* schema) {
    m->node_size = VDB_PAGE_SIZE;
    m->pk_counter = 1;
    m->schema = schema;
}

void tree_init(FILE* f, struct VdbSchema* schema) {
    struct TreeMeta tm;
    _tree_init_tree_meta(&tm, schema);
    uint8_t* buf3 = calloc_w(VDB_PAGE_SIZE, sizeof(uint8_t));
    _tree_serialize_tree_meta(buf3, &tm);
    fseek_w(f, 0, SEEK_SET);
    fwrite_w(buf3, sizeof(uint8_t), VDB_PAGE_SIZE, f);
    free(buf3);

    struct InternMeta m;
    m.type = VDBN_INTERN;
    m.offsets_size = 0;
    m.cells_size = 0;
    m.freelist = 0;
    struct NodeCell nc = {0, 1, 2};
    m.right_ptr = nc;

    //init root
    uint8_t* buf = calloc_w(VDB_PAGE_SIZE, sizeof(uint8_t));
    _tree_serialize_intern_meta(buf, &m);

    fseek_w(f, VDB_PAGE_SIZE, SEEK_SET);
    fwrite_w(buf, sizeof(uint8_t), VDB_PAGE_SIZE, f);
    free(buf);

    //init first leaf
    struct InternMeta lm;
    lm.type = VDBN_LEAF;
    lm.offsets_size = 0;
    lm.cells_size = 0;
    lm.freelist = 0;
    struct NodeCell lnc = {0, 0, 0};
    lm.right_ptr = lnc; //Not used

    uint8_t* buf2 = calloc_w(VDB_PAGE_SIZE, sizeof(uint8_t));
    _tree_serialize_intern_meta(buf2, &lm);
    fseek_w(f, VDB_PAGE_SIZE * 2, SEEK_SET);
    fwrite_w(buf2, sizeof(uint8_t), VDB_PAGE_SIZE, f);
    free(buf2);
}

struct IndexList _tree_traverse_to(struct VdbTree t, uint32_t pk) {
    struct IndexList il;
    il_init(&il);

    struct VdbPage* node = pager_get_page(t.pager, t.f, 1);

    while (true) {
        struct InternMeta m = _tree_deserialize_intern_meta(node->buf);
        il_append(&il, node->idx);

        //TODO: switch to binary search
        int child_idx = -1;
        uint32_t i = OFFSETS_START;
        while (i < OFFSETS_START + m.offsets_size) {
            uint32_t off = *((uint32_t*)&node->buf[i]);
            struct NodeCell nc = _tree_deserialize_nodecell(&node->buf[off]);
            if (i == OFFSETS_START) {
                if (pk <= nc.key) {
                    child_idx = nc.block_idx;
                    break;
                }
            } else {
                uint32_t prev_off = *((uint32_t*)&node->buf[i - sizeof(uint32_t)]);
                struct NodeCell pnc = _tree_deserialize_nodecell(&node->buf[prev_off]);
                if (pnc.key < pk && pk <= nc.key) {
                    child_idx = nc.block_idx;
                    break;
                }
            }

            i += sizeof(uint32_t);
        }

        if (child_idx == -1) {
            child_idx = m.right_ptr.block_idx;
        }
        struct VdbPage* child = pager_get_page(t.pager, t.f, child_idx);
        struct InternMeta cm = _tree_deserialize_intern_meta(child->buf);

        if (cm.type == VDBN_INTERN) {
            node = child;
        } else {
            il_append(&il, child->idx);
            break;
        }
    }

    return il;
}


int _tree_data_size(struct VdbData* d) {
    uint32_t data_size = 0;
    for (uint32_t i = 0; i < d->count; i++) {
        struct VdbDatum* f = &d->data[i];
        switch (f->type) {
            case VDBF_INT:
                data_size += sizeof(uint64_t);
                break;
            case VDBF_STR:
                data_size += sizeof(uint32_t) + f->as.Str->len;
                break;
            case VDBF_BOOL:
                data_size += sizeof(bool);
                break;
        }
    }

    return data_size;
}

bool _tree_node_is_full(struct VdbPage* node, uint32_t dc_size) {
    struct InternMeta lm = _tree_deserialize_intern_meta(node->buf);
    uint32_t current_size = OFFSETS_START + lm.cells_size + lm.offsets_size;
    return current_size + dc_size >= VDB_PAGE_SIZE;
}

bool _tree_node_is_leaf(uint8_t* buf) {
    int off = 0;
    uint32_t type;
    _tree_read_uint32_t(&type, buf, &off);
    
    return type == (uint32_t)VDBN_LEAF;
}

void _tree_append_datacells(struct VdbPage* dst, uint8_t* src, uint32_t start, uint32_t size, struct VdbSchema* schema) {
    struct InternMeta dm = _tree_deserialize_intern_meta(dst->buf);
    assert(dm.type == VDBN_LEAF);

    for (uint32_t i = start; i < start + size; i += sizeof(uint32_t)) {
        uint32_t off = *((uint32_t*)(src + i));
        struct DataCell* dc = _tree_deserialize_datacell(src + off, schema);
        dm.cells_size += sizeof(uint32_t) * 3 + dc->size;
        _tree_serialize_datacell(dst->buf + VDB_PAGE_SIZE - dm.cells_size, dc);
        _tree_free_datacell(dc);

        uint32_t new_offset = VDB_PAGE_SIZE - dm.cells_size;
        memcpy(dst->buf + OFFSETS_START + dm.offsets_size, &new_offset, sizeof(uint32_t));
        dm.offsets_size += sizeof(uint32_t);
    }

    _tree_serialize_intern_meta(dst->buf, &dm);
}

void _tree_append_nodecells(struct VdbPage* dst, uint8_t* src, uint32_t start, uint32_t size) {
    struct InternMeta dm = _tree_deserialize_intern_meta(dst->buf);
    assert(dm.type == VDBN_INTERN);

    for (uint32_t i = start; i < start + size; i += sizeof(uint32_t)) {
        uint32_t off = *((uint32_t*)(src + i));
        struct NodeCell nc = _tree_deserialize_nodecell(src + off);
        dm.cells_size += sizeof(uint32_t) * 3;
        uint32_t new_off = VDB_PAGE_SIZE - dm.cells_size;
        _tree_serialize_nodecell(dst->buf + new_off, &nc);
        memcpy(dst->buf + OFFSETS_START + dm.offsets_size, &new_off, sizeof(uint32_t));
        dm.offsets_size += sizeof(uint32_t);
    }

    _tree_serialize_intern_meta(dst->buf, &dm);
}

void _tree_traverse_to_depth(struct VdbPager* pager, FILE* f, struct IndexList* il, uint32_t idx, uint32_t cur_depth, uint32_t max_depth) {
    if (cur_depth == max_depth) {
        il_append(il, idx);
        return;
    }

    struct VdbPage* node = pager_get_page(pager, f, idx);
    struct InternMeta m = _tree_deserialize_intern_meta(node->buf);
    if (!_tree_node_is_leaf(node->buf)) {
        for (uint32_t i = OFFSETS_START; i < OFFSETS_START + m.offsets_size; i += sizeof(uint32_t)) {
            //deserialize node cell
            uint32_t off = *((uint32_t*)(node->buf + i));
            struct NodeCell nc = _tree_deserialize_nodecell(node->buf + off);
            _tree_traverse_to_depth(pager, f, il, nc.block_idx, cur_depth + 1, max_depth);
        }
        _tree_traverse_to_depth(pager, f, il, m.right_ptr.block_idx, cur_depth + 1, max_depth);
    }
}


struct IndexList _tree_get_nodes_at_depth(struct VdbPager* pager, FILE* f, uint32_t depth) {
    struct IndexList il;
    il_init(&il);

    _tree_traverse_to_depth(pager, f, &il, 1, 0, depth);

    //may need a separate function to call recursively
    //just need to find nodes at depth - 1, and add all those children (which will be at depth) to il
    
    return il;
}

void _tree_rebalance_internals(struct VdbPager* pager, FILE* f, uint32_t depth) {
    struct IndexList il = _tree_get_nodes_at_depth(pager, f, depth);
    for (uint32_t i = 0; i < il.count; i++) {
        printf("%d, ", il.indices[i]);
    }
    printf("\n");

    //compute total number of keys, and split among nodes
    //
    //check if final set of keys (key counter >= other key counts) will fit in node
    //if not
    //  add new node
    //
    //split cells among all nodes
    il_free(&il);
}

struct VdbPage* _tree_split_internal(struct VdbTree t, struct VdbPage* node) {
    uint32_t left_idx = pager_allocate_page(t.f);
    uint32_t right_idx = pager_allocate_page(t.f);

    struct VdbPage* left_internal = pager_get_page(t.pager, t.f, left_idx);
    struct VdbPage* right_internal = pager_get_page(t.pager, t.f, right_idx);

    struct InternMeta nm = _tree_deserialize_intern_meta(node->buf);
    uint32_t offsets_mid = OFFSETS_START + nm.offsets_size / sizeof(uint32_t) / 2 * sizeof(uint32_t);

    //copy data from internal to left/right internal
    struct InternMeta lm;
    lm.type = VDBN_INTERN;
    lm.offsets_size = 0;
    lm.cells_size = 0;
    lm.freelist = 0;
    uint32_t off_mid = *((uint32_t*)(node->buf + offsets_mid - sizeof(uint32_t)));
    lm.right_ptr = _tree_deserialize_nodecell(node->buf + off_mid);

    struct InternMeta rm;
    rm.type = VDBN_INTERN;
    rm.offsets_size = 0;
    rm.cells_size = 0;
    rm.freelist = 0;
    rm.right_ptr = nm.right_ptr;

    _tree_serialize_intern_meta(left_internal->buf, &lm);
    _tree_serialize_intern_meta(right_internal->buf, &rm);

    _tree_append_nodecells(left_internal, node->buf, OFFSETS_START, offsets_mid - sizeof(uint32_t) - OFFSETS_START);
    _tree_append_nodecells(right_internal, node->buf, offsets_mid, OFFSETS_START + nm.offsets_size - offsets_mid - sizeof(uint32_t));

    memset(node->buf, 0, VDB_PAGE_SIZE);
    nm.offsets_size = 0;
    nm.cells_size = 0;
    struct NodeCell new_right = { 0, rm.right_ptr.key, right_idx };
    nm.right_ptr = new_right;

    nm.cells_size += sizeof(uint32_t) * 3;
    uint32_t left_off = VDB_PAGE_SIZE - nm.cells_size;
    struct NodeCell nc = { 0, lm.right_ptr.key, left_idx };
    _tree_serialize_nodecell(node->buf + left_off, &nc);
    memcpy(node->buf + OFFSETS_START + nm.offsets_size, &left_off, sizeof(uint32_t));
    nm.offsets_size += sizeof(uint32_t);

    _tree_serialize_intern_meta(node->buf, &nm);
    return node;
}

void _tree_split_leaf(struct VdbTree t, struct IndexList il) {
    struct VdbPage* leaf = pager_get_page(t.pager, t.f, il.indices[il.count - 1]);
    struct LeafMeta lm = _tree_deserialize_leaf_meta(leaf->buf);
    struct VdbPage* meta_page = pager_get_page(t.pager, t.f, 0);
    struct TreeMeta* tm = _tree_deserialize_tree_meta(meta_page->buf);
    struct VdbPage* parent = pager_get_page(t.pager, t.f, il.indices[il.count - 2]);

    uint32_t new_idx = pager_allocate_page(t.f);
    struct VdbPage* new_leaf = pager_get_page(t.pager, t.f, new_idx);

    struct LeafMeta nlm;
    nlm.type = VDBN_LEAF;
    nlm.offsets_size = 0;
    nlm.cells_size = 0;
    nlm.freelist = 0;

    uint8_t* buf = malloc_w(VDB_PAGE_SIZE);
    memcpy(buf, leaf->buf, VDB_PAGE_SIZE);
    memset(leaf->buf, 0, VDB_PAGE_SIZE);

    uint32_t prev_offsets_size = lm.offsets_size;

    lm.offsets_size = 0;
    lm.cells_size = 0;
    lm.freelist = 0;

    uint32_t half = prev_offsets_size / sizeof(uint32_t) / 2 * sizeof(uint32_t);
    uint32_t final_off = *((uint32_t*)(buf + OFFSETS_START + half - sizeof(uint32_t)));
    struct DataCell* final_dc = _tree_deserialize_datacell(buf + final_off, tm->schema);
    uint32_t final_key = final_dc->key;
    _tree_free_datacell(final_dc);

    _tree_serialize_leaf_meta(new_leaf->buf, &nlm);
    _tree_append_datacells(new_leaf, buf, OFFSETS_START, half, tm->schema);

    _tree_serialize_leaf_meta(leaf->buf, &lm);
    _tree_append_datacells(leaf, buf, OFFSETS_START + half, prev_offsets_size - half, tm->schema);

    free(buf);

    uint32_t nc_size = sizeof(uint32_t) * 3;
    if (_tree_node_is_full(parent, nc_size)) {
        parent = _tree_split_internal(t, parent);
        struct InternMeta m = _tree_deserialize_intern_meta(parent->buf);
        parent = pager_get_page(t.pager, t.f, m.right_ptr.block_idx);
    }

    struct NodeCell nc;
    nc.next = 0;
    nc.key = final_key;
    nc.block_idx = new_idx;

    struct InternMeta pm = _tree_deserialize_intern_meta(parent->buf);
    pm.cells_size += sizeof(uint32_t) * 3;
    uint32_t dc_off = VDB_PAGE_SIZE - pm.cells_size;
    memcpy(parent->buf + OFFSETS_START + pm.offsets_size, &dc_off, sizeof(uint32_t));
    pm.offsets_size += sizeof(uint32_t);
    _tree_serialize_nodecell(parent->buf + dc_off, &nc);
    _tree_serialize_intern_meta(parent->buf, &pm);

    _tree_free_tree_meta(tm);
}

void tree_insert_record(struct VdbTree t, struct VdbData* d) {
    uint32_t root_idx = 1;
    struct VdbPage* root = pager_get_page(t.pager, t.f, root_idx);
    struct InternMeta m = _tree_deserialize_intern_meta(root->buf);

    struct VdbPage* meta_page = pager_get_page(t.pager, t.f, 0);
    struct TreeMeta* tm = _tree_deserialize_tree_meta(meta_page->buf);

    uint32_t pk = tm->pk_counter;


    struct IndexList il = _tree_traverse_to(t, pk);
    struct VdbPage* leaf = pager_get_page(t.pager, t.f, il.indices[il.count - 1]);

    struct LeafMeta lm = _tree_deserialize_leaf_meta(leaf->buf);

    uint32_t data_size = _tree_data_size(d);
    uint32_t dc_meta_size = sizeof(uint32_t) * 3;
    struct DataCell dc = { 0, pk, data_size, d };

    if (_tree_node_is_full(leaf, dc_meta_size + data_size)) {
        _tree_split_leaf(t, il);
        il_free(&il);

        il = _tree_traverse_to(t, pk);
        leaf = pager_get_page(t.pager, t.f, il.indices[il.count - 1]);

        m = _tree_deserialize_intern_meta(root->buf);
        lm = _tree_deserialize_leaf_meta(leaf->buf);
    }

    _tree_serialize_datacell(leaf->buf + VDB_PAGE_SIZE - lm.cells_size - data_size - dc_meta_size, &dc);

    //write offset
    uint32_t data_off = VDB_PAGE_SIZE - lm.cells_size - data_size - dc_meta_size;
    memcpy(leaf->buf + OFFSETS_START + lm.offsets_size, &data_off, sizeof(uint32_t));

    lm.offsets_size += sizeof(uint32_t);
    lm.cells_size += data_size + dc_meta_size;
    _tree_serialize_leaf_meta(leaf->buf, &lm);

    m.right_ptr.key = pk;
    _tree_serialize_intern_meta(root->buf, &m);
    tm->pk_counter++;
    _tree_serialize_tree_meta(meta_page->buf, tm);
    _tree_free_tree_meta(tm);

    //update right pointers, and flush internal nodes
    for (uint32_t i = 0; i < il.count - 1; i++) {
        struct VdbPage* node = pager_get_page(t.pager, t.f, il.indices[i]);
        struct InternMeta lp = _tree_deserialize_intern_meta(node->buf);
        lp.right_ptr.key = pk;
        _tree_serialize_intern_meta(node->buf, &lp);
        fseek_w(t.f, VDB_PAGE_SIZE * il.indices[i], SEEK_SET);
        fwrite_w(node->buf, VDB_PAGE_SIZE, 1, t.f);
    }

    il_free(&il);

    //flush leaf
    fseek_w(t.f, VDB_PAGE_SIZE * leaf->idx, SEEK_SET);
    fwrite_w(leaf->buf, VDB_PAGE_SIZE, 1, t.f);
}


struct VdbData* tree_fetch_record(struct VdbTree t, uint32_t key) {
    struct IndexList il = _tree_traverse_to(t, key);
    struct VdbPage* leaf = pager_get_page(t.pager, t.f, il.indices[il.count - 1]);
    il_free(&il);

    struct LeafMeta lm = _tree_deserialize_leaf_meta(leaf->buf);

    struct VdbPage* meta_node = pager_get_page(t.pager, t.f, 0);
    struct TreeMeta* tm = _tree_deserialize_tree_meta(meta_node->buf);

    struct DataCell* dc = NULL;
    for (uint32_t i = OFFSETS_START; i < OFFSETS_START + lm.offsets_size; i += sizeof(uint32_t)) {
        uint32_t offset = *((uint32_t*)&leaf->buf[i]);
        dc = _tree_deserialize_datacell(leaf->buf + offset, tm->schema);

        if (dc->key == key)
            break;

        _tree_free_datacell(dc);
        dc = NULL;
    }

    if (!dc)
        return NULL;

    struct VdbData* result = dc->data;
    free(dc);
    _tree_free_tree_meta(tm);

    return result;
}

void debug_print_tree(struct VdbTree t, uint32_t idx, uint32_t depth) {
    struct VdbPager* pager = t.pager;
    FILE* f = t.f;

    struct VdbPage* node = pager_get_page(pager, f, idx);    
    if (_tree_node_is_leaf(node->buf)) {
        struct LeafMeta m = _tree_deserialize_leaf_meta(node->buf);
        struct VdbPage* meta_node = pager_get_page(pager, f, 0);
        struct TreeMeta* tm = _tree_deserialize_tree_meta(meta_node->buf);
        char s[depth * 4 + 1];
        for (uint32_t i = 0; i < depth * 4; i++) {
            s[i] = ' ';
        }
        s[depth * 4] = '\0';
        printf("%s%d: (keys ", s, node->idx);

        if (m.offsets_size > 0) {
            uint32_t off = *((uint32_t*)(node->buf + OFFSETS_START));
            struct DataCell* dc = _tree_deserialize_datacell(node->buf + off, tm->schema);
            printf("%d - ", dc->key);
            _tree_free_datacell(dc);

            off = *((uint32_t*)(node->buf + OFFSETS_START + m.offsets_size - sizeof(uint32_t)));
            dc = _tree_deserialize_datacell(node->buf + off, tm->schema);
            printf("%d)", dc->key);
            _tree_free_datacell(dc);
        }
        printf("\n");

        _tree_free_tree_meta(tm);
    } else {
        struct InternMeta m = _tree_deserialize_intern_meta(node->buf);
        char s[depth * 4 + 1];
        for (uint32_t i = 0; i < depth * 4; i++) {
            s[i] = ' ';
        }
        s[depth * 4] = '\0';
        printf("%s%d\n", s, node->idx);
        for (uint32_t i = OFFSETS_START; i < OFFSETS_START + m.offsets_size; i += sizeof(uint32_t)) {
            uint32_t off = *((uint32_t*)(node->buf + i));
            struct NodeCell nc = _tree_deserialize_nodecell(node->buf + off);
            debug_print_tree(t, nc.block_idx, depth + 1);
        }
        debug_print_tree(t, m.right_ptr.block_idx, depth + 1); 
    }
}

