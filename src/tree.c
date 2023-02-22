#include <string.h>
#include <stdlib.h>
#include <assert.h>

#include "tree.h"
#include "util.h"
#include "pager.h"


void _tree_free_meta(struct NodeMeta* m) {
    free(m->schema->fields);
    free(m->schema);
    free(m);
}

void _tree_read_uint32_t(uint32_t* dst, uint8_t* buf, int* off) {
    *dst = *((uint32_t*)(buf + *off));
    *off += sizeof(uint32_t);
}

void _tree_write_uint32_t(uint8_t* dst, uint32_t v, int* off) {
    *((uint32_t*)(dst + *off)) = v;
    *off += sizeof(uint32_t);
}

struct NodeMeta* _tree_deserialize_meta(uint8_t* buf) {
    struct NodeMeta* m = malloc_w(sizeof(struct NodeMeta));
    struct VdbSchema* s = malloc_w(sizeof(struct VdbSchema));
    m->schema = s;
    int off = 0;

    _tree_read_uint32_t(&m->type, buf, &off);
    _tree_read_uint32_t(&m->node_size, buf, &off);
    _tree_read_uint32_t(&m->offsets_size, buf, &off);
    _tree_read_uint32_t(&m->cells_size, buf, &off);
    _tree_read_uint32_t(&m->freelist, buf, &off);
    _tree_read_uint32_t(&m->right_ptr.next, buf, &off);
    _tree_read_uint32_t(&m->right_ptr.key, buf, &off);
    _tree_read_uint32_t(&m->right_ptr.block_idx, buf, &off);
    _tree_read_uint32_t(&m->schema->count, buf, &off);

    m->schema->fields = malloc_w(sizeof(enum VdbField) * m->schema->count);
    for (uint32_t i = 0; i < m->schema->count; i++) {
        uint32_t f;
        _tree_read_uint32_t(&f, buf, &off);
        m->schema->fields[i] = f;
    }

    return m;
}

int _tree_serialize_meta(uint8_t* buf, struct NodeMeta* m) {
    int off = 0;
    _tree_write_uint32_t(buf, (uint32_t)m->type, &off);
    _tree_write_uint32_t(buf, (uint32_t)m->node_size, &off);
    _tree_write_uint32_t(buf, (uint32_t)m->offsets_size, &off);
    _tree_write_uint32_t(buf, (uint32_t)m->cells_size, &off);
    _tree_write_uint32_t(buf, (uint32_t)m->freelist, &off);
    _tree_write_uint32_t(buf, (uint32_t)m->right_ptr.next, &off);
    _tree_write_uint32_t(buf, (uint32_t)m->right_ptr.key, &off);
    _tree_write_uint32_t(buf, (uint32_t)m->right_ptr.block_idx, &off);
    _tree_write_uint32_t(buf, (uint32_t)m->schema->count, &off);
    for (uint32_t i = 0; i < m->schema->count; i++) {
        uint32_t field = m->schema->fields[i];
        _tree_write_uint32_t(buf, field, &off);
    }

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
            memcpy(buf + off, &f->as.Int, sizeof(uint32_t));
            off += sizeof(f->as.Int);
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
            dc->data->data[i].as.Int = *((uint64_t*)(buf + off));
            off += sizeof(uint64_t);
            break;
        case VDBF_STR:
            dc->data->data[i].as.Str = malloc_w(sizeof(struct VdbString));
            _tree_read_uint32_t(&dc->data->data[i].as.Str->len, buf, &off);
            dc->data->data[i].as.Str->start = malloc_w(sizeof(char) * dc->data->data[i].as.Str->len);
            memcpy(dc->data->data[i].as.Str->start, buf + off, dc->data->data[i].as.Str->len);
            off += dc->data->data[i].as.Str->len;
            break;
        case VDBF_BOOL:
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

void tree_init(FILE* f, struct VdbSchema* schema) {
    struct NodeMeta m;
    m.type = VDBN_INTERN;
    m.node_size = VDB_PAGE_SIZE;
    m.offsets_size = 0;
    m.cells_size = 0;
    struct NodeCell nc = {0, 1, 1};
    m.right_ptr = nc;
    m.freelist = 0;
    m.schema = schema;

    //init root
    uint8_t* buf = calloc_w(VDB_PAGE_SIZE, sizeof(uint8_t));
    _tree_serialize_meta(buf, &m);

    fseek_w(f, 0, SEEK_SET);
    fwrite_w(buf, sizeof(uint8_t), VDB_PAGE_SIZE, f);

    free(buf);

    //init first leaf
    struct NodeMeta lm;
    lm.type = VDBN_LEAF;
    lm.node_size = VDB_PAGE_SIZE;
    lm.offsets_size = 0;
    lm.cells_size = 0;
    lm.freelist = 0;
    struct NodeCell lnc = {0, 0, 0};
    lm.right_ptr = lnc; //Not used
    lm.schema = schema;

    uint8_t* buf2 = calloc_w(VDB_PAGE_SIZE, sizeof(uint8_t));
    _tree_serialize_meta(buf2, &lm);
    fseek_w(f, VDB_PAGE_SIZE, SEEK_SET);
    fwrite_w(buf2, sizeof(uint8_t), VDB_PAGE_SIZE, f);
    free(buf2);
}

struct VdbPage* _tree_traverse_to_leaf(struct VdbPager* pager, FILE* f, const char* table, struct VdbPage* node, uint32_t pk) {
    struct NodeMeta* m = _tree_deserialize_meta(node->buf);
    assert(m->type == VDBN_INTERN);

    //TODO: switch to binary search
    int child_idx = -1;
    for (uint32_t i = OFFSETS_START; i < OFFSETS_START + m->cells_size; i += sizeof(uint32_t)) {
        struct NodeCell nc = _tree_deserialize_nodecell(&node->buf[i]);
        if (i == OFFSETS_START) {
            if (pk <= nc.key) {
                child_idx = nc.block_idx;
                break;
            }
        } else {
            struct NodeCell pnc = _tree_deserialize_nodecell(&node->buf[i - sizeof(uint32_t)]);
            if (pnc.key < pk && pk <= nc.key) {
                child_idx = nc.block_idx;
                break;
            }
        }
    }

    if (child_idx == -1) {
        child_idx = m->right_ptr.block_idx;
    }

    struct VdbPage* child = pager_get_page(pager, f, table, child_idx);
    struct NodeMeta* cm = _tree_deserialize_meta(child->buf);

    if (cm->type == VDBN_INTERN) {
        child = _tree_traverse_to_leaf(pager, f, table, child, pk);
    }

    _tree_free_meta(cm); 
    _tree_free_meta(m);
    return child;
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
                data_size += f->as.Str->len;
                break;
            case VDBF_BOOL:
                data_size += sizeof(bool);
                break;
        }
    }

    return data_size;
}

void tree_insert_record(struct VdbPager* pager, FILE* f, const char* table_name, struct VdbData* d) {
    uint32_t root_idx = 0;
    struct VdbPage* root = pager_get_page(pager, f, table_name, root_idx);
    struct NodeMeta* m = _tree_deserialize_meta(root->buf);

    uint32_t pk = m->right_ptr.key;

    struct VdbPage* leaf = _tree_traverse_to_leaf(pager, f, table_name, root, pk);
    struct NodeMeta* lm = _tree_deserialize_meta(leaf->buf);

    uint32_t data_size = _tree_data_size(d);
    uint32_t dc_meta_size = sizeof(uint32_t) * 3;
    struct DataCell dc = { 0, pk, data_size, d };
    _tree_serialize_datacell(leaf->buf + VDB_PAGE_SIZE - lm->cells_size - data_size - dc_meta_size, &dc);

    //write offset
    uint32_t data_off = VDB_PAGE_SIZE - lm->cells_size - data_size - dc_meta_size;
    memcpy(leaf->buf + OFFSETS_START + lm->offsets_size, &data_off, sizeof(uint32_t));

    lm->offsets_size += sizeof(uint32_t);
    lm->cells_size += data_size + dc_meta_size;
    _tree_serialize_meta(leaf->buf, lm);

    m->right_ptr.key++;
    _tree_serialize_meta(root->buf, m);

    _tree_free_meta(m);
    _tree_free_meta(lm);

    //TODO: flush pages here to test if function works
    //flushing should be a pager function
    fseek_w(f, 0, SEEK_SET);
    fwrite_w(root->buf, VDB_PAGE_SIZE, 1, f);
    fwrite_w(leaf->buf, VDB_PAGE_SIZE, 1, f);

    //    pager_flush(root);
    //    pager_flush(leaf);
}


struct VdbData* tree_fetch_record(struct VdbPager* pager, FILE* f, const char* table, uint32_t key) {
    uint32_t root_idx = 0;
    struct VdbPage* root = pager_get_page(pager, f, table, root_idx);
    struct VdbPage* leaf = _tree_traverse_to_leaf(pager, f, table, root, key);
    struct NodeMeta* lm = _tree_deserialize_meta(leaf->buf);

    struct DataCell* dc = NULL;
    for (uint32_t i = OFFSETS_START; i < OFFSETS_START + lm->offsets_size; i += sizeof(uint32_t)) {
        uint32_t offset = *((uint32_t*)&leaf->buf[i]);
        dc = _tree_deserialize_datacell(leaf->buf + offset, lm->schema);

        if (dc->key == key)
            break;

        _tree_free_datacell(dc);
        dc = NULL;
    }

    if (!dc)
        return NULL;

    struct VdbData* result = dc->data;
    free(dc);

    return result;
}

