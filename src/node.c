#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#include "node.h"
#include "util.h"
#include "vdb.h"

struct VdbNodePtrList* _pl_alloc() {
    struct VdbNodePtrList* list = malloc_w(sizeof(struct VdbNodePtrList));
    list->count = 0;
    list->capacity = 8;
    list->pointers = malloc_w(sizeof(struct VdbNodePtr) * list->capacity);

    return list;
}

void _pl_append(struct VdbNodePtrList* list, struct VdbNodePtr ptr) {
    if (list->capacity == list->count) {
        list->capacity *= 2;
        list->pointers = realloc_w(list->pointers, sizeof(struct VdbNodePtr) * list->capacity);
    }

    list->pointers[list->count++] = ptr;
}

void _pl_free(struct VdbNodePtrList* list) {
    free(list->pointers);
    free(list);
}

struct VdbRecordList* _rl_alloc() {
    struct VdbRecordList* list = malloc_w(sizeof(struct VdbRecordList));
    list->count = 0;
    list->capacity = 8;
    list->records = malloc_w(sizeof(struct VdbRecord) * list->capacity);

    return list;
}

void _rl_append(struct VdbRecordList* list, struct VdbRecord rec) {
    if (list->capacity == list->count) {
        list->capacity *= 2;
        list->records = realloc_w(list->records, sizeof(struct VdbRecord) * list->capacity);
    }

    list->records[list->count++] = rec;
}

void _rl_free(struct VdbRecordList* list) {
    free(list->records);
    free(list);
}

struct VdbNodePtr _tree_deserialize_nodeptr(uint8_t* buf) {
    struct VdbNodePtr ptr;
    int off = 0;
    read_u32(&ptr.key, buf, &off);
    read_u32(&ptr.idx, buf, &off);

    return ptr;
}


struct VdbNode node_deserialize_meta(uint8_t* buf) {
    struct VdbNode node;
    int off = 0;
    read_u32(&node.type, buf, &off);
    read_u32(&node.idx, buf, &off);
    read_u32(&node.as.meta.pk_counter, buf, &off);

    node.as.meta.schema = malloc_w(sizeof(struct VdbSchema));
    read_u32(&node.as.meta.schema->count, buf, &off);
    node.as.meta.schema->fields = malloc_w(sizeof(enum VdbField) * node.as.meta.schema->count);
    for (uint32_t i = 0; i < node.as.meta.schema->count; i++) {
        uint32_t field;
        read_u32(&field, buf, &off);
        node.as.meta.schema->fields[i] = field;
    }

    return node;
}

void _node_serialize_meta(uint8_t* buf, struct VdbNode* node) {
    int off = 0;
    write_u32(buf, (uint32_t)node->type, &off);
    write_u32(buf, (uint32_t)node->idx, &off);
    write_u32(buf, (uint32_t)node->as.meta.pk_counter, &off);
    write_u32(buf, (uint32_t)node->as.meta.schema->count, &off);
    for (uint32_t i = 0; i < node->as.meta.schema->count; i++) {
        uint32_t field = node->as.meta.schema->fields[i];
        write_u32(buf, field, &off);
    }
}

struct VdbNode node_deserialize_intern(uint8_t* buf) {
    struct VdbNode node;
    int off = 0;
    read_u32(&node.type, buf, &off);
    read_u32(&node.idx, buf, &off);
    uint32_t ptr_count;
    read_u32(&ptr_count, buf, &off);
    node.as.intern.pl = _pl_alloc();

    int idx_off = VDB_OFFSETS_START;
    for (uint32_t i = 0; i < ptr_count; i++) {
        uint32_t idx;
        read_u32(&idx, buf, &idx_off);
        _pl_append(node.as.intern.pl, _tree_deserialize_nodeptr(buf + idx));
    }

    return node;
}

void _node_serialize_intern(uint8_t* buf, struct VdbNode* node) {
    int off = 0;
    write_u32(buf, (uint32_t)node->type, &off);
    write_u32(buf, (uint32_t)node->idx, &off);
    write_u32(buf, (uint32_t)node->as.intern.pl->count, &off);

    uint32_t cell_off = VDB_PAGE_SIZE;
    int idx_off = VDB_OFFSETS_START;

    for (uint32_t i = 0; i < node->as.intern.pl->count; i++) {
        struct VdbNodePtr* ptr = &node->as.intern.pl->pointers[i];
        cell_off -= sizeof(uint32_t) * 2;
        write_u32(buf, cell_off, &idx_off);
        memcpy(buf + cell_off, &ptr->key, sizeof(uint32_t));
        memcpy(buf + cell_off + sizeof(uint32_t), &ptr->idx, sizeof(uint32_t));
    }
}

struct VdbRecord _node_deserialize_record(uint8_t* buf, uint8_t* data_buf, struct VdbSchema* schema) {
    int buf_off = 0;

    struct VdbRecord r;
    r.count = schema->count;
    r.data = malloc_w(sizeof(struct VdbDatum) * r.count);
    read_u32(&r.key, buf, &buf_off);

    for (uint32_t i = 0; i < r.count; i++) {
        enum VdbField f = schema->fields[i];
        switch (f) {
            case VDBF_INT:
                r.data[i].type = VDBF_INT;
                r.data[i].as.Int = *((uint64_t*)(buf + buf_off));
                buf_off += sizeof(uint64_t);
                break;
            case VDBF_STR: {
                uint32_t ustring_off;
                read_u32(&ustring_off, buf, &buf_off);
                int string_off = ustring_off;

                r.data[i].type = VDBF_STR;
                r.data[i].as.Str = malloc_w(sizeof(struct VdbString));
                uint32_t next;
                read_u32(&next, data_buf, &string_off);
                read_u32(&r.data[i].as.Str->len, data_buf, &string_off);
                r.data[i].as.Str->start = malloc_w(sizeof(char) * r.data[i].as.Str->len);
                memcpy(r.data[i].as.Str->start, data_buf + string_off, r.data[i].as.Str->len);
                break;
            }
            case VDBF_BOOL:
                r.data[i].type = VDBF_BOOL;
                r.data[i].as.Bool = *((bool*)(buf + buf_off));
                buf_off += sizeof(bool);
                break;
        }
    }

    return r;
}

struct VdbNode node_deserialize_leaf(uint8_t* buf, uint8_t* data_buf, struct VdbSchema* schema) {
    struct VdbNode node;
    int off = 0;
    read_u32(&node.type, buf, &off);
    read_u32(&node.idx, buf, &off);
    read_u32(&node.as.leaf.data_idx, buf, &off);

    uint32_t rec_count;
    read_u32(&rec_count, buf, &off);

    node.as.leaf.rl = _rl_alloc();
    int idx_off = VDB_OFFSETS_START;
   
    for (uint32_t i = 0; i < rec_count; i++) {
        uint32_t rec_off;
        read_u32(&rec_off, buf, &idx_off);

        struct VdbRecord r = _node_deserialize_record(buf + rec_off, data_buf, schema);
        _rl_append(node.as.leaf.rl, r);
    }

    return node;
}

struct U32List* _node_serialize_var_len_data(uint8_t* data_buf, struct VdbNode* node) {
    struct U32List* string_off_list = u32l_alloc();
    if (data_buf)  {
        int data_off = 0;
        write_u32(data_buf, (uint32_t)VDBN_DATA, &data_off);
        write_u32(data_buf, (uint32_t)node->as.leaf.data_idx, &data_off);

        int strings_off = VDB_OFFSETS_START;

        for (uint32_t i = 0; i < node->as.leaf.rl->count; i++) {
            struct VdbRecord* r = &node->as.leaf.rl->records[i];
            for (uint32_t j = 0 ; j < r->count; j++) {
                struct VdbDatum* d = &r->data[j];
                if (d->type == VDBF_STR) {
                    u32l_append(string_off_list, strings_off);
                    uint32_t next = 0;
                    write_u32(data_buf, next, &strings_off);
                    write_u32(data_buf, d->as.Str->len, &strings_off);
                    memcpy(data_buf + strings_off, d->as.Str->start, d->as.Str->len);
                    strings_off += d->as.Str->len;
                }
            }
        } 
    }

    return string_off_list;
}

void _node_serialize_fixed_len_record(uint8_t* buf, struct VdbRecord* rec, struct U32List* string_offsets, int* off_idx) {
    int off = 0;
    write_u32(buf, rec->key, &off);
    for (uint32_t j = 0; j < rec->count; j++) {
        struct VdbDatum* d = &rec->data[j];
        switch (d->type) {
            case VDBF_INT: {
                memcpy(buf + off, &d->as.Int, sizeof(uint64_t));
                off += sizeof(uint64_t);
                break;
            }
            case VDBF_STR: {
                write_u32(buf, string_offsets->values[*off_idx], &off);
                *off_idx += 1;
                break;
            }
            case VDBF_BOOL: {
                bool b = d->as.Bool;
                memcpy(buf + off, &b, sizeof(bool));
                off += sizeof(bool);
                break;
            }
        }
    }
}

void _node_serialize_leaf(uint8_t* buf, uint8_t* data_buf, struct VdbNode* node) {
    struct U32List* string_off_list = _node_serialize_var_len_data(data_buf, node);

    int off = 0;
    write_u32(buf, (uint32_t)node->type, &off);
    write_u32(buf, (uint32_t)node->idx, &off);
    write_u32(buf, (uint32_t)node->as.leaf.data_idx, &off);
    write_u32(buf, (uint32_t)node->as.leaf.rl->count, &off);

    off = VDB_OFFSETS_START;
    uint32_t rec_off = VDB_PAGE_SIZE;
    int k = 0;
    for (uint32_t i = 0; i < node->as.leaf.rl->count; i++) {
        struct VdbRecord* rec = &node->as.leaf.rl->records[i];
        rec_off -= vdb_fixed_rec_size(rec);
        write_u32(buf, rec_off, &off);

        _node_serialize_fixed_len_record(buf + rec_off, rec, string_off_list, &k);
    }

    u32l_free(string_off_list);
}


void node_serialize(uint8_t* buf, uint8_t* data_buf, struct VdbNode* node) {
    switch (node->type) {
        case VDBN_META:
            _node_serialize_meta(buf, node);
            break;
        case VDBN_INTERN:
            _node_serialize_intern(buf, node);
            break;
        case VDBN_LEAF:
            _node_serialize_leaf(buf, data_buf, node);
            break;
        case VDBN_DATA:
            printf("should not ever manually serialize data block\n");
            break;
    }
}

struct VdbNode node_init_intern(uint32_t idx) {
    struct VdbNode intern;
    intern.type = VDBN_INTERN;
    intern.idx = idx;
    intern.as.intern.pl = _pl_alloc();
    return intern;
}

void node_append_nodeptr(struct VdbNode* node, struct VdbNodePtr ptr) {
    _pl_append(node->as.intern.pl, ptr);
}

void node_clear_nodeptrs(struct VdbNode* node) {
    node->as.intern.pl->count = 0;
}

struct VdbNode node_init_leaf(uint32_t idx) {
    struct VdbNode leaf;
    leaf.type = VDBN_LEAF;
    leaf.idx = idx;
    leaf.as.leaf.data_idx = 0;
    leaf.as.leaf.rl = _rl_alloc();
    return leaf;
}

void node_append_record(struct VdbNode* node, struct VdbRecord rec) {
    _rl_append(node->as.leaf.rl, rec);
}

struct VdbNode node_init_data(uint32_t idx) {
    struct VdbNode data;
    data.type = VDBN_DATA;
    data.idx = idx;
    data.as.data.next = 0;
    return data;
}

void node_free(struct VdbNode* node) {
    switch (node->type) {
        case VDBN_META:
            free(node->as.meta.schema->fields);
            free(node->as.meta.schema);
            break;
        case VDBN_INTERN:
            _pl_free(node->as.intern.pl);
            break;
        case VDBN_LEAF:
            _rl_free(node->as.leaf.rl);
            break;
        case VDBN_DATA:
            break;
    }
}

bool _node_intern_is_full(struct VdbNode* node, uint32_t insert_size) {
    int empty = VDB_PAGE_SIZE - VDB_OFFSETS_START - insert_size;

    for (uint32_t i = 0; i < node->as.intern.pl->count; i++) {
        empty -= node_nodeptr_size();
    }

    return empty < 0;
}

bool _node_leaf_is_full(struct VdbNode* node, uint32_t insert_size) {
    int empty = VDB_PAGE_SIZE - VDB_OFFSETS_START - insert_size;

    for (uint32_t i = 0; i < node->as.leaf.rl->count; i++) {
        empty -= sizeof(uint32_t);
        struct VdbRecord* rec = &node->as.leaf.rl->records[i];
        empty -= vdb_fixed_rec_size(rec);
    }

    return empty < 0;
}

bool node_is_full(struct VdbNode* node, uint32_t insert_size) {
    switch (node->type) {
        case VDBN_INTERN:
            return _node_intern_is_full(node, insert_size);
        case VDBN_LEAF:
            return _node_leaf_is_full(node, insert_size);
        default:
            return false; 
    }
}

uint32_t node_nodeptr_size() {
    return sizeof(uint32_t) * 3;
}

bool node_is_root(struct VdbNode* node) {
    return node->idx == 1;
}

uint32_t node_read_data_idx(uint8_t* buf) {
    uint32_t data_idx;
    int off = sizeof(uint32_t) * 2;
    read_u32(&data_idx, buf, &off);
    return data_idx;
}
