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

struct VdbNode node_deserialize_leaf(uint8_t* buf, struct VdbSchema* schema) {
    struct VdbNode node;
    int off = 0;
    read_u32(&node.type, buf, &off);
    read_u32(&node.idx, buf, &off);

    uint32_t rec_count;
    read_u32(&rec_count, buf, &off);

    node.as.leaf.rl = _rl_alloc();
    int idx_off = VDB_OFFSETS_START;
   
    for (uint32_t i = 0; i < rec_count; i++) {
        uint32_t rec_off;
        read_u32(&rec_off, buf, &idx_off);
        _rl_append(node.as.leaf.rl, vdb_deserialize_record(buf + rec_off, schema));
    }

    return node;
}

void _node_serialize_leaf(uint8_t* buf, struct VdbNode* node) {
    int off = 0;
    write_u32(buf, (uint32_t)node->type, &off);
    write_u32(buf, (uint32_t)node->idx, &off);
    write_u32(buf, (uint32_t)node->as.leaf.rl->count, &off);

    off = VDB_OFFSETS_START;
    uint32_t rec_off = VDB_PAGE_SIZE;

    for (uint32_t i = 0; i < node->as.leaf.rl->count; i++) {
        struct VdbRecord* rec = &node->as.leaf.rl->records[i];
        rec_off -= vdb_get_rec_size(rec);
        write_u32(buf, rec_off, &off);
        vdb_serialize_record(buf + rec_off, rec);
    }
}


void node_serialize(uint8_t* buf, struct VdbNode* node) {
    switch (node->type) {
        case VDBN_META:
            _node_serialize_meta(buf, node);
            break;
        case VDBN_INTERN:
            _node_serialize_intern(buf, node);
            break;
        case VDBN_LEAF:
            _node_serialize_leaf(buf, node);
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
    leaf.as.leaf.rl = _rl_alloc();
    return leaf;
}

void node_append_record(struct VdbNode* node, struct VdbRecord rec) {
    _rl_append(node->as.leaf.rl, rec);
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
        empty -= vdb_get_rec_size(rec);
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
