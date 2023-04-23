#include <stddef.h>
#include <stdlib.h>
#include <assert.h>

#include "node.h"
#include "util.h"
#include "record.h"

struct VdbNode* vdb_node_init(enum VdbNodeType type, uint32_t parent_idx) {
    struct VdbNode* node = malloc_w(sizeof(struct VdbNode));
    node->parent_idx = parent_idx;
    node->dirty = true;

    switch (type) {
        case VDBN_META:
            node->type = VDBN_META;
            node->as.meta.pk_counter = 0;
            node->as.meta.root_idx = 1;
            break;
        case VDBN_INTERN:
            node->type = VDBN_INTERN;
            node->as.intern.right_idx = 0;
            node->as.intern.pointers = vdb_ptrlist_alloc();
            break;
        case VDBN_LEAF:
            node->type = VDBN_LEAF;
            node->as.leaf.data_idx = 0;
            node->as.leaf.records = vdb_recordlist_alloc();
            break;
        case VDBN_DATA:
            node->type = VDBN_DATA;
            node->as.data.next_idx = 0;
            break;
    }

    return node;
}

void vdb_node_free(struct VdbNode* node) {
    switch (node->type) {
        case VDBN_META:
            vdb_schema_free(node->as.meta.schema);
            break;
        case VDBN_INTERN:
            vdb_ptrlist_free(node->as.intern.pointers);
            break;
        case VDBN_LEAF:
            vdb_recordlist_free(node->as.leaf.records);
            break;
        case VDBN_DATA:
            break;
    }
    free(node);
}

void vdb_node_serialize(uint8_t* buf, struct VdbNode* node) {
    int off = 0;
    write_u32(buf, node->type, &off);
    write_u32(buf, node->parent_idx, &off);

    switch (node->type) {
        case VDBN_META:
            write_u32(buf, node->as.meta.pk_counter, &off);
            write_u32(buf, node->as.meta.root_idx, &off);
            vdb_schema_serialize(buf, node->as.meta.schema, &off);
            break;
        case VDBN_INTERN:
            write_u32(buf, node->as.intern.right_idx, &off);
            break;
        case VDBN_LEAF:
            write_u32(buf, node->as.leaf.data_idx, &off);
            break;
        case VDBN_DATA:
            write_u32(buf, node->as.data.next_idx, &off);
            break;
    }
}

struct VdbNode* vdb_node_deserialize(uint8_t* buf) {
    struct VdbNode* node = malloc_w(sizeof(struct VdbNode));

    int off = 0;
    uint32_t type;
    read_u32(&type, buf, &off);
    node->type = type;
    read_u32(&node->parent_idx, buf, &off);

    switch (node->type) {
        case VDBN_META:
            read_u32(&node->as.meta.pk_counter, buf, &off);
            read_u32(&node->as.meta.root_idx, buf, &off);
            node->as.meta.schema = vdb_schema_deserialize(buf, &off);
            break;
        case VDBN_INTERN:
            read_u32(&node->as.intern.right_idx, buf, &off);
            node->as.intern.pointers = vdb_ptrlist_alloc();
            break;
        case VDBN_LEAF:
            read_u32(&node->as.leaf.data_idx, buf, &off);
            node->as.leaf.records = vdb_recordlist_alloc();
            break;
        case VDBN_DATA:
            read_u32(&node->as.data.next_idx, buf, &off);
            break;
    }

    return node;
}

bool vdb_node_leaf_full(struct VdbNode* node, struct VdbRecord* rec) {
    /*
    assert(node->type == VDBN_LEAF);
    rec = rec; //to silence warning for now
    //TODO: should check if record will fit into leaf
    return node->as.leaf.records->count >= 5;*/
    return true;
}

bool vdb_node_intern_full(struct VdbNode* node) {
    /*
    assert(node->type == VDBN_INTERN);
    //TODO: check if free page size is not large enough in real version
    return node->as.intern.nodes->count >= 1; //1 nodes + 1 right pointer for a total of three nodes for now*/
    return true;
}

struct VdbPtrList* vdb_ptrlist_alloc() {
    struct VdbPtrList* pl = malloc_w(sizeof(struct VdbPtrList));
    pl->count = 0;
    pl->capacity = 8; 
    pl->pointers = malloc_w(sizeof(struct VdbPtr) * pl->capacity);
    return pl;
}

void vdb_ptrlist_append_ptr(struct VdbPtrList* pl, struct VdbPtr ptr) {
    if (pl->count == pl->capacity) {
        pl->capacity *= 2;
        pl->pointers = realloc_w(pl->pointers, sizeof(struct VdbPtr) * pl->capacity);
    }

    pl->pointers[pl->count++] = ptr;
}

void vdb_ptrlist_free(struct VdbPtrList* pl) {
    free(pl->pointers);
    free(pl);
}

