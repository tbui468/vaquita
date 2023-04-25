#include <stddef.h>
#include <stdlib.h>
#include <assert.h>

#include "node.h"
#include "util.h"
#include "record.h"
#include "pager.h"

//meta node
//[type|parent_idx|pk_counter|root_idx|schema count|schema...]
//internal node
//[type|parent_idx|right_ptr idx|right_ptr key|ptr_count]
//leaf node
//[type|parent_idx|data_idx|rec_count|data_size| ... |records....
//record
//[next|size|data....]



uint32_t vdbnode_leaf_read_data_size(uint8_t* buf) {
    return *((uint32_t*)(buf + sizeof(uint32_t) * 4));
}

void vdbnode_leaf_write_data_size(uint8_t* buf, uint32_t size) {
    *((uint32_t*)(buf + sizeof(uint32_t) * 4)) = size;
}

uint32_t vdbnode_leaf_read_record_key(uint8_t* buf, uint32_t idx) {
    int off = VDB_PAGE_HDR_SIZE + sizeof(uint32_t) * 2 * idx;

    uint32_t data_off;
    read_u32(&data_off, buf, &off);

    return *((uint32_t*)(buf + data_off + sizeof(uint32_t) * 2));
}

void vdbnode_leaf_write_record(uint8_t* buf, struct VdbRecord* rec) {
    uint32_t rec_count = vdbnode_leaf_read_record_count(buf);

    uint32_t rec_size = vdbrecord_size(rec);
    uint32_t prev_data_size = vdbnode_leaf_read_data_size(buf);
    int data_off = VDB_PAGE_SIZE - prev_data_size - rec_size - sizeof(uint32_t) * 2;

    int off = VDB_PAGE_HDR_SIZE + rec_count * sizeof(uint32_t);
    write_u32(buf, data_off, &off);

    write_u32(buf, 0, &data_off);
    write_u32(buf, rec_size, &data_off);

    vdbrecord_write(buf + data_off, rec);

    uint32_t new_data_size = prev_data_size + rec_size + sizeof(uint32_t) * 2;
    vdbnode_leaf_write_data_size(buf, new_data_size);
}

struct VdbPtr vdbnode_intern_read_right_ptr(uint8_t* buf) {
    struct VdbPtr ptr;
    ptr.idx = *((uint32_t*)(buf + sizeof(uint32_t) * 2));
    ptr.key = *((uint32_t*)(buf + sizeof(uint32_t) * 3));
    return ptr;
}

uint32_t vdbnode_intern_read_ptr_count(uint8_t* buf) {
    return *((uint32_t*)(buf + sizeof(uint32_t) * 4));
}

uint32_t vdbnode_leaf_read_record_count(uint8_t* buf) {
    return *((uint32_t*)(buf + sizeof(uint32_t) * 3));
}

struct VdbPtr vdbnode_intern_read_ptr(uint8_t* buf, uint32_t idx) {
    struct VdbPtr ptr;

    int off = VDB_PAGE_HDR_SIZE + idx * 2 * sizeof(uint32_t);

    read_u32(&ptr.idx, buf, &off);
    read_u32(&ptr.key, buf, &off);

    return ptr;
}

enum VdbNodeType vdbnode_type(uint8_t* buf) {
    return *((uint32_t*)(buf)); 
}

uint32_t vdb_node_meta_read_root(uint8_t* buf) {
    return *((uint32_t*)(buf + sizeof(uint32_t) * 3));
}

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

