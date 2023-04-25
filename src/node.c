#include <stddef.h>
#include <stdlib.h>
#include <assert.h>

#include "node.h"
#include "util.h"
#include "record.h"
#include "pager.h"


/*
 * Meta node de/serialization
 * [type|parent_idx|pk_counter|root_idx|schema count|schema...]
 */

uint32_t vdbnode_meta_read_primary_key_counter(uint8_t* buf) {
    return *((uint32_t*)(buf + sizeof(uint32_t) * 2));
}

uint32_t vdb_node_meta_read_root(uint8_t* buf) {
    return *((uint32_t*)(buf + sizeof(uint32_t) * 3));
}

struct VdbSchema* vdbnode_meta_read_schema(uint8_t* buf) {
    int off = 0;
    return vdb_schema_deserialize(buf + sizeof(uint32_t) * 4, &off);
}

void vdbnode_meta_write_type(uint8_t* buf) {
    *((uint32_t*)(buf)) = (uint32_t)VDBN_META;
}

void vdbnode_meta_write_parent(uint8_t* buf, uint32_t parent_idx) {
    *((uint32_t*)(buf + sizeof(uint32_t))) = parent_idx;
}

void vdbnode_meta_write_primary_key_counter(uint8_t* buf, uint32_t pk_counter) {
    *((uint32_t*)(buf + 2 * sizeof(uint32_t))) = pk_counter;
}

void vdbnode_meta_write_root(uint8_t* buf, uint32_t root_idx) {
    *((uint32_t*)(buf + 3 * sizeof(uint32_t))) = root_idx;
}

void vdbnode_meta_write_schema(uint8_t* buf, struct VdbSchema* schema) {
    int off = 0;
    vdb_schema_serialize(buf + sizeof(uint32_t) * 4, schema, &off);
}


/*
 * Internal node de/serialization
 * [type|parent_idx|right ptr idx|right ptr key|ptr count|datacells size| ... |index cells...datacells]
 */

struct VdbPtr vdbnode_intern_read_right_ptr(uint8_t* buf) {
    struct VdbPtr ptr;
    ptr.idx = *((uint32_t*)(buf + sizeof(uint32_t) * 2));
    ptr.key = *((uint32_t*)(buf + sizeof(uint32_t) * 3));
    return ptr;
}

uint32_t vdbnode_intern_read_ptr_count(uint8_t* buf) {
    return *((uint32_t*)(buf + sizeof(uint32_t) * 4));
}

struct VdbPtr vdbnode_intern_read_ptr(uint8_t* buf, uint32_t idx) {
    struct VdbPtr ptr;

    int off = VDB_PAGE_HDR_SIZE + idx * 2 * sizeof(uint32_t);

    read_u32(&ptr.idx, buf, &off);
    read_u32(&ptr.key, buf, &off);

    return ptr;
}

void vdbnode_intern_write_type(uint8_t* buf) {
    *((uint32_t*)(buf)) = (uint32_t)VDBN_INTERN;
}

void vdbnode_intern_write_parent(uint8_t* buf, uint32_t parent_idx) {
    *((uint32_t*)(buf + sizeof(uint32_t))) = parent_idx;
}

void vdbnode_intern_write_right_ptr(uint8_t* buf, struct VdbPtr ptr) {
    *((uint32_t*)(buf + sizeof(uint32_t) * 2)) = ptr.idx;
    *((uint32_t*)(buf + sizeof(uint32_t) * 3)) = ptr.key;
}

void vdbnode_intern_write_ptr_count(uint8_t* buf, uint32_t count) {
    *((uint32_t*)(buf + sizeof(uint32_t) * 4)) = count;
}

void vdbnode_intern_write_datacells_size(uint8_t* buf, uint32_t size) {
    *((uint32_t*)(buf + sizeof(uint32_t) * 5)) = size;
}

/* 
 * Leaf node de/serialization
 * [type|parent_idx|data block idx|record count|datacells size| ... |index cells ... datacells]
 */

uint32_t vdbnode_leaf_read_datacells_size(uint8_t* buf) {
    return *((uint32_t*)(buf + sizeof(uint32_t) * 4));
}

uint32_t vdbnode_leaf_read_record_key(uint8_t* buf, uint32_t idx) {
    int off = VDB_PAGE_HDR_SIZE + sizeof(uint32_t) * idx;

    uint32_t data_off;
    read_u32(&data_off, buf, &off);

    return *((uint32_t*)(buf + data_off + sizeof(uint32_t) * 2));
}

uint32_t vdbnode_leaf_read_record_count(uint8_t* buf) {
    return *((uint32_t*)(buf + sizeof(uint32_t) * 3));
}

void vdbnode_leaf_write_type(uint8_t* buf) {
    *((uint32_t*)(buf)) = (uint32_t)VDBN_LEAF;
}

void vdbnode_leaf_write_parent(uint8_t* buf, uint32_t parent_idx) {
    *((uint32_t*)(buf + sizeof(uint32_t) * 1)) = parent_idx;
}

void vdbnode_leaf_write_data_block_idx(uint8_t* buf, uint32_t data_idx) {
    *((uint32_t*)(buf + sizeof(uint32_t) * 2)) = data_idx;
}

void vdbnode_leaf_write_record_count(uint8_t* buf, uint32_t count) {
    *((uint32_t*)(buf + sizeof(uint32_t) * 3)) = count;
}

void vdbnode_leaf_write_datacells_size(uint8_t* buf, uint32_t size) {
    *((uint32_t*)(buf + sizeof(uint32_t) * 4)) = size;
}

void vdbnode_leaf_write_record(uint8_t* buf, struct VdbRecord* rec) {
    uint32_t rec_count = vdbnode_leaf_read_record_count(buf);

    uint32_t rec_size = vdbrecord_size(rec);
    uint32_t prev_data_size = vdbnode_leaf_read_datacells_size(buf);
    int data_off = VDB_PAGE_SIZE - prev_data_size - rec_size - sizeof(uint32_t) * 2;

    int off = VDB_PAGE_HDR_SIZE + rec_count * sizeof(uint32_t);
    write_u32(buf, data_off, &off);

    write_u32(buf, 0, &data_off);
    write_u32(buf, rec_size, &data_off);

    vdbrecord_write(buf + data_off, rec);

    uint32_t new_data_size = prev_data_size + rec_size + sizeof(uint32_t) * 2;
    vdbnode_leaf_write_datacells_size(buf, new_data_size);

    rec_count++;
    vdbnode_leaf_write_record_count(buf, rec_count);
}

/*
 * Other functions
 */
enum VdbNodeType vdbnode_type(uint8_t* buf) {
    return *((uint32_t*)(buf)); 
}


