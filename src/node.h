#ifndef VDB_NODE_H
#define VDB_NODE_H

#include <stdint.h>
#include <stdbool.h>

#include "record.h"

enum VdbNodeType {
    VDBN_META,
    VDBN_INTERN,
    VDBN_LEAF,
    VDBN_DATA
};

struct VdbPtr {
    uint32_t idx;
    uint32_t key;
};

//meta buffer de/serialization
uint32_t vdbnode_meta_read_primary_key_counter(uint8_t* buf);
uint32_t vdb_node_meta_read_root(uint8_t* buf);
struct VdbSchema* vdbnode_meta_read_schema(uint8_t* buf);

void vdbnode_meta_write_type(uint8_t* buf);
void vdbnode_meta_write_parent(uint8_t* buf, uint32_t parent_idx);
void vdbnode_meta_write_primary_key_counter(uint8_t* buf, uint32_t pk_counter);
void vdbnode_meta_write_root(uint8_t* buf, uint32_t root_idx);
void vdbnode_meta_write_schema(uint8_t* buf, struct VdbSchema* schema);

//intern
struct VdbPtr vdbnode_intern_read_right_ptr(uint8_t* buf);
uint32_t vdbnode_intern_read_ptr_count(uint8_t* buf);
struct VdbPtr vdbnode_intern_read_ptr(uint8_t* buf, uint32_t idx);

void vdbnode_intern_write_type(uint8_t* buf);
void vdbnode_intern_write_parent(uint8_t* buf, uint32_t parent_idx);
void vdbnode_intern_write_right_ptr(uint8_t* buf, struct VdbPtr ptr);
void vdbnode_intern_write_ptr_count(uint8_t* buf, uint32_t count);
void vdbnode_intern_write_datacells_size(uint8_t* buf, uint32_t size);

//leaf
uint32_t vdbnode_leaf_read_record_key(uint8_t* buf, uint32_t idx);
uint32_t vdbnode_leaf_read_record_count(uint8_t* buf);
uint32_t vdbnode_leaf_read_datacells_size(uint8_t* buf);

void vdbnode_leaf_write_type(uint8_t* buf);
void vdbnode_leaf_write_parent(uint8_t* buf, uint32_t parent_idx);
void vdbnode_leaf_write_data_block_idx(uint8_t* buf, uint32_t data_idx);
void vdbnode_leaf_write_record_count(uint8_t* buf, uint32_t count);
void vdbnode_leaf_write_record(uint8_t* buf, struct VdbRecord* rec);
void vdbnode_leaf_write_datacells_size(uint8_t* buf, uint32_t size);

//other
enum VdbNodeType vdbnode_type(uint8_t* buf);

#endif //VDB_NODE_H
