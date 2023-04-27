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
void vdbnode_intern_write_new_ptr(uint8_t* buf, struct VdbPtr ptr);

//leaf
uint32_t vdbnode_leaf_read_data_block(uint8_t* buf);
uint32_t vdbnode_leaf_read_record_key(uint8_t* buf, uint32_t idx);
uint32_t vdbnode_leaf_read_record_count(uint8_t* buf);
uint32_t vdbnode_leaf_read_datacells_size(uint8_t* buf);
struct VdbRecord* vdbnode_leaf_read_fixedlen_record(uint8_t* buf, struct VdbSchema* schema, uint32_t rec_idx);
struct VdbDatum vdbnode_leaf_read_varlen_datum(uint8_t* buf, uint32_t off);

void vdbnode_leaf_write_type(uint8_t* buf);
void vdbnode_leaf_write_parent(uint8_t* buf, uint32_t parent_idx);
void vdbnode_leaf_write_data_block(uint8_t* buf, uint32_t data_idx);
void vdbnode_leaf_write_record_count(uint8_t* buf, uint32_t count);
void vdbnode_leaf_write_fixedlen_data(uint8_t* buf, struct VdbRecord* rec);
void vdbnode_leaf_write_datacells_size(uint8_t* buf, uint32_t size);
bool vdbnode_leaf_can_fit(uint8_t* buf, uint32_t size);

//data
uint32_t vdbnode_data_read_next(uint8_t* buf);
uint32_t vdbnode_data_read_free_size(uint8_t* buf);
struct VdbDatum vdbnode_data_read_datum(uint8_t* buf, uint32_t off);

void vdbnode_data_write_type(uint8_t* buf);
void vdbnode_data_write_parent(uint8_t* buf, uint32_t parent_idx);
void vdbnode_data_write_next(uint8_t* buf, uint32_t next_idx);
void vdbnode_data_write_free_size(uint8_t* buf, uint32_t free_size);
uint32_t vdbnode_data_write_datum(uint8_t* buf, struct VdbDatum* datum, uint32_t* len_written); //returns offset of written datum
void vdbnode_data_write_datum_overflow(uint8_t* buf, uint32_t datum_off, uint32_t block_idx, uint32_t offset_idx);

//other
enum VdbNodeType vdbnode_type(uint8_t* buf);
uint32_t vdbnode_read_parent(uint8_t* buf);

#endif //VDB_NODE_H
