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

//meta data node
uint32_t vdbnode_meta_read_primary_key_counter(uint8_t* buf);
uint32_t vdb_node_meta_read_root(uint8_t* buf);
struct VdbSchema* vdbnode_meta_read_schema(uint8_t* buf);

void vdbnode_meta_write_primary_key_counter(uint8_t* buf, uint32_t pk_counter);
void vdbnode_meta_write_root(uint8_t* buf, uint32_t root_idx);
void vdbnode_meta_write_schema(uint8_t* buf, struct VdbSchema* schema);

//internal node
struct VdbPtr vdbnode_intern_read_right_ptr(uint8_t* buf);
uint32_t vdbnode_intern_read_ptr_count(uint8_t* buf);
struct VdbPtr vdbnode_intern_read_ptr(uint8_t* buf, uint32_t idx);

void vdbnode_intern_write_right_ptr(uint8_t* buf, struct VdbPtr ptr);
void vdbnode_intern_write_ptr_count(uint8_t* buf, uint32_t count);
void vdbnode_intern_write_datacells_size(uint8_t* buf, uint32_t size);
void vdbnode_intern_write_new_ptr(uint8_t* buf, struct VdbPtr ptr);
bool vdbnode_intern_can_fit_ptr(uint8_t* buf);

//leaf node
uint32_t vdbnode_leaf_read_data_block(uint8_t* buf);
uint32_t vdbnode_leaf_read_record_key(uint8_t* buf, uint32_t idx);
uint32_t vdbnode_leaf_read_record_count(uint8_t* buf);
uint32_t vdbnode_leaf_read_datacells_size(uint8_t* buf);
struct VdbRecord* vdbnode_leaf_read_fixedlen_record(uint8_t* buf, struct VdbSchema* schema, uint32_t rec_idx);
struct VdbDatum vdbnode_leaf_read_varlen_datum(uint8_t* buf, uint32_t off);

void vdbnode_leaf_write_data_block(uint8_t* buf, uint32_t data_idx);
void vdbnode_leaf_write_record_count(uint8_t* buf, uint32_t count);
void vdbnode_leaf_write_record(uint8_t* buf, uint32_t rec_idx, struct VdbRecord* rec);
void vdbnode_leaf_append_record(uint8_t* buf, struct VdbRecord* rec);
void vdbnode_leaf_write_datacells_size(uint8_t* buf, uint32_t size);

//Data Node
uint32_t vdbdata_read_next(uint8_t* buf);
uint32_t vdbdata_read_idx_count(uint8_t* buf);
uint32_t vdbdata_read_datacells_size(uint8_t* buf);
uint32_t vdbdata_read_idx_freelist_head(uint8_t* buf);
uint32_t vdbdata_read_datacells_freelist_head(uint8_t* buf);
void vdbdata_write_next(uint8_t* buf, uint32_t next_idx);
void vdbdata_write_idx_count(uint8_t* buf, uint32_t count);
void vdbdata_write_datacells_size(uint8_t* buf, uint32_t free_size);
void vdbdata_write_idx_freelist_head(uint8_t* buf, uint32_t freelist_head_off);
void vdbdata_write_datacells_freelist_head(uint8_t* buf, uint32_t freelist_head_off);

//Data Node Datacell
uint32_t vdbdata_read_datacell_next(uint8_t* buf, uint32_t off);
void vdbdata_read_datacell_overflow(uint8_t* buf, uint32_t off, uint32_t* block_idx, uint32_t* datum_idx);
uint32_t vdbdata_read_datacell_size(uint8_t* buf, uint32_t off);
struct VdbDatum vdbdata_read_datacell_datum(uint8_t* buf, uint32_t off);

void vdbdata_write_datacell_next(uint8_t* buf, uint32_t off, uint32_t next);
void vdbdata_write_datacell_overflow(uint8_t* buf, uint32_t datum_off, uint32_t block_idx, uint32_t datum_idx);
uint32_t vdbdata_write_datacell_size(uint8_t* buf, uint32_t size);
uint32_t vdbdata_write_datacell_datum(uint8_t* buf, struct VdbDatum* datum, uint32_t* len_written); //returns offset of written datum
uint32_t vdbdata_datacell_header_size(void);

//other
enum VdbNodeType vdbnode_read_type(uint8_t* buf);
void vdbnode_write_type(uint8_t* buf, enum VdbNodeType type);
uint32_t vdbnode_read_parent(uint8_t* buf);
void vdbnode_write_parent(uint8_t* buf, uint32_t parent);

#endif //VDB_NODE_H
