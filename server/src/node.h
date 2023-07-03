#ifndef VDB_NODE_H
#define VDB_NODE_H

#include <stdint.h>
#include <stdbool.h>
#include "value.h"

enum VdbNodeType {
    VDBN_META,
    VDBN_INTERN,
    VDBN_LEAF,
    VDBN_DATA
};

struct VdbPtr {
    uint32_t block_idx;
    struct VdbValue key;
};

struct VdbRecordPtr {
    uint32_t block_idx;
    uint32_t idxcell_idx;
    struct VdbValue key;
};

//meta data node
uint32_t* vdbmeta_auto_counter_ptr(uint8_t* buf);
uint32_t* vdbmeta_root_ptr(uint8_t* buf);
uint32_t* vdbmeta_last_leaf(uint8_t* buf);
uint32_t* vdbmeta_largest_key_size(uint8_t* buf);
void* vdbmeta_largest_key(uint8_t* buf);
void vdbmeta_allocate_schema_ptr(uint8_t* buf, uint32_t size);
uint32_t* vdbmeta_data_block_ptr(uint8_t* buf);
void* vdbmeta_schema_ptr(uint8_t* buf);

//internal node
uint32_t* vdbintern_rightptr_block(uint8_t* buf);
void* vdbintern_rightptr_key(uint8_t* buf);

//leaf node
void* vdbleaf_record_ptr(uint8_t* buf, uint32_t idx);
uint32_t* vdbleaf_record_occupied_ptr(uint8_t* buf, uint32_t idx);
void vdbleaf_insert_record_cell(uint8_t* buf, uint32_t idxcell_idx, uint32_t rec_size);
void vdbleaf_delete_idxcell(uint8_t* buf, uint32_t idxcell_idx);

//node functions
enum VdbNodeType* vdbnode_type(uint8_t* buf);
uint32_t* vdbnode_parent(uint8_t* buf);
uint32_t* vdbnode_next(uint8_t* buf);
uint32_t* vdbnode_idxcell_count(uint8_t* buf);
uint32_t* vdbnode_datacells_size(uint8_t* buf);
bool vdbnode_can_fit(uint8_t* buf, uint32_t datacell_size);
uint32_t vdbnode_append_idxcell(uint8_t* buf, uint32_t datacell_size);
void vdbnode_insert_idxcell(uint8_t* buf, uint32_t idxcell_idx, uint32_t datacell_size);
void* vdbnode_datacell(uint8_t* buf, uint32_t idxcell_idx);

#endif //VDB_NODE_H
