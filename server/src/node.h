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

struct VdbRecPtr {
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

//node functions
enum VdbNodeType* vdbnode_type(uint8_t* buf);
uint32_t* vdbnode_parent(uint8_t* buf);
uint32_t* vdbnode_next(uint8_t* buf);
uint32_t* vdbnode_idxcell_count(uint8_t* buf);
uint32_t* vdbnode_datacells_size(uint8_t* buf);
uint32_t* vdbnode_idxcells_freelist(uint8_t* buf);
uint32_t* vdbnode_datacells_freelist(uint8_t* buf);
void vdbnode_free_cell_and_defrag_datacells_only(uint8_t* buf, uint32_t idxcell_idx);
void vdbnode_free_cell_and_defrag_node(uint8_t* buf, uint32_t idxcell_idx);

bool vdbnode_can_fit(uint8_t* buf, uint32_t datacell_size);
uint32_t vdbnode_new_idxcell(uint8_t* buf, uint32_t datacell_size);
void vdbnode_insert_idxcell(uint8_t* buf, uint32_t idxcell_idx, uint32_t datacell_size);
void* vdbnode_datacell(uint8_t* buf, uint32_t idxcell_idx);
void vdbnode_free_cell(uint8_t* buf, uint32_t idxcell_idx);

#endif //VDB_NODE_H
