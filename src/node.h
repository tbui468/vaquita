#ifndef VDB_NODE_H
#define VDB_NODE_H

#include <stdint.h>
#include <stdbool.h>

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
uint32_t* vdbmeta_auto_counter_ptr(uint8_t* buf);
uint32_t* vdbmeta_root_ptr(uint8_t* buf);
void* vdbmeta_schema_ptr(uint8_t* buf);
//uint32_t* vdbmeta_data_block_ptr(uint8_t* buf);

//internal node
struct VdbPtr* vdbintern_rightptr_ptr(uint8_t* buf);
uint32_t* vdbintern_nodeptr_count_ptr(uint8_t* buf);
struct VdbPtr* vdbintern_nodeptr_ptr(uint8_t* buf, uint32_t idx);
uint32_t* vdbintern_datacells_size_ptr(uint8_t* buf);

void vdbintern_append_nodeptr(uint8_t* buf, struct VdbPtr ptr);
bool vdbintern_can_fit_nodeptr(uint8_t* buf);

//leaf node
uint32_t* vdbleaf_data_block_ptr(uint8_t* buf);
uint32_t* vdbleaf_record_count_ptr(uint8_t* buf);
uint32_t* vdbleaf_datacells_size_ptr(uint8_t* buf);
uint32_t* vdbleaf_next_leaf_ptr(uint8_t* buf);
void* vdbleaf_record_ptr(uint8_t* buf, uint32_t idx);
uint32_t* vdbleaf_record_occupied_ptr(uint8_t* buf, uint32_t idx);

uint32_t vdbleaf_append_record_cell(uint8_t* buf, uint32_t fixedlen_size);

//data node
void vdbdata_init(uint8_t* buf, uint32_t parent_idx);
uint32_t* vdbdata_next_ptr(uint8_t* buf);
uint32_t* vdbdata_idx_count_ptr(uint8_t* buf);

uint32_t vdbdata_get_free_space(uint8_t* buf);
uint8_t* vdbdata_datacell_ptr(uint8_t* buf, uint32_t idxcell_idx);

uint32_t vdbdata_allocate_new_string_space(uint8_t* buf, uint32_t requested_len, uint32_t* allocated_len);
uint8_t* vdbdata_string_ptr(uint8_t* buf, uint32_t idxcell_idx);
void vdbdata_free_cells(uint8_t* buf, uint32_t idxcell_idx, uint32_t* overflow_block_idx, uint32_t* overflow_idxcell_idx);
void vdbdata_data_write_overflow(uint8_t* buf, uint32_t idxcell_idx, uint32_t of_block_idx, uint32_t of_idxcell_idx);


//shared node functions
enum VdbNodeType* vdbnode_type(uint8_t* buf);
uint32_t* vdbnode_parent(uint8_t* buf);

#endif //VDB_NODE_H
