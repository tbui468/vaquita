#include <stddef.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include "node.h"
#include "util.h"
#include "pager.h"


/*
 * Meta node de/serialization
 * [type|parent_idx|pk_counter|root_idx|schema...]
 */


uint32_t* vdbmeta_auto_counter_ptr(uint8_t* buf) {
    return (uint32_t*)(buf + sizeof(uint32_t) * 2);
}

uint32_t* vdbmeta_root_ptr(uint8_t* buf) {
    return (uint32_t*)(buf + sizeof(uint32_t) * 3);
}

void* vdbmeta_schema_ptr(uint8_t* buf) {
    return (void*)(buf + sizeof(uint32_t) * 4);
}


/*
 * Internal node de/serialization
 * [type|parent_idx|ptr count|datacells size|right ptr| ... |index cells...datacells]
 */

uint32_t* vdbintern_nodeptr_count_ptr(uint8_t* buf) {
    return (uint32_t*)(buf + sizeof(uint32_t) * 2);
}

uint32_t* vdbintern_datacells_size_ptr(uint8_t* buf) {
    return (uint32_t*)(buf + sizeof(uint32_t) * 3);
}

struct VdbPtr* vdbintern_rightptr_ptr(uint8_t* buf) {
    return (struct VdbPtr*)(buf + sizeof(uint32_t) * 4);
}

struct VdbPtr* vdbintern_nodeptr_ptr(uint8_t* buf, uint32_t idx) {
    int indexcell_off = VDB_PAGE_HDR_SIZE + idx * sizeof(uint32_t);
    int datacell_off = *((uint32_t*)(buf + indexcell_off));

    return (struct VdbPtr*)(buf + datacell_off);
}

void vdbintern_append_nodeptr(uint8_t* buf, struct VdbPtr ptr) {
    uint32_t count = *vdbintern_nodeptr_count_ptr(buf);
    uint32_t indexcell_off = VDB_PAGE_HDR_SIZE + sizeof(uint32_t) * count;
    uint32_t datacells_size = *vdbintern_datacells_size_ptr(buf);
    uint32_t datacell_off = VDB_PAGE_SIZE - datacells_size - sizeof(uint32_t) - sizeof(struct VdbValue);

    *((uint32_t*)(buf + indexcell_off)) = datacell_off;
    *((uint32_t*)(buf + datacell_off + sizeof(uint32_t) * 0)) = ptr.block_idx;
    *((struct VdbValue*)(buf + datacell_off + sizeof(uint32_t) * 1)) = ptr.key;

    *vdbintern_nodeptr_count_ptr(buf) = count + 1;
    *vdbintern_datacells_size_ptr(buf) = datacells_size + sizeof(uint32_t) + sizeof(struct VdbValue);
}

bool vdbintern_can_fit_nodeptr(uint8_t* buf) {
    uint32_t indexcells_size = *vdbintern_nodeptr_count_ptr(buf) * sizeof(uint32_t);
    uint32_t datacells_size = *vdbintern_datacells_size_ptr(buf);
    uint32_t ptr_entry_size = sizeof(uint32_t) * 2 + sizeof(struct VdbValue); //index cell + block_idx + key

    return VDB_PAGE_SIZE - VDB_PAGE_HDR_SIZE >= indexcells_size + datacells_size + ptr_entry_size;
}

/* 
 * Leaf node de/serialization
 * [type|parent_idx|record count|datacells size|next leaf idx| ... |index cells ... datacells]
 */

uint32_t* vdbleaf_record_count_ptr(uint8_t* buf) {
    return (uint32_t*)(buf + sizeof(uint32_t) * 2);
}

uint32_t* vdbleaf_datacells_size_ptr(uint8_t* buf) {
    return (uint32_t*)(buf + sizeof(uint32_t) * 3);
}

uint32_t* vdbleaf_next_leaf_ptr(uint8_t* buf) {
    return (uint32_t*)(buf + sizeof(uint32_t) * 4);
}

void* vdbleaf_record_ptr(uint8_t* buf, uint32_t idx) {
    int off = VDB_PAGE_HDR_SIZE + idx * sizeof(uint32_t);
    int data_off = *((uint32_t*)(buf + off));

    return (void*)(buf + data_off + sizeof(uint32_t) * 2); //skip next and occcupied fields
}

uint32_t* vdbleaf_record_occupied_ptr(uint8_t* buf, uint32_t idx) {
    int idx_off = VDB_PAGE_HDR_SIZE + idx * sizeof(uint32_t);
    int datacell_off = *((uint32_t*)(buf + idx_off));
    return (uint32_t*)(buf + datacell_off + sizeof(uint32_t));
}

uint32_t vdbleaf_append_record_cell(uint8_t* buf, uint32_t fixedlen_size) {
    uint32_t new_rec_idx = *vdbleaf_record_count_ptr(buf);
    uint32_t new_datacells_size = *vdbleaf_datacells_size_ptr(buf) + sizeof(uint32_t) * 2 + fixedlen_size;
    *((uint32_t*)(buf + VDB_PAGE_HDR_SIZE + new_rec_idx * sizeof(uint32_t))) = VDB_PAGE_SIZE - new_datacells_size;

    *vdbleaf_datacells_size_ptr(buf) = new_datacells_size;
    *vdbleaf_record_count_ptr(buf) = new_rec_idx + 1;

    int idx_off = VDB_PAGE_HDR_SIZE + new_rec_idx * sizeof(uint32_t);
    int datacell_off = *((uint32_t*)(buf + idx_off));
    *((uint32_t*)(buf + datacell_off)) = 0; //next field
    *((uint32_t*)(buf + datacell_off + sizeof(uint32_t))) = (uint32_t)true; //occupied field

    return new_rec_idx;
}

/*
 * Shared node functions
 */
enum VdbNodeType* vdbnode_type(uint8_t* buf) {
    return (enum VdbNodeType*)(buf);
}

uint32_t* vdbnode_parent(uint8_t* buf) {
    return (uint32_t*)(buf + sizeof(uint32_t));
}


