#include <stddef.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include "node.h"
#include "util.h"
#include "pager.h"


/*
 * Meta node de/serialization
 * [type|parent_idx|pk_counter|root_idx|schema_off|data_block_idx|last_leaf_idx|largest_key...|...|...schema]
 */

uint32_t* vdbmeta_auto_counter_ptr(uint8_t* buf) {
    return (uint32_t*)(buf + sizeof(uint32_t) * 2);
}

uint32_t* vdbmeta_root_ptr(uint8_t* buf) {
    return (uint32_t*)(buf + sizeof(uint32_t) * 3);
}

void vdbmeta_allocate_schema_ptr(uint8_t* buf, uint32_t size) {
    *((uint32_t*)(buf + sizeof(uint32_t) * 4)) = VDB_PAGE_SIZE - size;
}

uint32_t* vdbmeta_data_block_ptr(uint8_t* buf) {
    return (uint32_t*)(buf + sizeof(uint32_t) * 5);
}

uint32_t* vdbmeta_last_leaf(uint8_t* buf) {
    return (uint32_t*)(buf + sizeof(uint32_t) * 6);
}

void* vdbmeta_largest_key(uint8_t* buf) {
    return (void*)(buf + sizeof(uint32_t) * 7);
}

void* vdbmeta_schema_ptr(uint8_t* buf) {
    uint32_t off = *((uint32_t*)(buf + sizeof(uint32_t) * 4));
    assert(off != 0 && "schema pointer not set to end of meta node");
    return (void*)(buf + off);
}

/*
 * Internal node de/serialization
 * [type|parent_idx|next|idxcell count|datacells size|right ptr block|right ptr key| ... |index cells...datacells]
 */

uint32_t* vdbintern_rightptr_block(uint8_t* buf) {
    return (uint32_t*)(buf + sizeof(uint32_t) * 5);
}

void* vdbintern_rightptr_key(uint8_t* buf) {
    return (void*)(buf + sizeof(uint32_t) * 6);
}

/* 
 * Leaf node de/serialization
 * [type|parent_idx|record count|datacells size|next leaf idx| ... |index cells ... datacells]
 */

/*
 * Shared node functions
 * [type|parent_idx|next|idxcell count|datacells size|<block specific header data> ... |index cells...datacells]
 */

enum VdbNodeType* vdbnode_type(uint8_t* buf) {
    return (enum VdbNodeType*)(buf);
}

uint32_t* vdbnode_parent(uint8_t* buf) {
    return (uint32_t*)(buf + sizeof(uint32_t));
}

uint32_t* vdbnode_next(uint8_t* buf) {
    return (uint32_t*)(buf + sizeof(uint32_t) * 2);
}

uint32_t* vdbnode_idxcell_count(uint8_t* buf) {
    return (uint32_t*)(buf + sizeof(uint32_t) * 3);
}

uint32_t* vdbnode_datacells_size(uint8_t* buf) {
    return (uint32_t*)(buf + sizeof(uint32_t) * 4);
}

bool vdbnode_can_fit(uint8_t* buf, uint32_t datacell_size) {
    uint32_t used_size = *vdbnode_idxcell_count(buf) * sizeof(uint32_t) + *vdbnode_datacells_size(buf);
    uint32_t idxcell_size = sizeof(uint32_t);
    uint32_t datacell_header_size = sizeof(uint32_t) * 2; //next and size fields
    return VDB_PAGE_SIZE - VDB_PAGE_HDR_SIZE >= datacell_size + idxcell_size + datacell_header_size + used_size;
}

uint32_t vdbnode_append_idxcell(uint8_t* buf, uint32_t datacell_size) {
    assert(vdbnode_can_fit(buf, datacell_size) && "node must be able to fit data");

    uint32_t new_idx = (*vdbnode_idxcell_count(buf))++;
    (*vdbnode_datacells_size(buf)) += datacell_size + sizeof(uint32_t) * 2; //next and size fields

    uint32_t idxcell_off = VDB_PAGE_HDR_SIZE + new_idx * sizeof(uint32_t);
    *((uint32_t*)(buf + idxcell_off)) = VDB_PAGE_SIZE - *vdbnode_datacells_size(buf);

    int data_off = *((uint32_t*)(buf + idxcell_off));
    *((uint32_t*)(buf + data_off + sizeof(uint32_t))) = datacell_size;

    return new_idx;
}

void vdbnode_insert_idxcell(uint8_t* buf, uint32_t idxcell_idx, uint32_t datacell_size) {
    assert(vdbnode_can_fit(buf, datacell_size) && "node must be able to fit data");

    uint8_t* src = buf + VDB_PAGE_HDR_SIZE + idxcell_idx * sizeof(uint32_t);
    uint8_t* dst = src + sizeof(uint32_t);
    size_t size = (*vdbnode_idxcell_count(buf) - idxcell_idx) * sizeof(uint32_t);
    memmove(dst, src, size);

    *vdbnode_datacells_size(buf) += datacell_size + sizeof(uint32_t) * 2; //next and size fields
    *((uint32_t*)(buf + VDB_PAGE_HDR_SIZE + idxcell_idx * sizeof(uint32_t))) = VDB_PAGE_SIZE - *vdbnode_datacells_size(buf);

    (*vdbnode_idxcell_count(buf))++;

    int off = VDB_PAGE_HDR_SIZE + idxcell_idx * sizeof(uint32_t);
    int data_off = *((uint32_t*)(buf + off));
    *((uint32_t*)(buf + data_off + sizeof(uint32_t))) = datacell_size;
}

void* vdbnode_datacell(uint8_t* buf, uint32_t idxcell_idx) {
    int off = VDB_PAGE_HDR_SIZE + idxcell_idx * sizeof(uint32_t);
    int data_off = *((uint32_t*)(buf + off));

    return (void*)(buf + data_off + sizeof(uint32_t) * 2); //skip next and size fields
}

void vdbnode_free_cell(uint8_t* buf, uint32_t idxcell_idx) {
    (*vdbnode_idxcell_count(buf))--;
    uint8_t* dst = buf + VDB_PAGE_HDR_SIZE + idxcell_idx * sizeof(uint32_t);
    uint8_t* src = dst + sizeof(uint32_t);
    size_t size = (*vdbnode_idxcell_count(buf) - idxcell_idx) * sizeof(uint32_t);
    memmove(dst, src, size);
}
