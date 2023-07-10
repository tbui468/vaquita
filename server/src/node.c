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
    return (uint32_t*)(buf + sizeof(uint32_t) * 7);
}

void* vdbintern_rightptr_key(uint8_t* buf) {
    return (void*)(buf + sizeof(uint32_t) * 8);
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

uint32_t* vdbnode_idxcells_freelist(uint8_t* buf) {
    return (uint32_t*)(buf + sizeof(uint32_t) * 5);
}

uint32_t* vdbnode_datacells_freelist(uint8_t* buf) {
    return (uint32_t*)(buf + sizeof(uint32_t) * 6);
}

bool vdbnode_can_fit(uint8_t* buf, uint32_t datacell_size) {
    uint32_t used_size = *vdbnode_idxcell_count(buf) * sizeof(uint32_t) + *vdbnode_datacells_size(buf);
    uint32_t idxcell_size = sizeof(uint32_t);
    uint32_t datacell_header_size = sizeof(uint32_t) * 2; //next and size fields
    return VDB_PAGE_SIZE - VDB_PAGE_HDR_SIZE >= datacell_size + idxcell_size + datacell_header_size + used_size;
}

//reuses free idxcell or appends a new one
uint32_t vdbnode_new_idxcell(uint8_t* buf, uint32_t datacell_size) {
    assert(vdbnode_can_fit(buf, datacell_size) && "node must be able to fit data");

    uint32_t new_idx;
    if (*vdbnode_idxcells_freelist(buf) == 0) {
        new_idx = (*vdbnode_idxcell_count(buf))++;
    } else {
        new_idx = (*vdbnode_idxcells_freelist(buf) - VDB_PAGE_HDR_SIZE) / sizeof(uint32_t);
        *vdbnode_idxcells_freelist(buf) = *((uint32_t*)(buf + VDB_PAGE_HDR_SIZE + new_idx * sizeof(uint32_t)));
    }
   
    (*vdbnode_datacells_size(buf)) += datacell_size + sizeof(uint32_t) * 2; //next and size fields

    uint32_t idxcell_off = VDB_PAGE_HDR_SIZE + new_idx * sizeof(uint32_t);
    *((uint32_t*)(buf + idxcell_off)) = VDB_PAGE_SIZE - *vdbnode_datacells_size(buf);

    int data_off = *((uint32_t*)(buf + idxcell_off));
    *((uint32_t*)(buf + data_off + sizeof(uint32_t))) = datacell_size;

    return new_idx;
}

void vdbnode_insert_idxcell(uint8_t* buf, uint32_t idxcell_idx, uint32_t datacell_size) {
    assert(vdbnode_can_fit(buf, datacell_size) && "node must be able to fit data");
    assert(*vdbnode_idxcell_freelist(buf) == 0 && "cannot use 'vdbnode_insert_idxcell' when idxcells are fragmented");

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

static void vdbnode_vacuum_datacell(uint8_t* buf, uint32_t datacell_off, uint32_t datacell_size) {
    uint8_t* shift_src = buf + VDB_PAGE_HDR_SIZE + *(vdbnode_idxcell_count(buf)) * sizeof(uint32_t);
    uint8_t* shift_dst = shift_src + datacell_size;
    uint32_t shift_size = datacell_off - (VDB_PAGE_HDR_SIZE + *(vdbnode_idxcell_count(buf)) * sizeof(uint32_t));
    memmove(shift_dst, shift_src, shift_size);

    //shift idxcell ptrs based on shift during defragging of datacells
    for (uint32_t i = 0; i < *(vdbnode_idxcell_count(buf)); i++) {
        uint32_t off = *((uint32_t*)(buf + VDB_PAGE_HDR_SIZE + i * sizeof(uint32_t))); 

        //only shift offset if idxcell is pointing datacell
        //if offset is pointing to an idxcell, then it must be a free idxcell, so don't shift
        uint32_t idxcell_end = VDB_PAGE_HDR_SIZE + *(vdbnode_idxcell_count(buf)) * sizeof(uint32_t);
        if (off < idxcell_end)
            continue;

        if (off < datacell_off) {
            *((uint32_t*)(buf + VDB_PAGE_HDR_SIZE + i * sizeof(uint32_t))) += datacell_size;
        }
    }

    *vdbnode_datacells_size(buf) -= datacell_size;
}

//leaf node recptrs references idxcells in data nodes, so defragging idxcells
//in data nodes would cause havoc.  Instead freed idxcells are added to freelist for reuse
void vdbnode_free_cell_and_defrag_datacells_only(uint8_t* buf, uint32_t idxcell_idx) {
    uint32_t datacell_off = *((uint32_t*)(buf + VDB_PAGE_HDR_SIZE + idxcell_idx * sizeof(uint32_t)));
    uint32_t datacell_size = *((uint32_t*)(buf + datacell_off + sizeof(uint32_t))) + sizeof(uint32_t) * 2;

    //add idxcell to freelist to allow reuse
    uint32_t idxcell_off = VDB_PAGE_HDR_SIZE + idxcell_idx * sizeof(uint32_t);
    *((uint32_t*)(buf + idxcell_off)) = *vdbnode_idxcells_freelist(buf);
    *vdbnode_idxcells_freelist(buf) = idxcell_off;

    vdbnode_vacuum_datacell(buf, datacell_off, datacell_size);
}

void vdbnode_free_cell_and_defrag_node(uint8_t* buf, uint32_t idxcell_idx) {
    uint32_t datacell_off = *((uint32_t*)(buf + VDB_PAGE_HDR_SIZE + idxcell_idx * sizeof(uint32_t)));
    uint32_t datacell_size = *((uint32_t*)(buf + datacell_off + sizeof(uint32_t))) + sizeof(uint32_t) * 2;

    //defrag idxcells
    (*vdbnode_idxcell_count(buf))--;
    uint8_t* dst = buf + VDB_PAGE_HDR_SIZE + idxcell_idx * sizeof(uint32_t);
    uint8_t* src = dst + sizeof(uint32_t);
    size_t size = (*vdbnode_idxcell_count(buf) - idxcell_idx) * sizeof(uint32_t);
    memmove(dst, src, size);

    vdbnode_vacuum_datacell(buf, datacell_off, datacell_size);
}
