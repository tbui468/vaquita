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
 * [type|parent_idx|right ptr idx|right ptr key|ptr count|datacells size| ... |index cells...datacells]
 */

struct VdbPtr* vdbintern_rightptr_ptr(uint8_t* buf) {
    return (struct VdbPtr*)(buf + sizeof(uint32_t) * 2);
}

uint32_t* vdbintern_nodeptr_count_ptr(uint8_t* buf) {
    return (uint32_t*)(buf + sizeof(uint32_t) * 4);
}

uint32_t* vdbintern_datacells_size_ptr(uint8_t* buf) {
    return (uint32_t*)(buf + sizeof(uint32_t) * 5);
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
    uint32_t datacell_off = VDB_PAGE_SIZE - datacells_size - sizeof(uint32_t) * 2;

    *((uint32_t*)(buf + indexcell_off)) = datacell_off;
    *((uint32_t*)(buf + datacell_off + sizeof(uint32_t) * 0)) = ptr.idx;
    *((uint32_t*)(buf + datacell_off + sizeof(uint32_t) * 1)) = ptr.key;

    *vdbintern_nodeptr_count_ptr(buf) = count + 1;
    *vdbintern_datacells_size_ptr(buf) = datacells_size + sizeof(uint32_t) * 2;
}

bool vdbintern_can_fit_nodeptr(uint8_t* buf) {
    uint32_t indexcells_size = *vdbintern_nodeptr_count_ptr(buf) * sizeof(uint32_t);
    uint32_t datacells_size = *vdbintern_datacells_size_ptr(buf);
    uint32_t ptr_entry_size = sizeof(uint32_t) * 3; //index cell + two pointer fields (idx and key)

    return VDB_PAGE_SIZE - VDB_PAGE_HDR_SIZE >= indexcells_size + datacells_size + ptr_entry_size;
}

/* 
 * Leaf node de/serialization
 * [type|parent_idx|data block idx|record count|datacells size|next leaf idx| ... |index cells ... datacells]
 */

uint32_t* vdbleaf_data_block_ptr(uint8_t* buf) {
    return (uint32_t*)(buf + sizeof(uint32_t) * 2);
}

uint32_t* vdbleaf_record_count_ptr(uint8_t* buf) {
    return (uint32_t*)(buf + sizeof(uint32_t) * 3);
}

uint32_t* vdbleaf_datacells_size_ptr(uint8_t* buf) {
    return (uint32_t*)(buf + sizeof(uint32_t) * 4);
}

uint32_t* vdbleaf_next_leaf_ptr(uint8_t* buf) {
    return (uint32_t*)(buf + sizeof(uint32_t) * 5);
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
    *((uint32_t*)(buf + datacell_off + sizeof(uint32_t))) = true; //occupied field

    return new_rec_idx;
}


/* 
 * Data Node Header
 * [type|parent_idx|next data block idx| index count | datacells size|index freelist head|datacells freelist head]
 * [ 0  |     1    |         2         |     3       |       4       |         5         |            6          ]
 *
 * Data Node Datacell Header - index 0 is overflow block idx if used cell, next pointer if free cell
 * [ overflow block idx / next | overflow index cell offset |  size  ]
 * [            0              |              1             |    2   ]
 */

void vdbdata_init(uint8_t* buf, uint32_t parent_idx) {
    *((uint32_t*)(buf + sizeof(uint32_t) * 0)) = VDBN_DATA;
    *((uint32_t*)(buf + sizeof(uint32_t) * 1)) = parent_idx;
    *((uint32_t*)(buf + sizeof(uint32_t) * 2)) = 0;
    *((uint32_t*)(buf + sizeof(uint32_t) * 3)) = 0;
    *((uint32_t*)(buf + sizeof(uint32_t) * 4)) = 0;
    *((uint32_t*)(buf + sizeof(uint32_t) * 5)) = 0;
    *((uint32_t*)(buf + sizeof(uint32_t) * 6)) = 0;
}

uint32_t vdbdata_datacell_header_size(void) {
    return sizeof(uint32_t) * 3;
}

uint32_t* vdbdata_next_ptr(uint8_t* buf) {
    return (uint32_t*)(buf + sizeof(uint32_t) * 2);
}

uint32_t* vdbdata_idx_count_ptr(uint8_t* buf) {
    return (uint32_t*)(buf + sizeof(uint32_t) * 3);
}

static uint32_t* vdbdata_datacells_size_ptr(uint8_t* buf) {
    return (uint32_t*)(buf + sizeof(uint32_t) * 4);
}

static uint32_t* vdbdata_idxcell_freelist_ptr(uint8_t* buf) {
    return (uint32_t*)(buf + sizeof(uint32_t) * 5);
}

static uint32_t* vdbdata_datacell_freelist_ptr(uint8_t* buf) {
    return (uint32_t*)(buf + sizeof(uint32_t) * 6);
}

static uint32_t* vdbdata_idxcell_ptr(uint8_t* buf, uint32_t idx) {
    return (uint32_t*)(buf + VDB_PAGE_HDR_SIZE + idx * sizeof(uint32_t));
}

uint8_t* vdbdata_datacell_ptr(uint8_t* buf, uint32_t idxcell_idx) {
    uint32_t off = *((uint32_t*)(buf + VDB_PAGE_HDR_SIZE + idxcell_idx * sizeof(uint32_t)));

    return buf + off;
}

static void vdbdata_defrag(uint8_t* buf) {
    uint32_t free_data_off = *vdbdata_datacell_freelist_ptr(buf);
    *vdbdata_datacell_freelist_ptr(buf) = 0;

    while (free_data_off) {
        //caching free_data_off to use in the rest of loop since its
        //values will be overwritten when shifting right with memmove
        uint32_t cached_free_data_off = free_data_off;
        free_data_off = *((uint32_t*)(buf + free_data_off + sizeof(uint32_t) * 0));

        uint32_t datum_size = *((uint32_t*)(buf + cached_free_data_off + sizeof(uint32_t) * 2));
        uint32_t right_shift_size = vdbdata_datacell_header_size() + datum_size;

        uint32_t start_off = VDB_PAGE_HDR_SIZE + sizeof(uint32_t) * *vdbdata_idx_count_ptr(buf);
        uint8_t* src = buf + start_off;
        assert(cached_free_data_off >= start_off);
        memmove(src + right_shift_size, src, cached_free_data_off - start_off);

        //if idxcells refer to any offset that was shifted right, increment those idxcell values to realign
        uint32_t end_of_idxcells = VDB_PAGE_HDR_SIZE + sizeof(uint32_t) * *vdbdata_idx_count_ptr(buf);
        for (uint32_t i = 0; i < *vdbdata_idx_count_ptr(buf); i++) {
            uint32_t off = *vdbdata_idxcell_ptr(buf, i);
            if (end_of_idxcells <= off && off < cached_free_data_off) {
                *vdbdata_idxcell_ptr(buf, i) += right_shift_size;
            }
        }

        //idxcells can be reused but not freed, so don't modify idxcells_count field
        uint32_t prev_datacells_size = *vdbdata_datacells_size_ptr(buf);
        *vdbdata_datacells_size_ptr(buf) = prev_datacells_size - right_shift_size;
    }
}

uint32_t vdbdata_get_free_space(uint8_t* buf) {
    vdbdata_defrag(buf);
    //TODO: split this line into multiple lines
    return VDB_PAGE_SIZE - VDB_PAGE_HDR_SIZE - *vdbdata_datacells_size_ptr(buf) - vdbdata_datacell_header_size() - *vdbdata_idx_count_ptr(buf) * sizeof(uint32_t);
}

uint32_t vdbdata_allocate_new_string_space(uint8_t* buf, uint32_t requested_len, uint32_t* allocated_len) {

    uint32_t free = vdbdata_get_free_space(buf);
    uint32_t idxcell_off = *vdbdata_idxcell_freelist_ptr(buf);
    uint32_t idxcell_idx;
    if (idxcell_off > 0) { //reuse unused idxcell
        idxcell_idx = (idxcell_off - VDB_PAGE_HDR_SIZE) / sizeof(uint32_t);
        *vdbdata_idxcell_freelist_ptr(buf) = *((uint32_t*)(buf + idxcell_off));
    } else { //create new idxcell
        idxcell_idx = *vdbdata_idx_count_ptr(buf);
        free -= sizeof(uint32_t);
        *vdbdata_idx_count_ptr(buf) = idxcell_idx + 1;
    }

    uint32_t datacells_size = *vdbdata_datacells_size_ptr(buf);
    uint32_t header_size = vdbdata_datacell_header_size();

    uint32_t can_fit = free < requested_len ? free : requested_len;
    uint32_t off = VDB_PAGE_SIZE - datacells_size - can_fit - header_size;

    *((uint32_t*)(buf + off + sizeof(uint32_t) * 0)) = 0;
    *((uint32_t*)(buf + off + sizeof(uint32_t) * 1)) = 0;
    *((uint32_t*)(buf + off + sizeof(uint32_t) * 2)) = can_fit;

    *allocated_len = can_fit;

    *vdbdata_datacells_size_ptr(buf) = datacells_size + header_size + can_fit;

    *vdbdata_idxcell_ptr(buf, idxcell_idx) = off;

    return idxcell_idx;
}

uint8_t* vdbdata_string_ptr(uint8_t* buf, uint32_t idxcell_idx) {
    uint32_t data_off = *vdbdata_idxcell_ptr(buf, idxcell_idx);
    data_off += sizeof(uint32_t) * 3; //skip overflow block_idx, overflow idxcell_idx and size
    return buf + data_off;
}

//sets overflow_block and overflow_idxcell_off to values in freed data cell - up to caller to free any overflow data
void vdbdata_free_cells(uint8_t* buf, uint32_t idxcell_idx, uint32_t* overflow_block_idx, uint32_t* overflow_idxcell_idx) {
    uint32_t idxcell_off = VDB_PAGE_HDR_SIZE + idxcell_idx * sizeof(uint32_t);
    uint32_t datacell_off = *((uint32_t*)(buf + idxcell_off));

    *overflow_block_idx = *((uint32_t*)(buf + datacell_off + sizeof(uint32_t) * 0));
    *overflow_idxcell_idx = *((uint32_t*)(buf + datacell_off + sizeof(uint32_t) * 1));

    //insert freed cells into freelists
    *((uint32_t*)(buf + idxcell_off)) = *vdbdata_idxcell_freelist_ptr(buf);
    *vdbdata_idxcell_freelist_ptr(buf) = idxcell_off;

    *((uint32_t*)(buf + datacell_off + sizeof(uint32_t) * 0)) = *vdbdata_datacell_freelist_ptr(buf);
    *vdbdata_datacell_freelist_ptr(buf) = datacell_off;
}

void vdbdata_data_write_overflow(uint8_t* buf, uint32_t idxcell_idx, uint32_t of_block_idx, uint32_t of_idxcell_idx) {
    uint32_t data_off = *vdbdata_idxcell_ptr(buf, idxcell_idx);

    *((uint32_t*)(buf + data_off + sizeof(uint32_t) * 0)) = of_block_idx;
    *((uint32_t*)(buf + data_off + sizeof(uint32_t) * 1)) = of_idxcell_idx;
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


