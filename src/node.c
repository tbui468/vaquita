#include <stddef.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include "node.h"
#include "util.h"
#include "record.h"
#include "pager.h"


/*
 * Meta node de/serialization
 * [type|parent_idx|pk_counter|root_idx|schema count|schema...]
 */

uint32_t vdbnode_meta_read_primary_key_counter(uint8_t* buf) {
    return *((uint32_t*)(buf + sizeof(uint32_t) * 2));
}

uint32_t vdb_node_meta_read_root(uint8_t* buf) {
    return *((uint32_t*)(buf + sizeof(uint32_t) * 3));
}

struct VdbSchema* vdbnode_meta_read_schema(uint8_t* buf) {
    int off = 0;
    return vdb_schema_deserialize(buf + sizeof(uint32_t) * 4, &off);
}

void vdbnode_meta_write_primary_key_counter(uint8_t* buf, uint32_t pk_counter) {
    *((uint32_t*)(buf + 2 * sizeof(uint32_t))) = pk_counter;
}

void vdbnode_meta_write_root(uint8_t* buf, uint32_t root_idx) {
    *((uint32_t*)(buf + 3 * sizeof(uint32_t))) = root_idx;
}

void vdbnode_meta_write_schema(uint8_t* buf, struct VdbSchema* schema) {
    int off = 0;
    vdb_schema_serialize(buf + sizeof(uint32_t) * 4, schema, &off);
}


/*
 * Internal node de/serialization
 * [type|parent_idx|right ptr idx|right ptr key|ptr count|datacells size| ... |index cells...datacells]
 */

struct VdbPtr vdbnode_intern_read_right_ptr(uint8_t* buf) {
    struct VdbPtr ptr;
    ptr.idx = *((uint32_t*)(buf + sizeof(uint32_t) * 2));
    ptr.key = *((uint32_t*)(buf + sizeof(uint32_t) * 3));
    return ptr;
}

uint32_t vdbnode_intern_read_ptr_count(uint8_t* buf) {
    return *((uint32_t*)(buf + sizeof(uint32_t) * 4));
}

struct VdbPtr vdbnode_intern_read_ptr(uint8_t* buf, uint32_t idx) {
    struct VdbPtr ptr;

    int indexcell_off = VDB_PAGE_HDR_SIZE + idx * sizeof(uint32_t);
    int datacell_off = *((uint32_t*)(buf + indexcell_off));

    read_u32(&ptr.idx, buf, &datacell_off);
    read_u32(&ptr.key, buf, &datacell_off);

    return ptr;
}

uint32_t vdbnode_intern_read_datacells_size(uint8_t* buf) {
    return *((uint32_t*)(buf + sizeof(uint32_t) * 5));
}

void vdbnode_intern_write_right_ptr(uint8_t* buf, struct VdbPtr ptr) {
    *((uint32_t*)(buf + sizeof(uint32_t) * 2)) = ptr.idx;
    *((uint32_t*)(buf + sizeof(uint32_t) * 3)) = ptr.key;
}

void vdbnode_intern_write_ptr_count(uint8_t* buf, uint32_t count) {
    *((uint32_t*)(buf + sizeof(uint32_t) * 4)) = count;
}

void vdbnode_intern_write_datacells_size(uint8_t* buf, uint32_t size) {
    *((uint32_t*)(buf + sizeof(uint32_t) * 5)) = size;
}

void vdbnode_intern_write_new_ptr(uint8_t* buf, struct VdbPtr ptr) {
    uint32_t count = vdbnode_intern_read_ptr_count(buf);
    uint32_t indexcell_off = VDB_PAGE_HDR_SIZE + sizeof(uint32_t) * count;
    uint32_t datacells_size = vdbnode_intern_read_datacells_size(buf);
    uint32_t datacell_off = VDB_PAGE_SIZE - datacells_size - sizeof(uint32_t) * 2;

    *((uint32_t*)(buf + indexcell_off)) = datacell_off;
    *((uint32_t*)(buf + datacell_off + sizeof(uint32_t) * 0)) = ptr.idx;
    *((uint32_t*)(buf + datacell_off + sizeof(uint32_t) * 1)) = ptr.key;

    vdbnode_intern_write_ptr_count(buf, count + 1);
    vdbnode_intern_write_datacells_size(buf, datacells_size + sizeof(uint32_t) * 2);
}

bool vdbnode_intern_can_fit_ptr(uint8_t* buf) {
    uint32_t indexcells_size = vdbnode_intern_read_ptr_count(buf) * sizeof(uint32_t);
    uint32_t datacells_size = vdbnode_intern_read_datacells_size(buf);
    uint32_t ptr_entry_size = sizeof(uint32_t) * 3; //index cell + two pointer fields (idx and key)

    return VDB_PAGE_SIZE - VDB_PAGE_HDR_SIZE >= indexcells_size + datacells_size + ptr_entry_size;
}

/* 
 * Leaf node de/serialization
 * [type|parent_idx|data block idx|record count|datacells size| ... |index cells ... datacells]
 */

uint32_t vdbnode_leaf_read_data_block(uint8_t* buf) {
    return *((uint32_t*)(buf + sizeof(uint32_t) * 2));
}

uint32_t vdbnode_leaf_read_datacells_size(uint8_t* buf) {
    return *((uint32_t*)(buf + sizeof(uint32_t) * 4));
}

uint32_t vdbnode_leaf_read_record_key(uint8_t* buf, uint32_t idx) {
    int off = VDB_PAGE_HDR_SIZE + sizeof(uint32_t) * idx;

    uint32_t data_off;
    read_u32(&data_off, buf, &off);

    return *((uint32_t*)(buf + data_off + sizeof(uint32_t) * 2));
}

uint32_t vdbnode_leaf_read_record_count(uint8_t* buf) {
    return *((uint32_t*)(buf + sizeof(uint32_t) * 3));
}

struct VdbRecord* vdbnode_leaf_read_fixedlen_record(uint8_t* buf, struct VdbSchema* schema, uint32_t rec_idx) {
    int off = VDB_PAGE_HDR_SIZE + sizeof(uint32_t) * rec_idx;

    uint32_t data_off;
    read_u32(&data_off, buf, &off);

    struct VdbRecord* rec = malloc_w(sizeof(struct VdbRecord));
    rec->count = schema->count;
    data_off += sizeof(uint32_t) * 2; //skip next and size fields
    rec->key = *((uint32_t*)(buf + data_off));
    data_off += sizeof(uint32_t);
    rec->data = malloc_w(sizeof(struct VdbDatum) * rec->count);

    for (uint32_t i = 0; i < schema->count; i++) {
        enum VdbField f = schema->fields[i];
        rec->data[i].type = f;
        switch (f) {
            case VDBF_INT:
                rec->data[i].as.Int = *((uint64_t*)(buf + data_off));
                data_off += sizeof(uint64_t);
                break;
            case VDBF_STR:
                rec->data[i].block_idx = *((uint32_t*)(buf + data_off));
                data_off += sizeof(uint32_t);
                rec->data[i].idxcell_idx = *((uint32_t*)(buf + data_off));
                data_off += sizeof(uint32_t);
                break;
            case VDBF_BOOL:
                rec->data[i].as.Bool = *((bool*)(buf + data_off));
                data_off += sizeof(bool);
                break;
        }
    }

    return rec;
}

struct VdbDatum vdbnode_leaf_read_varlen_datum(uint8_t* buf, uint32_t idx) {
    uint32_t off = *((uint32_t*)(buf + VDB_PAGE_HDR_SIZE + idx * sizeof(uint32_t)));
    

    struct VdbDatum d;
    d.type = VDBF_STR;
    d.block_idx = *((uint32_t*)(buf + off + sizeof(uint32_t) * 0));
    d.idxcell_idx = *((uint32_t*)(buf + off + sizeof(uint32_t) * 1));

    d.as.Str = malloc_w(sizeof(struct VdbString));
    d.as.Str->len = *((uint32_t*)(buf + off + sizeof(uint32_t) * 2));
    d.as.Str->start = malloc_w(sizeof(char) * d.as.Str->len);
    memcpy(d.as.Str->start, buf + off + sizeof(uint32_t) * 3, d.as.Str->len);
    return d;
}

void vdbnode_leaf_write_data_block(uint8_t* buf, uint32_t data_idx) {
    *((uint32_t*)(buf + sizeof(uint32_t) * 2)) = data_idx;
}

void vdbnode_leaf_write_record_count(uint8_t* buf, uint32_t count) {
    *((uint32_t*)(buf + sizeof(uint32_t) * 3)) = count;
}

void vdbnode_leaf_write_datacells_size(uint8_t* buf, uint32_t size) {
    *((uint32_t*)(buf + sizeof(uint32_t) * 4)) = size;
}

void vdbnode_leaf_write_record(uint8_t* buf, uint32_t rec_idx, struct VdbRecord* rec) {
    int off = VDB_PAGE_HDR_SIZE + rec_idx * sizeof(uint32_t);
    int data_off = *((uint32_t*)(buf + off));

    uint32_t next_field = 0;
    write_u32(buf, next_field, &data_off);
    uint32_t rec_size = vdbrecord_fixedlen_size(rec);
    write_u32(buf, rec_size, &data_off);

    vdbrecord_write(buf + data_off, rec);
}

void vdbnode_leaf_append_record(uint8_t* buf, struct VdbRecord* rec) {
    uint32_t new_rec_idx = vdbnode_leaf_read_record_count(buf);
    uint32_t new_datacells_size = vdbnode_leaf_read_datacells_size(buf) + sizeof(uint32_t) * 2 + vdbrecord_fixedlen_size(rec);
    *((uint32_t*)(buf + VDB_PAGE_HDR_SIZE + new_rec_idx * sizeof(uint32_t))) = VDB_PAGE_SIZE - new_datacells_size;

    vdbnode_leaf_write_record(buf, new_rec_idx, rec);

    vdbnode_leaf_write_datacells_size(buf, new_datacells_size);
    vdbnode_leaf_write_record_count(buf, new_rec_idx + 1);
}

void vdbleaf_write_record_key(uint8_t* buf, uint32_t rec_idx, uint32_t key) {
    uint32_t rec_off = *((uint32_t*)(buf + VDB_PAGE_HDR_SIZE + sizeof(uint32_t) * rec_idx));

    *((uint32_t*)(buf + rec_off + sizeof(uint32_t) * 2)) = key;
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

static uint32_t vdbdata_datacell_header_size(void) {
    return sizeof(uint32_t) * 3;
}

static uint32_t vdbdata_read_datacells_size(uint8_t* buf) {
    return *((uint32_t*)(buf + sizeof(uint32_t) * 4));
}

static void vdbdata_write_datacells_size(uint8_t* buf, uint32_t size) {
    *((uint32_t*)(buf + sizeof(uint32_t) * 4)) = size;
}

uint32_t vdbdata_read_next(uint8_t* buf) {
    return *((uint32_t*)(buf + sizeof(uint32_t) * 2));
}

uint32_t vdbdata_read_idx_count(uint8_t* buf) {
    return *((uint32_t*)(buf + sizeof(uint32_t) * 3));
}

struct VdbDatum vdbdata_read_datum(uint8_t* buf, uint32_t idxcell_idx) {
    uint32_t datacell_off = *((uint32_t*)(buf + VDB_PAGE_HDR_SIZE + idxcell_idx * sizeof(uint32_t)));

    struct VdbDatum d;

    d.type = VDBF_STR;
    d.as.Str = malloc_w(sizeof(struct VdbString));
    d.block_idx = *((uint32_t*)(buf + datacell_off + sizeof(uint32_t) * 0));
    d.idxcell_idx = *((uint32_t*)(buf + datacell_off + sizeof(uint32_t) * 1));
    d.as.Str->len = *((uint32_t*)(buf + datacell_off + sizeof(uint32_t) * 2));
    d.as.Str->start = malloc_w(sizeof(char) * d.as.Str->len);
    memcpy(d.as.Str->start, buf + datacell_off + sizeof(uint32_t) * 3, d.as.Str->len);

    return d;
}

static void vdbdata_defrag(uint8_t* buf) {
    uint32_t free_data_off = *((uint32_t*)(buf + sizeof(uint32_t) * 6));

    while (free_data_off) {
        uint32_t datum_size = *((uint32_t*)(buf + free_data_off + sizeof(uint32_t) * 2));
        uint32_t right_shift_size = vdbdata_datacell_header_size() + datum_size;

        uint32_t start_off = VDB_PAGE_HDR_SIZE + sizeof(uint32_t) * vdbdata_read_idx_count(buf);
        uint8_t* src = buf + start_off;
        assert(free_data_off > start_off); //TODO why is free_data_off larger than start_off
        memmove(src + right_shift_size, src, free_data_off - start_off);

        //for all idxcells, shift right if less than free_data_off AND greater than max idxcell offset
        uint32_t end_of_idxcells = VDB_PAGE_HDR_SIZE + sizeof(uint32_t) * vdbdata_read_idx_count(buf);
        for (uint32_t i = 0; i < vdbdata_read_idx_count(buf); i++) {
            uint32_t off = *((uint32_t*)(buf + VDB_PAGE_HDR_SIZE + sizeof(uint32_t) * i));
            if (end_of_idxcells <= off && off < free_data_off) {
                *((uint32_t*)(buf + VDB_PAGE_HDR_SIZE + sizeof(uint32_t) * i)) += right_shift_size;
            }
        }

        //idxcells can be reused but not freed, so don't modify idxcells_count field
        uint32_t prev_datacells_size = vdbdata_read_datacells_size(buf);
        vdbdata_write_datacells_size(buf, prev_datacells_size - right_shift_size);

        free_data_off = *((uint32_t*)(buf + free_data_off + sizeof(uint32_t) * 0));
    }
}

uint32_t vdbdata_get_free_space(uint8_t* buf) {
    vdbdata_defrag(buf);
    //TODO: split this line into multiple lines
    return VDB_PAGE_SIZE - VDB_PAGE_HDR_SIZE - vdbdata_read_datacells_size(buf) - vdbdata_datacell_header_size() - vdbdata_read_idx_count(buf) * sizeof(uint32_t);
}

void vdbdata_write_next(uint8_t* buf, uint32_t next_idx) {
    *((uint32_t*)(buf + sizeof(uint32_t) * 2)) = next_idx;
}

void vdbdata_write_idx_count(uint8_t* buf, uint32_t count) {
    *((uint32_t*)(buf + sizeof(uint32_t) * 3)) = count;
}

uint32_t vdbdata_append_datum(uint8_t* buf, struct VdbDatum* datum, uint32_t* len_written) {
    uint32_t free = vdbdata_get_free_space(buf);

    uint32_t idxcell_off = *((uint32_t*)(buf + sizeof(uint32_t) * 5));
    uint32_t idxcell_idx;
    if (idxcell_off > 0) {
        idxcell_idx = (idxcell_off - VDB_PAGE_HDR_SIZE) / sizeof(uint32_t);
        *((uint32_t*)(buf + sizeof(uint32_t) * 5)) = *((uint32_t*)(buf + idxcell_idx * sizeof(uint32_t)));
    } else {
        idxcell_idx = vdbdata_read_idx_count(buf);
        free -= sizeof(uint32_t);
        vdbdata_write_idx_count(buf, idxcell_idx + 1);
    }

    uint32_t datacells_size = vdbdata_read_datacells_size(buf);
    uint32_t header_size = vdbdata_datacell_header_size();

    uint32_t can_fit = free < datum->as.Str->len - *len_written ? free : datum->as.Str->len - *len_written;
    uint32_t off = VDB_PAGE_SIZE - datacells_size - can_fit - header_size;

    *((uint32_t*)(buf + off + sizeof(uint32_t) * 0)) = 0;
    *((uint32_t*)(buf + off + sizeof(uint32_t) * 1)) = 0;
    *((uint32_t*)(buf + off + sizeof(uint32_t) * 2)) = can_fit;

    memcpy(buf + off + header_size, datum->as.Str->start + *len_written, can_fit);
    *len_written += can_fit;

    vdbdata_write_datacells_size(buf, datacells_size + header_size + can_fit);

    *((uint32_t*)(buf + VDB_PAGE_HDR_SIZE + idxcell_idx * sizeof(uint32_t))) = off;

    return idxcell_idx;
}

//sets overflow_block and overflow_idxcell_off to values in freed data cell - up to caller to free any overflow data
void vdbdata_free_cells(uint8_t* buf, uint32_t idxcell_idx, uint32_t* overflow_block_idx, uint32_t* overflow_idxcell_idx) {
    uint32_t idxcell_off = VDB_PAGE_HDR_SIZE + idxcell_idx * sizeof(uint32_t);
    uint32_t datacell_off = *((uint32_t*)(buf + idxcell_off));

    *overflow_block_idx = *((uint32_t*)(buf + datacell_off + sizeof(uint32_t) * 0));
    *overflow_idxcell_idx = *((uint32_t*)(buf + datacell_off + sizeof(uint32_t) * 1));

    //put idxcell at head of idxcell freelist
    uint32_t idxcell_freelist_head = *((uint32_t*)(buf + sizeof(uint32_t) * 5));
    *((uint32_t*)(buf + idxcell_off)) = idxcell_freelist_head;
    *((uint32_t*)(buf + sizeof(uint32_t) * 5)) = idxcell_off;

    //put datacell into datacell freelist
    uint32_t datacell_freelist_head = *((uint32_t*)(buf + sizeof(uint32_t) * 6));
    *((uint32_t*)(buf + datacell_off + sizeof(uint32_t) * 0)) = datacell_freelist_head;
    *((uint32_t*)(buf + sizeof(uint32_t) * 6)) = datacell_off;

    printf("idxcell_off: %d, datacell_off: %d\n", idxcell_off, datacell_off);
}

void vdbdata_data_write_overflow(uint8_t* buf, uint32_t idxcell_idx, uint32_t of_block_idx, uint32_t of_idxcell_idx) {
    uint32_t data_off = *((uint32_t*)(buf + VDB_PAGE_HDR_SIZE + sizeof(uint32_t) * idxcell_idx));

    *((uint32_t*)(buf + data_off + sizeof(uint32_t) * 0)) = of_block_idx;
    *((uint32_t*)(buf + data_off + sizeof(uint32_t) * 1)) = of_idxcell_idx;
}


/*
 * Other functions
 */
enum VdbNodeType vdbnode_read_type(uint8_t* buf) {
    return *((uint32_t*)(buf)); 
}

void vdbnode_write_type(uint8_t* buf, enum VdbNodeType type) {
    *((uint32_t*)(buf)) = (uint32_t)type;
}

uint32_t vdbnode_read_parent(uint8_t* buf) {
    return *((uint32_t*)(buf + sizeof(uint32_t)));
}

void vdbnode_write_parent(uint8_t* buf, uint32_t parent) {
    *((uint32_t*)(buf + sizeof(uint32_t))) = parent;
}

