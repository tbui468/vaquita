#ifndef VDB_H
#define VDB_H

#include <stdio.h>
#include <stdbool.h>
#include <stdint.h>

#include "pager.h"

typedef void* VDBHANDLE;

struct DB {
    char* name;
    FILE* table_files[128];
    char* table_names[128];
    uint32_t table_count;
    struct VdbPager* pager;
};


enum VdbField {
    VDBF_INT,
    VDBF_STR,
    VDBF_BOOL
};

struct VdbSchema {
    enum VdbField* fields;
    uint32_t count;
};

struct VdbString {
    char* start;
    uint32_t len;
};

struct VdbDatum {
    enum VdbField type;
    union {
        uint64_t Int;
        struct VdbString* Str;
        bool Bool;
    } as;
};

struct VdbRecord {
    struct VdbDatum* data;
    uint32_t count;
    uint32_t key;
};

VDBHANDLE vdb_create(const char* name);
VDBHANDLE vdb_open(const char* name);
void vdb_close(VDBHANDLE h);

void vdb_free_record(struct VdbRecord* data);
void vdb_serialize_record(uint8_t* buf, struct VdbRecord* rec);
struct VdbRecord vdb_deserialize_record(uint8_t* buf, struct VdbSchema* schema);
uint32_t vdb_get_rec_size(struct VdbRecord* rec);
struct VdbRecord* vdb_copy_record(struct VdbRecord* rec);

int vdb_create_table(VDBHANDLE h, const char* table, struct VdbSchema* schema);
int vdb_drop_table(VDBHANDLE h, const char* table);
int vdb_insert_record(VDBHANDLE h, const char* table, struct VdbRecord* d);
void vdb_debug_print_tree(VDBHANDLE h, const char* table);
struct VdbRecord* vdb_fetch_record(VDBHANDLE h, const char* table, uint32_t key);

#endif //VDB_H
