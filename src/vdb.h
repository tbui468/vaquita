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

struct VdbData {
    struct VdbDatum* data;
    uint32_t count;
};

VDBHANDLE vdb_create(const char* name);
VDBHANDLE vdb_open(const char* name);
void vdb_close(VDBHANDLE h);

int vdb_create_table(VDBHANDLE h, const char* table_name, struct VdbSchema* schema);
int vdb_drop_table(VDBHANDLE h, const char* table_name);
int vdb_insert_record(VDBHANDLE h, const char* table, struct VdbData* d);
struct VdbData* vdb_fetch_record(VDBHANDLE h, const char* table, uint32_t key);
void vdb_free_data(struct VdbData* data);
void vdb_debug_print_tree(VDBHANDLE h, const char* table);
void vdb_debug_print_keys(VDBHANDLE h, const char* table, uint32_t block_idx);

#endif //VDB_H
