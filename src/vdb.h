#ifndef VDB_H
#define VDB_H

#include <stdio.h>
#include <stdint.h>

#include "schema.h"
#include "record.h"

typedef void* VDBHANDLE;

struct Vdb {
    char* name;
    struct VdbTreeList* trees;
};

VDBHANDLE vdb_open(const char* name);
void vdb_close(VDBHANDLE h);

struct VdbSchema* vdb_alloc_schema(int count, ...);
void vdb_free_schema(struct VdbSchema* schema);

void vdb_create_table(VDBHANDLE h, const char* name, struct VdbSchema* schema);
void vdb_drop_table(VDBHANDLE h, const char* name);


void vdb_insert_record(VDBHANDLE h, const char* name, ...);
struct VdbRecord* vdb_fetch_record(VDBHANDLE h, const char* name, uint32_t key);
bool vdb_update_record(VDBHANDLE h, const char* name, uint32_t key, ...);
void vdb_debug_print_tree(VDBHANDLE h, const char* name);

/*
int vdb_insert_record(VDBHANDLE h, const char* table, struct VdbRecord* d);
struct VdbRecord* vdb_fetch_record(VDBHANDLE h, const char* table, uint32_t key);*/
//delete record
//update record

#endif //VDB_H
