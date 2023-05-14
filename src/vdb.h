#ifndef VDB_H
#define VDB_H

#include <stdio.h>
#include <stdint.h>

#include "schema.h"
#include "record.h"
#include "pager.h"
#include "tree.h"
#include "error.h"

typedef void* VDBHANDLE;

struct Vdb {
    char* name;
    struct VdbPager* pager;
    struct VdbTreeList* trees;
};

VDBHANDLE vdb_open_db(const char* name);
enum VdbReturnCode vdb_create_db(const char* name);

enum VdbReturnCode vdb_show_dbs(char*** dbs, int* count);
enum VdbReturnCode vdb_show_tabs(VDBHANDLE h, char*** tabs, int* count);
bool vdb_close(VDBHANDLE h);
char* vdb_dbname(VDBHANDLE h);

enum VdbReturnCode vdb_drop_db(const char* name);

struct VdbSchema* vdb_alloc_schema(int count, ...);
void vdb_free_schema(struct VdbSchema* schema);

bool vdb_create_table(VDBHANDLE h, const char* name, struct VdbSchema* schema);
enum VdbReturnCode vdb_drop_table(VDBHANDLE h, const char* name);

void vdb_insert_record(VDBHANDLE h, const char* name, ...);
struct VdbRecord* vdb_fetch_record(VDBHANDLE h, const char* name, uint32_t key);
bool vdb_delete_record(VDBHANDLE h, const char* name, uint32_t key);
bool vdb_update_record(VDBHANDLE h, const char* name, uint32_t key, ...);
void vdb_debug_print_tree(VDBHANDLE h, const char* name);


#endif //VDB_H
