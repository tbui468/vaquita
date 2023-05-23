#ifndef VDB_H
#define VDB_H

#include <stdio.h>
#include <stdint.h>

#include "schema.h"
#include "record.h"
#include "pager.h"
#include "tree.h"
#include "error.h"
#include "parser.h"


typedef void* VDBHANDLE;

struct Vdb {
    char* name;
    struct VdbPager* pager;
    struct VdbTreeList* trees;
};

struct VdbCursor {
    struct Vdb* db;
    char* table_name;
    uint32_t cur_node_idx;
    uint32_t cur_rec_idx;
};

VDBHANDLE vdb_open_db(const char* name);
enum VdbReturnCode vdb_create_db(const char* name);

enum VdbReturnCode vdb_show_dbs(struct VdbValueList** vl);
enum VdbReturnCode vdb_show_tabs(VDBHANDLE h, struct VdbValueList** vl);
bool vdb_close(VDBHANDLE h);
char* vdb_dbname(VDBHANDLE h);

enum VdbReturnCode vdb_describe_table(VDBHANDLE h, const char* name, struct VdbValueList** vl);

enum VdbReturnCode vdb_drop_db(const char* name);

struct VdbSchema* vdb_alloc_schema(int count, ...);
void vdb_free_schema(struct VdbSchema* schema);

bool vdb_create_table(VDBHANDLE h, const char* name, struct VdbSchema* schema);
enum VdbReturnCode vdb_drop_table(VDBHANDLE h, const char* name);

enum VdbReturnCode vdb_insert_new(VDBHANDLE h, const char* name, struct VdbTokenList* attrs, struct VdbExprList* values);
void vdb_insert_record(VDBHANDLE h, const char* name, ...);

struct VdbRecord* vdb_fetch_record(VDBHANDLE h, const char* name, uint32_t key);
bool vdb_delete_record(VDBHANDLE h, const char* name, uint32_t key);
bool vdb_update_record(VDBHANDLE h, const char* name, uint32_t key, ...);
void vdb_debug_print_tree(VDBHANDLE h, const char* name);

struct VdbCursor* vdbcursor_init(VDBHANDLE h, const char* table_name, uint32_t key);
void vdbcursor_free(struct VdbCursor* cursor);
bool vdbcursor_on_final_record(struct VdbCursor* cursor);
struct VdbRecord* vdbcursor_read_record(struct VdbCursor* cursor);
void vdbcursor_increment(struct VdbCursor* cursor);

bool vdbcursor_apply_selection(struct VdbCursor* cursor, struct VdbRecord* rec, struct VdbExpr* selection);
void vdbcursor_apply_projection(struct VdbCursor* cursor, struct VdbRecord* rec, struct VdbTokenList* projection);

#endif //VDB_H
