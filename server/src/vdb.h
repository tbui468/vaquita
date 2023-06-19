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
#include "util.h"


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

struct Vdb* vdb_init();
void vdb_free(struct Vdb* db);

VDBHANDLE vdb_open_db(const char* name);
enum VdbReturnCode vdb_create_db(const char* name);

enum VdbReturnCode vdb_show_dbs(struct VdbValueList** vl);
enum VdbReturnCode vdb_show_tabs(VDBHANDLE h, struct VdbValueList** vl);
bool vdb_close(VDBHANDLE h);
char* vdb_dbname(VDBHANDLE h);

enum VdbReturnCode vdb_describe_table(VDBHANDLE h, const char* name, struct VdbValueList** vl);

enum VdbReturnCode vdb_drop_db(const char* name);

void vdb_free_schema(struct VdbSchema* schema);

bool vdb_create_table(VDBHANDLE h, const char* name, struct VdbSchema* schema);
enum VdbReturnCode vdb_drop_table(VDBHANDLE h, const char* name);

enum VdbReturnCode vdb_insert_new(VDBHANDLE h, const char* name, struct VdbTokenList* attrs, struct VdbExprList* values);
void vdb_insert_record(VDBHANDLE h, const char* name, ...);

bool vdb_delete_record(VDBHANDLE h, const char* name, uint32_t key);
bool vdb_update_record(VDBHANDLE h, const char* name, uint32_t key, ...);
void vdb_debug_print_tree(VDBHANDLE h, const char* name);

struct VdbCursor* vdbcursor_init(VDBHANDLE h, const char* table_name, struct VdbValue key);
void vdbcursor_free(struct VdbCursor* cursor);
struct VdbRecord* vdbcursor_fetch_record(struct VdbCursor* cursor);
void vdbcursor_delete_record(struct VdbCursor* cursor);
void vdbcursor_update_record(struct VdbCursor* cursor, struct VdbTokenList* attributes, struct VdbExprList* values);

bool vdbcursor_apply_selection(struct VdbCursor* cursor, struct VdbRecordSet* rs, struct VdbExpr* selection);
bool vdbcursor_record_passes_selection(struct VdbCursor* cursor, struct VdbExpr* selection);
struct VdbRecordSet* vdbcursor_apply_projection(struct VdbCursor* cursor, struct VdbRecordSet* head, struct VdbExprList* projection, bool aggregate);
bool vdbcursor_apply_having(struct VdbCursor* cursor, struct VdbRecordSet* rs, struct VdbExpr* expr);
void vdbcursor_apply_limit(struct VdbCursor* cursor, struct VdbRecordSet* rs, struct VdbExpr* expr);
struct VdbByteList* vdbcursor_key_from_cols(struct VdbCursor* cursor, struct VdbRecordSet* rs, struct VdbExprList* cols);
void vdbcursor_sort_linked_list(struct VdbCursor* cursor, struct VdbRecordSet** head, struct VdbExprList* ordering_cols, bool order_desc);


#endif //VDB_H
