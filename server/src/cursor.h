#ifndef VDB_CURSOR_H
#define VDB_CURSOR_H

#include <stdint.h>

#include "tree.h"
#include "parser.h"
#include "util.h"

struct VdbCursor {
    struct VdbTree* tree;
    uint32_t cur_node_idx;
    uint32_t cur_rec_idx;
};

struct VdbCursor* vdbcursor_init(struct VdbTree* tree);
void vdbcursor_seek(struct VdbCursor* cursor, struct VdbValue key);
bool vdbcursor_at_end(struct VdbCursor* cursor);
void vdbcursor_free(struct VdbCursor* cursor);
struct VdbRecord* vdbcursor_fetch_record(struct VdbCursor* cursor);
void vdbcursor_insert_record(struct VdbCursor* cursor, struct VdbRecord* rec);
void vdbcursor_delete_record(struct VdbCursor* cursor);
void vdbcursor_update_record(struct VdbCursor* cursor, struct VdbTokenList* attributes, struct VdbExprList* values);

bool vdbcursor_apply_selection(struct VdbCursor* cursor, struct VdbRecordSet* rs, struct VdbExpr* selection);
bool vdbcursor_record_passes_selection(struct VdbCursor* cursor, struct VdbExpr* selection);
struct VdbRecordSet* vdbcursor_apply_projection(struct VdbCursor* cursor, struct VdbRecordSet* head, struct VdbExprList* projection, bool aggregate);
bool vdbcursor_apply_having(struct VdbCursor* cursor, struct VdbRecordSet* rs, struct VdbExpr* expr);
void vdbcursor_apply_limit(struct VdbCursor* cursor, struct VdbRecordSet* rs, struct VdbExpr* expr);
struct VdbByteList* vdbcursor_key_from_cols(struct VdbCursor* cursor, struct VdbRecordSet* rs, struct VdbExprList* cols);
void vdbcursor_sort_linked_list(struct VdbCursor* cursor, struct VdbRecordSet** head, struct VdbExprList* ordering_cols, bool order_desc);


#endif //VDB_CURSOR_H
