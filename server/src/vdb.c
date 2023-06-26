#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "vdb.h"
#include "schema.h"
#include "record.h"
#include "util.h"
#include "tree.h"

struct VdbCursor* vdbcursor_init(VDBHANDLE h, const char* table_name, struct VdbValue key) {
    struct VdbCursor* cursor = malloc_w(sizeof(struct VdbCursor));
    cursor->db = (struct Vdb*)h;
    cursor->table_name = strdup_w(table_name);

    struct VdbTree* tree = vdb_treelist_get_tree(cursor->db->trees, table_name);
    uint32_t root_idx = vdbtree_meta_read_root(tree);
    cursor->cur_node_idx = vdb_tree_traverse_to(tree, root_idx, key);
    cursor->cur_rec_idx = 0;

    while (cursor->cur_node_idx != 0 && cursor->cur_rec_idx >= vdbtree_leaf_read_record_count(tree, cursor->cur_node_idx)) {
        cursor->cur_rec_idx = 0;
        cursor->cur_node_idx = vdbtree_leaf_read_next_leaf(tree, cursor->cur_node_idx);
    }

    //TODO: should increment cursor all the way to key

    return cursor;
}

void vdbcursor_free(struct VdbCursor* cursor) {
    free_w(cursor->table_name, sizeof(char) * (strlen(cursor->table_name) + 1)); //include null terminator
    free_w(cursor, sizeof(struct VdbCursor));
}

struct VdbRecord* vdbcursor_fetch_record(struct VdbCursor* cursor) {
    if (cursor->cur_node_idx == 0) {
        return NULL;
    }

    struct VdbTree* tree = vdb_treelist_get_tree(cursor->db->trees, cursor->table_name);
    struct VdbRecord* rec = vdbtree_leaf_read_record(tree, cursor->cur_node_idx, cursor->cur_rec_idx);

    cursor->cur_rec_idx++;

    while (cursor->cur_node_idx != 0 && cursor->cur_rec_idx >= vdbtree_leaf_read_record_count(tree, cursor->cur_node_idx)) {
        cursor->cur_rec_idx = 0;
        cursor->cur_node_idx = vdbtree_leaf_read_next_leaf(tree, cursor->cur_node_idx);
    }

    return rec;
}


bool vdbcursor_record_passes_selection(struct VdbCursor* cursor, struct VdbExpr* selection) {
    struct VdbTree* tree = vdb_treelist_get_tree(cursor->db->trees, cursor->table_name);

    struct VdbRecord* rec = vdbtree_leaf_read_record(tree, cursor->cur_node_idx, cursor->cur_rec_idx);
    struct VdbRecordSet* rs = vdbrecordset_init(NULL);
    vdbrecordset_append_record(rs, rec);

    struct VdbValue v = vdbexpr_eval(selection, rs, tree->schema);
    bool result;
    if (v.type == VDBT_TYPE_NULL) {
        result = false;
    } else if (v.type == VDBT_TYPE_BOOL) {
        result = v.as.Bool;
    } else {
        assert(false && "condition is not boolean expression");
        result = false;
    }

    vdbrecordset_free(rs);

    return result;
}

void vdbcursor_delete_record(struct VdbCursor* cursor) {
    if (cursor->cur_node_idx == 0) {
        return;
    }

    struct VdbTree* tree = vdb_treelist_get_tree(cursor->db->trees, cursor->table_name);
    vdbtree_leaf_delete_record(tree, cursor->cur_node_idx, cursor->cur_rec_idx);

    if (cursor->cur_rec_idx >= vdbtree_leaf_read_record_count(tree, cursor->cur_node_idx)) {
        cursor->cur_rec_idx = 0;
        cursor->cur_node_idx = vdbtree_leaf_read_next_leaf(tree, cursor->cur_node_idx);
    }

}

void vdbcursor_update_record(struct VdbCursor* cursor, struct VdbTokenList* attributes, struct VdbExprList* values) {
    struct VdbTree* tree = vdb_treelist_get_tree(cursor->db->trees, cursor->table_name);
    struct VdbValueList* vl = vdbvaluelist_init();

    for (int i = 0; i < values->count; i++) {
        struct VdbValue v = vdbexpr_eval(values->exprs[i], NULL, NULL);
        vdbvaluelist_append_value(vl, v);
    }

    vdbtree_leaf_update_record(tree, cursor->cur_node_idx, cursor->cur_rec_idx, attributes, vl);

    vdbvaluelist_free(vl);

    cursor->cur_rec_idx++;
    if (cursor->cur_rec_idx >= vdbtree_leaf_read_record_count(tree, cursor->cur_node_idx)) {
        cursor->cur_rec_idx = 0;
        cursor->cur_node_idx = vdbtree_leaf_read_next_leaf(tree, cursor->cur_node_idx);
    }
}

bool vdbcursor_apply_selection(struct VdbCursor* cursor, struct VdbRecordSet* rs, struct VdbExpr* selection) {
    struct VdbTree* tree = vdb_treelist_get_tree(cursor->db->trees, cursor->table_name);

    struct VdbValue v = vdbexpr_eval(selection, rs, tree->schema);
    bool result;
    if (v.type == VDBT_TYPE_NULL) {
        result = false;
    } else if (v.type == VDBT_TYPE_BOOL) {
        result = v.as.Bool;
    } else {
        assert(false && "condition is not boolean expression");
        result = false;
    }

    return result;
}

struct VdbRecordSet* vdbcursor_apply_projection(struct VdbCursor* cursor, struct VdbRecordSet* head, struct VdbExprList* projection, bool aggregate) {
    struct VdbTree* tree = vdb_treelist_get_tree(cursor->db->trees, cursor->table_name);

    struct VdbRecordSet* final = vdbrecordset_init(NULL);
    struct VdbRecordSet* cur = head;

    while (cur) {
        uint32_t max = aggregate ? 1 : cur->count;
        for (uint32_t i = 0; i < max; i++) {
            struct VdbRecord* rec = cur->records[i];
            struct VdbExpr* expr = projection->exprs[0];
            if (expr->type != VDBET_IDENTIFIER || expr->as.identifier.token.type != VDBT_STAR) { //Skip if projection is *
                struct VdbValue* data = malloc_w(sizeof(struct VdbValue) * projection->count);
                for (int i = 0; i < projection->count; i++) {
                    data[i] = vdbexpr_eval(projection->exprs[i], cur, tree->schema);
                }

                free_w(rec->data, sizeof(struct VdbValue) * rec->count);
                rec->data = data;
                rec->count = projection->count;
            }
            vdbrecordset_append_record(final, rec);
        }

        cur = cur->next;
    }

    return final;
}

bool vdbcursor_apply_having(struct VdbCursor* cursor, struct VdbRecordSet* rs, struct VdbExpr* expr) {
    struct VdbTree* tree = vdb_treelist_get_tree(cursor->db->trees, cursor->table_name);

    struct VdbValue result = vdbexpr_eval(expr, rs, tree->schema);

    return result.as.Bool;
}

void vdbcursor_apply_limit(struct VdbCursor* cursor, struct VdbRecordSet* rs, struct VdbExpr* expr) {
    struct VdbTree* tree = vdb_treelist_get_tree(cursor->db->trees, cursor->table_name);
    if (expr == NULL)
        return;

    struct VdbValue limit = vdbexpr_eval(expr, rs, tree->schema);

    if (limit.as.Int >= rs->count)
        return;

    for (uint32_t i = limit.as.Int; i < rs->count; i++) {
        vdbrecord_free(rs->records[i]);
    }

    rs->count = limit.as.Int;
}

struct VdbByteList* vdbcursor_key_from_cols(struct VdbCursor* cursor, struct VdbRecordSet* rs, struct VdbExprList* cols) {
    struct VdbTree* tree = vdb_treelist_get_tree(cursor->db->trees, cursor->table_name);

    struct VdbByteList* bl = vdbbytelist_init();

    for (int i = 0; i < cols->count; i++) {
        struct VdbValue v = vdbexpr_eval(cols->exprs[i], rs, tree->schema);
        vdbbytelist_serialize_data(bl, v, vdbvalue_serialize, vdbvalue_serialized_size);
    }

    return bl;
}

struct VdbRsPtrList {
    struct VdbRecordSet** ps;
    int capacity;
    int count;
};

struct VdbRsPtrList* vdbrsptrlist_init() {
    struct VdbRsPtrList* pl = malloc_w(sizeof(struct VdbRsPtrList));
    pl->capacity = 8;
    pl->count = 0;
    pl->ps = malloc_w(sizeof(struct VdbRecordSet*) * pl->capacity);
    return pl;
}

void vdbrsptrlist_free(struct VdbRsPtrList* pl) {
    free_w(pl->ps, pl->capacity * sizeof(struct VdbRecordSet*));
    free_w(pl, sizeof(struct VdbRsPtrList));
}

void vdbrsptrlist_enqueue(struct VdbRsPtrList* pl, struct VdbRecordSet* rs) {
    if (pl->count + 1 > pl->capacity) {
        int old_cap = pl->capacity;
        pl->capacity *= 2;
        pl->ps = realloc_w(pl->ps, sizeof(struct VdbRecordSet*) * pl->capacity, sizeof(struct VdbRecordSet*) * old_cap);
    }

    pl->ps[pl->count] = rs;
    pl->count++;
}

struct VdbRecordSet* vdbrsptrlist_dequeue(struct VdbRsPtrList* pl) {
    struct VdbRecordSet* result = pl->ps[0];

    memmove(pl->ps, pl->ps + 1, sizeof(struct VdbRecordSet*) * (pl->count - 1));
    pl->count--;

    return result;
}

int vdb_compare_cols(struct VdbCursor* cursor, struct VdbRecordSet* left, struct VdbRecordSet* right, struct VdbExprList* ordering_cols) {
    struct VdbTree* tree = vdb_treelist_get_tree(cursor->db->trees, cursor->table_name);

    for (int i = 0; i < ordering_cols->count; i++) {
        struct VdbValue lv = vdbexpr_eval(ordering_cols->exprs[i], left, tree->schema);
        struct VdbValue rv = vdbexpr_eval(ordering_cols->exprs[i], right, tree->schema);
        int result = vdbvalue_compare(lv, rv);
        if (result != 0)
            return result;
    }


    return vdbvalue_compare(left->records[0]->data[tree->schema->key_idx], right->records[0]->data[tree->schema->key_idx]);
}

//TODO: external merge sort is recommended in literature since the entire tuple set may not fit into memory
//Assuming entire tuple set fits into memory for now.  Should switch to external merge sort when needed
void vdbcursor_sort_linked_list(struct VdbCursor* cursor, struct VdbRecordSet** head, struct VdbExprList* ordering_cols, bool order_desc) {
    if (!(*head))
        return;

    struct VdbRsPtrList* pl = vdbrsptrlist_init();
    struct VdbRecordSet* cur = *head;

    while (cur) {
        struct VdbRecordSet* p = cur;
        cur = cur->next;
        p->next = NULL;
        vdbrsptrlist_enqueue(pl, p);
    }

    while (pl->count > 1) {
        struct VdbRecordSet* left = vdbrsptrlist_dequeue(pl);
        struct VdbRecordSet* right = vdbrsptrlist_dequeue(pl);

        struct VdbRecordSet* merged = NULL;
        struct VdbRecordSet** tail = &merged;
        struct VdbRecordSet* cur_l = left;
        struct VdbRecordSet* cur_r = right;

        while (true) {
            if (!cur_l) {
                *tail = cur_r;
                break;
            }
            if (!cur_r) {
                *tail = cur_l;
                break;
            }

            int result = vdb_compare_cols(cursor, cur_l, cur_r, ordering_cols);
            if (order_desc) {
                if (result >= 0) {
                    *tail = cur_l;
                    cur_l = cur_l->next;
                } else {
                    *tail = cur_r;
                    cur_r = cur_r->next;
                }
            } else {
                if (result <= 0) {
                    *tail = cur_l;
                    cur_l = cur_l->next;
                } else {
                    *tail = cur_r;
                    cur_r = cur_r->next;
                }
            }

            tail = &(*tail)->next;
        }

        vdbrsptrlist_enqueue(pl, merged);
    }

    *head = pl->ps[0];

    vdbrsptrlist_free(pl);
}


