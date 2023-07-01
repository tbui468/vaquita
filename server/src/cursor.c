#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "cursor.h"

struct VdbCursor* vdbcursor_init(struct VdbTree* tree, struct VdbValue key) {
    struct VdbCursor* cursor = malloc_w(sizeof(struct VdbCursor));

    cursor->tree = tree;

    struct VdbPage* meta_page = vdbpager_pin_page(tree->pager, tree->name, tree->f, 0);

    //get leaf
    uint32_t leaf_idx;
    if (*vdbmeta_last_leaf(meta_page->buf) != 0) { //not the first record being inserted
        struct VdbValue largest_key;
        vdbvalue_deserialize(&largest_key, vdbmeta_largest_key(meta_page->buf));
        vdbtree_deserialize_from_data_block_if_varlen(tree, &largest_key);

        //right append
        if (vdbvalue_compare(key, largest_key) > 0) {
            leaf_idx = *vdbmeta_last_leaf(meta_page->buf);
        } else {
            leaf_idx = vdb_tree_traverse_to(tree, vdbtree_meta_read_root(tree), key);
        }
    } else { //first record being inserted
        leaf_idx = vdb_tree_traverse_to(tree, vdbtree_meta_read_root(tree), key);
    }

    cursor->cur_rec_idx = 0;
    cursor->cur_node_idx = leaf_idx;


    while (cursor->cur_node_idx != 0 && cursor->cur_rec_idx >= vdbtree_leaf_read_record_count(cursor->tree, cursor->cur_node_idx)) {
        cursor->cur_rec_idx = 0;
        cursor->cur_node_idx = vdbtree_leaf_read_next_leaf(cursor->tree, cursor->cur_node_idx);
    }

    return cursor;
}

void vdbcursor_free(struct VdbCursor* cursor) {
    free_w(cursor, sizeof(struct VdbCursor));
}

struct VdbRecord* vdbcursor_fetch_record(struct VdbCursor* cursor) {
    if (cursor->cur_node_idx == 0) {
        return NULL;
    }

    struct VdbTree* tree = cursor->tree;
    struct VdbRecord* rec = vdbtree_leaf_read_record(tree, cursor->cur_node_idx, cursor->cur_rec_idx);

    cursor->cur_rec_idx++;

    while (cursor->cur_node_idx != 0 && cursor->cur_rec_idx >= vdbtree_leaf_read_record_count(tree, cursor->cur_node_idx)) {
        cursor->cur_rec_idx = 0;
        cursor->cur_node_idx = vdbtree_leaf_read_next_leaf(tree, cursor->cur_node_idx);
    }

    return rec;
}

void vdbcursor_insert_record(struct VdbCursor* cursor, struct VdbRecord* rec) {
    struct VdbTree* tree = cursor->tree;
    struct VdbValue rec_key = rec->data[tree->schema->key_idx];

    if (!vdbtree_leaf_can_fit_record(tree, cursor->cur_node_idx, rec)) {
        cursor->cur_node_idx = vdbtree_leaf_split(tree, cursor->cur_node_idx, rec_key);
    }

    struct VdbPage* meta_page = vdbpager_pin_page(tree->pager, tree->name, tree->f, 0);
    //sets largest key in meta block if necessary
    if (*vdbmeta_last_leaf(meta_page->buf) != 0) { //not the first record being inserted
        struct VdbValue largest_key;
        vdbvalue_deserialize(&largest_key, vdbmeta_largest_key(meta_page->buf));
        vdbtree_deserialize_from_data_block_if_varlen(tree, &largest_key);

        //right append
        if (vdbvalue_compare(rec_key, largest_key) > 0) {
            vdbtree_serialize_to_data_block_if_varlen(tree, &rec_key);
            vdbvalue_serialize(vdbmeta_largest_key(meta_page->buf), rec_key);
        }
    } else { //first record being inserted
        vdbtree_serialize_to_data_block_if_varlen(tree, &rec_key);
        vdbvalue_serialize(vdbmeta_largest_key(meta_page->buf), rec_key);

        //Need to traverse again since cursor init will result in a node/record index of 0
        //when vdbcursor_init is called.  vdcursor_init will skip any leaves with 0 records
        //since records may be deleted.  
        cursor->cur_node_idx = vdb_tree_traverse_to(tree, vdbtree_meta_read_root(tree), rec_key);
    }

    *vdbmeta_last_leaf(meta_page->buf) = cursor->cur_node_idx;
    meta_page->dirty = true;
    vdbpager_unpin_page(meta_page);

    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->name, tree->f, cursor->cur_node_idx);
    page->dirty = true;

    //binary search
    uint32_t i = 0;
    uint32_t left = 0;
    uint32_t right = vdbtree_leaf_read_record_count(tree, cursor->cur_node_idx);
    while (right > 0) {
        i = (right - left) / 2 + left;
        struct VdbValue key = vdbtree_leaf_read_record_key(tree, cursor->cur_node_idx, i);

        //record key is smaller than key at i
        int result = vdbvalue_compare(rec_key, key);
        if (result < 0) {
            if (right - left <= 1) {
                break;
            }
            right = i;
        //record key is larger than key at i
        } else if (result > 0) {
            if (right - left <= 1) {
                i++;
                break;
            }
            left = i;
        } else {
            assert(false && "duplicate keys not allowed");
        }

    }

    vdbleaf_insert_record_cell(page->buf, i, vdbrecord_serialized_size(rec));
    vdbtree_leaf_write_record(tree, cursor->cur_node_idx, i, rec);

    vdbpager_unpin_page(page);
}

bool vdbcursor_record_passes_selection(struct VdbCursor* cursor, struct VdbExpr* selection) {
    struct VdbRecord* rec = vdbtree_leaf_read_record(cursor->tree, cursor->cur_node_idx, cursor->cur_rec_idx);
    struct VdbRecordSet* rs = vdbrecordset_init(NULL);
    vdbrecordset_append_record(rs, rec);

    struct VdbValue v = vdbexpr_eval(selection, rs, cursor->tree->schema);
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

    struct VdbTree* tree = cursor->tree;
    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->name, tree->f, cursor->cur_node_idx);
    page->dirty = true;

    vdbleaf_delete_idxcell(page->buf, cursor->cur_rec_idx);

    vdbpager_unpin_page(page);

    if (cursor->cur_rec_idx >= vdbtree_leaf_read_record_count(tree, cursor->cur_node_idx)) {
        cursor->cur_rec_idx = 0;
        cursor->cur_node_idx = vdbtree_leaf_read_next_leaf(tree, cursor->cur_node_idx);
    }

}

void vdbcursor_update_record(struct VdbCursor* cursor, struct VdbTokenList* attrs, struct VdbExprList* values) {
    struct VdbTree* tree = cursor->tree;
    struct VdbValueList* vl = vdbvaluelist_init();

    for (int i = 0; i < values->count; i++) {
        struct VdbValue v = vdbexpr_eval(values->exprs[i], NULL, NULL);
        vdbvaluelist_append_value(vl, v);
    }

    struct VdbPage* page = vdbpager_pin_page(tree->pager, tree->name, tree->f, cursor->cur_node_idx);
    page->dirty = true;

    struct VdbRecord* rec = vdbtree_leaf_read_record(tree, cursor->cur_node_idx, cursor->cur_rec_idx);

    for (int i = 0; i < attrs->count; i++) {
        for (uint32_t j = 0; j < tree->schema->count; j++) {
            if (strncmp(attrs->tokens[i].lexeme, tree->schema->names[j], attrs->tokens[i].len) == 0) {
                vdbvalue_free(rec->data[j]);
                rec->data[j] = vdbvalue_copy(vl->values[i]);
            }
        }
    }

    vdbtree_leaf_write_record(tree, cursor->cur_node_idx, cursor->cur_rec_idx, rec);

    vdbrecord_free(rec);
    vdbpager_unpin_page(page);
    vdbvaluelist_free(vl);

    cursor->cur_rec_idx++;
    if (cursor->cur_rec_idx >= vdbtree_leaf_read_record_count(tree, cursor->cur_node_idx)) {
        cursor->cur_rec_idx = 0;
        cursor->cur_node_idx = vdbtree_leaf_read_next_leaf(tree, cursor->cur_node_idx);
    }
}

bool vdbcursor_apply_selection(struct VdbCursor* cursor, struct VdbRecordSet* rs, struct VdbExpr* selection) {
    struct VdbValue v = vdbexpr_eval(selection, rs, cursor->tree->schema);
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
                    data[i] = vdbexpr_eval(projection->exprs[i], cur, cursor->tree->schema);
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
    struct VdbValue result = vdbexpr_eval(expr, rs, cursor->tree->schema);

    return result.as.Bool;
}

void vdbcursor_apply_limit(struct VdbCursor* cursor, struct VdbRecordSet* rs, struct VdbExpr* expr) {
    if (expr == NULL)
        return;

    struct VdbValue limit = vdbexpr_eval(expr, rs, cursor->tree->schema);

    if (limit.as.Int >= rs->count)
        return;

    for (uint32_t i = limit.as.Int; i < rs->count; i++) {
        vdbrecord_free(rs->records[i]);
    }

    rs->count = limit.as.Int;
}

struct VdbByteList* vdbcursor_key_from_cols(struct VdbCursor* cursor, struct VdbRecordSet* rs, struct VdbExprList* cols) {
    struct VdbByteList* bl = vdbbytelist_init();

    for (int i = 0; i < cols->count; i++) {
        struct VdbValue v = vdbexpr_eval(cols->exprs[i], rs, cursor->tree->schema);
        if (v.type == VDBT_TYPE_STR) {
            vdbbytelist_resize(bl, vdbvalue_serialized_string_size(v));
            bl->count += vdbvalue_serialize_string(bl->values + bl->count, &v);
        } else {
            vdbbytelist_resize(bl, vdbvalue_serialized_size(v));
            bl->count += vdbvalue_serialize(bl->values + bl->count, v);
        }
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
    struct VdbTree* tree = cursor->tree;

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


