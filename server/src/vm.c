#include <string.h>

#include "vm.h"
#include "util.h"
#include "lexer.h"
#include "binarytree.h"
#include "hashtable.h"

static char* to_static_string(struct VdbToken t) {
    static char s[FILENAME_MAX];
    memcpy(s, t.lexeme, t.len);
    s[t.len] = '\0';
    return s;
}

//TODO: need an generator that converst the stmtlist to bytecode
//Should go in vm.c
bool vdb_execute(struct VdbStmtList* sl, VDBHANDLE* h, struct VdbString* output) {
    for (int i = 0; i < sl->count; i++) {
        struct VdbStmt* stmt = &sl->stmts[i];
        switch (stmt->type) {
            case VDBST_CONNECT: {
                //TODO
                //should return a connection handle
                break;
            }
            case VDBST_SHOW_DBS: {
                struct VdbValueList* vl;
                if (vdb_show_dbs(&vl) == VDBRC_SUCCESS) {
                    vdbstring_concat(output, "databases:\n");
                    for (int i = 0; i < vl->count; i++) {
                        vdbstring_concat(output, "\t%.*s\n", vl->values[i].as.Str.len, vl->values[i].as.Str.start);
                    }
                    vdbvaluelist_free(vl);
                } else {
                    vdbstring_concat(output, "execution error: cannot show databases\n");
                }
                break;
            }
            case VDBST_SHOW_TABS: {
                struct VdbValueList* vl;
                if (vdb_show_tabs(*h, &vl) == VDBRC_SUCCESS) {
                    vdbstring_concat(output, "tables:\n");
                    for (int i = 0; i < vl->count; i++) {
                        vdbstring_concat(output, "\t%.*s\n", vl->values[i].as.Str.len, vl->values[i].as.Str.start);
                    }
                    vdbvaluelist_free(vl);
                } else {
                    vdbstring_concat(output, "execution error: cannot show tables\n");
                }
                break;
            }
            case VDBST_CREATE_DB: {
                char* db_name = to_static_string(stmt->target);
                if (vdb_create_db(db_name) == VDBRC_SUCCESS) {
                    vdbstring_concat(output, "created database %s\n", db_name);
                } else {
                    vdbstring_concat(output, "failed to create database %s\n", db_name);
                }
                break;
            }
            case VDBST_CREATE_TAB: {
                int count = stmt->as.create.attributes->count;

                struct VdbSchema* schema = vdbschema_alloc(count, stmt->as.create.attributes, stmt->as.create.types, stmt->as.create.key_idx);

                char* table_name = to_static_string(stmt->target);
                if (vdb_create_table(*h, table_name, schema)) {
                    vdbstring_concat(output, "created table %s\n", table_name);
                } else {
                    vdbstring_concat(output, "failed to create table %s\n", table_name);
                }

                vdb_free_schema(schema);
                break;
            }
            case VDBST_IF_EXISTS_DROP_DB: {
                bool found = false;
                char* db_name = to_static_string(stmt->target);
                struct VdbValueList *vl;
                if (vdb_show_dbs(&vl) == VDBRC_SUCCESS) {
                    for (int i = 0; i < vl->count; i++) {
                        if (strncmp(vl->values[i].as.Str.start, db_name, strlen(db_name)) == 0) {
                            found = true;
                        }
                    }
                    vdbvaluelist_free(vl);

                    if (found) {
                        if (vdb_drop_db(db_name) == VDBRC_SUCCESS) {
                            vdbstring_concat(output, "dropped database %s\n", db_name);
                        } else {
                            vdbstring_concat(output, "failed to drop database %s\n", db_name);
                        }
                    }
                }
                break;
            }
            case VDBST_IF_EXISTS_DROP_TAB: {
                bool found = false;
                char* tab_name = to_static_string(stmt->target);
                struct VdbValueList* vl;
                if (vdb_show_tabs(*h, &vl) == VDBRC_SUCCESS) {
                    for (int i = 0; i < vl->count; i++) {
                        if (strncmp(vl->values[i].as.Str.start, tab_name, strlen(tab_name)) == 0) {
                            found = true;
                        }
                    }
                    vdbvaluelist_free(vl);

                    if (found) {
                        if (vdb_drop_table(*h, tab_name) == VDBRC_SUCCESS) {
                            vdbstring_concat(output, "dropped table %s\n", tab_name);
                        } else {
                            vdbstring_concat(output, "failed to drop table %s\n", tab_name);
                        }
                    }
                }
                break;
            }
            case VDBST_DROP_DB: {
                char* db_name = to_static_string(stmt->target);
                if (vdb_drop_db(db_name) == VDBRC_SUCCESS) {
                    vdbstring_concat(output, "dropped database %s\n", db_name);
                } else {
                    vdbstring_concat(output, "failed to drop database %s\n", db_name);
                }
                break;
            }
            case VDBST_DROP_TAB: {
                char* table_name = to_static_string(stmt->target);
                if (vdb_drop_table(*h, table_name) == VDBRC_SUCCESS) {
                    vdbstring_concat(output, "dropped table %s\n", table_name);
                } else {
                    vdbstring_concat(output, "failed to drop table %s\n", table_name);
                }
                break;
            }
            case VDBST_OPEN: {
                char* db_name = to_static_string(stmt->target);
                if ((*h = vdb_open_db(db_name))) {
                    vdbstring_concat(output, "opened database %s\n", db_name);
                } else {
                    vdbstring_concat(output, "failed to open database %s\n", db_name);
                }
                break;
            }
            case VDBST_CLOSE: {
                char* db_name = to_static_string(stmt->target);
                if (vdb_close(*h)) {
                    vdbstring_concat(output, "closed database %s\n", db_name);
                    *h = NULL;
                } else {
                    vdbstring_concat(output, "failed to close database %s\n", db_name);
                }
                break;
            }
            case VDBST_DESCRIBE: {
                char* table_name = to_static_string(stmt->target);
                struct VdbValueList* vl;
                if (vdb_describe_table(*h, table_name, &vl) == VDBRC_SUCCESS) {
                    vdbstring_concat(output, "%s:\n", table_name);
                    for (int i = 0; i < vl->count; i += 2) {
                        struct VdbValue attr = vl->values[i];
                        struct VdbValue type = vl->values[i + 1];
                        vdbstring_concat(output, "\t%.*s: %.*s\n", attr.as.Str.len, attr.as.Str.start, type.as.Str.len, type.as.Str.start);
                    }
                    vdbvaluelist_free(vl);
                } else {
                    vdbstring_concat(output, "failed to describe table %s\n", table_name);
                }
                break;
            }
            case VDBST_INSERT: {
                char* table_name = to_static_string(stmt->target);
                int rec_count = stmt->as.insert.values->count / stmt->as.insert.attributes->count;

                for (int i = 0; i < rec_count; i++) {
                    struct VdbExprList* el = vdbexprlist_init();
                    int off = i * stmt->as.insert.attributes->count;
                    for (int j = off; j < off + stmt->as.insert.attributes->count; j++) {
                        vdbexprlist_append_expr(el, stmt->as.insert.values->exprs[j]);
                    }
                    vdb_insert_new(*h, table_name, stmt->as.insert.attributes, el);

                    //not calling vdbexprlist_free(el) since the expressions will be freed when stmt is freed
                    free_w(el->exprs, sizeof(struct VdbExpr*) * el->capacity);
                    free_w(el, sizeof(struct VdbExprList));
                }
                vdbstring_concat(output, "inserted %d record(s) into %s\n", rec_count, table_name);
                break;
            }
            case VDBST_UPDATE: {
                char* table_name = to_static_string(stmt->target);
                struct VdbCursor* cursor = vdbcursor_init(*h, table_name, vdbint(0)); //cursor to begining of table

                while (cursor->cur_node_idx != 0) {
                    if (vdbcursor_record_passes_selection(cursor, stmt->as.update.selection)) {
                        vdbcursor_update_record(cursor, stmt->as.update.attributes, stmt->as.update.values);
                    } else {
                        struct VdbRecord* rec = vdbcursor_fetch_record(cursor);
                        vdbrecord_free(rec);
                    }
                }

                vdbcursor_free(cursor);
                break;
            }
            case VDBST_DELETE: {
                char* table_name = to_static_string(stmt->target);
                struct VdbCursor* cursor = vdbcursor_init(*h, table_name, vdbint(0)); //cursor to begining of table

                while (cursor->cur_node_idx != 0) {
                    if (vdbcursor_record_passes_selection(cursor, stmt->as.delete.selection)) {
                        vdbcursor_delete_record(cursor);
                    } else {
                        struct VdbRecord* rec = vdbcursor_fetch_record(cursor);
                        vdbrecord_free(rec);
                    }
                }

                vdbcursor_free(cursor);
                break;
            }
            case VDBST_SELECT: {
                char* table_name = to_static_string(stmt->target);

                struct VdbCursor* cursor = vdbcursor_init(*h, table_name, vdbint(0)); //cursor to beginning of table

                struct VdbBinaryTree* bt = vdbbinarytree_init();
                struct VdbHashTable* distinct_table = vdbhashtable_init();
                struct VdbHashTable* grouping_table = vdbhashtable_init();

                struct VdbRecord* rec;

                while ((rec = vdbcursor_fetch_record(cursor))) {
                    struct VdbRecordSet* rs = vdbrecordset_init(NULL);
                    vdbrecordset_append_record(rs, rec);
                    if (stmt->as.select.grouping->count == 0) {
                        if (vdbcursor_apply_selection(cursor, rs, stmt->as.select.selection)) { //applying select to individual record
                            if (stmt->as.select.distinct) {
                                struct VdbByteList* key = vdbcursor_key_from_cols(cursor, rs, stmt->as.select.projection);
                                if (!vdbhashtable_contains_key(distinct_table, key)) {
                                    vdbhashtable_insert_entry(distinct_table, key, rec); //using hashtable to check for duplicates bc of 'distinct' keyword
                                    rs->key = vdbcursor_key_from_cols(cursor, rs, stmt->as.select.ordering);
                                    vdbbinarytree_insert_node(bt, rs);
                                }
                            } else {
                                rs->key = vdbcursor_key_from_cols(cursor, rs, stmt->as.select.ordering);
                                //single record in record set - probably not the most efficient, but it works for now
                                vdbbinarytree_insert_node(bt, rs);
                            }
                        } else {
                            vdbrecordset_free(rs);
                        }
                    } else {
                        //not applying selection if 'group by' is used
                        //will apply 'having' clause later after aggregates can be applied to entire group
                        struct VdbByteList* key = vdbcursor_key_from_cols(cursor, rs, stmt->as.select.grouping);
                        //TODO: freeing recordset rs since it's not used, but kinda messy
                        free_w(rs->records, sizeof(struct VdbRecord*) * rs->capacity);
                        free_w(rs, sizeof(struct VdbRecordSet));
                        vdbhashtable_insert_entry(grouping_table, key, rec);
                    }
                }

                //moves recordsets from hashtable to binary tree ('next' field in recordsets no longer used)
                //TODO: should not manually use hash table internals like this - error prone.  Should be done with hashtable interface
                if (stmt->as.select.grouping->count > 0) {
                    for (int i = 0; i < VDB_MAX_BUCKETS; i++) {
                        struct VdbRecordSet* cur = grouping_table->entries[i];
                        while (cur) {
                            //eval 'having' clause, and only insert into bt if true
                            if (vdbcursor_apply_having(cursor, cur, stmt->as.select.having)) {
                                struct VdbByteList* ordering_key = vdbcursor_key_from_cols(cursor, cur, stmt->as.select.ordering);
                                cur->key = ordering_key;
                                vdbbinarytree_insert_node(bt, cur);
                            }
                            cur = cur->next;
                        }
                    }
                }

                //returns head of linked-list of recordsets
                struct VdbRecordSet* head = stmt->as.select.order_desc ? 
                                            vdbbinarytree_flatten_desc(bt) : 
                                            vdbbinarytree_flatten_asc(bt);

                //apply projections to each recordset in linked-list, and return final single recordset
                struct VdbRecordSet* final = vdbcursor_apply_projection(cursor, head, stmt->as.select.projection, stmt->as.select.grouping->count > 0);

                //apply limits
                vdbcursor_apply_limit(cursor, final, stmt->as.select.limit);

                for (uint32_t i = 0; i < final->count; i++) {
                    vdbrecord_print(output, final->records[i]);
                    vdbstring_concat(output, "\n");
                }

                vdbcursor_free(cursor);
                vdbrecordset_free(final);
                vdbbinarytree_free(bt);
                vdbhashtable_free(distinct_table);
                vdbhashtable_free(grouping_table);

                break;
            }
            case VDBST_EXIT: {
                if (*h) {
                    vdbstring_concat(output, "close database %s before exiting\n", vdb_dbname(*h));
                    return false;
                } else {
                    return true;
                }
            }
            default:
                break;
        }
    }

    return false;
}
