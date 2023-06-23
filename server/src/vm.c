#include <string.h>

#include "vm.h"
#include "util.h"
#include "lexer.h"
#include "binarytree.h"
#include "hashtable.h"

static char* to_static_string(struct VdbToken t) {
    static char s[256];
    memcpy(s, t.lexeme, t.len);
    s[t.len] = '\0';
    return s;
}

void vdbvm_output_string(struct VdbByteList* bl, const char* buf, size_t size) {
    uint32_t one = 1;
    vdbbytelist_append_bytes(bl, (uint8_t*)&one, sizeof(uint32_t));
    vdbbytelist_append_bytes(bl, (uint8_t*)&one, sizeof(uint32_t));
    uint8_t type = VDBT_TYPE_STR;
    vdbbytelist_append_byte(bl, type);
    uint32_t len = size;
    vdbbytelist_append_bytes(bl, (uint8_t*)&len, sizeof(uint32_t));
    vdbbytelist_append_bytes(bl, (uint8_t*)buf, size);
}

//TODO: need an generator that converst the stmtlist to bytecode
bool vdb_execute(struct VdbStmtList* sl, VDBHANDLE* h, struct VdbByteList* output) {
    int MAX_BUF_SIZE = 1024;
    char buf[MAX_BUF_SIZE];
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
                    const char* msg = "databases:";
                    vdbvm_output_string(output, msg, strlen(msg));
                    for (int i = 0; i < vl->count; i++) {
                        snprintf_w(buf, MAX_BUF_SIZE, "\t%.*s", vl->values[i].as.Str.len, vl->values[i].as.Str.start);
                        vdbvm_output_string(output, buf, strlen(buf));
                    }
                    vdbvaluelist_free(vl);
                } else {
                    const char* msg = "execution error: cannot show databases";
                    vdbvm_output_string(output, msg, strlen(msg));
                }
                break;
            }
            case VDBST_SHOW_TABS: {
                struct VdbValueList* vl;
                if (vdb_show_tabs(*h, &vl) == VDBRC_SUCCESS) {
                    const char* msg = "tables:";
                    vdbvm_output_string(output, msg, strlen(msg));
                    for (int i = 0; i < vl->count; i++) {
                        snprintf_w(buf, MAX_BUF_SIZE, "\t%.*s", vl->values[i].as.Str.len, vl->values[i].as.Str.start);
                        vdbvm_output_string(output, buf, strlen(buf));
                    }
                    vdbvaluelist_free(vl);
                } else {
                    const char* msg = "execution error: cannot show tables";
                    vdbvm_output_string(output, msg, strlen(msg));
                }
                break;
            }
            case VDBST_CREATE_DB: {
                char* db_name = to_static_string(stmt->target);
                if (vdb_create_db(db_name) == VDBRC_SUCCESS) {
                    snprintf(buf, MAX_BUF_SIZE, "created database %s", db_name);
                    vdbvm_output_string(output, buf, strlen(buf));
                } else {
                    snprintf(buf, MAX_BUF_SIZE, "failed to create database %s", db_name);
                    vdbvm_output_string(output, buf, strlen(buf));
                }
                break;
            }
            case VDBST_CREATE_TAB: {
                int count = stmt->as.create.attributes->count;

                struct VdbSchema* schema = vdbschema_alloc(count, stmt->as.create.attributes, stmt->as.create.types, stmt->as.create.key_idx);

                char* table_name = to_static_string(stmt->target);
                if (vdb_create_table(*h, table_name, schema)) {
                    snprintf(buf, MAX_BUF_SIZE, "created table %s", table_name);
                    vdbvm_output_string(output, buf, strlen(buf));
                } else {
                    snprintf(buf, MAX_BUF_SIZE, "failed to create table %s", table_name);
                    vdbvm_output_string(output, buf, strlen(buf));
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
                            snprintf(buf, MAX_BUF_SIZE, "dropped database %s", db_name);
                            vdbvm_output_string(output, buf, strlen(buf));
                        } else {
                            snprintf(buf, MAX_BUF_SIZE, "failed to drop database %s", db_name);
                            vdbvm_output_string(output, buf, strlen(buf));
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
                            snprintf(buf, MAX_BUF_SIZE, "dropped table %s", tab_name);
                            vdbvm_output_string(output, buf, strlen(buf));
                        } else {
                            snprintf(buf, MAX_BUF_SIZE, "failed to drop table %s", tab_name);
                            vdbvm_output_string(output, buf, strlen(buf));
                        }
                    }
                }
                break;
            }
            case VDBST_DROP_DB: {
                char* db_name = to_static_string(stmt->target);
                if (vdb_drop_db(db_name) == VDBRC_SUCCESS) {
                    snprintf(buf, MAX_BUF_SIZE, "dropped database %s", db_name);
                    vdbvm_output_string(output, buf, strlen(buf));
                } else {
                    snprintf(buf, MAX_BUF_SIZE, "failed to drop database %s", db_name);
                    vdbvm_output_string(output, buf, strlen(buf));
                }
                break;
            }
            case VDBST_DROP_TAB: {
                char* table_name = to_static_string(stmt->target);
                if (vdb_drop_table(*h, table_name) == VDBRC_SUCCESS) {
                    snprintf(buf, MAX_BUF_SIZE, "dropped table %s", table_name);
                    vdbvm_output_string(output, buf, strlen(buf));
                } else {
                    snprintf(buf, MAX_BUF_SIZE, "failed to drop table %s", table_name);
                    vdbvm_output_string(output, buf, strlen(buf));
                }
                break;
            }
            case VDBST_OPEN: {
                char* db_name = to_static_string(stmt->target);
                if ((*h = vdb_open_db(db_name))) {
                    snprintf(buf, MAX_BUF_SIZE, "opened database %s", db_name);
                    vdbvm_output_string(output, buf, strlen(buf));
                } else {
                    snprintf(buf, MAX_BUF_SIZE, "failed to open database %s", db_name);
                    vdbvm_output_string(output, buf, strlen(buf));
                }
                break;
            }
            case VDBST_CLOSE: {
                char* db_name = to_static_string(stmt->target);
                if (vdb_close(*h)) {
                    snprintf(buf, MAX_BUF_SIZE, "closed database %s", db_name);
                    vdbvm_output_string(output, buf, strlen(buf));
                    *h = NULL;
                } else {
                    snprintf(buf, MAX_BUF_SIZE, "failed to close database %s", db_name);
                    vdbvm_output_string(output, buf, strlen(buf));
                }
                break;
            }
            case VDBST_DESCRIBE: {
                char* table_name = to_static_string(stmt->target);
                struct VdbValueList* vl;
                if (vdb_describe_table(*h, table_name, &vl) == VDBRC_SUCCESS) {
                    snprintf(buf, MAX_BUF_SIZE, "%s:", table_name);
                    vdbvm_output_string(output, buf, strlen(buf));
                    for (int i = 0; i < vl->count; i += 2) {
                        struct VdbValue attr = vl->values[i];
                        struct VdbValue type = vl->values[i + 1];
                        snprintf(buf, MAX_BUF_SIZE, "\t%.*s: %.*s", attr.as.Str.len, attr.as.Str.start, type.as.Str.len, type.as.Str.start);
                        vdbvm_output_string(output, buf, strlen(buf));
                    }
                    vdbvaluelist_free(vl);
                } else {
                    snprintf(buf, MAX_BUF_SIZE, "failed to describe table %s", table_name);
                    vdbvm_output_string(output, buf, strlen(buf));
                }
                break;
            }
            case VDBST_INSERT: {
                char* table_name = to_static_string(stmt->target);
                int rec_count = stmt->as.insert.values->count / stmt->as.insert.attributes->count;

                //TODO: allocate struct VdbExprList here
                struct VdbExprList* el = vdbexprlist_init();

                for (int i = 0; i < rec_count; i++) {
                    int off = i * stmt->as.insert.attributes->count;
                    //TODO: could just memcpy here (after making sure capacity is sufficient
                    for (int j = off; j < off + stmt->as.insert.attributes->count; j++) {
                        vdbexprlist_append_expr(el, stmt->as.insert.values->exprs[j]);
                    }
                    vdb_insert_new(*h, table_name, stmt->as.insert.attributes, el);

                    //not calling vdbexprlist_free(el) since the expressions will be freed when stmt is freed
                    //TODO: just set set struct VdbExprList count to 0 here
                    //free_w(el->exprs, sizeof(struct VdbExpr*) * el->capacity);
                    //free_w(el, sizeof(struct VdbExprList));
                    el->count = 0;
                }

                //TODO: free struct VdbExprList here
                free_w(el->exprs, sizeof(struct VdbExpr*) * el->capacity);
                free_w(el, sizeof(struct VdbExprList));

                snprintf(buf, MAX_BUF_SIZE, "inserted %d record(s) into %s", rec_count, table_name);
                vdbvm_output_string(output, buf, strlen(buf));
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

                struct VdbHashTable* distinct_table = vdbhashtable_init();
                struct VdbHashTable* grouping_table = vdbhashtable_init();

                struct VdbRecord* rec;

                struct VdbRecordSet* head = NULL;

                while ((rec = vdbcursor_fetch_record(cursor))) {
                    struct VdbRecordSet* rs = vdbrecordset_init(NULL);
                    vdbrecordset_append_record(rs, rec);

                    //grouping (will apply 'having' clause later)
                    if (stmt->as.select.grouping->count > 0) {
                        //not applying selection if 'group by' is used
                        //will apply 'having' clause later after aggregates can be applied to entire group
                        struct VdbByteList* key = vdbcursor_key_from_cols(cursor, rs, stmt->as.select.grouping);
                        //TODO: freeing recordset rs since it's not used, but kinda messy
                        free_w(rs->records, sizeof(struct VdbRecord*) * rs->capacity);
                        free_w(rs, sizeof(struct VdbRecordSet));
                        vdbhashtable_insert_entry(grouping_table, key, rec);
                        continue;
                    }

                    //does not pass selection criteria
                    if (!vdbcursor_apply_selection(cursor, rs, stmt->as.select.selection)) {
                        vdbrecordset_free(rs);
                        continue;
                    }

                    //passes selection criteria
                    if (stmt->as.select.distinct) {
                        struct VdbByteList* key = vdbcursor_key_from_cols(cursor, rs, stmt->as.select.projection);
                        if (!vdbhashtable_contains_key(distinct_table, key)) {
                            vdbhashtable_insert_entry(distinct_table, key, rec); //using hashtable to check for duplicates bc of 'distinct' keyword

                            rs->next = head;
                            head = rs;
                        }
                    } else {
                        rs->next = head;
                        head = rs;
                    }

                }

                //TODO: should not manually use hash table internals like this - error prone.  Should be done with hashtable interface
                if (stmt->as.select.grouping->count > 0) {
                    for (int i = 0; i < VDB_MAX_BUCKETS; i++) {
                        struct VdbRecordSet* cur = grouping_table->entries[i];
                        while (cur) {
                            //eval 'having' clause, and only insert into bt if true
                            struct VdbRecordSet* cached_next = cur->next;
                            if (vdbcursor_apply_having(cursor, cur, stmt->as.select.having)) {
                                cur->next = head;
                                head = cur;
                            }
                            cur = cached_next;
                        }
                    }
                }

                vdbcursor_sort_linked_list(cursor, &head, stmt->as.select.ordering, stmt->as.select.order_desc);

                //apply projections to each recordset in linked-list, and return final single recordset
                struct VdbRecordSet* final = vdbcursor_apply_projection(cursor, head, stmt->as.select.projection, stmt->as.select.grouping->count > 0);

                vdbcursor_apply_limit(cursor, final, stmt->as.select.limit);

                vdbbytelist_append_bytes(output, (uint8_t*)&final->count, sizeof(uint32_t));
                if (final->count > 0) {
                    vdbbytelist_append_bytes(output, (uint8_t*)&final->records[0]->count, sizeof(uint32_t));
                } else {
                    uint32_t zero = 0;
                    vdbbytelist_append_bytes(output, (uint8_t*)&zero, sizeof(uint32_t));
                }

                for (uint32_t i = 0; i < final->count; i++) {
                    struct VdbRecord* r = final->records[i];
                    vdbbytelist_serialize_data(output, r, vdbrecord_serialize, vdbrecord_serialized_size);
                }


                vdbcursor_free(cursor);
                vdbrecordset_free(final);
                vdbhashtable_free(distinct_table);
                vdbhashtable_free(grouping_table);

                break;
            }
            case VDBST_EXIT: {
                if (*h) {
                    snprintf_w(buf, MAX_BUF_SIZE, "close database %s before exiting", vdb_dbname(*h));
                    vdbvm_output_string(output, buf, strlen(buf));
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
