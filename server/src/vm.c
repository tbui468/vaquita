#include <string.h>
#include <assert.h>

#include "vm.h"
#include "util.h"
#include "lexer.h"
#include "binarytree.h"
#include "hashtable.h"
#include "cursor.h"

static char* to_static_string(struct VdbToken t) {
    static char s[256];
    memcpy(s, t.lexeme, t.len);
    s[t.len] = '\0';
    return s;
}

VDBHANDLE vdbvm_open_db(const char* name) {
    //intialize struct Vdb
    struct Vdb* db = malloc_w(sizeof(struct Vdb));
    db->pager = vdbpager_init();
    db->trees = vdb_treelist_init();
    db->name = strdup_w(name);

    char dirname[FILENAME_MAX];
    dirname[0] = '\0';
    strcat(dirname, name);
    strcat(dirname, ".vdb");

    //error if database doesn't exist
    DIR* d;
    if (!(d = opendir(dirname))) {
        return NULL;
    } 

    //open all table files
    struct dirent* ent;
    while ((ent = readdir(d)) != NULL) {
        int entry_len = strlen(ent->d_name);
        const char* ext = ".vtb";
        int ext_len = strlen(ext);

        if (entry_len <= ext_len)
            continue;

        if (strncmp(ent->d_name + entry_len - ext_len, ext, ext_len) != 0)
            continue;

        char path[FILENAME_MAX];
        path[0] = '\0';
        strcat(path, dirname);
        strcat(path, "/");
        strcat(path, ent->d_name);
        FILE* f = fopen_w(path, "r+");
        setbuf(f, NULL);

        char* s = malloc_w(sizeof(char) * (entry_len - 3));
        memcpy(s, ent->d_name, entry_len - 4);
        s[entry_len - 3] = '\0';
        struct VdbTree* tree = vdb_tree_open(s, f, db->pager);
        vdb_treelist_append_tree(db->trees, tree);
    }

    closedir_w(d);
    
    return (VDBHANDLE)db;
}

static void vdbvm_output_string(struct VdbByteList* bl, const char* buf, size_t size) {
    uint8_t is_tuple = 0;
    vdbbytelist_append_byte(bl, is_tuple);

    uint8_t type = VDBT_TYPE_STR;
    vdbbytelist_append_byte(bl, type);
    uint32_t len = size;
    vdbbytelist_append_bytes(bl, (uint8_t*)&len, sizeof(uint32_t));
    vdbbytelist_append_bytes(bl, (uint8_t*)buf, size);
}

//used for record insertion - autofills non-specified columns with null
static void vdbvm_assign_column_values(struct VdbValue* data, struct VdbSchema* schema, struct VdbTokenList* cols, struct VdbExprList* values) {
    for (uint32_t i = 0; i < schema->count; i++) {
        
        bool found = false;
        for (int j = 0; j < cols->count; j++) {
            if (strncmp(schema->names[i], cols->tokens[j].lexeme, cols->tokens[j].len) != 0) 
                continue;

            //TODO: passing in NULL for struct VdbRecord* and struct Schema* since values expression shouldn't have identifiers
            //  but holy hell this is ugly - should fix this
            data[i] = vdbexpr_eval(values->exprs[j], NULL, NULL);

            found = true;
            break;

        }

        //user manually inserted null for a string value
        if (found && data[i].type == VDBT_TYPE_NULL && schema->types[i] == VDBT_TYPE_STR) {
            data[i] = vdbstring("0", 1); //insert dummy string
            data[i].type = VDBT_TYPE_NULL;
        }

        if (!found) {
            data[i].type = VDBT_TYPE_NULL;

            //automacally insert null value for string
            if (schema->types[i] == VDBT_TYPE_STR) {
                data[i] = vdbstring("0", 1); //insert dummy string
                data[i].type = VDBT_TYPE_NULL;
            }
        }

    }
}

static bool vdbvm_table_exists(VDBHANDLE h, const char* tab_name) {
    struct Vdb* db = (struct Vdb*)(h);

    for (uint32_t i = 0; i < db->trees->count; i++) {
        struct VdbTree* tree = db->trees->trees[i];
        if (strncmp(tree->name, tab_name, strlen(tab_name)) == 0) {
            return true;
        }
    }

    return false;
}

static bool vdbvm_db_exists(const char* db_name) {
    DIR* d;
    if (!(d = opendir("./"))) {
        return true;
    } 

    bool found = false;
    struct dirent* ent;
    while ((ent = readdir(d)) != NULL) {
        int entry_len = strlen(ent->d_name);
        const char* ext = ".vdb";
        int ext_len = strlen(ext);

        if (entry_len <= ext_len)
            continue;

        if (strncmp(ent->d_name + entry_len - ext_len, ext, ext_len) != 0)
            continue;

        if (strncmp(ent->d_name, db_name, strlen(db_name)) == 0) {
            found = true;
            break;
        }
    }

    closedir_w(d);

    return found;
}


enum VdbReturnCode vdbvm_drop_db(const char* name) {
    char dirname[FILENAME_MAX];
    dirname[0] = '\0';
    strcat(dirname, name);
    strcat(dirname, ".vdb");

    DIR* d;
    if (!(d = opendir(dirname))) {
        return VDBRC_ERROR;
    }

    struct dirent* ent;
    while ((ent = readdir(d)) != NULL) {
        if (strncmp(ent->d_name, ".", 1) == 0 || strncmp(ent->d_name, "..", 2) == 0)
            continue;

        char path[FILENAME_MAX];
        path[0] = '\0';
        strcat(path, dirname);
        strcat(path, "/");
        strcat(path, ent->d_name);

        remove_w(path);
    }

    closedir_w(d);

    rmdir_w(dirname);

    return VDBRC_SUCCESS;
}

enum VdbReturnCode vdbvm_drop_table(VDBHANDLE h, const char* name) {
    struct Vdb* db = (struct Vdb*)h;
    struct VdbTree* tree = vdb_treelist_remove_tree(db->trees, name);

    if (tree) {
        char path[FILENAME_MAX];
        path[0] = '\0';
        strcat(path, db->name);
        strcat(path, ".vdb/");
        strcat(path, tree->name);
        strcat(path, ".vtb");
        
        vdb_tree_close(tree);
        vdbpager_evict_pages(db->pager, name);

        remove_w(path);

        return VDBRC_SUCCESS;
    }

    return VDBRC_ERROR;
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
                DIR* d;
                if (!(d = opendir("./"))) {
                    const char* msg = "execution error: cannot show databases";
                    vdbvm_output_string(output, msg, strlen(msg));
                    break;
                } 

                struct VdbRecordSet* final = vdbrecordset_init(NULL);

                struct VdbValue data = vdbstring("databases", strlen("databases")); 
                struct VdbRecord* r = vdbrecord_init(1, &data);
                vdbrecordset_append_record(final, r);

                struct dirent* ent;
                while ((ent = readdir(d)) != NULL) {
                    int entry_len = strlen(ent->d_name);
                    const char* ext = ".vdb";
                    int ext_len = strlen(ext);

                    if (entry_len <= ext_len)
                        continue;

                    if (strncmp(ent->d_name + entry_len - ext_len, ext, ext_len) != 0)
                        continue;

                    snprintf_w(buf, MAX_BUF_SIZE, "%.*s", entry_len - ext_len, ent->d_name);

                    struct VdbValue data = vdbstring(buf, strlen(buf)); 
                    struct VdbRecord* r = vdbrecord_init(1, &data);
                    vdbrecordset_append_record(final, r);
                }

                closedir_w(d);

                vdbrecordset_serialize(final, output);
                vdbrecordset_free(final);

                break;
            }
            case VDBST_SHOW_TABS: {
                struct Vdb* db = (struct Vdb*)(*h);

                struct VdbRecordSet* final = vdbrecordset_init(NULL);

                struct VdbValue data = vdbstring("tables", strlen("tables")); 
                struct VdbRecord* r = vdbrecord_init(1, &data);
                vdbrecordset_append_record(final, r);

                for (uint32_t i = 0; i < db->trees->count; i++) {
                    struct VdbTree* tree = db->trees->trees[i];
                    snprintf_w(buf, MAX_BUF_SIZE, "%.*s", strlen(tree->name), tree->name);

                    struct VdbValue data = vdbstring(buf, strlen(buf)); 
                    struct VdbRecord* r = vdbrecord_init(1, &data);
                    vdbrecordset_append_record(final, r);
                }


                vdbrecordset_serialize(final, output);
                vdbrecordset_free(final);

                break;
            }
            case VDBST_CREATE_DB: {
                char* db_name = to_static_string(stmt->target);
                char dirname[FILENAME_MAX];
                dirname[0] = '\0';
                strcat(dirname, db_name);
                strcat(dirname, ".vdb");

                DIR* d;
                if (!(d = opendir(dirname))) {
                    mkdir_w(dirname, 0777);
                } else {
                    closedir_w(d);
                    snprintf(buf, MAX_BUF_SIZE, "failed to create database %s", db_name);
                    vdbvm_output_string(output, buf, strlen(buf));
                    break;
                } 

                snprintf(buf, MAX_BUF_SIZE, "created database %s", db_name);
                vdbvm_output_string(output, buf, strlen(buf));

                break;
            }
            case VDBST_CREATE_TAB: {
                char* table_name = to_static_string(stmt->target);

                if (vdbvm_table_exists(*h, table_name)) {
                    snprintf(buf, MAX_BUF_SIZE, "failed to create table %s", table_name);
                    vdbvm_output_string(output, buf, strlen(buf));
                    break;
                }

                struct Vdb* db = (struct Vdb*)(*h);

                char path[FILENAME_MAX];
                path[0] = '\0';
                strcat(path, db->name);
                strcat(path, ".vdb");
                strcat(path, "/");
                strcat(path, table_name);
                strcat(path, ".vtb");

                struct VdbSchema* schema = vdbschema_alloc(stmt->as.create.attributes->count, //why is this necessary if passing attributes already
                                                           stmt->as.create.attributes,
                                                           stmt->as.create.types, 
                                                           stmt->as.create.key_idx);

                FILE* f = fopen_w(path, "w+");
                struct VdbTree* tree = vdb_tree_init(table_name, schema, db->pager, f);
                vdb_treelist_append_tree(db->trees, tree);

                snprintf(buf, MAX_BUF_SIZE, "created table %s", table_name);
                vdbvm_output_string(output, buf, strlen(buf));

                break;
            }
            case VDBST_IF_EXISTS_DROP_DB: {
                char* db_name = to_static_string(stmt->target);

                if (vdbvm_db_exists(db_name)) {
                    if (vdbvm_drop_db(db_name) == VDBRC_SUCCESS) {
                        snprintf(buf, MAX_BUF_SIZE, "dropped database %s", db_name);
                        vdbvm_output_string(output, buf, strlen(buf));
                    } else {
                        snprintf(buf, MAX_BUF_SIZE, "failed to drop database %s", db_name);
                        vdbvm_output_string(output, buf, strlen(buf));
                    }
                }
                break;
            }
            case VDBST_IF_EXISTS_DROP_TAB: {
                char* tab_name = to_static_string(stmt->target);

                if (vdbvm_table_exists(*h, tab_name)) {
                    if (vdbvm_drop_table(*h, tab_name) == VDBRC_SUCCESS) {
                        snprintf(buf, MAX_BUF_SIZE, "dropped table %s", tab_name);
                        vdbvm_output_string(output, buf, strlen(buf));
                    } else {
                        snprintf(buf, MAX_BUF_SIZE, "failed to drop table %s", tab_name);
                        vdbvm_output_string(output, buf, strlen(buf));
                    }
                }

                break;
            }
            case VDBST_DROP_DB: {
                char* db_name = to_static_string(stmt->target);
                if (vdbvm_drop_db(db_name) == VDBRC_SUCCESS) {
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
                if (vdbvm_drop_table(*h, table_name) == VDBRC_SUCCESS) {
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
                if (vdbvm_db_exists(db_name)) {
                    if ((*h = vdbvm_open_db(db_name))) {
                        snprintf(buf, MAX_BUF_SIZE, "opened database %s", db_name);
                        vdbvm_output_string(output, buf, strlen(buf));
                    } else {
                        snprintf(buf, MAX_BUF_SIZE, "failed to open database %s", db_name);
                        vdbvm_output_string(output, buf, strlen(buf));
                    }
                } else {
                    snprintf(buf, MAX_BUF_SIZE, "database %s does not exist", db_name);
                    vdbvm_output_string(output, buf, strlen(buf));
                }
                break;
            }
            case VDBST_CLOSE: {
                char* db_name = to_static_string(stmt->target);
                struct Vdb* db = (struct Vdb*)(*h);

                vdbpager_free(db->pager);
                vdb_treelist_free(db->trees);
                free_w(db->name, sizeof(char) * (strlen(db->name) + 1)); //include null terminator
                free_w(db, sizeof(struct Vdb));

                snprintf(buf, MAX_BUF_SIZE, "closed database %s", db_name);
                vdbvm_output_string(output, buf, strlen(buf));
                *h = NULL;
                break;
            }
            case VDBST_DESCRIBE: {

                struct Vdb* db = (struct Vdb*)(*h);
                char* table_name = to_static_string(stmt->target);
                struct VdbTree* tree = vdb_treelist_get_tree(db->trees, table_name);

                struct VdbRecordSet* final = vdbrecordset_init(NULL);

                struct VdbValue data[3];
                data[0] = vdbstring("field", strlen("field")); 
                data[1] = vdbstring("type", strlen("type")); 
                data[2] = vdbstring("key", strlen("key")); 
                struct VdbRecord* r = vdbrecord_init(3, data);
                vdbrecordset_append_record(final, r);

                for (uint32_t i = 0; i < tree->schema->count; i++) {
                    char* type;
                    int len;
                    switch (tree->schema->types[i]) {
                        case VDBT_TYPE_INT:
                            type = "int";
                            len = 3;
                            break;
                        case VDBT_TYPE_STR:
                            type = "string";
                            len = 6;
                            break;
                        case VDBT_TYPE_FLOAT:
                            type = "float";
                            len = 5;
                            break;
                        case VDBT_TYPE_BOOL:
                            type = "bool";
                            len = 4;
                            break;
                        default:
                            type = "int"; // to silence warning
                            len = 0; // to silence warning
                            assert(false && "invalid schema data type");
                            break;
                    }

                    struct VdbValue data[3];
                    data[0] = vdbstring(tree->schema->names[i], strlen(tree->schema->names[i])); 
                    data[1] = vdbstring(type, len); 
                    if (i == tree->schema->key_idx) {
                        data[2] = vdbstring("pri", 3);
                    } else {
                        data[2] = vdbstring(" ", 1);
                    }
                    struct VdbRecord* r = vdbrecord_init(3, data);
                    vdbrecordset_append_record(final, r);
                }


                vdbrecordset_serialize(final, output);
                vdbrecordset_free(final);

                break;
            }
            case VDBST_INSERT: {
                char* table_name = to_static_string(stmt->target);
                int rec_count = stmt->as.insert.values->count / stmt->as.insert.attributes->count;

                struct Vdb* db = (struct Vdb*)(*h);
                struct VdbTree* tree = vdb_treelist_get_tree(db->trees, table_name);

                struct VdbExprList* el = vdbexprlist_init();

                for (int i = 0; i < rec_count; i++) {
                    //make record
                    int off = i * stmt->as.insert.attributes->count;

                    for (int j = off; j < off + stmt->as.insert.attributes->count; j++) {
                        vdbexprlist_append_expr(el, stmt->as.insert.values->exprs[j]);
                    }

                    struct VdbValue data[tree->schema->count];
                    vdbvm_assign_column_values(data, tree->schema, stmt->as.insert.attributes, el);

                    struct VdbRecord rec;
                    rec.count = tree->schema->count;
                    rec.data = data;
                 
                    struct VdbCursor* cursor = vdbcursor_init(tree);
                    vdbcursor_seek(cursor, rec.data[tree->schema->key_idx]);
                    vdbcursor_insert_record(cursor, &rec);
                    vdbcursor_free(cursor);

                    //free record
                    for (uint32_t i = 0; i < rec.count; i++) {
                        struct VdbValue* d = &rec.data[i];
                        if (d->type == VDBT_TYPE_STR) {
                            free_w(d->as.Str.start, sizeof(char) * d->as.Str.len);
                        }
                    }

                    //reuse expression list
                    el->count = 0;
                }

                //not calling vdbexprlist_free(el) since the expressions will be freed when stmt is freed
                free_w(el->exprs, sizeof(struct VdbExpr*) * el->capacity);
                free_w(el, sizeof(struct VdbExprList));

                snprintf(buf, MAX_BUF_SIZE, "inserted %d record(s) into %s", rec_count, table_name);
                vdbvm_output_string(output, buf, strlen(buf));
                break;
            }
            case VDBST_UPDATE: {
                char* table_name = to_static_string(stmt->target);
                struct Vdb* db = (struct Vdb*)(*h);
                struct VdbTree* tree = vdb_treelist_get_tree(db->trees, table_name);
                struct VdbCursor* cursor = vdbcursor_init(tree);

                while (!vdbcursor_at_end(cursor)) {
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
                struct Vdb* db = (struct Vdb*)(*h);
                struct VdbTree* tree = vdb_treelist_get_tree(db->trees, table_name);
                struct VdbCursor* cursor = vdbcursor_init(tree);

                while (!vdbcursor_at_end(cursor)) {
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
                struct Vdb* db = (struct Vdb*)(*h);
                struct VdbTree* tree = vdb_treelist_get_tree(db->trees, table_name);
                struct VdbCursor* cursor = vdbcursor_init(tree);

                struct VdbHashTable* distinct_table = vdbhashtable_init();
                struct VdbHashTable* grouping_table = vdbhashtable_init();

                struct VdbRecordSet* head = NULL;

                while (!vdbcursor_at_end(cursor)) {
                    struct VdbRecord* rec = vdbcursor_fetch_record(cursor);
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

                vdbrecordset_serialize(final, output);

                vdbcursor_free(cursor);
                vdbrecordset_free(final);
                vdbhashtable_free(distinct_table);
                vdbhashtable_free(grouping_table);

                break;
            }
            case VDBST_EXIT: {
                if (*h) {
                    struct Vdb* db = (struct Vdb*)(*h);
                    snprintf_w(buf, MAX_BUF_SIZE, "close database %s before exiting", db->name);
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
