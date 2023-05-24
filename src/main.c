#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

#include "vdb.h"
#include "util.h"
#include "lexer.h"
#include "parser.h"

char* to_string(struct VdbToken t) {
    static char s[FILENAME_MAX];
    memcpy(s, t.lexeme, t.len);
    s[t.len] = '\0';
    return s;
}

//TODO: need an generator that converst the stmtlist to bytecode
//Should go in vm.c
bool vdb_execute(struct VdbStmtList* sl, VDBHANDLE* h) {
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
                    printf("databases:\n");
                    for (int i = 0; i < vl->count; i++) {
                        printf("\t%.*s\n", vl->values[i].as.Str.len, vl->values[i].as.Str.start);
                    }
                    vdbvaluelist_free(vl);
                } else {
                    printf("execution error: cannot show databases\n");
                }
                break;
            }
            case VDBST_SHOW_TABS: {
                struct VdbValueList* vl;
                if (vdb_show_tabs(*h, &vl) == VDBRC_SUCCESS) {
                    printf("tables:\n");
                    for (int i = 0; i < vl->count; i++) {
                        printf("\t%.*s\n", vl->values[i].as.Str.len, vl->values[i].as.Str.start);
                    }
                    vdbvaluelist_free(vl);
                } else {
                    printf("execution error: cannot show tables\n");
                }
                break;
            }
            case VDBST_CREATE_DB: {
                char* db_name = to_string(stmt->target);
                if (vdb_create_db(db_name) == VDBRC_SUCCESS) {
                    printf("created database %s\n", db_name);
                } else {
                    printf("failed to create database %s\n", db_name);
                }
                break;
            }
            case VDBST_CREATE_TAB: {
                int count = stmt->as.create.attributes->count;

                struct VdbSchema* schema = vdbschema_alloc(count, stmt->as.create.attributes, stmt->as.create.types);

                char* table_name = to_string(stmt->target);
                if (vdb_create_table(*h, table_name, schema)) {
                    printf("created table %s\n", table_name);
                } else {
                    printf("failed to create table %s\n", table_name);
                }

                vdb_free_schema(schema);
                break;
            }
            case VDBST_IF_EXISTS_DROP_DB: {
                bool found = false;
                char* db_name = to_string(stmt->target);
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
                            printf("dropped database %s\n", db_name);
                        } else {
                            printf("failed to drop database %s\n", db_name);
                        }
                    }
                }
                break;
            }
            case VDBST_IF_EXISTS_DROP_TAB: {
                bool found = false;
                char* tab_name = to_string(stmt->target);
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
                            printf("dropped table %s\n", tab_name);
                        } else {
                            printf("failed to drop table %s\n", tab_name);
                        }
                    }
                }
                break;
            }
            case VDBST_DROP_DB: {
                char* db_name = to_string(stmt->target);
                if (vdb_drop_db(db_name) == VDBRC_SUCCESS) {
                    printf("dropped database %s\n", db_name);
                } else {
                    printf("failed to drop database %s\n", db_name);
                }
                break;
            }
            case VDBST_DROP_TAB: {
                char* table_name = to_string(stmt->target);
                if (vdb_drop_table(*h, table_name) == VDBRC_SUCCESS) {
                    printf("dropped table %s\n", table_name);
                } else {
                    printf("failed to drop table %s\n", table_name);
                }
                break;
            }
            case VDBST_OPEN: {
                char* db_name = to_string(stmt->target);
                if ((*h = vdb_open_db(db_name))) {
                    printf("opened database %s\n", db_name);
                } else {
                    printf("failed to open database %s\n", db_name);
                }
                break;
            }
            case VDBST_CLOSE: {
                char* db_name = to_string(stmt->target);
                if (vdb_close(*h)) {
                    printf("closed database %s\n", db_name);
                    *h = NULL;
                } else {
                    printf("failed to close database %s\n", db_name);
                }
                break;
            }
            case VDBST_DESCRIBE: {
                char* table_name = to_string(stmt->target);
                struct VdbValueList* vl;
                if (vdb_describe_table(*h, table_name, &vl) == VDBRC_SUCCESS) {
                    printf("%s:\n", table_name);
                    for (int i = 0; i < vl->count; i += 2) {
                        struct VdbValue attr = vl->values[i];
                        struct VdbValue type = vl->values[i + 1];
                        printf("\t%.*s: %.*s\n", attr.as.Str.len, attr.as.Str.start, type.as.Str.len, type.as.Str.start);
                    }
                    vdbvaluelist_free(vl);
                } else {
                    printf("failed to describe table %s\n", table_name);
                }
                break;
            }
            case VDBST_INSERT: {
                char* table_name = to_string(stmt->target);
                int rec_count = stmt->as.insert.values->count / stmt->as.insert.attributes->count;
                int attr_count_without_id = stmt->as.insert.attributes->count;

                //inserting 'id' as last column name (inserting actual key at end in vdb_insert_new)
                struct VdbToken attr_token;
                attr_token.type = VDBT_IDENTIFIER;
                attr_token.lexeme = "id";
                attr_token.len = 2;
                vdbtokenlist_append_token(stmt->as.insert.attributes, attr_token);

                for (int i = 0; i < rec_count; i++) {
                    struct VdbExprList* el = vdbexprlist_init();
                    int off = i * attr_count_without_id;
                    for (int j = off; j < off + attr_count_without_id; j++) {
                        vdbexprlist_append_expr(el, stmt->as.insert.values->exprs[j]);
                    }
                    vdb_insert_new(*h, table_name, stmt->as.insert.attributes, el);

                    //not calling vdbexprlist_free(el) since the expressions will be freed when stmt is freed
                    free_w(el->exprs, sizeof(struct VdbExpr*) * el->capacity);
                    free_w(el, sizeof(struct VdbExprList));
                }
                printf("inserted %d record(s) into %s\n", rec_count, table_name);
                break;
            }
            case VDBST_UPDATE: {
                //TODO
                break;
            }
            case VDBST_DELETE: {
                //TODO
                break;
            }
            case VDBST_SELECT: {
                char* table_name = to_string(stmt->target);
                struct VdbRecordSet* rs = vdbrecordset_init();
                struct VdbCursor* cursor = vdbcursor_init(*h, table_name, 1); //cursor to begining of table

                while (true) {
                    struct VdbRecord* rec = vdbcursor_read_record(cursor);
                    if (vdbcursor_apply_selection(cursor, rec, stmt->as.select.selection)) {
                        vdbcursor_apply_projection(cursor, rec, stmt->as.select.projection);
                        vdbrecordset_append_record(rs, rec);
                    } else {
                        vdb_record_free(rec);
                    }

                    if (vdbcursor_on_final_record(cursor))
                        break;

                    vdbcursor_increment(cursor);
                }

                //TODO: apply projections to record set - also need to reorder data

                for (uint32_t i = 0; i < rs->count; i++) {
                    vdbrecord_print(rs->records[i]);
                    printf("\n");
                }

                vdbcursor_free(cursor);
                vdbrecordset_free(rs);

                break;
            }
            case VDBST_EXIT: {
                if (*h) {
                    printf("close database %s before exiting\n", vdb_dbname(*h));
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

void run_cli() {
    char* line = NULL; //not including this in memory allocation tracker
    size_t len = 0;
    ssize_t nread;
    VDBHANDLE h = NULL;

    while (true) {
        printf("vdb");
        if (h) {
            printf(":%s", vdb_dbname(h));
        }
        printf(" > ");
        nread = getline(&line, &len, stdin);
        if (nread == -1)
            break;
        line[strlen(line) - 1] = '\0'; //get rid of newline

        struct VdbTokenList* tokens;
        struct VdbErrorList* lex_errors;

        if (vdblexer_lex(line, &tokens, &lex_errors) == VDBRC_ERROR) {
            for (int i = 0; i < 1; i++) {
                struct VdbError e = lex_errors->errors[i];
                printf("error [%d]: %s\n", e.line, e.msg);
            }
            vdbtokenlist_free(tokens);
            vdberrorlist_free(lex_errors);
            continue;
        }

        struct VdbStmtList* stmts;
        struct VdbErrorList* parse_errors;

        if (vdbparser_parse(tokens, &stmts, &parse_errors) == VDBRC_ERROR) {
            for (int i = 0; i < 1; i++) {
                struct VdbError e = parse_errors->errors[i];
                printf("error [%d]: %s\n", e.line, e.msg);
            }
            vdbtokenlist_free(tokens);
            vdberrorlist_free(lex_errors);
            vdbstmtlist_free(stmts);
            vdberrorlist_free(parse_errors);
            continue;
        }

        bool end = vdb_execute(stmts, &h);

        vdbtokenlist_free(tokens);
        vdberrorlist_free(lex_errors);
        vdbstmtlist_free(stmts);
        vdberrorlist_free(parse_errors);

        if (end) {
            break;
        }
    }

    free(line); //not including this in memory allocation tracker
}

int run_script(const char* path) {
    FILE* f = fopen_w(path, "r");
    VDBHANDLE h = NULL;

    fseek_w(f, 0, SEEK_END);
    long fsize = ftell_w(f);
    fseek_w(f, 0, SEEK_SET);
    char* buf = malloc_w(sizeof(char) * (fsize)); //will put null terminator on eof character
    fread_w(buf, fsize, sizeof(char), f);
    buf[fsize - 1] = '\0';

    fclose_w(f);

    struct VdbTokenList* tokens;
    struct VdbErrorList* lex_errors;

    if (vdblexer_lex(buf, &tokens, &lex_errors) == VDBRC_ERROR) {
        for (int i = 0; i < 1; i++) {
            struct VdbError e = lex_errors->errors[i];
            printf("error [%d]: %s\n", e.line, e.msg);
        }
        vdbtokenlist_free(tokens);
        vdberrorlist_free(lex_errors);
        free_w(buf, sizeof(char) * fsize);
        return -1;
    }

    //vdbtokenlist_print(tokens);

    struct VdbStmtList* stmts;
    struct VdbErrorList* parse_errors;

    if (vdbparser_parse(tokens, &stmts, &parse_errors) == VDBRC_ERROR) {
        for (int i = 0; i < 1; i++) {
            struct VdbError e = parse_errors->errors[i];
            printf("error [%d]: %s\n", e.line, e.msg);
        }
        vdbtokenlist_free(tokens);
        vdberrorlist_free(lex_errors);
        vdbstmtlist_free(stmts);
        vdberrorlist_free(parse_errors);
        free_w(buf, sizeof(char) * fsize);
        return -1;
    }

    /*
    for (int i = 0; i < stmts->count; i++) {
        vdbstmt_print(&stmts->stmts[i]);
    }*/

    vdb_execute(stmts, &h);

    vdbtokenlist_free(tokens);
    vdberrorlist_free(lex_errors);
    vdbstmtlist_free(stmts);
    vdberrorlist_free(parse_errors);
    free_w(buf, sizeof(char) * fsize);

    return 0;
}

int main(int argc, char** argv) {
    if (argc > 1) {
        int result = run_script(argv[1]);
        printf("allocated memory: %ld\n", allocated_memory);
        return result;
    } else {
        run_cli();
    }



    return 0;
}
