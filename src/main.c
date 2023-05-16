#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>

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
                int count;
                char** dbs;
                if (vdb_show_dbs(&dbs, &count) == VDBRC_SUCCESS) {
                    printf("databases:\n");
                    for (int i = 0; i < count; i++) {
                        printf("\t%s\n", dbs[i]);
                        free(dbs[i]);
                    }
                    free(dbs);
                } else {
                    printf("execution error: cannot show databases\n");
                }
                break;
            }
            case VDBST_SHOW_TABS: {
                int count;
                char** tabs;
                if (vdb_show_tabs(*h, &tabs, &count) == VDBRC_SUCCESS) {
                    printf("tables:\n");
                    for (int i = 0; i < count; i++) {
                        printf("\t%s\n", tabs[i]);
                        free(tabs[i]);
                    }
                    free(tabs);
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
                int count;
                char** dbs;
                bool found = false;
                char* db_name = to_string(stmt->target);
                if (vdb_show_dbs(&dbs, &count) == VDBRC_SUCCESS) {
                    for (int i = 0; i < count; i++) {
                        if (strncmp(dbs[i], db_name, strlen(db_name)) == 0) {
                            found = true;
                        }
                        free(dbs[i]);
                    }
                    free(dbs);

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
                int count;
                char** tabs;
                bool found = false;
                char* tab_name = to_string(stmt->target);
                if (vdb_show_tabs(*h, &tabs, &count) == VDBRC_SUCCESS) {
                    for (int i = 0; i < count; i++) {
                        if (strncmp(tabs[i], tab_name, strlen(tab_name)) == 0) {
                            found = true;
                        }
                        free(tabs[i]);
                    }
                    free(tabs);

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
                int count;
                char** attributes;
                char** types;
                char* table_name = to_string(stmt->target);
                if (vdb_describe_table(*h, table_name, &attributes, &types, &count) == VDBRC_SUCCESS) {
                    printf("%s:\n", table_name);
                    for (int i = 0; i < count; i++) {
                        printf("\t%s: %s\n", attributes[i], types[i]);
                        free(attributes[i]);
                        free(types[i]);
                    }

                    free(attributes);
                    free(types);
                } else {
                    printf("failed to describe table %s\n", table_name);
                }
                break;
            }
            case VDBST_INSERT: {
                char* table_name = to_string(stmt->target);
                vdb_insert_record(*h, table_name, "Mars");
                printf("inserted 1 record(s) into %s\n", table_name);

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
                struct VdbRecord* r = vdb_fetch_record(*h, table_name, 1);
                if (r) {
                    printf("key %d: %.*s\n", r->key, r->data[0].as.Str->len, r->data[0].as.Str->start);
                    vdb_record_free(r);
                }
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
    char* line = NULL;
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
//        vdbtokenlist_print(tokens);       
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

    free(line);
}

void run_script(const char* path) {
    FILE* f = fopen_w(path, "r");
    char* line = NULL;
    size_t len = 0;
    ssize_t nread;
    VDBHANDLE h = NULL;

    while ((nread = getline(&line, &len, f)) > 0) {
        line[strlen(line) - 1] = '\0';

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

    free(line);
    fclose_w(f);
}

int main(int argc, char** argv) {
    /*
    if (argc > 1) {
        run_script(argv[1]);
    } else {
        run_cli();
    }*/

    VDBHANDLE h;
    vdb_create_db("andromeda");
    h = vdb_open_db("andromeda");

    struct VdbSchema* schema = vdb_alloc_schema(3, VDBT_TYPE_INT, "age", VDBT_TYPE_STR, "name", VDBT_TYPE_BOOL, "grad");

    vdb_create_table(h, "planets", schema);

    vdb_free_schema(schema);

    char word[100];
    memset(word, 'x', sizeof(char) * 100);
    word[99] = '\0';

    printf("table created\n");

/*    
    const char* words[] = {word, "dogs", "turtles"};
    for (int i = 1; i <= 200; i++) {
        vdb_insert_record(h, "planets", i * 2, words[i % 3], i % 2 == 0);
    }
*/
    struct VdbTokenList* attrs = vdbtokenlist_init();
    struct VdbToken age_attr = {VDBT_IDENTIFIER, "age", 3};
    struct VdbToken name_attr = {VDBT_IDENTIFIER, "name", 4};
    struct VdbToken grad_attr = {VDBT_IDENTIFIER, "grad", 4};
    vdbtokenlist_append_token(attrs, name_attr);
    vdbtokenlist_append_token(attrs, grad_attr);
    vdbtokenlist_append_token(attrs, age_attr);
    struct VdbTokenList* values = vdbtokenlist_init();
    struct VdbToken age_value = {VDBT_INT, "9", 1};
    struct VdbToken name_value = {VDBT_STR, "Neptune", 7};
    struct VdbToken grad_value = {VDBT_TRUE, "true", 4};
    vdbtokenlist_append_token(values, name_value);
    vdbtokenlist_append_token(values, grad_value);
    vdbtokenlist_append_token(values, age_value);
    vdb_insert_new(h, "planets", attrs, values);

    struct VdbTokenList* values2 = vdbtokenlist_init();
    struct VdbToken age_value2 = {VDBT_INT, "88", 2};
    struct VdbToken name_value2 = {VDBT_STR, "Mars", 4};
    struct VdbToken grad_value2 = {VDBT_TRUE, "false", 5};
    vdbtokenlist_append_token(values2, name_value2);
    vdbtokenlist_append_token(values2, grad_value2);
    vdbtokenlist_append_token(values2, age_value2);
    vdb_insert_new(h, "planets", attrs, values2);

    //vdb_debug_print_tree(h, "planets");
    //vdb_delete_record(h, "planets", 1);
    //vdb_update_record(h, "planets", 99, 99, "cats", false);
    //vdb_delete_record(h, "planets", 200);
    printf("records inserted\n");

    int keys[] = {0, 1, 2, 99, 198, 199, 200, 201};

    for (int i = 0; i < 8; i++) {
        struct VdbRecord* r = vdb_fetch_record(h, "planets", keys[i]);
        if (r) {
            printf("key %d: ", r->key);
            if (!r->data[0].is_null) printf("%ld, ", r->data[0].as.Int); else printf("null, ");
            if (!r->data[1].is_null)printf("%.*s, ", r->data[1].as.Str->len, r->data[1].as.Str->start); else printf("null, ");
            if (!r->data[2].is_null)printf("%d", r->data[2].as.Bool); else printf("null");
            printf("\n");
            vdb_record_free(r);
        } else {
            printf("not found!\n");
        }
    }

    vdb_close(h);

    return 0;
}
