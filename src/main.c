#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>

#include "vdb.h"
#include "util.h"
#include "lexer.h"
#include "parser.h"

//TODO: need an generator that converst the stmtlist to bytecode
//Should go in vm.c
bool vdb_execute(struct VdbStmtList* sl, VDBHANDLE* h) {
    for (int i = 0; i < sl->count; i++) {
        struct VdbStmt* stmt = &sl->stmts[i];
        switch (stmt->type) {
            case VDBST_CONNECT: {
                //TODO
                break;
            }
            case VDBST_SHOW_DBS: {
                //TODO
                break;
            }
            case VDBST_SHOW_TABS: {
                //TODO
                break;
            }
            case VDBST_CREATE_DB: {
                //TODO
                break;
            }
            case VDBST_CREATE_TAB: {
                int count = stmt->as.create.attributes->count;
                struct VdbSchema* schema = vdbschema_alloc(count, stmt->as.create.attributes, stmt->as.create.types);

                int len = stmt->target.len;
                char table_name[len + 1];
                memcpy(table_name, stmt->target.lexeme, len);
                table_name[len] = '\0';
                if (vdb_create_table(*h, table_name, schema)) {
                    printf("Created table %s\n", table_name);
                } else {
                    printf("Failed to create table %s\n", table_name);
                }

                vdb_free_schema(schema);
                break;
            }
            case VDBST_DROP_DB: {
                //TODO
                break;
            }
            case VDBST_DROP_TAB: {
                //TODO
                break;
            }
            case VDBST_OPEN: {
                int len = stmt->target.len;
                char db_name[len + 1];
                memcpy(db_name, stmt->target.lexeme, len);
                db_name[len] = '\0';
                if ((*h = vdb_open(db_name))) {
                    printf("opened database %s\n", db_name);
                } else {
                    printf("failed to open database %s\n", db_name);
                }
                break;
            }
            case VDBST_CLOSE: {
                int len = stmt->target.len;
                char db_name[len + 1];
                memcpy(db_name, stmt->target.lexeme, len);
                db_name[len] = '\0';
                if (vdb_close(*h)) {
                    printf("closed database %s\n", db_name);
                    *h = NULL;
                } else {
                    printf("failed to close database %s\n", db_name);
                }
                break;
            }
            case VDBST_DESCRIBE: {
                //TODO
                break;
            }
            case VDBST_INSERT: {
                //TODO
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
                //TODO
                break;
            }
            case VDBST_EXIT: {
                return true;
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
    char* db_name = NULL;

    while (true) {
        printf("vdb");
        if (h) {
            db_name = vdb_dbname(h);
            printf(":%s", db_name);
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
/*
        bool end = vdb_execute(stmts, &h);

        vdbtokenlist_free(tokens);
        vdberrorlist_free(lex_errors);
        vdbstmtlist_free(stmts);
        vdberrorlist_free(parse_errors);

        if (end) {
            break;
        }*/
    }

    free(line);
}

int main(int argc, char** argv) {
    argc = argc;
    argv = argv; //TODO: should be the port value dbms allows connections on

    run_cli();

    /*
    VDBHANDLE h;
    if ((h = vdb_open("school"))) {
        printf("Opened 'school' database\n");    
    } else {
        printf("Failed to open 'school' database\n");
    }


    struct VdbSchema* schema = vdb_alloc_schema(3, VDBF_INT, "age", VDBF_STR, "name", VDBF_BOOL, "graduated");

    if (vdb_create_table(h, "students", schema)) {
        printf("Created 'students' table\n");
    } else {
        printf("Failed to create 'students' table\n");
    }

    vdb_free_schema(schema);

    char word[100];
    memset(word, 'x', sizeof(char) * 100);
    word[99] = '\0';
    
    const char* words[] = {word, "dogs", "turtles"};
    for (int i = 1; i <= 200; i++) {
        vdb_insert_record(h, "students", i * 2, words[i % 3], i % 2 == 0);
    }

    vdb_debug_print_tree(h, "students");
    vdb_delete_record(h, "students", 1);
    vdb_update_record(h, "students", 99, 99, "cats", false);
    vdb_delete_record(h, "students", 200);

    int keys[] = {0, 1, 2, 99, 198, 199, 200, 201};

    for (int i = 0; i < 8; i++) {
        struct VdbRecord* r = vdb_fetch_record(h, "students", keys[i]);
        if (r) {
            printf("key %d: %ld, %.*s, %d\n", r->key, r->data[0].as.Int, r->data[1].as.Str->len, r->data[1].as.Str->start, r->data[2].as.Bool);
            vdb_record_free(r);
        } else {
            printf("not found!\n");
        }
    }

    if (vdb_close(h)) {
        printf("Closed 'school' database\n");
    } else {
        printf("Failed to close 'school' database\n");
    }*/

    return 0;
}
