#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>

#include "vdb.h"
#include "lexer.h"
#include "parser.h"
#include "vm.h"


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

//    vdbtokenlist_print(tokens);

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

    //TODO: uncomment later 
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
//        printf("allocated memory: %ld\n", allocated_memory);
        return result;
    } else {
        run_cli();
    }



    return 0;
}
