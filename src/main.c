#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>

#include "vdb.h"
#include "util.h"

enum VdbTokenType {
    VDBT_EXIT,
    VDBT_OPEN,
    VDBT_CLOSE,
    VDBT_CREATE,
    VDBT_DROP,
    VDBT_IDENTIFIER,
    VDBT_STRING,
    VDBT_INT
};

struct VdbToken {
    enum VdbTokenType type;
    char* lexeme;
    int len;
};

struct VdbTokenList {
    struct VdbToken* tokens;
    int count;
    int capacity;
};

enum VdbExprType {
    VDBEX_IDENTIFIER,
    VDBEX_SCHEMA
    /*
    VDBEX_DTYPE,
    VDBEX_ATTRIBUTES, //tuple of attribute names during insertion
    VDBEX_VALUES, //tuple of values during insertion
    VDBEX_WHERE,
    VDBEX_ASSIGN //eg, age = 5 when updating records
    */
};

struct VdbExpr {
    enum VdbExprType type;
    union {
        struct {
            struct VdbToken token; 
        } identifier;
        struct {
            struct VdbTokenList* names;
            struct VdbTokenList* types;
        } schema;
    } as;
};

enum VdbStmtType {
    VDBST_EXIT,
    VDBST_OPEN,
    VDBST_CLOSE,
    VDBST_CREATE,
    VDBST_DROP
};

struct VdbStmt {
    enum VdbStmtType type;
    union {
        struct {
            struct VdbExpr db_name;
        } open;
        struct {
            struct VdbExpr db_name;
        } close;
        struct {
            struct VdbExpr table_name;
        } create;
        struct {
            struct VdbExpr table_name;
        } drop;
    } as;
};

struct VdbStmtList {
    struct VdbStmt* stmts;
    int count;
    int capacity;
};


struct VdbParser {
    struct VdbTokenList* tl;
    int current;
};


struct VdbStmtList* vdbstmtlist_init() {
    struct VdbStmtList* sl = malloc_w(sizeof(struct VdbStmtList));
    sl->count = 0;
    sl->capacity = 8;
    sl->stmts = malloc_w(sizeof(struct VdbStmt) * sl->capacity);

    return sl;
}

void vdbstmtlist_free(struct VdbStmtList* sl) {
    free(sl->stmts);
    free(sl);
}

void vdbstmtlist_append_stmt(struct VdbStmtList* sl, struct VdbStmt stmt) {
    if (sl->count + 1 > sl->capacity) {
        sl->capacity *= 2;
        sl->stmts = realloc_w(sl->stmts, sizeof(struct VdbStmt) * sl->capacity);
    }

    sl->stmts[sl->count++] = stmt;
}

struct VdbTokenList* vdbtokenlist_init() {
    struct VdbTokenList* tl = malloc_w(sizeof(struct VdbTokenList));
    tl->count = 0;
    tl->capacity = 8;
    tl->tokens = malloc_w(sizeof(struct VdbToken) * tl->capacity);

    return tl;
}

void vdbtokenlist_free(struct VdbTokenList* tl) {
    free(tl->tokens);
    free(tl);
}

void vdbtokenlist_append_token(struct VdbTokenList* tl, struct VdbToken t) {
    if (tl->count + 1 > tl->capacity) {
        tl->capacity *= 2;
        tl->tokens = realloc_w(tl->tokens, sizeof(struct VdbToken) * tl->capacity);
    }

    tl->tokens[tl->count++] = t;
}

static void vdblexer_skip_whitespace(char* command, int* cur) {
    char c;
    while ((c = command[*cur]) == ' ' || c == '\n' || c == '\t') {
        (*cur)++;
    }
}

static struct VdbToken vdblexer_read_number(char* command, int* cur) {
    struct VdbToken t;
    t.type = VDBT_INT;
    t.lexeme = &command[*cur];

    t.len = 1;
   
    char c; 
    while ((c = command[++(*cur)]) != ' ' && c != EOF && c != '\0') {
        t.len++;
    }

    return t;
}

static struct VdbToken vdblexer_read_string(char* command, int* cur) {
    struct VdbToken t;
    t.type = VDBT_STRING;
    t.lexeme = &command[*cur + 1]; //skip first "

    t.len = 1;
   
    char c; 
    while ((c = command[++(*cur)]) != '\"' && c != EOF && c != '\0') {
        t.len++;
    }

    //go past final "
    ++(*cur);

    return t;
}

static struct VdbToken vdblexer_read_word(char* command, int* cur) {
    struct VdbToken t;
    t.type = VDBT_IDENTIFIER;
    t.lexeme = &command[*cur];

    t.len = 1;
   
    char c; 
    while ((c = command[++(*cur)]) != ' ' && c != EOF && c != '\0') {
        t.len++;
    }

    if (strncmp(t.lexeme, "exit", 4) == 0) {
        t.type = VDBT_EXIT;
    } else if (strncmp(t.lexeme, "open", 4) == 0) {
        t.type = VDBT_OPEN;
    } else if (strncmp(t.lexeme, "close", 5) == 0) {
        t.type = VDBT_CLOSE;
    } else if (strncmp(t.lexeme, "create", 6) == 0) {
        t.type = VDBT_CREATE;
    } else if (strncmp(t.lexeme, "drop", 4) == 0) {
        t.type = VDBT_DROP;
    }

    return t;
}

struct VdbToken vdblexer_read_token(char* command, int* cur) {
    vdblexer_skip_whitespace(command, cur); 

    char c = command[*cur];
    switch (c) {
        case '.':
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
            return vdblexer_read_number(command, cur);
        case '\"':
            return vdblexer_read_string(command, cur);
        default:
            return vdblexer_read_word(command, cur);
    }
}

struct VdbTokenList* vdb_lex(char* command) {
    struct VdbTokenList* tl = vdbtokenlist_init();
    int cur = 0;
    while (cur < (int)strlen(command)) {
        struct VdbToken t = vdblexer_read_token(command, &cur);
        vdbtokenlist_append_token(tl, t);
    }

    return tl;
}

struct VdbToken vdbparser_peek_token(struct VdbParser* parser) {
    return parser->tl->tokens[parser->current];
}

struct VdbToken vdbparser_consume_token(struct VdbParser* parser) {
    return parser->tl->tokens[parser->current++];
}

struct VdbExpr vdbparser_parse_identifier(struct VdbParser* parser) {
    struct VdbExpr expr;
    switch (vdbparser_peek_token(parser).type) {
        case VDBT_IDENTIFIER: {
            expr.type = VDBEX_IDENTIFIER;
            expr.as.identifier.token = vdbparser_consume_token(parser);
            break;
        }
        default:
            printf("not implemented\n");
            break;
    }
    return expr;
}

struct VdbExpr vdbparser_parse_expr(struct VdbParser* parser) {
    return vdbparser_parse_identifier(parser); 
}

struct VdbStmt vdbparser_parse_stmt(struct VdbParser* parser) {
    struct VdbStmt stmt;
    switch (vdbparser_consume_token(parser).type) {
        case VDBT_OPEN: {
            stmt.type = VDBST_OPEN;
            stmt.as.open.db_name = vdbparser_parse_expr(parser);
            break;
        }
        case VDBT_CLOSE: {
            stmt.type = VDBST_CLOSE;
            stmt.as.close.db_name = vdbparser_parse_expr(parser);
            break;
        }
        case VDBT_EXIT: {
            stmt.type = VDBST_EXIT;
            break; 
        }
        default:
            printf("not implemented yet\n");
            break;
    }

    return stmt;
}

struct VdbStmtList* vdb_parse(struct VdbTokenList* tl) {
    struct VdbStmtList* sl = vdbstmtlist_init();

    struct VdbParser parser;
    parser.tl = tl;
    parser.current = 0;
   
    while (parser.current < parser.tl->count) {
        vdbstmtlist_append_stmt(sl, vdbparser_parse_stmt(&parser));
    }

    return sl;
}

bool vdb_execute(struct VdbStmtList* sl) {
    for (int i = 0; i < sl->count; i++) {
        struct VdbStmt* stmt = &sl->stmts[i];
        switch (stmt->type) {
            case VDBST_EXIT: {
                printf("query plan: exit\n");
                return true;
            }
            case VDBST_OPEN: {
                struct VdbExpr expr = stmt->as.open.db_name;
                printf("query plan: open %.*s\n", expr.as.identifier.token.len, expr.as.identifier.token.lexeme);
                break;
            }
            case VDBST_CLOSE: {
                struct VdbExpr expr = stmt->as.close.db_name;
                printf("query plan: close %.*s\n", expr.as.identifier.token.len, expr.as.identifier.token.lexeme);
                break;
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
    while (true) {
        printf("> ");
        nread = getline(&line, &len, stdin);
        if (nread == -1)
            break;

        struct VdbTokenList* tl = vdb_lex(line);
        struct VdbStmtList* sl = vdb_parse(tl);
        //struct VdbOpList* ol = vdb_generate_code(sl);
        //vdb_execute(ol);
        bool end = vdb_execute(sl);

        vdbstmtlist_free(sl);
        vdbtokenlist_free(tl);

        if (end) {
            break;
        }
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
