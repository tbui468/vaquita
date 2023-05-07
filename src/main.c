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
    VDBT_TABLE,
    VDBT_IDENTIFIER,
    VDBT_STR,
    VDBT_INT,
    VDBT_BOOL,
    VDBT_TYPE_STR,
    VDBT_TYPE_INT,
    VDBT_TYPE_BOOL,
    VDBT_LPAREN,
    VDBT_RPAREN,
    VDBT_COMMA
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

void vdbtokenlist_print(struct VdbTokenList* tl) {
    for (int i = 0; i < tl->count; i++) {
        struct VdbToken t = tl->tokens[i];
        switch (t.type) {
            case VDBT_EXIT: printf("VDBT_EXIT\n"); break;
            case VDBT_OPEN: printf("VDBT_OPEN\n"); break;
            case VDBT_CLOSE: printf("VDBT_CLOSE\n"); break;
            case VDBT_CREATE: printf("VDBT_CREATE\n"); break;
            case VDBT_DROP: printf("VDBT_DROP\n"); break;
            case VDBT_TABLE: printf("VDBT_TABLE\n"); break;
            case VDBT_IDENTIFIER: printf("VDBT_IDENTIFIER\n"); break;
            case VDBT_STR: printf("VDBT_STR\n"); break;
            case VDBT_INT: printf("VDBT_INT\n"); break;
            case VDBT_BOOL: printf("VDBT_BOOL\n"); break;
            case VDBT_TYPE_STR: printf("VDBT_TYPE_STR\n"); break;
            case VDBT_TYPE_INT: printf("VDBT_TYPE_INT\n"); break;
            case VDBT_TYPE_BOOL: printf("VDBT_TYPE_BOOL\n"); break;
            case VDBT_LPAREN: printf("VDBT_LPAREN\n"); break;
            case VDBT_RPAREN: printf("VDBT_RPAREN\n"); break;
            case VDBT_COMMA: printf("VDBT_COMMA\n"); break;
            default: break;
        }
    }
}

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
            struct VdbExpr schema;
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
    t.type = VDBT_STR;
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

bool is_numeric(char c) {
    return '0' <= c && c <= '9';
}

bool is_alpha_numeric(char c) {
    return ('A' <= c && c <= 'Z') || ('a' <= c && c <= 'z') || is_numeric(c) || c == '_';
}

static struct VdbToken vdblexer_read_word(char* command, int* cur) {
    struct VdbToken t;
    t.type = VDBT_IDENTIFIER;
    t.lexeme = &command[*cur];

    t.len = 1;
   
    char c; 
    while ((c = command[++(*cur)]) != ' ' && c != EOF && c != '\0' && is_alpha_numeric(c)) {
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
    } else if (strncmp(t.lexeme, "table", 5) == 0) {
        t.type = VDBT_TABLE;
    } else if (strncmp(t.lexeme, "string", 6) == 0) {
        t.type = VDBT_TYPE_STR;
    } else if (strncmp(t.lexeme, "int", 3) == 0) {
        t.type = VDBT_TYPE_INT;
    } else if (strncmp(t.lexeme, "bool", 4) == 0) {
        t.type = VDBT_TYPE_BOOL;
    }

    return t;
}

struct VdbToken vdblexer_read_token(char* command, int* cur) {
    vdblexer_skip_whitespace(command, cur); 

    char c = command[*cur];
    switch (c) {
        case '(': {
            struct VdbToken t;
            t.type = VDBT_LPAREN;
            t.lexeme = &command[++(*cur)];
            t.len = 1;
            return t;
        }
        case ')': {
            struct VdbToken t;
            t.type = VDBT_RPAREN;
            t.lexeme = &command[++(*cur)];
            t.len = 1;
            return t;
        }
        case ',': {
            struct VdbToken t;
            t.type = VDBT_COMMA;
            t.lexeme = &command[++(*cur)];
            t.len = 1;
            return t;
        }
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

struct VdbExpr vdbparser_parse_schema(struct VdbParser* parser) {
    vdbparser_consume_token(parser); //left paren
    struct VdbExpr expr;
    expr.type = VDBEX_SCHEMA;
    expr.as.schema.names = vdbtokenlist_init();
    expr.as.schema.types = vdbtokenlist_init();

    while (vdbparser_peek_token(parser).type != VDBT_RPAREN) {
        vdbtokenlist_append_token(expr.as.schema.names, vdbparser_consume_token(parser));
        vdbtokenlist_append_token(expr.as.schema.types, vdbparser_consume_token(parser));
        if (vdbparser_peek_token(parser).type == VDBT_COMMA) {
            vdbparser_consume_token(parser);
        }
    }

    vdbparser_consume_token(parser); //right paren
    return expr;
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

struct VdbStmt vdbparser_parse_stmt(struct VdbParser* parser) {
    struct VdbStmt stmt;
    switch (vdbparser_consume_token(parser).type) {
        case VDBT_OPEN: {
            stmt.type = VDBST_OPEN;
            stmt.as.open.db_name = vdbparser_parse_identifier(parser);
            break;
        }
        case VDBT_CLOSE: {
            stmt.type = VDBST_CLOSE;
            stmt.as.close.db_name = vdbparser_parse_identifier(parser);
            break;
        }
        case VDBT_CREATE: {
            stmt.type = VDBST_CREATE;
            vdbparser_consume_token(parser); //TODO: parse error if not keyword 'table'
            stmt.as.create.table_name = vdbparser_parse_identifier(parser);
            stmt.as.create.schema = vdbparser_parse_schema(parser);
            break;
        }
        case VDBT_DROP: {
            stmt.type = VDBST_DROP;
            vdbparser_consume_token(parser); //TODO: parse error if not keyword 'table'
            stmt.as.create.table_name = vdbparser_parse_identifier(parser);
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

//TODO: actually make API call to storage engine
bool vdb_execute(struct VdbStmtList* sl) {
    for (int i = 0; i < sl->count; i++) {
        struct VdbStmt* stmt = &sl->stmts[i];
        switch (stmt->type) {
            case VDBST_EXIT: {
                printf("<exit>\n");
                return true;
            }
            case VDBST_OPEN: {
                struct VdbExpr expr = stmt->as.open.db_name;
                printf("<open [%.*s]>\n", expr.as.identifier.token.len, expr.as.identifier.token.lexeme);
                break;
            }
            case VDBST_CLOSE: {
                struct VdbExpr expr = stmt->as.close.db_name;
                printf("<close [%.*s]>\n", expr.as.identifier.token.len, expr.as.identifier.token.lexeme);
                break;
            }
            case VDBST_CREATE: {
                struct VdbExpr expr = stmt->as.create.table_name;
                printf("<create table [%.*s]>\n", expr.as.identifier.token.len, expr.as.identifier.token.lexeme);
                struct VdbExpr schema = stmt->as.create.schema;
                for (int i = 0; i < schema.as.schema.names->count; i++) {
                    struct VdbToken name = schema.as.schema.names->tokens[i];
                    struct VdbToken type = schema.as.schema.types->tokens[i];
                    printf("    [%.*s]: %.*s\n", name.len, name.lexeme, type.len, type.lexeme);
                }
                break;
            }
            case VDBST_DROP: {
                struct VdbExpr expr = stmt->as.create.table_name;
                printf("<drop table [%.*s]>\n", expr.as.identifier.token.len, expr.as.identifier.token.lexeme);
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
        line[strlen(line) - 1] = '\0'; //get rid of newline
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
