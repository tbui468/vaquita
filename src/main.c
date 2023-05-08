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
    VDBT_INSERT,
    VDBT_INTO,
    VDBT_VALUES,
    VDBT_IDENTIFIER,
    VDBT_STR,
    VDBT_INT,
    VDBT_TRUE,
    VDBT_FALSE,
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

enum VdbStmtType {
    VDBST_EXIT,
    VDBST_OPEN,
    VDBST_CLOSE,
    VDBST_CREATE,
    VDBST_DROP,
    VDBST_INSERT
};

struct VdbStmt {
    enum VdbStmtType type;
    union {
        struct VdbToken db_name;
        struct VdbToken table_name;
        struct {
            struct VdbToken table_name;
            struct VdbTokenList* attributes;
            struct VdbTokenList* types;
        } create;
        struct {
            struct VdbToken table_name;
            struct VdbTokenList* attributes;
            struct VdbTokenList* values;
        } insert;
    } get;
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
            case VDBT_IDENTIFIER: printf("VDBT_IDENTIFIER: %.*s\n", t.len, t.lexeme); break;
            case VDBT_STR: printf("VDBT_TYPE_STR: %.*s\n", t.len, t.lexeme); break;
            case VDBT_INT: printf("VDBT_INT: %.*s\n", t.len, t.lexeme); break;
            case VDBT_TYPE_STR: printf("VDBT_TYPE_STR\n"); break;
            case VDBT_TYPE_INT: printf("VDBT_TYPE_INT\n"); break;
            case VDBT_TYPE_BOOL: printf("VDBT_TYPE_BOOL\n"); break;
            case VDBT_LPAREN: printf("VDBT_LPAREN\n"); break;
            case VDBT_RPAREN: printf("VDBT_RPAREN\n"); break;
            case VDBT_COMMA: printf("VDBT_COMMA\n"); break;
            case VDBT_INSERT: printf("VDBT_INSERT\n"); break;
            case VDBT_INTO: printf("VDBT_INTO\n"); break;
            case VDBT_VALUES: printf("VDBT_VALUES\n"); break;
            case VDBT_TRUE: printf("VDBT_TRUE\n"); break;
            case VDBT_FALSE: printf("VDBT_FALSE\n"); break;
            default: break;
        }
    }
}


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

bool is_alpha(char c) {
    return ('A' <= c && c <= 'Z') || ('a' <= c && c <= 'z');
}

bool is_numeric(char c) {
    return '0' <= c && c <= '9';
}

bool is_alpha_numeric(char c) {
    return is_alpha(c) || is_numeric(c) || c == '_';
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
    while ((c = command[++(*cur)]) != ' ' && c != EOF && c != '\0' && is_numeric(c)) {
        t.len++;
    }

    return t;
}

static struct VdbToken vdblexer_read_string(char* command, int* cur) {
    struct VdbToken t;
    t.type = VDBT_STR;
    t.lexeme = &command[*cur + 1]; //skip first "

    t.len = 0;
   
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
    } else if (strncmp(t.lexeme, "insert", 6) == 0) {
        t.type = VDBT_INSERT;
    } else if (strncmp(t.lexeme, "into", 4) == 0) {
        t.type = VDBT_INTO;
    } else if (strncmp(t.lexeme, "values", 6) == 0) {
        t.type = VDBT_VALUES;
    } else if (strncmp(t.lexeme, "true", 4) == 0) {
        t.type = VDBT_TRUE;
    } else if (strncmp(t.lexeme, "false", 5) == 0) {
        t.type = VDBT_FALSE;
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

struct VdbStmt vdbparser_parse_stmt(struct VdbParser* parser) {
    struct VdbStmt stmt;
    switch (vdbparser_consume_token(parser).type) {
        case VDBT_OPEN: {
            stmt.type = VDBST_OPEN;
            stmt.get.db_name = vdbparser_consume_token(parser);
            break;
        }
        case VDBT_CLOSE: {
            stmt.type = VDBST_CLOSE;
            stmt.get.db_name = vdbparser_consume_token(parser);
            break;
        }
        case VDBT_CREATE: {
            stmt.type = VDBST_CREATE;
            vdbparser_consume_token(parser); //TODO: parse error if not keyword 'table'
            stmt.get.create.table_name = vdbparser_consume_token(parser);
            stmt.get.create.attributes = vdbtokenlist_init();
            stmt.get.create.types = vdbtokenlist_init();
            vdbparser_consume_token(parser); //left paren
            while (vdbparser_peek_token(parser).type != VDBT_RPAREN) {
                vdbtokenlist_append_token(stmt.get.create.attributes, vdbparser_consume_token(parser));
                vdbtokenlist_append_token(stmt.get.create.types, vdbparser_consume_token(parser));
                if (vdbparser_peek_token(parser).type == VDBT_COMMA) {
                    vdbparser_consume_token(parser);
                }
            }

            vdbparser_consume_token(parser); //right paren
            break;
        }
        case VDBT_DROP: {
            stmt.type = VDBST_DROP;
            vdbparser_consume_token(parser); //TODO: parse error if not keyword 'table'
            stmt.get.table_name = vdbparser_consume_token(parser);
            break;
        }
        case VDBT_INSERT: {
            stmt.type = VDBST_INSERT;
            vdbparser_consume_token(parser); //TODO: skip 'into'
            stmt.get.insert.table_name = vdbparser_consume_token(parser);

            stmt.get.insert.attributes = vdbtokenlist_init();
            vdbparser_consume_token(parser); //left paren
            while (vdbparser_peek_token(parser).type != VDBT_RPAREN) {
                vdbtokenlist_append_token(stmt.get.insert.attributes, vdbparser_consume_token(parser));
                if (vdbparser_peek_token(parser).type == VDBT_COMMA) {
                    vdbparser_consume_token(parser);
                }
            }
            vdbparser_consume_token(parser); //right paren

            vdbparser_consume_token(parser); //'values' token

            stmt.get.insert.values = vdbtokenlist_init();
            vdbparser_consume_token(parser); //left paren
            while (vdbparser_peek_token(parser).type != VDBT_RPAREN) {
                vdbtokenlist_append_token(stmt.get.insert.values, vdbparser_consume_token(parser));
                if (vdbparser_peek_token(parser).type == VDBT_COMMA) {
                    vdbparser_consume_token(parser);
                }
            }
            vdbparser_consume_token(parser); //right paren
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
                printf("<open [%.*s]>\n", stmt->get.db_name.len, stmt->get.db_name.lexeme);
                break;
            }
            case VDBST_CLOSE: {
                printf("<close [%.*s]>\n", stmt->get.db_name.len, stmt->get.db_name.lexeme);
                break;
            }
            case VDBST_CREATE: {
                printf("<create table [%.*s]>\n", stmt->get.create.table_name.len, stmt->get.create.table_name.lexeme);
                for (int i = 0; i < stmt->get.create.attributes->count; i++) {
                    struct VdbToken attribute = stmt->get.create.attributes->tokens[i];
                    struct VdbToken type = stmt->get.create.types->tokens[i];
                    printf("    [%.*s]: %.*s\n", attribute.len, attribute.lexeme, type.len, type.lexeme);
                }
                break;
            }
            case VDBST_DROP: {
                printf("<drop table [%.*s]>\n", stmt->get.table_name.len, stmt->get.table_name.lexeme);
                break;
            }
            case VDBST_INSERT: {
                printf("<insert into table [%.*s]>\n", stmt->get.insert.table_name.len, stmt->get.insert.table_name.lexeme);
                for (int i = 0; i < stmt->get.insert.attributes->count; i++) {
                    struct VdbToken attribute = stmt->get.insert.attributes->tokens[i];
                    struct VdbToken value = stmt->get.insert.values->tokens[i];
                    printf("    [%.*s]: %.*s\n", attribute.len, attribute.lexeme, value.len, value.lexeme);
                }
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
        //vdbtokenlist_print(tl);
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
