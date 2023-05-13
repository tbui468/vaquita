#include <string.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

#include "lexer.h"
#include "util.h"

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
            case VDBT_EQUALS: printf("VDBT_EQUALS\n"); break;
            case VDBT_LESS: printf("VDBT_LESS\n"); break;
            case VDBT_GREATER: printf("VDBT_GREATER\n"); break;
            case VDBT_LESS_EQUALS: printf("VDBT_LESS_EQUALS\n"); break;
            case VDBT_GREATER_EQUALS: printf("VDBT_GREATER_EQUALS\n"); break;
            case VDBT_PLUS: printf("VDBT_PLUS\n"); break;
            case VDBT_MINUS: printf("VDBT_MINUS\n"); break;
            case VDBT_UPDATE: printf("VDBT_UPDATE\n"); break;
            case VDBT_DELETE: printf("VDBT_DELETE\n"); break;
            case VDBT_SELECT: printf("VDBT_SELECT\n"); break;
            case VDBT_WHERE: printf("VDBT_WHERE\n"); break;
            case VDBT_SET: printf("VDBT_SET\n"); break;
            case VDBT_FROM: printf("VDBT_FROM\n"); break;
            case VDBT_STAR: printf("VDBT_STAR\n"); break;
            case VDBT_SEMICOLON: printf("VDBT_SEMICOLON\n"); break;
            case VDBT_SHOW: printf("VDBT_SHOW\n"); break;
            case VDBT_DATABASES: printf("VDBT_DATABASES\n"); break;
            case VDBT_DATABASE: printf("VDBT_DATABASE\n"); break;
            case VDBT_TABLES: printf("VDBT_TABLES\n"); break;
            case VDBT_DESCRIBE: printf("VDBT_DESCRIBE\n"); break;
            case VDBT_CONNECT: printf("VDBT_CONNECT\n"); break;
            case VDBT_INVALID: printf("VDBT_INVALID: %.*s\n", t.len, t.lexeme); break;
            default: printf("not implemented\n"); break;
        }
    }
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

void vdblexer_skip_whitespace(struct VdbLexer* lexer) {
    char c;
    while ((c = lexer->src[lexer->cur]) == ' ' || c == '\n' || c == '\t') {
        lexer->cur++;
    }
}

enum VdbReturnCode vdblexer_read_until_whitespace(struct VdbLexer* lexer, struct VdbToken* t) {
    t->type = VDBT_INVALID;
    t->lexeme = &lexer->src[lexer->cur];
    t->len = 1;
    char c;
    while ((c = lexer->src[++lexer->cur]) != ' ' && c != '\n' && c != '\t' && c != ';') {
        t->len++;
    }

    return VDBRC_SUCCESS;
}

enum VdbReturnCode vdblexer_lex(char* src, struct VdbTokenList** tokens, struct VdbErrorList** errors) {
    struct VdbLexer lexer;
    lexer.src = src;
    lexer.cur = 0;
    *tokens = vdbtokenlist_init();
    *errors = vdberrorlist_init();

    while (lexer.cur < (int)strlen(lexer.src)) {
        struct VdbToken t;
        if (vdblexer_read_token(&lexer, &t) == VDBRC_SUCCESS) {
            vdbtokenlist_append_token(*tokens, t);
        } else {
            vdberrorlist_append_error(*errors, 1, "unrecognized token: %.*s", t.len, t.lexeme);
        }
    }

    if ((*errors)->count > 0)
        return VDBRC_ERROR;
    else
        return VDBRC_SUCCESS;
}

enum VdbReturnCode vdblexer_read_number(struct VdbLexer* lexer, struct VdbToken* t) {
    t->type = VDBT_INT;
    t->lexeme = &lexer->src[lexer->cur];
    t->len = 1;
   
    char c; 
    while ((c = lexer->src[++lexer->cur]) != ' ' && c != EOF && c != '\0' && is_numeric(c)) {
        t->len++;
    }

    return VDBRC_SUCCESS;
}

enum VdbReturnCode vdblexer_read_string(struct VdbLexer* lexer, struct VdbToken* t) {
    t->type = VDBT_STR;
    t->lexeme = &lexer->src[lexer->cur + 1]; //skip first "
    t->len = 0;
   
    char c; 
    while ((c = lexer->src[++lexer->cur]) != '\"' && c != EOF && c != '\0') {
        t->len++;
    }

    //go past final "
    lexer->cur++;

    return VDBRC_SUCCESS;
}

enum VdbReturnCode vdblexer_read_word(struct VdbLexer* lexer, struct VdbToken* t) {
    t->type = VDBT_IDENTIFIER;
    t->lexeme = &lexer->src[lexer->cur];
    t->len = 1;
   
    char c; 
    while ((c = lexer->src[++lexer->cur]) != ' ' && c != EOF && c != '\0' && is_alpha_numeric(c)) {
        t->len++;
    }

    if (strncmp(t->lexeme, "exit", 4) == 0) {
        t->type = VDBT_EXIT;
    } else if (strncmp(t->lexeme, "open", 4) == 0) {
        t->type = VDBT_OPEN;
    } else if (strncmp(t->lexeme, "close", 5) == 0) {
        t->type = VDBT_CLOSE;
    } else if (strncmp(t->lexeme, "create", 6) == 0) {
        t->type = VDBT_CREATE;
    } else if (strncmp(t->lexeme, "drop", 4) == 0) {
        t->type = VDBT_DROP;
    } else if (strncmp(t->lexeme, "tables", 6) == 0) {
        t->type = VDBT_TABLES;
    } else if (strncmp(t->lexeme, "table", 5) == 0) {
        t->type = VDBT_TABLE;
    } else if (strncmp(t->lexeme, "string", 6) == 0) {
        t->type = VDBT_TYPE_STR;
    } else if (strncmp(t->lexeme, "int", 3) == 0) {
        t->type = VDBT_TYPE_INT;
    } else if (strncmp(t->lexeme, "bool", 4) == 0) {
        t->type = VDBT_TYPE_BOOL;
    } else if (strncmp(t->lexeme, "insert", 6) == 0) {
        t->type = VDBT_INSERT;
    } else if (strncmp(t->lexeme, "into", 4) == 0) {
        t->type = VDBT_INTO;
    } else if (strncmp(t->lexeme, "values", 6) == 0) {
        t->type = VDBT_VALUES;
    } else if (strncmp(t->lexeme, "true", 4) == 0) {
        t->type = VDBT_TRUE;
    } else if (strncmp(t->lexeme, "false", 5) == 0) {
        t->type = VDBT_FALSE;
    } else if (strncmp(t->lexeme, "update", 6) == 0) {
        t->type = VDBT_UPDATE;
    } else if (strncmp(t->lexeme, "delete", 6) == 0) {
        t->type = VDBT_DELETE;
    } else if (strncmp(t->lexeme, "select", 6) == 0) {
        t->type = VDBT_SELECT;
    } else if (strncmp(t->lexeme, "where", 5) == 0) {
        t->type = VDBT_WHERE;
    } else if (strncmp(t->lexeme, "set", 3) == 0) {
        t->type = VDBT_SET;
    } else if (strncmp(t->lexeme, "from", 4) == 0) {
        t->type = VDBT_FROM;
    } else if (strncmp(t->lexeme, "show", 4) == 0) {
        t->type = VDBT_SHOW;
    } else if (strncmp(t->lexeme, "databases", 9) == 0) {
        t->type = VDBT_DATABASES;
    } else if (strncmp(t->lexeme, "database", 8) == 0) {
        t->type = VDBT_DATABASE;
    } else if (strncmp(t->lexeme, "describe", 8) == 0) {
        t->type = VDBT_DESCRIBE;
    } else if (strncmp(t->lexeme, "connect", 7) == 0) {
        t->type = VDBT_CONNECT;
    }

    return VDBRC_SUCCESS;
}

enum VdbReturnCode vdblexer_read_token(struct VdbLexer* lexer, struct VdbToken* t) {
    vdblexer_skip_whitespace(lexer); 

    char c = lexer->src[lexer->cur];
    switch (c) {
        case '(': {
            t->type = VDBT_LPAREN;
            t->lexeme = &lexer->src[lexer->cur++];
            t->len = 1;
            break;
        }
        case ')': {
            t->type = VDBT_RPAREN;
            t->lexeme = &lexer->src[lexer->cur++];
            t->len = 1;
            break;
        }
        case ',': {
            t->type = VDBT_COMMA;
            t->lexeme = &lexer->src[lexer->cur++];
            t->len = 1;
            break;
        }
        case '=': {
            t->type = VDBT_EQUALS;
            t->lexeme = &lexer->src[lexer->cur++];
            t->len = 1;
            break;
        }
        case '<': {
            t->lexeme = &lexer->src[lexer->cur++];
            if (lexer->src[lexer->cur] == '=') {
                t->type = VDBT_LESS_EQUALS;
                lexer->cur++;
                t->len = 2;
            } else {
                t->type = VDBT_LESS;
                t->len = 1;
            }
            break;
        }
        case '>': {
            t->type = VDBT_GREATER;
            t->lexeme = &lexer->src[lexer->cur++];
            if (lexer->src[lexer->cur] == '=') {
                t->type = VDBT_GREATER_EQUALS;
                lexer->cur++;
                t->len = 2;
            } else {
                t->type = VDBT_GREATER;
                t->len = 1;
            }
            break;
        }
        case '+': {
            t->type = VDBT_PLUS;
            t->lexeme = &lexer->src[lexer->cur++];
            t->len = 1;
            break;
        }
        case '-': {
            t->type = VDBT_MINUS;
            t->lexeme = &lexer->src[lexer->cur++];
            t->len = 1;
            break;
        }
        case '*': {
            t->type = VDBT_STAR;
            t->lexeme = &lexer->src[lexer->cur++];
            t->len = 1;
            break;
        }
        case ';': {
            t->type = VDBT_SEMICOLON;
            t->lexeme = &lexer->src[lexer->cur++];
            t->len = 1;
            break;
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
            return vdblexer_read_number(lexer, t);
        case '\"':
            return vdblexer_read_string(lexer, t);
        default:
            if (is_alpha(c) || c == '_') {
                return vdblexer_read_word(lexer, t);
            } else {
                vdblexer_read_until_whitespace(lexer, t);
                return VDBRC_ERROR;
            }
    }

    return VDBRC_SUCCESS;
}
