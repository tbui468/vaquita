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

void vdblexer_skip_whitespace(char* command, int* cur) {
    char c;
    while ((c = command[*cur]) == ' ' || c == '\n' || c == '\t') {
        (*cur)++;
    }
}

struct VdbTokenList* vdblexer_lex(char* command) {
    struct VdbTokenList* tl = vdbtokenlist_init();
    int cur = 0;
    while (cur < (int)strlen(command)) {
        struct VdbToken t = vdblexer_read_token(command, &cur);
        vdbtokenlist_append_token(tl, t);
    }

    struct VdbToken t;
    t.type = VDBT_EOF;
    t.lexeme = NULL;
    t.len = 0;
    vdbtokenlist_append_token(tl, t);

    return tl;
}

struct VdbToken vdblexer_read_number(char* command, int* cur) {
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

struct VdbToken vdblexer_read_string(char* command, int* cur) {
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

struct VdbToken vdblexer_read_word(char* command, int* cur) {
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
    } else if (strncmp(t.lexeme, "update", 6) == 0) {
        t.type = VDBT_UPDATE;
    } else if (strncmp(t.lexeme, "delete", 6) == 0) {
        t.type = VDBT_DELETE;
    } else if (strncmp(t.lexeme, "select", 6) == 0) {
        t.type = VDBT_SELECT;
    } else if (strncmp(t.lexeme, "where", 5) == 0) {
        t.type = VDBT_WHERE;
    } else if (strncmp(t.lexeme, "set", 3) == 0) {
        t.type = VDBT_SET;
    } else if (strncmp(t.lexeme, "from", 4) == 0) {
        t.type = VDBT_FROM;
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
            t.lexeme = &command[(*cur)++];
            t.len = 1;
            return t;
        }
        case ')': {
            struct VdbToken t;
            t.type = VDBT_RPAREN;
            t.lexeme = &command[(*cur)++];
            t.len = 1;
            return t;
        }
        case ',': {
            struct VdbToken t;
            t.type = VDBT_COMMA;
            t.lexeme = &command[(*cur)++];
            t.len = 1;
            return t;
        }
        case '=': {
            struct VdbToken t;
            t.type = VDBT_EQUALS;
            t.lexeme = &command[(*cur)++];
            t.len = 1;
            return t;
        }
        case '<': {
            struct VdbToken t;
            t.lexeme = &command[(*cur)++];
            if (command[*cur] == '=') {
                t.type = VDBT_LESS_EQUALS;
                (*cur)++;
                t.len = 2;
            } else {
                t.type = VDBT_LESS;
                t.len = 1;
            }
            return t;
        }
        case '>': {
            struct VdbToken t;
            t.type = VDBT_GREATER;
            t.lexeme = &command[(*cur)++];
            if (command[*cur] == '=') {
                t.type = VDBT_GREATER_EQUALS;
                (*cur)++;
                t.len = 2;
            } else {
                t.type = VDBT_GREATER;
                t.len = 1;
            }
            return t;
        }
        case '+': {
            struct VdbToken t;
            t.type = VDBT_PLUS;
            t.lexeme = &command[(*cur)++];
            t.len = 1;
            return t;
        }
        case '-': {
            struct VdbToken t;
            t.type = VDBT_MINUS;
            t.lexeme = &command[(*cur)++];
            t.len = 1;
            return t;
        }
        case '*': {
            struct VdbToken t;
            t.type = VDBT_STAR;
            t.lexeme = &command[(*cur)++];
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
