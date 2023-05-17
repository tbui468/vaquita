#include <stdlib.h>

#include "token.h"
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
            case VDBT_STR: printf("VDBT_STR: %.*s\n", t.len, t.lexeme); break;
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
            case VDBT_TRUE: printf("VDBT_TRUE: %.*s\n", t.len, t.lexeme); break;
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
            case VDBT_NULL: printf("VDBT_NULL\n"); break;
            case VDBT_IF: printf("VDBT_IF\n"); break;
            case VDBT_EXISTS: printf("VDBT_EXISTS\n"); break;
            case VDBT_TYPE_NULL: printf("VDBT_TYPE_NULL\n"); break;
            case VDBT_ID: printf("VDBT_ID\n"); break;
            default: printf("not implemented\n"); break;
        }
    }
}
