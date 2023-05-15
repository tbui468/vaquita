#ifndef VDB_TOKEN_H
#define VDB_TOKEN_H

enum VdbTokenType {
    VDBT_TYPE_STR,
    VDBT_TYPE_INT,
    VDBT_TYPE_BOOL,
    VDBT_TYPE_NULL,
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
    VDBT_LPAREN,
    VDBT_RPAREN,
    VDBT_COMMA,
    VDBT_EQUALS,
    VDBT_LESS,
    VDBT_GREATER,
    VDBT_LESS_EQUALS,
    VDBT_GREATER_EQUALS,
    VDBT_PLUS,
    VDBT_MINUS,
    VDBT_UPDATE,
    VDBT_DELETE,
    VDBT_SELECT,
    VDBT_WHERE,
    VDBT_SET,
    VDBT_FROM,
    VDBT_STAR,
    VDBT_SEMICOLON,
    VDBT_SHOW,
    VDBT_DATABASES,
    VDBT_DATABASE,
    VDBT_TABLES,
    VDBT_DESCRIBE,
    VDBT_CONNECT,
    VDBT_INVALID
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

struct VdbTokenList* vdbtokenlist_init();
void vdbtokenlist_free(struct VdbTokenList* tl);
void vdbtokenlist_append_token(struct VdbTokenList* tl, struct VdbToken t);
void vdbtokenlist_print(struct VdbTokenList* tl);

#endif //VDB_TOKEN_H
