#ifndef VDB_LEXER_H
#define VDB_LEXER_H

#include "error.h"

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

struct VdbLexer {
    char* src;
    int cur;
};

//TODO: not all of these need to be public - make some static in .c file
struct VdbTokenList* vdbtokenlist_init();
void vdbtokenlist_free(struct VdbTokenList* tl);
void vdbtokenlist_append_token(struct VdbTokenList* tl, struct VdbToken t);
void vdbtokenlist_print(struct VdbTokenList* tl);

bool is_alpha(char c);
bool is_numeric(char c);
bool is_alpha_numeric(char c);

enum VdbReturnCode vdblexer_lex(char* src, struct VdbTokenList** tokens, struct VdbErrorList** errors);
void vdblexer_skip_whitespace(struct VdbLexer* lexer);
enum VdbReturnCode vdblexer_read_number(struct VdbLexer* lexer, struct VdbToken* t);
enum VdbReturnCode vdblexer_read_string(struct VdbLexer* lexer, struct VdbToken* t);
enum VdbReturnCode vdblexer_read_word(struct VdbLexer* lexer, struct VdbToken* t);
enum VdbReturnCode vdblexer_read_token(struct VdbLexer* lexer, struct VdbToken* t);

#endif //VDB_LEXER_H
