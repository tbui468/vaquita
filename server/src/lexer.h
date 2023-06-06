#ifndef VDB_LEXER_H
#define VDB_LEXER_H

#include "error.h"
#include "token.h"

struct VdbLexer {
    char* src;
    int cur;
};

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
