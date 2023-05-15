#include <string.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

#include "lexer.h"
#include "util.h"


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
    } else if (strncmp(t->lexeme, "into", 4) == 0) {
        t->type = VDBT_INTO;
    } else if (strncmp(t->lexeme, "int", 3) == 0) {
        t->type = VDBT_TYPE_INT;
    } else if (strncmp(t->lexeme, "bool", 4) == 0) {
        t->type = VDBT_TYPE_BOOL;
    } else if (strncmp(t->lexeme, "insert", 6) == 0) {
        t->type = VDBT_INSERT;
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
    } else if (strncmp(t->lexeme, "null", 4) == 0) {
        t->type = VDBT_TYPE_NULL;
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
