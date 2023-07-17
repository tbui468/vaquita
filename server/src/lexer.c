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

    int len = strlen(lexer.src);
    while (lexer.cur < len) {
        struct VdbToken t;
        if (vdblexer_read_token(&lexer, &t) == VDBRC_SUCCESS) {
            if (t.type != VDBT_COMMENT) {
                vdbtokenlist_append_token(*tokens, t);
            }
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
    t->lexeme = &lexer->src[lexer->cur];
    t->len = 1;

    bool is_decimal = *(t->lexeme) == '.';
   
    char c; 
    while ((c = lexer->src[++lexer->cur]) != ' ' && 
            c != EOF && 
            c != '\0' && 
            (is_numeric(c) || c == '.' || c == 'e' || c =='-')) {
        if (lexer->src[lexer->cur] == '.')
            is_decimal = true;
        t->len++;
    }

    if (is_decimal)
        t->type = VDBT_FLOAT;
    else
        t->type = VDBT_INT;

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

    switch (*(t->lexeme)) {
        case 'a':
            if (t->len == 3 && strncmp("and", t->lexeme, 3) == 0)
                t->type = VDBT_AND;
            if (t->len == 3 && strncmp("avg", t->lexeme, 3) == 0)
                t->type = VDBT_AVG;
            break;
        case 'b':
            if (t->len == 5 && strncmp("begin", t->lexeme, 5) == 0)
                t->type = VDBT_BEGIN;
            if (t->len == 4 && strncmp("bool", t->lexeme, 4) == 0)
                t->type = VDBT_TYPE_BOOL;
            if (t->len == 2 && strncmp("by", t->lexeme, 2) == 0)
                t->type = VDBT_BY;
            break;
        case 'c':
            if (t->len == 5 && strncmp("close", t->lexeme, 5) == 0)
                t->type = VDBT_CLOSE;
            if (t->len == 6 && strncmp("commit", t->lexeme, 6) == 0)
                t->type = VDBT_COMMIT;
            if (t->len == 5 && strncmp("count", t->lexeme, 5) == 0)
                t->type = VDBT_COUNT;
            if (t->len == 6 && strncmp("create", t->lexeme, 6) == 0)
                t->type = VDBT_CREATE;
            if (t->len == 7 && strncmp("connect", t->lexeme, 7) == 0)
                t->type = VDBT_CONNECT;
            break;
        case 'd':
            if (t->len == 4 && strncmp("drop", t->lexeme, 4) == 0)
                t->type = VDBT_DROP;
            if (t->len == 4 && strncmp("desc", t->lexeme, 4) == 0)
                t->type = VDBT_DESC;
            if (t->len == 6 && strncmp("delete", t->lexeme, 6) == 0)
                t->type = VDBT_DELETE;
            if (t->len == 8 && strncmp("describe", t->lexeme, 8) == 0)
                t->type = VDBT_DESCRIBE;
            if (t->len == 8 && strncmp("database", t->lexeme, 8) == 0)
                t->type = VDBT_DATABASE;
            if (t->len == 9 && strncmp("databases", t->lexeme, 9) == 0)
                t->type = VDBT_DATABASES;
            if (t->len == 8 && strncmp("distinct", t->lexeme, 8) == 0)
                t->type = VDBT_DISTINCT;
            break;
        case 'e':
            if (t->len == 4 && strncmp("exit", t->lexeme, 4) == 0)
                t->type = VDBT_EXIT;
            if (t->len == 6 && strncmp("exists", t->lexeme, 6) == 0)
                t->type = VDBT_EXISTS;
            break;
        case 'f':
            if (t->len == 5 && strncmp("float", t->lexeme, 5) == 0)
                t->type = VDBT_TYPE_FLOAT;
            if (t->len == 5 && strncmp("false", t->lexeme, 5) == 0)
                t->type = VDBT_FALSE;
            if (t->len == 4 && strncmp("from", t->lexeme, 4) == 0)
                t->type = VDBT_FROM;
            break;
        case 'g':
            if (t->len == 5 && strncmp("group", t->lexeme, 5) == 0)
                t->type = VDBT_GROUP;
            break;
        case 'h':
            if (t->len == 6 && strncmp("having", t->lexeme, 6) == 0)
                t->type = VDBT_HAVING;
            break;
        case 'i':
            if (t->len == 3 && strncmp("int", t->lexeme, 3) == 0)
                t->type = VDBT_TYPE_INT;
            if (t->len == 4 && strncmp("into", t->lexeme, 4) == 0)
                t->type = VDBT_INTO;
            if (t->len == 6 && strncmp("insert", t->lexeme, 6) == 0)
                t->type = VDBT_INSERT;
            if (t->len == 2 && strncmp("if", t->lexeme, 2) == 0)
                t->type = VDBT_IF;
            if (t->len == 2 && strncmp("is", t->lexeme, 2) == 0)
                t->type = VDBT_IS;
            break;
        case 'k':
            if (t->len == 3 && strncmp("key", t->lexeme, 3) == 0)
                t->type = VDBT_KEY;
            break;
        case 'l':
            if (t->len == 5 && strncmp("limit", t->lexeme, 5) == 0)
                t->type = VDBT_LIMIT;
            break;
        case 'm':
            if (t->len == 3 && strncmp("max", t->lexeme, 3) == 0)
                t->type = VDBT_MAX;
            if (t->len == 3 && strncmp("min", t->lexeme, 3) == 0)
                t->type = VDBT_MIN;
            break;
        case 'n':
            if (t->len == 4 && strncmp("null", t->lexeme, 4) == 0)
                t->type = VDBT_NULL;
            if (t->len == 3 && strncmp("not", t->lexeme, 3) == 0)
                t->type = VDBT_NOT;
            break;
        case 'o':
            if (t->len == 4 && strncmp("open", t->lexeme, 4) == 0)
                t->type = VDBT_OPEN;
            if (t->len == 2 && strncmp("or", t->lexeme, 2) == 0)
                t->type = VDBT_OR;
            if (t->len == 5 && strncmp("order", t->lexeme, 5) == 0)
                t->type = VDBT_ORDER;
            break;
        case 'r':
            if (t->len == 8 && strncmp("rollback", t->lexeme, 8) == 0)
                t->type = VDBT_ROLLBACK;
            break;
        case 's':
            if (t->len == 6 && strncmp("string", t->lexeme, 6) == 0)
                t->type = VDBT_TYPE_STR;
            if (t->len == 6 && strncmp("select", t->lexeme, 6) == 0)
                t->type = VDBT_SELECT;
            if (t->len == 3 && strncmp("set", t->lexeme, 3) == 0)
                t->type = VDBT_SET;
            if (t->len == 3 && strncmp("sum", t->lexeme, 3) == 0)
                t->type = VDBT_SUM;
            if (t->len == 4 && strncmp("show", t->lexeme, 4) == 0)
                t->type = VDBT_SHOW;
            break;
        case 't':
            if (t->len == 4 && strncmp("true", t->lexeme, 4) == 0)
                t->type = VDBT_TRUE;
            if (t->len == 5 && strncmp("table", t->lexeme, 5) == 0)
                t->type = VDBT_TABLE;
            if (t->len == 6 && strncmp("tables", t->lexeme, 6) == 0)
                t->type = VDBT_TABLES;
            break;
        case 'u':
            if (t->len == 6 && strncmp("update", t->lexeme, 6) == 0)
                t->type = VDBT_UPDATE;
            break;
        case 'v':
            if (t->len == 6 && strncmp("values", t->lexeme, 6) == 0)
                t->type = VDBT_VALUES;
            break;
        case 'w':
            if (t->len == 5 && strncmp("where", t->lexeme, 5) == 0)
                t->type = VDBT_WHERE;
            break;
        default:
            break;
    }


    return VDBRC_SUCCESS;
}

enum VdbReturnCode vdblexer_read_token(struct VdbLexer* lexer, struct VdbToken* t) {
    vdblexer_skip_whitespace(lexer); 

    t->len = 1;

    char c = lexer->src[lexer->cur];
    switch (c) {
        case '(': {
            t->type = VDBT_LPAREN;
            t->lexeme = &lexer->src[lexer->cur++];
            break;
        }
        case ')': {
            t->type = VDBT_RPAREN;
            t->lexeme = &lexer->src[lexer->cur++];
            break;
        }
        case ',': {
            t->type = VDBT_COMMA;
            t->lexeme = &lexer->src[lexer->cur++];
            break;
        }
        case '=': {
            t->type = VDBT_EQUALS;
            t->lexeme = &lexer->src[lexer->cur++];
            break;
        }
        case '<': {
            t->lexeme = &lexer->src[lexer->cur++];
            if (lexer->src[lexer->cur] == '=') {
                t->type = VDBT_LESS_EQUALS;
                lexer->cur++;
                t->len = 2;
            } else if (lexer->src[lexer->cur] == '>') {
                t->type = VDBT_NOT_EQUALS;
                lexer->cur++;
                t->len = 2;
            } else {
                t->type = VDBT_LESS;
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
            }
            break;
        }
        case '+': {
            t->type = VDBT_PLUS;
            t->lexeme = &lexer->src[lexer->cur++];
            break;
        }
        case '-': {
            t->type = VDBT_MINUS;
            t->lexeme = &lexer->src[lexer->cur++];
            break;
        }
        case '*': {
            t->type = VDBT_STAR;
            t->lexeme = &lexer->src[lexer->cur++];
            break;
        }
        case '/': {
            t->type = VDBT_SLASH;
            t->lexeme = &lexer->src[lexer->cur++];
            if (lexer->src[lexer->cur] == '/') {
                t->type = VDBT_COMMENT;
                lexer->cur++;
                t->len = 2;
                while ((c = lexer->src[lexer->cur]) != '\n') {
                    lexer->cur++;
                }
            }
            break;
        }
        case ';': {
            t->type = VDBT_SEMICOLON;
            t->lexeme = &lexer->src[lexer->cur++];
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
