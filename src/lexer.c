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
            break;
        case 'b':
            if (t->len == 4 && strncmp("bool", t->lexeme, 4) == 0)
                t->type = VDBT_TYPE_BOOL;
            if (t->len == 2 && strncmp("by", t->lexeme, 2) == 0)
                t->type = VDBT_BY;
            break;
        case 'c':
            if (t->len == 5 && strncmp("close", t->lexeme, 5) == 0)
                t->type = VDBT_CLOSE;
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
        case 's':
            if (t->len == 6 && strncmp("string", t->lexeme, 6) == 0)
                t->type = VDBT_TYPE_STR;
            if (t->len == 6 && strncmp("select", t->lexeme, 6) == 0)
                t->type = VDBT_SELECT;
            if (t->len == 3 && strncmp("set", t->lexeme, 3) == 0)
                t->type = VDBT_SET;
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

    /*
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
    } else if (strncmp(t->lexeme, "float", 5) == 0) {
        t->type = VDBT_TYPE_FLOAT;
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
        t->type = VDBT_NULL;
    } else if (strncmp(t->lexeme, "if", 2) == 0) {
        t->type = VDBT_IF;
    } else if (strncmp(t->lexeme, "exists", 6) == 0) {
        t->type = VDBT_EXISTS;
    } else if (strncmp(t->lexeme, "not", 3) == 0) {
        t->type = VDBT_NOT;
    } else if (strncmp(t->lexeme, "and", 3) == 0) {
        t->type = VDBT_AND;
    } else if (t->len == 2 && strncmp(t->lexeme, "or", 2) == 0) {
        t->type = VDBT_OR;
    } else if (t->len == 2 && strncmp(t->lexeme, "is", 2) == 0) {
        t->type = VDBT_IS;
    }*/

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
            } else if (lexer->src[lexer->cur] == '>') {
                t->type = VDBT_NOT_EQUALS;
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
        case '/': {
            t->type = VDBT_SLASH;
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
