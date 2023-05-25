#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "parser.h"
#include "util.h"


struct VdbExpr* vdbparser_parse_expr(struct VdbParser* parser);

void vdbparser_validate_attribute_name(struct VdbParser* parser, struct VdbToken t) {
    if (strncmp(t.lexeme, "id", 2) == 0)
        vdberrorlist_append_error(parser->errors, 1, "'id' cannot be used as attribute name");
}

enum VdbReturnCode vdbparser_parse(struct VdbTokenList* tokens, struct VdbStmtList** stmts, struct VdbErrorList** errors) {
    *stmts = vdbstmtlist_init();
    *errors = vdberrorlist_init();

    struct VdbParser parser;
    parser.tl = tokens;
    parser.current = 0;
    parser.errors = *errors;

    while (parser.current < tokens->count) {
        struct VdbStmt stmt;
        if (vdbparser_parse_stmt(&parser, &stmt) == VDBRC_SUCCESS) {
            vdbstmtlist_append_stmt(*stmts, stmt);
        } else {
            while (parser.current < tokens->count) {
                if (tokens->tokens[parser.current++].type == VDBT_SEMICOLON)
                    break;
            }
        }
    }

    if (parser.errors->count > 0)
        return VDBRC_ERROR;
    else
        return VDBRC_SUCCESS;
}

void vdbexpr_print(struct VdbExpr* expr) {
    switch (expr->type) {
        case VDBET_LITERAL:
            if (expr->as.literal.token.type == VDBT_STR) printf("\"");
            printf("%.*s", expr->as.literal.token.len, expr->as.literal.token.lexeme);
            if (expr->as.literal.token.type == VDBT_STR) printf("\"");
            break;
        case VDBET_IDENTIFIER:
            printf("[%.*s]", expr->as.identifier.token.len, expr->as.identifier.token.lexeme);
            break;
        case VDBET_UNARY:
            printf("(%.*s", expr->as.unary.op.len, expr->as.unary.op.lexeme);
            printf(" ");
            vdbexpr_print(expr->as.unary.right);
            printf(")");
            break;
        case VDBET_BINARY:
            printf("(%.*s", expr->as.binary.op.len, expr->as.binary.op.lexeme);
            printf(" ");
            vdbexpr_print(expr->as.binary.left);
            printf(" ");
            vdbexpr_print(expr->as.binary.right);
            printf(")");
            break;
        case VDBET_IS_NULL:
            printf("( ");
            vdbexpr_print(expr->as.is_null.left);
            printf(" is null )");
            break;
        case VDBET_IS_NOT_NULL:
            printf("( ");
            vdbexpr_print(expr->as.is_not_null.left);
            printf(" is not null )");
            break;
    }
}

struct VdbExpr* vdbexpr_init_literal(struct VdbToken token) {
    struct VdbExpr* expr = malloc_w(sizeof(struct VdbExpr));
    expr->type = VDBET_LITERAL;
    expr->as.literal.token = token;
    return expr;
}

struct VdbExpr* vdbexpr_init_identifier(struct VdbToken token) {
    struct VdbExpr* expr = malloc_w(sizeof(struct VdbExpr));
    expr->type = VDBET_IDENTIFIER; 
    expr->as.identifier.token = token;
    return expr;
}

struct VdbExpr* vdbexpr_init_unary(struct VdbToken op, struct VdbExpr* right) {
    struct VdbExpr* expr = malloc_w(sizeof(struct VdbExpr));
    expr->type = VDBET_UNARY;
    expr->as.unary.op = op;
    expr->as.unary.right = right;
    return expr;
}

struct VdbExpr* vdbexpr_init_binary(struct VdbToken op, struct VdbExpr* left, struct VdbExpr* right) {
    struct VdbExpr* expr = malloc_w(sizeof(struct VdbExpr));
    expr->type = VDBET_BINARY;
    expr->as.binary.op = op;
    expr->as.binary.left = left;
    expr->as.binary.right = right;
    return expr;
}

struct VdbExpr* vdbexpr_init_is_null(struct VdbExpr* left) {
    struct VdbExpr* expr = malloc_w(sizeof(struct VdbExpr));
    expr->type = VDBET_IS_NULL;
    expr->as.is_null.left = left;
    return expr;
}

struct VdbExpr* vdbexpr_init_is_not_null(struct VdbExpr* left) {
    struct VdbExpr* expr = malloc_w(sizeof(struct VdbExpr));
    expr->type = VDBET_IS_NOT_NULL;
    expr->as.is_not_null.left = left;
    return expr;
}

struct VdbValue vdbexpr_eval_identifier(struct VdbToken token, struct VdbRecord* rec, struct VdbSchema* schema) {
    uint32_t i;
    for (i = 0; i < schema->count; i++) {
        if (strncmp(schema->names[i], token.lexeme, token.len) == 0) {
            break;   
        }
    }

    if (rec->data[i].type == VDBT_TYPE_STR) {
        struct VdbValue d = rec->data[i];
        d.as.Str.len = rec->data[i].as.Str.len;
        d.as.Str.start = strdup_w(rec->data[i].as.Str.start);
        return d;
    }
    
    return rec->data[i];
}

struct VdbValue vdbexpr_eval_literal(struct VdbToken token) {
    struct VdbValue d;
    switch (token.type) {
        case VDBT_STR: {
            d.type = VDBT_TYPE_STR;
            d.as.Str.len = token.len;
            d.as.Str.start = malloc_w(token.len + 1);
            memcpy(d.as.Str.start, token.lexeme, token.len);
            d.as.Str.start[token.len] = '\0';
            break;
        }
        case VDBT_INT: {
            d.type = VDBT_TYPE_INT;
            int len = token.len;
            char s[len + 1];
            memcpy(s, token.lexeme, len);
            s[len] = '\0';
            d.as.Int = strtoll(s, NULL, 10);
            break;
        }
        case VDBT_FLOAT: {
            d.type = VDBT_TYPE_FLOAT;
            int len = token.len;
            char s[len + 1];
            memcpy(s, token.lexeme, len);
            s[len] = '\0';
            d.as.Float = strtod(s, NULL);
            break;
        }
        case VDBT_TRUE: {
            d.type = VDBT_TYPE_BOOL;
            d.as.Bool = true;
            break;
        }
        case VDBT_FALSE: {
            d.type = VDBT_TYPE_BOOL;
            d.as.Bool = false;
            break;
        }
        case VDBT_NULL: {
            d.type = VDBT_TYPE_NULL;
            break;
        }
        default: {
            assert(false && "literal type not supported");
            break;
        }
    }

    return d;
}

static struct VdbValue vdbexpr_eval_binary_equals(struct VdbValue* left, struct VdbValue* right) {
    if (vdbvalue_is_null(left) || vdbvalue_is_null(right)) {
        struct VdbValue d;
        d.type = VDBT_TYPE_NULL;
        return d; 
    }

    if (left->type != right->type) {
        assert(false && "comparing different types not implemented");
    }

    struct VdbValue d;
    d.type = VDBT_TYPE_BOOL;
    switch (left->type) {
        case VDBT_TYPE_STR:
            d.as.Bool = strncmp(left->as.Str.start, right->as.Str.start, left->as.Str.len) == 0;
            break;
        case VDBT_TYPE_INT:
            d.as.Bool = left->as.Int == right->as.Int;
            break;
        case VDBT_TYPE_FLOAT:
            d.as.Bool = left->as.Float == right->as.Float;
            break;
        case VDBT_TYPE_BOOL:
            d.as.Bool = left->as.Bool == right->as.Bool;
            break;
        default:
            assert(false && "data type not supported");
            break;
    }

    return d;
}

static struct VdbValue vdbexpr_eval_binary_not_equals(struct VdbValue* left, struct VdbValue* right) {
    if (vdbvalue_is_null(left) || vdbvalue_is_null(right)) {
        struct VdbValue d;
        d.type = VDBT_TYPE_NULL;
        return d; 
    }

    if (left->type != right->type) {
        assert(false && "comparing different types not implemented");
    }

    struct VdbValue d;
    d.type = VDBT_TYPE_BOOL;
    switch (left->type) {
        case VDBT_TYPE_STR:
            d.as.Bool = strncmp(left->as.Str.start, right->as.Str.start, left->as.Str.len) != 0;
            break;
        case VDBT_TYPE_INT:
            d.as.Bool = left->as.Int != right->as.Int;
            break;
        case VDBT_TYPE_FLOAT:
            d.as.Bool = left->as.Float != right->as.Float;
            break;
        case VDBT_TYPE_BOOL:
            d.as.Bool = left->as.Bool != right->as.Bool;
            break;
        default:
            assert(false && "data type not supported");
            break;
    }

    return d;
}
static struct VdbValue vdbexpr_eval_binary_less(struct VdbValue* left, struct VdbValue* right) {
    if (vdbvalue_is_null(left) || vdbvalue_is_null(right)) {
        struct VdbValue d;
        d.type = VDBT_TYPE_NULL;
        return d; 
    }

    if (left->type != right->type) {
        assert(false && "comparing different types not implemented");
    }

    struct VdbValue d;
    d.type = VDBT_TYPE_BOOL;
    switch (left->type) {
        case VDBT_TYPE_STR:
            d.as.Bool = strncmp(left->as.Str.start, right->as.Str.start, left->as.Str.len) < 0;
            break;
        case VDBT_TYPE_INT:
            d.as.Bool = left->as.Int < right->as.Int;
            break;
        case VDBT_TYPE_FLOAT:
            d.as.Bool = left->as.Float < right->as.Float;
            break;
        default:
            assert(false && "data type not supported");
            break;
    }

    return d;
}

static struct VdbValue vdbexpr_eval_binary_less_equals(struct VdbValue* left, struct VdbValue* right) {
    if (vdbvalue_is_null(left) || vdbvalue_is_null(right)) {
        struct VdbValue d;
        d.type = VDBT_TYPE_NULL;
        return d; 
    }

    if (left->type != right->type) {
        assert(false && "comparing different types not implemented");
    }

    struct VdbValue d;
    d.type = VDBT_TYPE_BOOL;
    switch (left->type) {
        case VDBT_TYPE_STR:
            d.as.Bool = strncmp(left->as.Str.start, right->as.Str.start, left->as.Str.len) <= 0;
            break;
        case VDBT_TYPE_INT:
            d.as.Bool = left->as.Int <= right->as.Int;
            break;
        case VDBT_TYPE_FLOAT:
            d.as.Bool = left->as.Float <= right->as.Float;
            break;
        default:
            assert(false && "data type not supported");
            break;
    }

    return d;
}

static struct VdbValue vdbexpr_eval_binary_greater(struct VdbValue* left, struct VdbValue* right) {
    if (vdbvalue_is_null(left) || vdbvalue_is_null(right)) {
        struct VdbValue d;
        d.type = VDBT_TYPE_NULL;
        return d; 
    }

    if (left->type != right->type) {
        assert(false && "comparing different types not implemented");
    }

    struct VdbValue d;
    d.type = VDBT_TYPE_BOOL;
    switch (left->type) {
        case VDBT_TYPE_STR:
            d.as.Bool = strncmp(left->as.Str.start, right->as.Str.start, left->as.Str.len) > 0;
            break;
        case VDBT_TYPE_INT:
            d.as.Bool = left->as.Int > right->as.Int;
            break;
        case VDBT_TYPE_FLOAT:
            d.as.Bool = left->as.Float > right->as.Float;
            break;
        default:
            assert(false && "data type not supported");
            break;
    }

    return d;
}

static struct VdbValue vdbexpr_eval_binary_greater_equals(struct VdbValue* left, struct VdbValue* right) {
    if (vdbvalue_is_null(left) || vdbvalue_is_null(right)) {
        struct VdbValue d;
        d.type = VDBT_TYPE_NULL;
        return d; 
    }

    if (left->type != right->type) {
        assert(false && "comparing different types not implemented");
    }

    struct VdbValue d;
    d.type = VDBT_TYPE_BOOL;
    switch (left->type) {
        case VDBT_TYPE_STR:
            d.as.Bool = strncmp(left->as.Str.start, right->as.Str.start, left->as.Str.len) >= 0;
            break;
        case VDBT_TYPE_INT:
            d.as.Bool = left->as.Int >= right->as.Int;
            break;
        case VDBT_TYPE_FLOAT:
            d.as.Bool = left->as.Float >= right->as.Float;
            break;
        default:
            assert(false && "data type not supported");
            break;
    }

    return d;
}

static struct VdbValue vdbexpr_eval_binary_and(struct VdbValue* left, struct VdbValue* right) {
    struct VdbValue d;
    d.type = VDBT_TYPE_BOOL;

    if (vdbvalue_is_null(left) && vdbvalue_is_null(right)) {
        d.type = VDBT_TYPE_NULL;
    } else if (vdbvalue_is_null(left) && right->type == VDBT_TYPE_BOOL) {
        if (right->as.Bool) {
            d.type = VDBT_TYPE_NULL;
        } else {
            d.as.Bool = false;
        }
    } else if (left->type == VDBT_TYPE_BOOL && vdbvalue_is_null(right)) {
        if (left->as.Bool) {
            d.type = VDBT_TYPE_NULL;
        } else {
            d.as.Bool = false;
        }
    } else if (left->type == VDBT_TYPE_BOOL && right->type == VDBT_TYPE_BOOL) {
        d.as.Bool = left->as.Bool && right->as.Bool;
    } else {
        assert(false && "data types not supported for 'and' operator");
    }

    return d;
}

static struct VdbValue vdbexpr_eval_binary_or(struct VdbValue* left, struct VdbValue* right) {
    struct VdbValue d;
    d.type = VDBT_TYPE_BOOL;

    if (vdbvalue_is_null(left) && vdbvalue_is_null(right)) {
        d.type = VDBT_TYPE_NULL;
    } else if (vdbvalue_is_null(left) && right->type == VDBT_TYPE_BOOL) {
        if (right->as.Bool) {
            d.as.Bool = true;
        } else {
            d.type = VDBT_TYPE_NULL;
        }
    } else if (left->type == VDBT_TYPE_BOOL && vdbvalue_is_null(right)) {
        if (left->as.Bool) {
            d.as.Bool = true;
        } else {
            d.type = VDBT_TYPE_NULL;
        }
    } else if (left->type == VDBT_TYPE_BOOL && right->type == VDBT_TYPE_BOOL) {
        d.as.Bool = left->as.Bool || right->as.Bool;
    } else {
        assert(false && "data types not supported for 'and' operator");
    }

    return d;
}

static struct VdbValue vdbexpr_eval_binary_plus(struct VdbValue* left, struct VdbValue* right) {
    if (vdbvalue_is_null(left) || vdbvalue_is_null(right)) {
        struct VdbValue d;
        d.type = VDBT_TYPE_NULL;
        return d; 
    }

    if (left->type != right->type) {
        assert(false && "adding different types not implemented");
    }

    struct VdbValue d;
    switch (left->type) {
        case VDBT_TYPE_STR:
            d.type = VDBT_TYPE_STR;
            int len = left->as.Str.len + right->as.Str.len;
            d.as.Str.len = len;
            d.as.Str.start = malloc_w(sizeof(char) * (len + 1));
            memcpy(d.as.Str.start, left->as.Str.start, left->as.Str.len);
            memcpy(d.as.Str.start + left->as.Str.len, right->as.Str.start, right->as.Str.len);
            d.as.Str.start[len] = '\0';
            break;
        case VDBT_TYPE_INT:
            d.type = VDBT_TYPE_INT;
            d.as.Int = left->as.Int + right->as.Int;
            break;
        case VDBT_TYPE_FLOAT:
            d.type = VDBT_TYPE_FLOAT;
            d.as.Int = left->as.Float + right->as.Float;
            break;
        default:
            assert(false && "addition with this data type not supported");
            break;
    }

    return d;
}

static struct VdbValue vdbexpr_eval_binary_minus(struct VdbValue* left, struct VdbValue* right) {
    if (vdbvalue_is_null(left) || vdbvalue_is_null(right)) {
        struct VdbValue d;
        d.type = VDBT_TYPE_NULL;
        return d; 
    }

    if (left->type != right->type) {
        assert(false && "adding different types not implemented");
    }

    struct VdbValue d;
    switch (left->type) {
        case VDBT_TYPE_INT:
            d.type = VDBT_TYPE_INT;
            d.as.Int = left->as.Int - right->as.Int;
            break;
        case VDBT_TYPE_FLOAT:
            d.type = VDBT_TYPE_FLOAT;
            d.as.Int = left->as.Float - right->as.Float;
            break;
        default:
            assert(false && "addition with this data type not supported");
            break;
    }

    return d;
}

static struct VdbValue vdbexpr_do_eval(struct VdbExpr* expr, struct VdbRecord* rec, struct VdbSchema* schema) {
    switch (expr->type) {
        case VDBET_BINARY: {
            struct VdbValue left = vdbexpr_do_eval(expr->as.binary.left, rec, schema);
            struct VdbValue right = vdbexpr_do_eval(expr->as.binary.right, rec, schema);

            struct VdbValue d;
            switch (expr->as.binary.op.type) {
                case VDBT_EQUALS:
                    d = vdbexpr_eval_binary_equals(&left, &right);
                    break;
                case VDBT_NOT_EQUALS:
                    d = vdbexpr_eval_binary_not_equals(&left, &right);
                    break;
                case VDBT_LESS:
                    d = vdbexpr_eval_binary_less(&left, &right);
                    break;
                case VDBT_LESS_EQUALS:
                    d = vdbexpr_eval_binary_less_equals(&left, &right);
                    break;
                case VDBT_GREATER:
                    d = vdbexpr_eval_binary_greater(&left, &right);
                    break;
                case VDBT_GREATER_EQUALS:
                    d = vdbexpr_eval_binary_greater_equals(&left, &right);
                    break;
                case VDBT_AND:
                    d = vdbexpr_eval_binary_and(&left, &right);
                    break;
                case VDBT_OR:
                    d = vdbexpr_eval_binary_or(&left, &right);
                    break;
                case VDBT_PLUS:
                    d = vdbexpr_eval_binary_plus(&left, &right);
                    break;
                case VDBT_MINUS:
                    d = vdbexpr_eval_binary_minus(&left, &right);
                    break;
                default:
                    assert(false && "invalid binary operator");
                    break;
            }

            if (left.type == VDBT_TYPE_STR) {
                free_w(left.as.Str.start, sizeof(char) * left.as.Str.len);
            }

            if (right.type == VDBT_TYPE_STR) {
                free_w(right.as.Str.start, sizeof(char) * right.as.Str.len);
            }

            return d;
        }
        case VDBET_UNARY: {
            struct VdbValue right = vdbexpr_do_eval(expr->as.unary.right, rec, schema);

            if (vdbvalue_is_null(&right)) {
                assert(false && "unary operator on null not implemented");
            }

            struct VdbValue d;
            switch (expr->as.unary.op.type) {
                case VDBT_NOT:
                    d = right;
                    d.as.Bool = !d.as.Bool;
                    break;
                case VDBT_MINUS:
                    d = right;
                    if (d.type == VDBT_TYPE_INT) {
                        d.as.Int = -d.as.Int;
                    } else if (d.type == VDBT_TYPE_FLOAT) {
                        d.as.Float = -d.as.Float;
                    } else {
                        assert(false && "unsupported type for unary operation");
                    }
                    break;
                default:
                    assert(false && "unsupported type for unary operation");
                    break;
            }
            return d;
        }
        case VDBET_IDENTIFIER: {
            return vdbexpr_eval_identifier(expr->as.identifier.token, rec, schema);
        }
        case VDBET_LITERAL: {
            return vdbexpr_eval_literal(expr->as.literal.token);
        }
        case VDBET_IS_NULL: {
            struct VdbValue left = vdbexpr_do_eval(expr->as.is_null.left, rec, schema);
            struct VdbValue d;
            d.type = VDBT_TYPE_BOOL;
            d.as.Bool = vdbvalue_is_null(&left);
            return d;
        }
        case VDBET_IS_NOT_NULL: {
            struct VdbValue left = vdbexpr_do_eval(expr->as.is_not_null.left, rec, schema);
            struct VdbValue d;
            d.type = VDBT_TYPE_BOOL;
            d.as.Bool = !vdbvalue_is_null(&left);
            return d;
        }
        default: {
            assert(false && "invalid expression type");
            struct VdbValue d;
            d.type = VDBT_TYPE_BOOL;
            d.as.Bool = false;
            return d;
        }
    }
}

struct VdbValue vdbexpr_eval(struct VdbExpr* expr, struct VdbRecord* rec, struct VdbSchema* schema) {
    if (!expr) {
        struct VdbValue d;
        d.type = VDBT_TYPE_BOOL;
        d.as.Bool = true;
        return d;
    }

    return vdbexpr_do_eval(expr, rec, schema);
}

void vdbexpr_free(struct VdbExpr* expr) {
    switch (expr->type) {
        case VDBET_LITERAL:
            break;
        case VDBET_IDENTIFIER:
            break;
        case VDBET_UNARY:
            vdbexpr_free(expr->as.unary.right);
            break;
        case VDBET_BINARY:
            vdbexpr_free(expr->as.binary.left);
            vdbexpr_free(expr->as.binary.right);
            break;
        case VDBET_IS_NULL:
            vdbexpr_free(expr->as.is_null.left);
            break;
        case VDBET_IS_NOT_NULL:
            vdbexpr_free(expr->as.is_not_null.left);
            break;
        default:
            assert(false && "expr type not freed");
            break;
    }

    free_w(expr, sizeof(struct VdbExpr));
}

struct VdbExprList* vdbexprlist_init() {
    struct VdbExprList* el = malloc_w(sizeof(struct VdbExprList));
    el->count = 0;
    el->capacity = 8;
    el->exprs = malloc_w(sizeof(struct VdbExpr*) * el->capacity);

    return el;
}

void vdbexprlist_free(struct VdbExprList* el) {
    for (int i = 0; i < el->count; i++) {
        vdbexpr_free(el->exprs[i]);
    }
    free_w(el->exprs, sizeof(struct VdbExpr*) * el->capacity);
    free_w(el, sizeof(struct VdbExprList));
}

void vdbexprlist_append_expr(struct VdbExprList* el, struct VdbExpr* expr) {
    if (el->count + 1 > el->capacity) {
        int old_cap = el->capacity;
        el->capacity *= 2;
        el->exprs = realloc_w(el->exprs, sizeof(struct VdbExpr*) * el->capacity, sizeof(struct VdbExpr*) * old_cap);
    }

    el->exprs[el->count++] = expr;
}



struct VdbStmtList* vdbstmtlist_init() {
    struct VdbStmtList* sl = malloc_w(sizeof(struct VdbStmtList));
    sl->count = 0;
    sl->capacity = 8;
    sl->stmts = malloc_w(sizeof(struct VdbStmt) * sl->capacity);

    return sl;
}

void vdbstmtlist_free(struct VdbStmtList* sl) {
    for (int i = 0; i < sl->count; i++) {
        vdbstmt_free_fields(&sl->stmts[i]);
    }
    free_w(sl->stmts, sizeof(struct VdbStmt) * sl->capacity);
    free_w(sl, sizeof(struct VdbStmtList));
}

void vdbstmtlist_append_stmt(struct VdbStmtList* sl, struct VdbStmt stmt) {
    if (sl->count + 1 > sl->capacity) {
        int old_cap = sl->capacity;
        sl->capacity *= 2;
        sl->stmts = realloc_w(sl->stmts, sizeof(struct VdbStmt) * sl->capacity, sizeof(struct VdbStmt) * old_cap);
    }

    sl->stmts[sl->count++] = stmt;
}

struct VdbToken vdbparser_peek_token(struct VdbParser* parser) {
    return parser->tl->tokens[parser->current];
}

struct VdbToken vdbparser_consume_token(struct VdbParser* parser, enum VdbTokenType type) {
    struct VdbToken token;
    if (parser->current >= parser->tl->count) {
        token.type = VDBT_INVALID;
        token.len = 0;
        token.lexeme = "";
    } else {
        token = parser->tl->tokens[parser->current++];
    }

    if (token.type != type) {
        vdberrorlist_append_error(parser->errors, 1, "unexpected token: %.*s", token.len, token.lexeme);
    }

    return token;
}

struct VdbToken vdbparser_next_token(struct VdbParser* parser) {
    struct VdbToken token;
    if (parser->current >= parser->tl->count) {
        token.type = VDBT_INVALID;
        token.len = 0;
        token.lexeme = NULL;
    } else {
        token = parser->tl->tokens[parser->current++];
    }

    return token;
}

struct VdbExpr* vdbparser_parse_primary(struct VdbParser* parser) {
    switch (vdbparser_peek_token(parser).type) {
        case VDBT_INT:
        case VDBT_FLOAT:
        case VDBT_STR:
        case VDBT_TRUE:
        case VDBT_FALSE:
        case VDBT_NULL:
            return vdbexpr_init_literal(vdbparser_next_token(parser));
        case VDBT_IDENTIFIER:
            return vdbexpr_init_identifier(vdbparser_consume_token(parser, VDBT_IDENTIFIER));
        case VDBT_LPAREN:
            vdbparser_consume_token(parser, VDBT_LPAREN);
            struct VdbExpr* expr = vdbparser_parse_expr(parser);
            vdbparser_consume_token(parser, VDBT_RPAREN);
            return expr;
        default:
            assert(false && "invalid token type");
            return NULL;
    }
}
struct VdbExpr* vdbparser_parse_unary(struct VdbParser* parser) {
    if (vdbparser_peek_token(parser).type == VDBT_MINUS) {
        struct VdbToken op = vdbparser_next_token(parser);
        return vdbexpr_init_unary(op, vdbparser_parse_unary(parser));
    } else {
        return vdbparser_parse_primary(parser);
    }
}

struct VdbExpr* vdbparser_parse_factor(struct VdbParser* parser) {
    struct VdbExpr* left = vdbparser_parse_unary(parser);
    while (vdbparser_peek_token(parser).type == VDBT_STAR || vdbparser_peek_token(parser).type == VDBT_SLASH) {
        struct VdbToken op = vdbparser_next_token(parser);
        left = vdbexpr_init_binary(op, left, vdbparser_parse_unary(parser));
    }

    return left;
}

struct VdbExpr* vdbparser_parse_term(struct VdbParser* parser) {
    struct VdbExpr* left = vdbparser_parse_factor(parser);
    while (vdbparser_peek_token(parser).type == VDBT_PLUS || vdbparser_peek_token(parser).type == VDBT_MINUS) {
        struct VdbToken op = vdbparser_next_token(parser);
        left = vdbexpr_init_binary(op, left, vdbparser_parse_factor(parser));
    }

    return left;
}

struct VdbExpr* vdbparser_parse_relational(struct VdbParser* parser) {
    struct VdbExpr* left = vdbparser_parse_term(parser);
    while (vdbparser_peek_token(parser).type == VDBT_LESS ||
           vdbparser_peek_token(parser).type == VDBT_GREATER ||
           vdbparser_peek_token(parser).type == VDBT_LESS_EQUALS ||
           vdbparser_peek_token(parser).type == VDBT_GREATER_EQUALS) {
        struct VdbToken op = vdbparser_next_token(parser);
        left = vdbexpr_init_binary(op, left, vdbparser_parse_term(parser));
    }

    return left;
}
struct VdbExpr* vdbparser_parse_equality(struct VdbParser* parser) {
    struct VdbExpr* left = vdbparser_parse_relational(parser);
    while (vdbparser_peek_token(parser).type == VDBT_EQUALS || 
           vdbparser_peek_token(parser).type == VDBT_NOT_EQUALS ||
           vdbparser_peek_token(parser).type == VDBT_IS) {

        struct VdbToken token = vdbparser_next_token(parser);
        if (token.type != VDBT_IS) {
            left = vdbexpr_init_binary(token, left, vdbparser_parse_relational(parser));
        } else if (vdbparser_peek_token(parser).type == VDBT_NOT) {
            vdbparser_consume_token(parser, VDBT_NOT);
            vdbparser_consume_token(parser, VDBT_NULL);
            left = vdbexpr_init_is_not_null(left);
        } else {
            vdbparser_consume_token(parser, VDBT_NULL);
            left = vdbexpr_init_is_null(left);
        }
    }

    return left;
}
struct VdbExpr* vdbparser_parse_not(struct VdbParser* parser) {
    if (vdbparser_peek_token(parser).type == VDBT_NOT) {
        struct VdbToken not = vdbparser_next_token(parser);
        return vdbexpr_init_unary(not, vdbparser_parse_not(parser));
    } else {
        return vdbparser_parse_equality(parser);
    }
}
struct VdbExpr* vdbparser_parse_and(struct VdbParser* parser) {
    struct VdbExpr* left = vdbparser_parse_not(parser);
    while (vdbparser_peek_token(parser).type == VDBT_AND) {
        struct VdbToken and = vdbparser_next_token(parser);
        left = vdbexpr_init_binary(and, left, vdbparser_parse_not(parser));
    }

    return left;
}
struct VdbExpr* vdbparser_parse_or(struct VdbParser* parser) {
    struct VdbExpr* left = vdbparser_parse_and(parser);
    while (vdbparser_peek_token(parser).type == VDBT_OR) {
        struct VdbToken or = vdbparser_next_token(parser);
        left = vdbexpr_init_binary(or, left, vdbparser_parse_and(parser));
    }

    return left;
}
/*
struct VdbExpr* vdbparser_parse_assignment(struct VdbParser* parser) {
}
struct VdbExpr* vdbparser_parse_sequence(struct VdbParser* parser) {
    //commas
}
*/
struct VdbExpr* vdbparser_parse_expr(struct VdbParser* parser) {
    return vdbparser_parse_or(parser);
}

void vdbparser_parse_identifier_tuple(struct VdbParser* parser, struct VdbTokenList* tl) {
    vdbparser_consume_token(parser, VDBT_LPAREN);
    while (vdbparser_peek_token(parser).type != VDBT_RPAREN) {
        struct VdbToken t = vdbparser_consume_token(parser, VDBT_IDENTIFIER);
        vdbparser_validate_attribute_name(parser, t);
        vdbtokenlist_append_token(tl, t);
        if (vdbparser_peek_token(parser).type == VDBT_COMMA) {
            vdbparser_consume_token(parser, VDBT_COMMA);
        } else {
            break;
        }
    }
    vdbparser_consume_token(parser, VDBT_RPAREN);
}

enum VdbReturnCode vdbparser_parse_stmt(struct VdbParser* parser, struct VdbStmt* stmt) {
    switch (vdbparser_next_token(parser).type) {
        case VDBT_CONNECT: {
            stmt->type = VDBST_CONNECT;
            //TODO: How to read in port?  What kind of token will that be?
            break;
        }
        case VDBT_SHOW: {
            if (vdbparser_peek_token(parser).type == VDBT_DATABASES) {
                stmt->type = VDBST_SHOW_DBS;
                vdbparser_consume_token(parser, VDBT_DATABASES);
            } else {
                stmt->type = VDBST_SHOW_TABS;
                vdbparser_consume_token(parser, VDBT_TABLES);
            }
            break;
        }
        case VDBT_CREATE: {
            if (vdbparser_peek_token(parser).type == VDBT_DATABASE) {
                stmt->type = VDBST_CREATE_DB;
                vdbparser_consume_token(parser, VDBT_DATABASE);
                stmt->target = vdbparser_consume_token(parser, VDBT_IDENTIFIER);
            } else {
                stmt->type = VDBST_CREATE_TAB;
                vdbparser_consume_token(parser, VDBT_TABLE);
                stmt->target = vdbparser_consume_token(parser, VDBT_IDENTIFIER);
                stmt->as.create.attributes = vdbtokenlist_init();
                stmt->as.create.types = vdbtokenlist_init();

                struct VdbToken attr_token;
                attr_token.type = VDBT_IDENTIFIER;
                attr_token.lexeme = "id";
                attr_token.len = 2;
                vdbtokenlist_append_token(stmt->as.create.attributes, attr_token);

                struct VdbToken type_token;
                type_token.type = VDBT_TYPE_INT;
                type_token.lexeme = "int";
                type_token.len = 3;
                vdbtokenlist_append_token(stmt->as.create.types, type_token);

                vdbparser_consume_token(parser, VDBT_LPAREN);
                while (vdbparser_peek_token(parser).type != VDBT_RPAREN) {
                    struct VdbToken t = vdbparser_consume_token(parser, VDBT_IDENTIFIER);
                    vdbparser_validate_attribute_name(parser, t);
                    vdbtokenlist_append_token(stmt->as.create.attributes, t);
                    vdbtokenlist_append_token(stmt->as.create.types, vdbparser_next_token(parser));

                    if (vdbparser_peek_token(parser).type == VDBT_COMMA) {
                        vdbparser_consume_token(parser, VDBT_COMMA);
                    } else {
                        break;
                    }
                }
                vdbparser_consume_token(parser, VDBT_RPAREN);
            }
            break;
        }
        case VDBT_IF: {
            vdbparser_consume_token(parser, VDBT_EXISTS);
            vdbparser_consume_token(parser, VDBT_DROP);
            if (vdbparser_peek_token(parser).type == VDBT_DATABASE) {
                stmt->type = VDBST_IF_EXISTS_DROP_DB;
                vdbparser_consume_token(parser, VDBT_DATABASE);
            } else {
                stmt->type = VDBST_IF_EXISTS_DROP_TAB;
                vdbparser_consume_token(parser, VDBT_TABLE);
            }
            stmt->target = vdbparser_consume_token(parser, VDBT_IDENTIFIER);
            break;
        }
        case VDBT_DROP: {
            if (vdbparser_peek_token(parser).type == VDBT_DATABASE) {
                stmt->type = VDBST_DROP_DB;
                vdbparser_consume_token(parser, VDBT_DATABASE);
                stmt->target = vdbparser_consume_token(parser, VDBT_IDENTIFIER);
            } else {
                stmt->type = VDBST_DROP_TAB;
                vdbparser_consume_token(parser, VDBT_TABLE);
                stmt->target = vdbparser_consume_token(parser, VDBT_IDENTIFIER);
            }
            break;
        }
        case VDBT_OPEN: {
            stmt->type = VDBST_OPEN;
            stmt->target = vdbparser_consume_token(parser, VDBT_IDENTIFIER);
            break;
        }
        case VDBT_CLOSE: {
            stmt->type = VDBST_CLOSE;
            stmt->target = vdbparser_consume_token(parser, VDBT_IDENTIFIER);
            break;
        }
        case VDBT_DESCRIBE: {
            stmt->type = VDBST_DESCRIBE;
            stmt->target = vdbparser_consume_token(parser, VDBT_IDENTIFIER);
            break;
        }
        case VDBT_INSERT: {
            stmt->type = VDBST_INSERT;
            vdbparser_consume_token(parser, VDBT_INTO);
            stmt->target = vdbparser_consume_token(parser, VDBT_IDENTIFIER);

            stmt->as.insert.attributes = vdbtokenlist_init();
            vdbparser_parse_identifier_tuple(parser, stmt->as.insert.attributes);

            vdbparser_consume_token(parser, VDBT_VALUES);

            stmt->as.insert.values = vdbexprlist_init();
            while (vdbparser_peek_token(parser).type == VDBT_LPAREN) {
                vdbparser_consume_token(parser, VDBT_LPAREN);
                while (vdbparser_peek_token(parser).type != VDBT_RPAREN) {
                    struct VdbExpr* expr = vdbparser_parse_expr(parser);
                    vdbexprlist_append_expr(stmt->as.insert.values, expr);
                    if (vdbparser_peek_token(parser).type == VDBT_COMMA) {
                        vdbparser_consume_token(parser, VDBT_COMMA);
                    }
                }
                vdbparser_consume_token(parser, VDBT_RPAREN);
                if (vdbparser_peek_token(parser).type == VDBT_COMMA) {
                    vdbparser_consume_token(parser, VDBT_COMMA);
                } else {
                    break;
                }
            }

            break;
        }
        case VDBT_UPDATE: {
            stmt->type = VDBST_UPDATE;
            stmt->target = vdbparser_consume_token(parser, VDBT_IDENTIFIER);
            vdbparser_consume_token(parser, VDBT_SET);

            stmt->as.update.attributes = vdbtokenlist_init();
            stmt->as.update.values = vdbtokenlist_init();

            while (true) {
                struct VdbToken t = vdbparser_consume_token(parser, VDBT_IDENTIFIER);
                vdbparser_validate_attribute_name(parser, t);
                vdbtokenlist_append_token(stmt->as.update.attributes, t);
                vdbparser_consume_token(parser, VDBT_EQUALS);
                vdbtokenlist_append_token(stmt->as.update.values, vdbparser_next_token(parser));

                if (vdbparser_peek_token(parser).type == VDBT_COMMA) {
                    vdbparser_consume_token(parser, VDBT_COMMA);
                    continue;
                }

                break;
            }

            stmt->as.update.selection = NULL;
            if (vdbparser_peek_token(parser).type == VDBT_WHERE) {
                vdbparser_consume_token(parser, VDBT_WHERE);
                stmt->as.update.selection = vdbparser_parse_expr(parser);
            }

            break;
        }
        case VDBT_DELETE: {
            stmt->type = VDBST_DELETE;
            vdbparser_consume_token(parser, VDBT_FROM);
            stmt->target = vdbparser_consume_token(parser, VDBT_IDENTIFIER);

            if (vdbparser_peek_token(parser).type == VDBT_WHERE) {
                vdbparser_consume_token(parser, VDBT_WHERE);
                stmt->as.delete.selection = vdbparser_parse_expr(parser);
            } else {
                struct VdbToken t;
                t.type = VDBT_TRUE;
                t.lexeme = "true";
                t.len = 4;
                stmt->as.delete.selection = vdbexpr_init_literal(t);
            }
            break;
        }
        case VDBT_SELECT:  {
            stmt->type = VDBST_SELECT;
            stmt->as.select.projection = vdbtokenlist_init();
            while (true) {
                vdbtokenlist_append_token(stmt->as.select.projection, vdbparser_next_token(parser));
                if (vdbparser_peek_token(parser).type == VDBT_COMMA) {
                    vdbparser_consume_token(parser, VDBT_COMMA);
                    continue;
                }
                break;
            }

            vdbparser_consume_token(parser, VDBT_FROM);
            stmt->target = vdbparser_consume_token(parser, VDBT_IDENTIFIER);
            if (vdbparser_peek_token(parser).type == VDBT_WHERE) {
                vdbparser_consume_token(parser, VDBT_WHERE);
                stmt->as.select.selection = vdbparser_parse_expr(parser);
            } else {
                struct VdbToken t;
                t.type = VDBT_TRUE;
                t.lexeme = "true";
                t.len = 4;
                stmt->as.select.selection = vdbexpr_init_literal(t);
            }
            break;
        }
        case VDBT_EXIT: {
            stmt->type = VDBST_EXIT;
            break; 
        }
        default:
            printf("not implemented yet\n");
            break;
    }

    vdbparser_consume_token(parser, VDBT_SEMICOLON);

    return VDBRC_SUCCESS;
}

void vdbstmt_free_fields(struct VdbStmt* stmt) {
    switch (stmt->type) {
        case VDBST_CREATE_TAB:
            vdbtokenlist_free(stmt->as.create.attributes);
            vdbtokenlist_free(stmt->as.create.types);
            break;
        case VDBST_INSERT:
            vdbtokenlist_free(stmt->as.insert.attributes);
            vdbexprlist_free(stmt->as.insert.values);
            break;
        case VDBST_UPDATE:
            vdbtokenlist_free(stmt->as.update.attributes);
            vdbtokenlist_free(stmt->as.update.values);
            vdbexpr_free(stmt->as.update.selection);
            break;
        case VDBST_DELETE:
            vdbexpr_free(stmt->as.delete.selection);
            break;
        case VDBST_SELECT:
            vdbtokenlist_free(stmt->as.select.projection);
            vdbexpr_free(stmt->as.select.selection);
            break;
        default:
            //nothing to free
            break;
    }
}

void vdbstmt_print(struct VdbStmt* stmt) {
    switch (stmt->type) {
        case VDBST_CONNECT: {
            printf("<connect>\n");
            break;
        }
        case VDBST_SHOW_DBS: {
            printf("<show database [%.*s]>\n", stmt->target.len, stmt->target.lexeme);
            break;
        }
        case VDBST_SHOW_TABS: {
            printf("<show table [%.*s]>\n", stmt->target.len, stmt->target.lexeme);
            break;
        }
        case VDBST_CREATE_DB: {
            printf("<create database [%.*s]>\n", stmt->target.len, stmt->target.lexeme);
            break;
        }
        case VDBST_CREATE_TAB: {
            printf("<create table [%.*s]>\n", stmt->target.len, stmt->target.lexeme);
            printf("\tschema:\n");
            for (int i = 0; i < stmt->as.create.attributes->count; i++) {
                struct VdbToken attribute = stmt->as.create.attributes->tokens[i];
                struct VdbToken type = stmt->as.create.types->tokens[i];
                printf("\t\t[%.*s]: %.*s\n", attribute.len, attribute.lexeme, type.len, type.lexeme);
            }
            break;
        }
        case VDBST_DROP_DB: {
            printf("<drop database [%.*s]>\n", stmt->target.len, stmt->target.lexeme);
            break;
        }
        case VDBST_DROP_TAB: {
            printf("<drop table [%.*s]>\n", stmt->target.len, stmt->target.lexeme);
            break;
        }
        case VDBST_OPEN: {
            printf("<open database [%.*s]>\n", stmt->target.len, stmt->target.lexeme);
            break;
        }
        case VDBST_CLOSE: {
            printf("<close database [%.*s]>\n", stmt->target.len, stmt->target.lexeme);
            break;
        }
        case VDBST_DESCRIBE: {
            printf("<describe table [%.*s]>\n", stmt->target.len, stmt->target.lexeme);
            break;
        }
        case VDBST_INSERT: {
            printf("<insert into table [%.*s]>\n", stmt->target.len, stmt->target.lexeme);
            printf("\tcolumns:\n");
            for (int i = 0; i < stmt->as.insert.attributes->count; i++) {
                struct VdbToken attribute = stmt->as.insert.attributes->tokens[i];
                printf("\t\t[%.*s]\n", attribute.len, attribute.lexeme);
            }
            printf("\tvalues:\n");
            for (int i = 0; i < stmt->as.insert.values->count; i += stmt->as.insert.attributes->count) {
                printf("\t\t");
                for (int j = i; j < i + stmt->as.insert.attributes->count; j++) {
                    vdbexpr_print(stmt->as.insert.values->exprs[j]);
                    if (j < i +  stmt->as.insert.attributes->count -1) {
                        printf(", ");
                    }
                }
                printf("\n");
            }
            break;
        }
        case VDBST_UPDATE: {
            printf("<update record(s) in table [%.*s]>\n", stmt->target.len, stmt->target.lexeme);
            printf("\tset:\n");
            for (int i = 0; i < stmt->as.update.attributes->count; i++) {
                struct VdbToken attribute = stmt->as.update.attributes->tokens[i];
                struct VdbToken value = stmt->as.update.values->tokens[i];
                printf("\t\t[%.*s]: %.*s\n", attribute.len, attribute.lexeme, value.len, value.lexeme);
            }
            printf("\tcondition:\n");
            if (stmt->as.update.selection) {
                printf("\t\t");
                vdbexpr_print(stmt->as.update.selection);
                printf("\n");
            }
            break;
        }
        case VDBST_DELETE: {
            printf("<delete record(s) from table [%.*s]>\n", stmt->target.len, stmt->target.lexeme);
            printf("\tcondition:\n");
            if (stmt->as.delete.selection) {
                printf("\t\t");
                vdbexpr_print(stmt->as.delete.selection);
                printf("\n");
            }
            break;
        }
        case VDBST_SELECT: {
            printf("<select record(s) from table [%.*s]>\n", stmt->target.len, stmt->target.lexeme);
            printf("\tcolumns:\n");
            for (int i = 0; i < stmt->as.select.projection->count; i++) {
                struct VdbToken t = stmt->as.select.projection->tokens[i];
                printf("\t\t[%.*s]\n", t.len, t.lexeme);
            }
            printf("\tcondition:\n");
            if (stmt->as.select.selection) {
                printf("\t\t");
                vdbexpr_print(stmt->as.select.selection);
                printf("\n");
            }
            break;
        }
        case VDBST_EXIT: {
            printf("<exit>\n");
            break;
        }
        default:
            break;
    }
}
