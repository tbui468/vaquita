#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "parser.h"
#include "util.h"


struct VdbExpr* vdbparser_parse_expr(struct VdbParser* parser);

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

char* vdbexpr_to_string(struct VdbExpr* expr) {
    switch (expr->type) {
        case VDBET_LITERAL: {
            int len = expr->as.literal.token.len;
            char* s = malloc_w(sizeof(char) * (len + 1));
            memcpy(s, expr->as.literal.token.lexeme, len);
            s[len] = '\0';
            return s;
        }
        case VDBET_IDENTIFIER: {
            int len = expr->as.identifier.token.len;
            char* s = malloc_w(sizeof(char) * (len + 1));
            memcpy(s, expr->as.identifier.token.lexeme, len);
            s[len] = '\0';
            return s;
        }
        case VDBET_UNARY: {
            char* right = vdbexpr_to_string(expr->as.unary.right);
            int len = expr->as.unary.op.len + strlen(right);
            char* s = malloc_w(sizeof(char) * (len + 1));
            memcpy(s, expr->as.unary.op.lexeme, expr->as.unary.op.len);
            memcpy(s + expr->as.unary.op.len, right, strlen(right));
            s[len] = '\0';
            free_w(right, strlen(right) + 1);
            return s;
        }
        case VDBET_BINARY: {
            char* left = vdbexpr_to_string(expr->as.binary.left);
            char* right = vdbexpr_to_string(expr->as.binary.right);
            int op_len = expr->as.binary.op.len;
            int len = strlen(left) + strlen(right) + op_len + 2; //2 spaces between op and operands
            char* s = malloc_w(sizeof(char) * (len + 1));
            memset(s, ' ', sizeof(char) * (len + 1));
            memcpy(s, left, strlen(left));
            memcpy(s + strlen(left) + 1, expr->as.binary.op.lexeme, op_len);
            memcpy(s + strlen(left) + op_len + 2, right, strlen(right));
            s[len] = '\0';
            free_w(left, strlen(left) + 1);
            free_w(right, strlen(right) + 1);
            return s;
        }
        case VDBET_IS_NULL: {
            char* left = vdbexpr_to_string(expr->as.is_null.left);
            char* null_s = " is null";
            int len = strlen(left) + strlen(null_s);
            char* s = malloc_w(sizeof(char) * (len + 1));
            memcpy(s, left, strlen(left));
            memcpy(s + strlen(left), null_s, strlen(null_s));
            s[len] = '\0';
            free_w(left, strlen(left) + 1);
            return s;
        }
        case VDBET_IS_NOT_NULL: {
            char* left = vdbexpr_to_string(expr->as.is_not_null.left);
            char* null_s = " is not null";
            int len = strlen(left) + strlen(null_s);
            char* s = malloc_w(sizeof(char) * (len + 1));
            memcpy(s, left, strlen(left));
            memcpy(s + strlen(left), null_s, strlen(null_s));
            s[len] = '\0';
            free_w(left, strlen(left) + 1);
            return s;
        }
        case VDBET_CALL: {
            char* arg = vdbexpr_to_string(expr->as.call.arg);
            int name_len = expr->as.call.fcn_name.len;
            int len = strlen(arg) + name_len + 2; //two parentheses
            char* s = malloc_w(sizeof(char) * (len + 1));
            memcpy(s, expr->as.call.fcn_name.lexeme, name_len);
            s[name_len] = '(';
            memcpy(s + name_len + 1, arg, strlen(arg));
            s[name_len + 1 + strlen(arg)] = ')';
            s[len] = '\0';
            free_w(arg, strlen(arg) + 1);
            return s;
        }
        default:
            assert(false && "invalid expr");
    }

    return NULL;
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
        case VDBET_WILDCARD:
            printf("[%.*s]", expr->as.wildcard.token.len, expr->as.wildcard.token.lexeme);
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
        case VDBET_CALL:
            printf("( %.*s ", expr->as.call.fcn_name.len, expr->as.call.fcn_name.lexeme);
            vdbexpr_print(expr->as.call.arg);
            printf(" )");
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

struct VdbExpr* vdbexpr_init_wildcard(struct VdbToken token) {
    struct VdbExpr* expr = malloc_w(sizeof(struct VdbExpr));
    expr->type = VDBET_WILDCARD;
    expr->as.wildcard.token = token;
    return expr;
}
            
struct VdbExpr* vdbexpr_init_fcn_call(struct VdbToken fcn_name, struct VdbExpr* arg) {
    struct VdbExpr* expr = malloc_w(sizeof(struct VdbExpr));
    expr->type = VDBET_CALL;
    expr->as.call.fcn_name = fcn_name;
    expr->as.call.arg = arg;
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

    if (rec->data[i].type == VDBT_TYPE_TEXT) {
        return vdbstring(rec->data[i].as.Str.start, rec->data[i].as.Str.len);
    }
    
    return rec->data[i];
}

struct VdbValue vdbexpr_eval_literal(struct VdbToken token) {
    struct VdbValue d;
    switch (token.type) {
        case VDBT_STR: {
            d = vdbstring(token.lexeme, token.len);
            break;
        }
        case VDBT_INT: {
            d.type = VDBT_TYPE_INT8;
            int len = token.len;
            char s[len + 1];
            memcpy(s, token.lexeme, len);
            s[len] = '\0';
            d.as.Int = strtoll(s, NULL, 10);
            break;
        }
        case VDBT_FLOAT: {
            d.type = VDBT_TYPE_FLOAT8;
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
        case VDBT_TYPE_TEXT:
            d.as.Bool = strncmp(left->as.Str.start, right->as.Str.start, left->as.Str.len) == 0;
            break;
        case VDBT_TYPE_INT8:
            d.as.Bool = left->as.Int == right->as.Int;
            break;
        case VDBT_TYPE_FLOAT8:
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
        case VDBT_TYPE_TEXT:
            d.as.Bool = strncmp(left->as.Str.start, right->as.Str.start, left->as.Str.len) != 0;
            break;
        case VDBT_TYPE_INT8:
            d.as.Bool = left->as.Int != right->as.Int;
            break;
        case VDBT_TYPE_FLOAT8:
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
        case VDBT_TYPE_TEXT:
            d.as.Bool = strncmp(left->as.Str.start, right->as.Str.start, left->as.Str.len) < 0;
            break;
        case VDBT_TYPE_INT8:
            d.as.Bool = left->as.Int < right->as.Int;
            break;
        case VDBT_TYPE_FLOAT8:
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
        case VDBT_TYPE_TEXT:
            d.as.Bool = strncmp(left->as.Str.start, right->as.Str.start, left->as.Str.len) <= 0;
            break;
        case VDBT_TYPE_INT8:
            d.as.Bool = left->as.Int <= right->as.Int;
            break;
        case VDBT_TYPE_FLOAT8:
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
        case VDBT_TYPE_TEXT:
            d.as.Bool = strncmp(left->as.Str.start, right->as.Str.start, left->as.Str.len) > 0;
            break;
        case VDBT_TYPE_INT8:
            d.as.Bool = left->as.Int > right->as.Int;
            break;
        case VDBT_TYPE_FLOAT8:
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
        case VDBT_TYPE_TEXT:
            d.as.Bool = strncmp(left->as.Str.start, right->as.Str.start, left->as.Str.len) >= 0;
            break;
        case VDBT_TYPE_INT8:
            d.as.Bool = left->as.Int >= right->as.Int;
            break;
        case VDBT_TYPE_FLOAT8:
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
        case VDBT_TYPE_TEXT:
            d.type = VDBT_TYPE_TEXT;
            int len = left->as.Str.len + right->as.Str.len;
            d.as.Str.len = len;
            d.as.Str.start = malloc_w(sizeof(char) * (len + 1));
            memcpy(d.as.Str.start, left->as.Str.start, left->as.Str.len);
            memcpy(d.as.Str.start + left->as.Str.len, right->as.Str.start, right->as.Str.len);
            d.as.Str.start[len] = '\0';
            break;
        case VDBT_TYPE_INT8:
            d.type = VDBT_TYPE_INT8;
            d.as.Int = left->as.Int + right->as.Int;
            break;
        case VDBT_TYPE_FLOAT8:
            d.type = VDBT_TYPE_FLOAT8;
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
        case VDBT_TYPE_INT8:
            d.type = VDBT_TYPE_INT8;
            d.as.Int = left->as.Int - right->as.Int;
            break;
        case VDBT_TYPE_FLOAT8:
            d.type = VDBT_TYPE_FLOAT8;
            d.as.Int = left->as.Float - right->as.Float;
            break;
        default:
            assert(false && "addition with this data type not supported");
            break;
    }

    return d;
}

static struct VdbValue vdbexpr_eval_arg(struct VdbExpr* arg, struct VdbRecord* rec, struct VdbSchema* schema) {
    switch (arg->type) {
        case VDBET_IDENTIFIER:
            return vdbexpr_eval_identifier(arg->as.identifier.token, rec, schema);
        case VDBET_LITERAL:
            return vdbexpr_eval_literal(arg->as.literal.token);
        default:
            assert(false && "aggregate function argument must be an identifier or literal");
            struct VdbValue d;
            d.type = VDBT_TYPE_NULL;
            return d;
    }

}

static struct VdbValue vdbexpr_eval_call_sum(struct VdbExpr* arg, struct VdbRecordSet* rs, struct VdbSchema* schema) {
    struct VdbValue first_value = vdbexpr_eval_arg(arg, rs->records[0], schema);
    if (first_value.type == VDBT_TYPE_INT8) {
        int64_t sum = 0;
        for (int i = 0; i < rs->count; i++) {
            struct VdbValue v = vdbexpr_eval_arg(arg, rs->records[i], schema);
            sum += v.as.Int;
        }
        struct VdbValue d;
        d.type = VDBT_TYPE_INT8;
        d.as.Int = sum;
        return d;
    } else if (first_value.type == VDBT_TYPE_FLOAT8) {
        double sum = 0.0;
        for (int i = 0; i < rs->count; i++) {
            struct VdbValue v = vdbexpr_eval_arg(arg, rs->records[i], schema);
            sum += v.as.Float;
        }
        struct VdbValue d;
        d.type = VDBT_TYPE_FLOAT8;
        d.as.Float = sum;
        return d;
    }

    assert(false && "avg function needs to be used with columns of int or float type");
    struct VdbValue d;
    d.type = VDBT_TYPE_NULL;
    return d;
}

static struct VdbValue vdbexpr_eval_call_avg(struct VdbExpr* arg, struct VdbRecordSet* rs, struct VdbSchema* schema)  {
    struct VdbValue v = vdbexpr_eval_arg(arg, rs->records[0], schema);
    struct VdbValue d;
    d.type = VDBT_TYPE_FLOAT8;
    if (v.type == VDBT_TYPE_INT8) {
        d.as.Float = (double)(vdbexpr_eval_call_sum(arg, rs, schema).as.Int) / (double)(rs->count);
        return d;
    } else if (v.type == VDBT_TYPE_FLOAT8) {
        d.as.Float = vdbexpr_eval_call_sum(arg, rs, schema).as.Float / (double)(rs->count);
        return d;
    }

    assert(false && "avg function needs to be used with columns of int or float type");
    d.type = VDBT_TYPE_NULL;
    return d;
}

static struct VdbValue vdbexpr_eval_call_count(struct VdbExpr* arg, struct VdbRecordSet* rs, struct VdbSchema* schema) {
    arg = arg;
    schema = schema;
    struct VdbValue d;
    d.type = VDBT_TYPE_INT8;
    d.as.Int = rs->count;
    return d;
}

static struct VdbValue vdbexpr_eval_call_max(struct VdbExpr* arg, struct VdbRecordSet* rs, struct VdbSchema* schema) {
    struct VdbValue first_value = vdbexpr_eval_arg(arg, rs->records[0], schema);
    if (first_value.type == VDBT_TYPE_INT8) {
        int64_t max = first_value.as.Int;
        for (int i = 1; i < rs->count; i++) {
            struct VdbValue v = vdbexpr_eval_arg(arg, rs->records[i], schema);
            if (v.as.Int > max)
                max = v.as.Int;
        }
        struct VdbValue d;
        d.type = VDBT_TYPE_INT8;
        d.as.Int = max;
        return d;
    } else if (first_value.type == VDBT_TYPE_FLOAT8) {
        double max = first_value.as.Float;
        for (int i = 1; i < rs->count; i++) {
            struct VdbValue v = vdbexpr_eval_arg(arg, rs->records[i], schema);
            if (v.as.Float > max)
                max = v.as.Float;
        }
        struct VdbValue d;
        d.type = VDBT_TYPE_FLOAT8;
        d.as.Float = max;
        return d;
    }

    assert(false && "max function needs to be used with columns of int or float type");
    struct VdbValue d;
    d.type = VDBT_TYPE_NULL;
    return d;
}

static struct VdbValue vdbexpr_eval_call_min(struct VdbExpr* arg, struct VdbRecordSet* rs, struct VdbSchema* schema) {
    struct VdbValue first_value = vdbexpr_eval_arg(arg, rs->records[0], schema);
    if (first_value.type == VDBT_TYPE_INT8) {
        int64_t min = first_value.as.Int;
        for (int i = 1; i < rs->count; i++) {
            struct VdbValue v = vdbexpr_eval_arg(arg, rs->records[i], schema);
            if (v.as.Int < min)
                min = v.as.Int;
        }
        struct VdbValue d;
        d.type = VDBT_TYPE_INT8;
        d.as.Int = min;
        return d;
    } else if (first_value.type == VDBT_TYPE_FLOAT8) {
        double min = first_value.as.Float;
        for (int i = 1; i < rs->count; i++) {
            struct VdbValue v = vdbexpr_eval_arg(arg, rs->records[i], schema);
            if (v.as.Float < min)
                min = v.as.Float;
        }
        struct VdbValue d;
        d.type = VDBT_TYPE_FLOAT8;
        d.as.Float = min;
        return d;
    }

    assert(false && "max function needs to be used with columns of int or float type");
    struct VdbValue d;
    d.type = VDBT_TYPE_NULL;
    return d;
}

static struct VdbValue vdbexpr_do_eval(struct VdbExpr* expr, struct VdbRecordSet* rs, struct VdbSchema* schema) {
    switch (expr->type) {
        case VDBET_BINARY: {
            struct VdbValue left = vdbexpr_do_eval(expr->as.binary.left, rs, schema);
            struct VdbValue right = vdbexpr_do_eval(expr->as.binary.right, rs, schema);

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

            if (left.type == VDBT_TYPE_TEXT) {
                free_w(left.as.Str.start, sizeof(char) * left.as.Str.len);
            }

            if (right.type == VDBT_TYPE_TEXT) {
                free_w(right.as.Str.start, sizeof(char) * right.as.Str.len);
            }

            return d;
        }
        case VDBET_UNARY: {
            struct VdbValue right = vdbexpr_do_eval(expr->as.unary.right, rs, schema);

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
                    if (d.type == VDBT_TYPE_INT8) {
                        d.as.Int = -d.as.Int;
                    } else if (d.type == VDBT_TYPE_FLOAT8) {
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
            return vdbexpr_eval_identifier(expr->as.identifier.token, rs->records[0], schema);
        }
        case VDBET_LITERAL: {
            return vdbexpr_eval_literal(expr->as.literal.token);
        }
        case VDBET_IS_NULL: {
            struct VdbValue left = vdbexpr_do_eval(expr->as.is_null.left, rs, schema);
            struct VdbValue d;
            d.type = VDBT_TYPE_BOOL;
            d.as.Bool = vdbvalue_is_null(&left);
            return d;
        }
        case VDBET_IS_NOT_NULL: {
            struct VdbValue left = vdbexpr_do_eval(expr->as.is_not_null.left, rs, schema);
            struct VdbValue d;
            d.type = VDBT_TYPE_BOOL;
            d.as.Bool = !vdbvalue_is_null(&left);
            return d;
        }
        case VDBET_CALL: {
            switch (expr->as.call.fcn_name.type) {
                case VDBT_AVG: return vdbexpr_eval_call_avg(expr->as.call.arg, rs, schema);
                case VDBT_COUNT: return vdbexpr_eval_call_count(expr->as.call.arg, rs, schema);
                case VDBT_MAX: return vdbexpr_eval_call_max(expr->as.call.arg, rs, schema);
                case VDBT_MIN: return vdbexpr_eval_call_min(expr->as.call.arg, rs, schema);
                case VDBT_SUM: return vdbexpr_eval_call_sum(expr->as.call.arg, rs, schema);
                default: assert(false && "invalid function name"); break;
            }
            break;
        }
        default: {
            break;
        }
    }

    assert(false && "invalid expression type");
    struct VdbValue d;
    d.type = VDBT_TYPE_BOOL;
    d.as.Bool = false;
    return d;
}

struct VdbValue vdbexpr_eval(struct VdbExpr* expr, struct VdbRecordSet* rs, struct VdbSchema* schema) {
    if (!expr) {
        struct VdbValue d;
        d.type = VDBT_TYPE_BOOL;
        d.as.Bool = true;
        return d;
    }

    return vdbexpr_do_eval(expr, rs, schema);
}

bool vdbexpr_attrs_valid(struct VdbExpr* expr, struct VdbSchema* schema) {
    switch (expr->type) {
        case VDBET_LITERAL:
            return true;
        case VDBET_IDENTIFIER: {
            struct VdbToken t = expr->as.identifier.token;
            for (int i = 0; i < schema->count; i++) {
                char* attr = schema->names[i];
                if (strlen(attr) == t.len && strncmp(attr, t.lexeme, t.len) == 0) {
                    return true;
                }
            }
            return false;
        }
        case VDBET_WILDCARD:
            return true;
        case VDBET_UNARY:
            return vdbexpr_attrs_valid(expr->as.unary.right, schema);
        case VDBET_BINARY:
            return vdbexpr_attrs_valid(expr->as.binary.left, schema) && vdbexpr_attrs_valid(expr->as.binary.right, schema);
        case VDBET_IS_NULL:
            return vdbexpr_attrs_valid(expr->as.is_null.left, schema);
        case VDBET_IS_NOT_NULL:
            return vdbexpr_attrs_valid(expr->as.is_not_null.left, schema);
        case VDBET_CALL:
            return vdbexpr_attrs_valid(expr->as.call.arg, schema);
        default:
            return false;
    }
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
        case VDBET_CALL:
            vdbexpr_free(expr->as.call.arg);
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
            return vdbexpr_init_identifier(vdbparser_next_token(parser));
        case VDBT_STAR:
            return vdbexpr_init_wildcard(vdbparser_next_token(parser));
        case VDBT_LPAREN:
            vdbparser_consume_token(parser, VDBT_LPAREN);
            struct VdbExpr* expr = vdbparser_parse_expr(parser);
            vdbparser_consume_token(parser, VDBT_RPAREN);
            return expr;
        case VDBT_AVG:
        case VDBT_COUNT:
        case VDBT_MAX:
        case VDBT_MIN:
        case VDBT_SUM: {
            struct VdbToken fcn_name = vdbparser_next_token(parser);
            vdbparser_consume_token(parser, VDBT_LPAREN);
            struct VdbExpr* expr = vdbexpr_init_fcn_call(fcn_name, vdbparser_parse_expr(parser));
            vdbparser_consume_token(parser, VDBT_RPAREN);
            return expr;
        }
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

struct VdbExpr* vdbparser_parse_expr(struct VdbParser* parser) {
    return vdbparser_parse_or(parser);
}

void vdbparser_parse_identifier_tuple(struct VdbParser* parser, struct VdbTokenList* tl) {
    vdbparser_consume_token(parser, VDBT_LPAREN);
    while (vdbparser_peek_token(parser).type != VDBT_RPAREN) {
        struct VdbToken t = vdbparser_consume_token(parser, VDBT_IDENTIFIER);
        vdbtokenlist_append_token(tl, t);
        if (vdbparser_peek_token(parser).type == VDBT_COMMA) {
            vdbparser_consume_token(parser, VDBT_COMMA);
        } else {
            break;
        }
    }
    vdbparser_consume_token(parser, VDBT_RPAREN);
}

bool vdbparser_valid_data_type(struct VdbToken t) {
    return t.type == VDBT_TYPE_TEXT ||
           t.type == VDBT_TYPE_INT8 ||
           t.type == VDBT_TYPE_FLOAT8 ||
           t.type == VDBT_TYPE_BOOL;
}

enum VdbReturnCode vdbparser_parse_stmt(struct VdbParser* parser, struct VdbStmt* stmt) {
    switch (vdbparser_next_token(parser).type) {
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
                stmt->as.create.key_idx = -1;

                vdbparser_consume_token(parser, VDBT_LPAREN);
                while (vdbparser_peek_token(parser).type != VDBT_RPAREN) {
                    struct VdbToken t = vdbparser_consume_token(parser, VDBT_IDENTIFIER);
                    vdbtokenlist_append_token(stmt->as.create.attributes, t);
                    struct VdbToken attr_type = vdbparser_next_token(parser);
                    if (!vdbparser_valid_data_type(attr_type)) {
                        vdberrorlist_append_error(parser->errors, 1, "invalid data type");
                    }
                    vdbtokenlist_append_token(stmt->as.create.types, attr_type);
                    if (vdbparser_peek_token(parser).type == VDBT_KEY) {
                        vdbparser_consume_token(parser, VDBT_KEY);
                        stmt->as.create.key_idx = stmt->as.create.types->count - 1;
                    }

                    if (vdbparser_peek_token(parser).type == VDBT_COMMA) {
                        vdbparser_consume_token(parser, VDBT_COMMA);
                    } else {
                        break;
                    }
                }
                vdbparser_consume_token(parser, VDBT_RPAREN);

                if (stmt->as.create.key_idx == -1) {
                    vdberrorlist_append_error(parser->errors, 1, "'create table' requires exactly one 'key' constraint");
                }
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
            stmt->as.update.values = vdbexprlist_init();

            while (true) {
                struct VdbToken t = vdbparser_consume_token(parser, VDBT_IDENTIFIER);
                vdbtokenlist_append_token(stmt->as.update.attributes, t);
                vdbparser_consume_token(parser, VDBT_EQUALS);
                vdbexprlist_append_expr(stmt->as.update.values, vdbparser_parse_expr(parser));

                if (vdbparser_peek_token(parser).type == VDBT_COMMA) {
                    vdbparser_consume_token(parser, VDBT_COMMA);
                    continue;
                }

                break;
            }

            if (vdbparser_peek_token(parser).type == VDBT_WHERE) {
                vdbparser_consume_token(parser, VDBT_WHERE);
                stmt->as.update.selection = vdbparser_parse_expr(parser);
            } else {
                struct VdbToken t;
                t.type = VDBT_TRUE;
                t.lexeme = "true";
                t.len = 4;
                stmt->as.update.selection = vdbexpr_init_literal(t);
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
            if (vdbparser_peek_token(parser).type == VDBT_DISTINCT) {
                vdbparser_consume_token(parser, VDBT_DISTINCT);
                stmt->as.select.distinct = true; 
            } else {
                stmt->as.select.distinct = false;
            }

            stmt->as.select.projection = vdbexprlist_init();
            while (true) {
                vdbexprlist_append_expr(stmt->as.select.projection, vdbparser_parse_expr(parser));
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
                struct VdbToken t = { VDBT_TRUE, "true", 4 };
                stmt->as.select.selection = vdbexpr_init_literal(t);
            }

            stmt->as.select.grouping = vdbexprlist_init();
            if (vdbparser_peek_token(parser).type == VDBT_GROUP) {
                vdbparser_consume_token(parser, VDBT_GROUP);
                vdbparser_consume_token(parser, VDBT_BY);
                while (true) {
                    vdbexprlist_append_expr(stmt->as.select.grouping, vdbparser_parse_expr(parser));
                    if (vdbparser_peek_token(parser).type == VDBT_COMMA) {
                        vdbparser_consume_token(parser, VDBT_COMMA);
                        continue;
                    }
                    break;
                }
            }

            if (vdbparser_peek_token(parser).type == VDBT_HAVING) {
                vdbparser_consume_token(parser, VDBT_HAVING);
                stmt->as.select.having = vdbparser_parse_expr(parser);
            } else {
                struct VdbToken t = { VDBT_TRUE, "true", 4 };
                stmt->as.select.having = vdbexpr_init_literal(t);
            }

            stmt->as.select.ordering = vdbexprlist_init();
            if (vdbparser_peek_token(parser).type == VDBT_ORDER) {
                vdbparser_consume_token(parser, VDBT_ORDER);
                vdbparser_consume_token(parser, VDBT_BY);
                while (true) {
                    vdbexprlist_append_expr(stmt->as.select.ordering, vdbparser_parse_expr(parser));
                    if (vdbparser_peek_token(parser).type == VDBT_COMMA) {
                        vdbparser_consume_token(parser, VDBT_COMMA);
                        continue;
                    }
                    break;
                }
            }

            if (vdbparser_peek_token(parser).type == VDBT_DESC) {
                vdbparser_consume_token(parser, VDBT_DESC);
                stmt->as.select.order_desc = true;
            } else {
                stmt->as.select.order_desc = false;
            }

            if (vdbparser_peek_token(parser).type == VDBT_LIMIT) {
                vdbparser_consume_token(parser, VDBT_LIMIT);
                stmt->as.select.limit = vdbparser_parse_expr(parser);
            } else {
                stmt->as.select.limit = NULL;
            }

            break;
        }
        case VDBT_EXIT: {
            stmt->type = VDBST_EXIT;
            break; 
        }
        case VDBT_BEGIN: {
            stmt->type = VDBST_BEGIN;
            break;
        }
        case VDBT_COMMIT: {
            stmt->type = VDBST_COMMIT;
            break;
        }
        case VDBT_ROLLBACK: {
            stmt->type = VDBST_ROLLBACK;
            break;
        }
        default:
            printf("invalid token\n");
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
            vdbexprlist_free(stmt->as.update.values);
            vdbexpr_free(stmt->as.update.selection);
            break;
        case VDBST_DELETE:
            vdbexpr_free(stmt->as.delete.selection);
            break;
        case VDBST_SELECT:
            vdbexprlist_free(stmt->as.select.projection);
            vdbexpr_free(stmt->as.select.selection);
            //TODO: free more fields
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
                printf("\t\t[%.*s]: ", attribute.len, attribute.lexeme);
                vdbexpr_print(stmt->as.update.values->exprs[i]);
                printf("\n");
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
                printf("\t\t");
                vdbexpr_print(stmt->as.select.projection->exprs[i]);
                printf("\n");
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

void vdbstmtlist_print(struct VdbStmtList* sl) {
    for (int i = 0; i < sl->count; i++) {
        vdbstmt_print(&sl->stmts[i]);
    }
}
