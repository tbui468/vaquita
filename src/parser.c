#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include "parser.h"
#include "util.h"

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
            printf("%.*s", expr->as.unary.op.len, expr->as.unary.op.lexeme);
            vdbexpr_print(expr->as.unary.right);
            break;
        case VDBET_BINARY:
            vdbexpr_print(expr->as.binary.left);
            printf(" %.*s ", expr->as.binary.op.len, expr->as.binary.op.lexeme);
            vdbexpr_print(expr->as.binary.right);
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

bool vdbexpr_eval(struct VdbExpr* expr) {
    expr = expr; //just to silence warnings for now
    //TODO: walk expression tree to evaluate actual result here
    return true;
}

void vdbexpr_free(struct VdbExpr* expr) {
    switch (expr->type) {
        case VDBET_UNARY:
            vdbexpr_free(expr->as.unary.right);
            break;
        case VDBET_BINARY:
            vdbexpr_free(expr->as.binary.left);
            vdbexpr_free(expr->as.binary.right);
            break;
         default:
            break;
    }

    free(expr);
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
    free(el->exprs);
    free(el);
}

void vdbexprlist_append_expr(struct VdbExprList* el, struct VdbExpr* expr) {
    if (el->count + 1 > el->capacity) {
        el->capacity *= 2;
        el->exprs = realloc_w(el->exprs, sizeof(struct VdbExpr*) * el->capacity);
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
    //TODO: free all token lists, expr lists in statements
    free(sl->stmts);
    free(sl);
}

void vdbstmtlist_append_stmt(struct VdbStmtList* sl, struct VdbStmt stmt) {
    if (sl->count + 1 > sl->capacity) {
        sl->capacity *= 2;
        sl->stmts = realloc_w(sl->stmts, sizeof(struct VdbStmt) * sl->capacity);
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

/*
struct VdbExpr* vdbparser_parse_group(struct VdbParser* parser) {
}*/
struct VdbExpr* vdbparser_parse_literal(struct VdbParser* parser) {
    switch (vdbparser_peek_token(parser).type) {
        case VDBT_INT:
        case VDBT_STR:
        case VDBT_TRUE:
        case VDBT_FALSE:
            return vdbexpr_init_literal(vdbparser_next_token(parser));
        case VDBT_IDENTIFIER:
            return vdbexpr_init_identifier(vdbparser_consume_token(parser, VDBT_IDENTIFIER));
        default:
            return NULL;
    }
}
/*
struct VdbExpr* vdbparser_parse_unary(struct VdbParser* parser) {
}
struct VdbExpr* vdbparser_parse_mul_div(struct VdbParser* parser) {
}
struct VdbExpr* vdbparser_parse_add_sub(struct VdbParser* parser) {
}*/
struct VdbExpr* vdbparser_parse_relational(struct VdbParser* parser) {
    struct VdbExpr* left = vdbparser_parse_literal(parser);
    while (vdbparser_peek_token(parser).type == VDBT_LESS ||
           vdbparser_peek_token(parser).type == VDBT_GREATER ||
           vdbparser_peek_token(parser).type == VDBT_LESS_EQUALS ||
           vdbparser_peek_token(parser).type == VDBT_GREATER_EQUALS) {
        struct VdbToken op = vdbparser_next_token(parser);
        left = vdbexpr_init_binary(op, left, vdbparser_parse_literal(parser));
    }

    return left;
}
struct VdbExpr* vdbparser_parse_equality(struct VdbParser* parser) {
    struct VdbExpr* left = vdbparser_parse_relational(parser);
    while (vdbparser_peek_token(parser).type == VDBT_EQUALS) {
        struct VdbToken equal = vdbparser_consume_token(parser, VDBT_EQUALS);
        left = vdbexpr_init_binary(equal, left, vdbparser_parse_relational(parser));
    }

    return left;
}
/*
struct VdbExpr* vdbparser_parse_and(struct VdbParser* parser) {
}
struct VdbExpr* vdbparser_parse_or(struct VdbParser* parser) {
}*/
struct VdbExpr* vdbparser_parse_expr(struct VdbParser* parser) {
    return vdbparser_parse_equality(parser);
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

void vdbparser_parse_value_tuple(struct VdbParser* parser, struct VdbTokenList* tl) {
    vdbparser_consume_token(parser, VDBT_LPAREN);
    while (vdbparser_peek_token(parser).type != VDBT_RPAREN) {
        vdbtokenlist_append_token(tl, vdbparser_next_token(parser));
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

            stmt->as.insert.values = vdbtokenlist_init();
            while (vdbparser_peek_token(parser).type == VDBT_LPAREN) {
                vdbparser_parse_value_tuple(parser, stmt->as.insert.values);
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
            stmt->as.delete.selection = NULL;
            if (vdbparser_peek_token(parser).type == VDBT_WHERE) {
                vdbparser_consume_token(parser, VDBT_WHERE);
                stmt->as.delete.selection = vdbparser_parse_expr(parser);
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
            stmt->as.select.selection = NULL;
            if (vdbparser_peek_token(parser).type == VDBT_WHERE) {
                vdbparser_consume_token(parser, VDBT_WHERE);
                stmt->as.select.selection = vdbparser_parse_expr(parser);
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
                    struct VdbToken value = stmt->as.insert.values->tokens[j];
                    printf("%.*s", value.len, value.lexeme);
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
