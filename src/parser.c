#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>

#include "parser.h"
#include "util.h"

struct VdbStmtList* vdbparser_parse(struct VdbTokenList* tl) {
    struct VdbStmtList* sl = vdbstmtlist_init();

    struct VdbParser parser;
    parser.tl = tl;
    parser.current = 0;
   
    while (parser.current < tl->count) {
        vdbstmtlist_append_stmt(sl, vdbparser_parse_stmt(&parser));
    }

    return sl;
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

struct VdbToken vdbparser_consume_token(struct VdbParser* parser) {
    struct VdbToken token = parser->tl->tokens[parser->current++];
    //should return error is token is unexpected
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
            return vdbexpr_init_literal(vdbparser_consume_token(parser));
        case VDBT_IDENTIFIER:
            return vdbexpr_init_identifier(vdbparser_consume_token(parser));
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
        struct VdbToken op = vdbparser_consume_token(parser);
        left = vdbexpr_init_binary(op, left, vdbparser_parse_literal(parser));
    }

    return left;
}
struct VdbExpr* vdbparser_parse_equality(struct VdbParser* parser) {
    struct VdbExpr* left = vdbparser_parse_relational(parser);
    while (vdbparser_peek_token(parser).type == VDBT_EQUALS) {
        struct VdbToken equal = vdbparser_consume_token(parser);
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

void vdbparser_parse_tuple(struct VdbParser* parser, struct VdbTokenList* tl) {
    vdbparser_consume_token(parser); //left paren
    while (vdbparser_peek_token(parser).type != VDBT_RPAREN) {
        vdbtokenlist_append_token(tl, vdbparser_consume_token(parser));
        if (vdbparser_peek_token(parser).type == VDBT_COMMA) {
            vdbparser_consume_token(parser);
        }
    }
    vdbparser_consume_token(parser); //right paren
}

struct VdbStmt vdbparser_parse_stmt(struct VdbParser* parser) {
    struct VdbStmt stmt;
    switch (vdbparser_consume_token(parser).type) {
        case VDBT_OPEN: {
            stmt.type = VDBST_OPEN;
            stmt.target = vdbparser_consume_token(parser);
            break;
        }
        case VDBT_CLOSE: {
            stmt.type = VDBST_CLOSE;
            stmt.target = vdbparser_consume_token(parser);
            break;
        }
        case VDBT_CREATE: {
            stmt.type = VDBST_CREATE_TAB;
            vdbparser_consume_token(parser); //TODO: parse error if not keyword 'table'
            stmt.target = vdbparser_consume_token(parser);
            stmt.get.create.attributes = vdbtokenlist_init();
            stmt.get.create.types = vdbtokenlist_init();
            vdbparser_consume_token(parser); //left paren
            while (vdbparser_peek_token(parser).type != VDBT_RPAREN) {
                vdbtokenlist_append_token(stmt.get.create.attributes, vdbparser_consume_token(parser));
                vdbtokenlist_append_token(stmt.get.create.types, vdbparser_consume_token(parser));
                if (vdbparser_peek_token(parser).type == VDBT_COMMA) {
                    vdbparser_consume_token(parser);
                }
            }

            vdbparser_consume_token(parser); //right paren
            break;
        }
        case VDBT_DROP: {
            stmt.type = VDBST_DROP_TAB;
            vdbparser_consume_token(parser); //TODO: parse error if not keyword 'table'
            stmt.target = vdbparser_consume_token(parser);
            break;
        }
        case VDBT_INSERT: {
            stmt.type = VDBST_INSERT;
            vdbparser_consume_token(parser); //TODO: skip 'into'
            stmt.target = vdbparser_consume_token(parser);

            stmt.get.insert.attributes = vdbtokenlist_init();
            vdbparser_parse_tuple(parser, stmt.get.insert.attributes);

            vdbparser_consume_token(parser); //'values' token

            stmt.get.insert.values = vdbtokenlist_init();
            while (vdbparser_peek_token(parser).type == VDBT_LPAREN) {
                vdbparser_parse_tuple(parser, stmt.get.insert.values);
                if (vdbparser_peek_token(parser).type == VDBT_COMMA) {
                    vdbparser_consume_token(parser);
                }
            }

            break;
        }
        case VDBT_UPDATE: {
            stmt.type = VDBST_UPDATE;
            stmt.target = vdbparser_consume_token(parser);
            vdbparser_consume_token(parser); //'set' keyword

            stmt.get.update.attributes = vdbtokenlist_init();
            stmt.get.update.values = vdbtokenlist_init();

            while (true) {
                vdbtokenlist_append_token(stmt.get.update.attributes, vdbparser_consume_token(parser));
                vdbparser_consume_token(parser); // '='
                vdbtokenlist_append_token(stmt.get.update.values, vdbparser_consume_token(parser));

                if (vdbparser_peek_token(parser).type == VDBT_COMMA) {
                    vdbparser_consume_token(parser);
                    continue;
                }

                break;
            }

            stmt.get.update.selection = NULL;
            if (vdbparser_peek_token(parser).type == VDBT_WHERE) {
                vdbparser_consume_token(parser); //'where' keyword
                stmt.get.update.selection = vdbparser_parse_expr(parser);
            }

            break;
        }
        case VDBT_DELETE: {
            stmt.type = VDBST_DELETE;
            vdbparser_consume_token(parser); //'from' keyword
            stmt.target = vdbparser_consume_token(parser);
            stmt.get.delete.selection = NULL;
            if (vdbparser_peek_token(parser).type == VDBT_WHERE) {
                vdbparser_consume_token(parser); //'where' keyword
                stmt.get.delete.selection = vdbparser_parse_expr(parser);
            }
            break;
        }
        case VDBT_SELECT:  {
            stmt.type = VDBST_SELECT;
            stmt.get.select.projection = vdbtokenlist_init();
            while (true) {
                vdbtokenlist_append_token(stmt.get.select.projection, vdbparser_consume_token(parser));
                if (vdbparser_peek_token(parser).type == VDBT_COMMA) {
                    vdbparser_consume_token(parser);
                    continue;
                }
                break;
            }

            vdbparser_consume_token(parser); // 'from' keyword
            stmt.target = vdbparser_consume_token(parser);
            stmt.get.select.selection = NULL;
            if (vdbparser_peek_token(parser).type == VDBT_WHERE) {
                vdbparser_consume_token(parser); //'where' keyword
                stmt.get.select.selection = vdbparser_parse_expr(parser);
            }
            break;
        }
        case VDBT_EXIT: {
            stmt.type = VDBST_EXIT;
            break; 
        }
        default:
            printf("not implemented yet\n");
            break;
    }

    vdbparser_consume_token(parser); //semicolon

    return stmt;
}

void vdbstmt_print(struct VdbStmt* stmt) {
    switch (stmt->type) {
        case VDBST_EXIT: {
            printf("<exit>\n");
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
        case VDBST_CREATE_TAB: {
            printf("<create table [%.*s]>\n", stmt->target.len, stmt->target.lexeme);
            printf("\tschema:\n");
            for (int i = 0; i < stmt->get.create.attributes->count; i++) {
                struct VdbToken attribute = stmt->get.create.attributes->tokens[i];
                struct VdbToken type = stmt->get.create.types->tokens[i];
                printf("\t\t[%.*s]: %.*s\n", attribute.len, attribute.lexeme, type.len, type.lexeme);
            }
            break;
        }
        case VDBST_DROP_TAB: {
            printf("<drop table [%.*s]>\n", stmt->target.len, stmt->target.lexeme);
            break;
        }
        case VDBST_INSERT: {
            printf("<insert into table [%.*s]>\n", stmt->target.len, stmt->target.lexeme);
            printf("\tcolumns:\n");
            for (int i = 0; i < stmt->get.insert.attributes->count; i++) {
                struct VdbToken attribute = stmt->get.insert.attributes->tokens[i];
                printf("\t\t[%.*s]\n", attribute.len, attribute.lexeme);
            }
            printf("\tvalues:\n");
            for (int i = 0; i < stmt->get.insert.values->count; i += stmt->get.insert.attributes->count) {
                printf("\t\t");
                for (int j = i; j < i + stmt->get.insert.attributes->count; j++) {
                    struct VdbToken value = stmt->get.insert.values->tokens[j];
                    printf("%.*s", value.len, value.lexeme);
                    if (j < i +  stmt->get.insert.attributes->count -1) {
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
            for (int i = 0; i < stmt->get.update.attributes->count; i++) {
                struct VdbToken attribute = stmt->get.update.attributes->tokens[i];
                struct VdbToken value = stmt->get.update.values->tokens[i];
                printf("\t\t[%.*s]: %.*s\n", attribute.len, attribute.lexeme, value.len, value.lexeme);
            }
            printf("\tcondition:\n");
            if (stmt->get.update.selection) {
                printf("\t\t");
                vdbexpr_print(stmt->get.update.selection);
                printf("\n");
            }
            break;
        }
        case VDBST_DELETE: {
            printf("<delete record(s) from table [%.*s]>\n", stmt->target.len, stmt->target.lexeme);
            printf("\tcondition:\n");
            if (stmt->get.delete.selection) {
                printf("\t\t");
                vdbexpr_print(stmt->get.delete.selection);
                printf("\n");
            }
            break;
        }
        case VDBST_SELECT: {
            printf("<select record(s) from table [%.*s]>\n", stmt->target.len, stmt->target.lexeme);
            printf("\tcolumns:\n");
            for (int i = 0; i < stmt->get.select.projection->count; i++) {
                struct VdbToken t = stmt->get.select.projection->tokens[i];
                printf("\t\t[%.*s]\n", t.len, t.lexeme);
            }
            printf("\tcondition:\n");
            if (stmt->get.select.selection) {
                printf("\t\t");
                vdbexpr_print(stmt->get.select.selection);
                printf("\n");
            }
            break;
        }
        default:
            break;
    }
}
