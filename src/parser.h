#ifndef VDB_PARSER_H
#define VDB_PARSER_H

#include "lexer.h"

enum VdbExprType {
    VDBET_LITERAL,
    VDBET_IDENTIFIER,
    VDBET_UNARY,
    VDBET_BINARY
};

struct VdbExpr {
    enum VdbExprType type;
    union {
        struct {
            struct VdbToken token;
        } literal;
        struct {
            struct VdbToken token;
        } identifier;
        struct {
            struct VdbToken op;
            struct VdbExpr* right;
        } unary;
        struct {
            struct VdbToken op;
            struct VdbExpr* left;
            struct VdbExpr* right;
        } binary;
    } as;
};

struct VdbExprList {
    struct VdbExpr** exprs;
    int count;
    int capacity;
};

enum VdbStmtType {
    VDBST_EXIT,
    VDBST_OPEN,
    VDBST_CLOSE,
    VDBST_CREATE_TAB,
    VDBST_DROP_TAB,
    VDBST_INSERT,
    VDBST_UPDATE,
    VDBST_DELETE,
    VDBST_SELECT,
    VDBST_SHOW_DBS,
    VDBST_CREATE_DB,
    VDBST_SHOW_TABS,
    VDBST_DESCRIBE_TAB,
    VDBST_DROP_DB,
    VDBST_CONNECT
};

struct VdbStmt {
    enum VdbStmtType type;
    struct VdbToken target;
    union {
        struct {
            struct VdbTokenList* attributes;
            struct VdbTokenList* types;
        } create;
        struct {
            struct VdbTokenList* attributes;
            struct VdbTokenList* values;
        } insert;
        struct {
            struct VdbTokenList* attributes;
            struct VdbTokenList* values;
            struct VdbExpr* selection;
        } update;
        struct {
            struct VdbExpr* selection;
        } delete;
        struct {
            struct VdbTokenList* projection;
            struct VdbExpr* selection;
        } select;
    } get;
};

struct VdbStmtList {
    struct VdbStmt* stmts;
    int count;
    int capacity;
};

struct VdbParser {
    struct VdbTokenList* tl;
    int current;
};

struct VdbStmtList* vdbparser_parse(struct VdbTokenList* tl);
void vdbexpr_print(struct VdbExpr* expr);
struct VdbExpr* vdbexpr_init_literal(struct VdbToken token);
struct VdbExpr* vdbexpr_init_identifier(struct VdbToken token);
struct VdbExpr* vdbexpr_init_unary(struct VdbToken op, struct VdbExpr* right);
struct VdbExpr* vdbexpr_init_binary(struct VdbToken op, struct VdbExpr* left, struct VdbExpr* right);
void vdbexpr_free(struct VdbExpr* expr);
struct VdbExprList* vdbexprlist_init();
void vdbexprlist_free(struct VdbExprList* el);
void vdbexprlist_append_expr(struct VdbExprList* el, struct VdbExpr* expr);
struct VdbStmtList* vdbstmtlist_init();
void vdbstmtlist_free(struct VdbStmtList* sl);
void vdbstmtlist_append_stmt(struct VdbStmtList* sl, struct VdbStmt stmt);
struct VdbToken vdbparser_peek_token(struct VdbParser* parser);
struct VdbToken vdbparser_consume_token(struct VdbParser* parser);
struct VdbExpr* vdbparser_parse_literal(struct VdbParser* parser);
struct VdbExpr* vdbparser_parse_relational(struct VdbParser* parser);
struct VdbExpr* vdbparser_parse_equality(struct VdbParser* parser);
struct VdbExpr* vdbparser_parse_expr(struct VdbParser* parser);
void vdbparser_parse_tuple(struct VdbParser* parser, struct VdbTokenList* tl);
struct VdbStmt vdbparser_parse_stmt(struct VdbParser* parser);
void vdbstmt_print(struct VdbStmt* stmt);

#endif //VDB_PARSER_H
