#ifndef VDB_PARSER_H
#define VDB_PARSER_H

#include "lexer.h"
#include "error.h"
#include "schema.h"
#include "record.h"

enum VdbExprType {
    VDBET_LITERAL,
    VDBET_IDENTIFIER,
    VDBET_UNARY,
    VDBET_BINARY,
    VDBET_IS_NULL,
    VDBET_IS_NOT_NULL,
    VDBET_CALL
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
        struct {
            struct VdbExpr* left;
        } is_null;
        struct {
            struct VdbExpr* left;
        } is_not_null;
        struct {
            struct VdbToken fcn_name;
            struct VdbExpr* arg; //TODO: are multiple arguments used in SQL?
        } call;
    } as;
};

struct VdbExprList {
    struct VdbExpr** exprs;
    int count;
    int capacity;
};

enum VdbStmtType {
    VDBST_CONNECT,
    VDBST_SHOW_DBS,
    VDBST_SHOW_TABS,
    VDBST_CREATE_DB,
    VDBST_CREATE_TAB,
    VDBST_IF_EXISTS_DROP_DB,
    VDBST_IF_EXISTS_DROP_TAB,
    VDBST_DROP_DB,
    VDBST_DROP_TAB,
    VDBST_OPEN,
    VDBST_CLOSE,
    VDBST_DESCRIBE,
    VDBST_INSERT,
    VDBST_UPDATE,
    VDBST_DELETE,
    VDBST_SELECT,
    VDBST_EXIT,
    VDBST_BEGIN,
    VDBST_COMMIT,
    VDBST_ROLLBACK
};

struct VdbStmt {
    enum VdbStmtType type;
    struct VdbToken target;
    union {
        struct {
            struct VdbTokenList* attributes;
            struct VdbTokenList* types;
            int key_idx;
        } create;
        struct {
            struct VdbTokenList* attributes;
            struct VdbExprList* values;
        } insert;
        struct {
            struct VdbTokenList* attributes;
            struct VdbExprList* values;
            struct VdbExpr* selection;
        } update;
        struct {
            struct VdbExpr* selection;
        } delete;
        struct {
            struct VdbExprList* projection;
            struct VdbExpr* selection;
            struct VdbExprList* grouping;
            struct VdbExprList* ordering;
            struct VdbExpr* having;
            bool order_desc;
            bool distinct;
            struct VdbExpr* limit;
        } select;
    } as;
};

struct VdbStmtList {
    struct VdbStmt* stmts;
    int count;
    int capacity;
};

struct VdbParser {
    struct VdbTokenList* tl;
    int current;
    struct VdbErrorList* errors;
};

enum VdbReturnCode vdbparser_parse(struct VdbTokenList* tokens, struct VdbStmtList** stmts, struct VdbErrorList** errors);
struct VdbExpr* vdbexpr_init_literal(struct VdbToken token);
struct VdbExpr* vdbexpr_init_identifier(struct VdbToken token);
struct VdbExpr* vdbexpr_init_unary(struct VdbToken op, struct VdbExpr* right);
struct VdbExpr* vdbexpr_init_binary(struct VdbToken op, struct VdbExpr* left, struct VdbExpr* right);
struct VdbExpr* vdbexpr_init_is_null(struct VdbExpr* left);
struct VdbExpr* vdbexpr_init_is_not_null(struct VdbExpr* left);
struct VdbValue vdbexpr_eval(struct VdbExpr* expr, struct VdbRecordSet* rs, struct VdbSchema* schema);
char* vdbexpr_to_string(struct VdbExpr* expr);

struct VdbExpr* vdbexpr_copy(struct VdbExpr* expr);
void vdbexpr_print(struct VdbExpr* expr);
void vdbexpr_free(struct VdbExpr* expr);
struct VdbExprList* vdbexprlist_init();
void vdbexprlist_free(struct VdbExprList* el);
void vdbexprlist_append_expr(struct VdbExprList* el, struct VdbExpr* expr);

struct VdbStmtList* vdbstmtlist_init();
void vdbstmtlist_free(struct VdbStmtList* sl);
void vdbstmtlist_append_stmt(struct VdbStmtList* sl, struct VdbStmt stmt);
struct VdbToken vdbparser_peek_token(struct VdbParser* parser);
struct VdbToken vdbparser_consume_token(struct VdbParser* parser, enum VdbTokenType type);
struct VdbToken vdbparser_next_token(struct VdbParser* parser);
struct VdbExpr* vdbparser_parse_literal(struct VdbParser* parser);
struct VdbExpr* vdbparser_parse_relational(struct VdbParser* parser);
struct VdbExpr* vdbparser_parse_equality(struct VdbParser* parser);
struct VdbExpr* vdbparser_parse_expr(struct VdbParser* parser);
void vdbparser_parse_tuple(struct VdbParser* parser, struct VdbTokenList* tl);
enum VdbReturnCode vdbparser_parse_stmt(struct VdbParser* parser, struct VdbStmt* stmt);
void vdbstmt_free_fields(struct VdbStmt* stmt);
void vdbstmt_print(struct VdbStmt* stmt);
void vdbstmtlist_print(struct VdbStmtList* sl);

#endif //VDB_PARSER_H
