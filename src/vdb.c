#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "vdb.h"
#include "schema.h"
#include "record.h"
#include "util.h"
#include "tree.h"

struct Vdb* vdb_init(const char* name) {
    struct Vdb* db = malloc_w(sizeof(struct Vdb));
    db->pager = vdb_pager_alloc();
    db->trees = vdb_treelist_init();
    db->name = strdup_w(name);

    return db;
}

void vdb_free(struct Vdb* db) {
    vdb_pager_free(db->pager);
    vdb_treelist_free(db->trees);
    free_w(db->name, sizeof(char) * (strlen(db->name) + 1)); //include null terminator
    free_w(db, sizeof(struct Vdb));
}

VDBHANDLE vdb_open_db(const char* name) {
    struct Vdb* db = vdb_init(name);

    char dirname[FILENAME_MAX];
    dirname[0] = '\0';
    strcat(dirname, name);
    strcat(dirname, ".vdb");

    //error if database doesn't exist
    DIR* d;
    if (!(d = opendir(dirname))) {
        vdb_close(db);
        return NULL;
    } 

    //open all table files
    struct dirent* ent;
    while ((ent = readdir(d)) != NULL) {
        int entry_len = strlen(ent->d_name);
        const char* ext = ".vtb";
        int ext_len = strlen(ext);

        if (entry_len <= ext_len)
            continue;

        if (strncmp(ent->d_name + entry_len - ext_len, ext, ext_len) != 0)
            continue;

        char path[FILENAME_MAX];
        path[0] = '\0';
        strcat(path, dirname);
        strcat(path, "/");
        strcat(path, ent->d_name);
        FILE* f = fopen_w(path, "r+");
        setbuf(f, NULL);

        char* s = malloc_w(sizeof(char) * (entry_len - 3));
        memcpy(s, ent->d_name, entry_len - 4);
        s[entry_len - 3] = '\0';
        struct VdbTree* tree = vdb_tree_open(s, f, db->pager);
        vdb_treelist_append_tree(db->trees, tree);
    }

    closedir_w(d);
    
    return (VDBHANDLE)db;
}

enum VdbReturnCode vdb_create_db(const char* name) {
    char dirname[FILENAME_MAX];
    dirname[0] = '\0';
    strcat(dirname, name);
    strcat(dirname, ".vdb");

    DIR* d;
    if (!(d = opendir(dirname))) {
        mkdir_w(dirname, 0777);
    } else {
        closedir_w(d);
        return VDBRC_ERROR;
    } 

    return VDBRC_SUCCESS;
}

enum VdbReturnCode vdb_show_dbs(struct VdbValueList** vl) {
    DIR* d;
    if (!(d = opendir("./"))) {
        return VDBRC_ERROR;
    } 

    *vl = vdbvaluelist_init();

    struct dirent* ent;
    while ((ent = readdir(d)) != NULL) {
        int entry_len = strlen(ent->d_name);
        const char* ext = ".vdb";
        int ext_len = strlen(ext);

        if (entry_len <= ext_len)
            continue;

        if (strncmp(ent->d_name + entry_len - ext_len, ext, ext_len) != 0)
            continue;

        struct VdbValue v = vdbvalue_init_string(ent->d_name, entry_len - ext_len);
        vdbvaluelist_append_value(*vl, v);
    }

    closedir_w(d);

    return VDBRC_SUCCESS;
}

enum VdbReturnCode vdb_show_tabs(VDBHANDLE h, struct VdbValueList** vl) {
    struct Vdb* db = (struct Vdb*)h;

    *vl = vdbvaluelist_init();

    for (uint32_t i = 0; i < db->trees->count; i++) {
        struct VdbTree* tree = db->trees->trees[i];
        struct VdbValue v = vdbvalue_init_string(tree->name, strlen(tree->name));
        vdbvaluelist_append_value(*vl, v);
    }

    return VDBRC_SUCCESS;
}

enum VdbReturnCode vdb_describe_table(VDBHANDLE h, const char* name, struct VdbValueList** vl) {
    struct Vdb* db = (struct Vdb*)h;
    struct VdbTree* tree = vdb_treelist_get_tree(db->trees, name);
    struct VdbSchema* schema = vdbtree_meta_read_schema(tree);

    *vl = vdbvaluelist_init();

    for (uint32_t i = 0; i < schema->count; i++) {
        struct VdbValue attr_name = vdbvalue_init_string(schema->names[i], strlen(schema->names[i]));
        vdbvaluelist_append_value(*vl, attr_name);

        switch (schema->types[i]) {
            case VDBT_TYPE_INT:
                vdbvaluelist_append_value(*vl, vdbvalue_init_string("int", 3));
                break;
            case VDBT_TYPE_STR:
                vdbvaluelist_append_value(*vl, vdbvalue_init_string("string", 6));
                break;
            case VDBT_TYPE_FLOAT:
                vdbvaluelist_append_value(*vl, vdbvalue_init_string("float", 5));
                break;
            case VDBT_TYPE_BOOL:
                vdbvaluelist_append_value(*vl, vdbvalue_init_string("bool", 4));
                break;
            default:
                assert(false && "invalid schema data type");
                break;
        }
    }

    vdb_schema_free(schema);

    return VDBRC_SUCCESS;
}

bool vdb_close(VDBHANDLE h) {
    struct Vdb* db = (struct Vdb*)h;
    vdb_free(db);

    return true;
}

char* vdb_dbname(VDBHANDLE h) {
    struct Vdb* db = (struct Vdb*)h;
    return db->name;
}

struct VdbSchema* vdb_alloc_schema(int count, ...) {
    
    va_list args;
    va_start(args, count);
    struct VdbSchema* schema = vdb_schema_alloc(count, args);
    va_end(args);

    return schema;

}

void vdb_free_schema(struct VdbSchema* schema) {
    vdb_schema_free(schema);
}

bool vdb_table_exists(VDBHANDLE h, const char* name) {
    struct Vdb* db = (struct Vdb*)h;

    char dirpath[FILENAME_MAX];
    dirpath[0] = '\0';
    strcat(dirpath, db->name);

    DIR* d;
    if (!(d = opendir(dirpath)))
        return false;

    char table_filename[FILENAME_MAX];
    table_filename[0] = '\0';
    strcat(table_filename, name);
    strcat(table_filename, ".vtb");

    bool found = false;
    struct dirent* ent;
    while ((ent = readdir(d)) != NULL) {
        if (strlen(ent->d_name) == strlen(table_filename)) {
            if (!strncmp(ent->d_name, table_filename, strlen(table_filename))) {
                found = true;
                break;
            }
        }
    }

    closedir_w(d);

    return found;
}

bool vdb_create_table(VDBHANDLE h, const char* name, struct VdbSchema* schema) {
    if (vdb_table_exists(h, name))
        return false;

    struct Vdb* db = (struct Vdb*)h;

    char path[FILENAME_MAX];
    path[0] = '\0';
    strcat(path, db->name);
    strcat(path, ".vdb");
    strcat(path, "/");
    strcat(path, name);
    strcat(path, ".vtb");

    FILE* f = fopen_w(path, "w+");
    struct VdbTree* tree = vdb_tree_init(name, schema, db->pager, f);
    vdb_treelist_append_tree(db->trees, tree);

    return true;
}

enum VdbReturnCode vdb_drop_db(const char* name) {
    char dirname[FILENAME_MAX];
    dirname[0] = '\0';
    strcat(dirname, name);
    strcat(dirname, ".vdb");

    DIR* d;
    if (!(d = opendir(dirname))) {
        return VDBRC_ERROR;
    }

    struct dirent* ent;
    while ((ent = readdir(d)) != NULL) {
        if (strncmp(ent->d_name, ".", 1) == 0 || strncmp(ent->d_name, "..", 2) == 0)
            continue;

        char path[FILENAME_MAX];
        path[0] = '\0';
        strcat(path, dirname);
        strcat(path, "/");
        strcat(path, ent->d_name);

        remove_w(path);
    }

    closedir_w(d);

    rmdir_w(dirname);

    return VDBRC_SUCCESS;
}

enum VdbReturnCode vdb_drop_table(VDBHANDLE h, const char* name) {
    struct Vdb* db = (struct Vdb*)h;
    struct VdbTree* tree = vdb_treelist_remove_tree(db->trees, name);

    if (tree) {
        char path[FILENAME_MAX];
        path[0] = '\0';
        strcat(path, db->name);
        strcat(path, ".vdb/");
        strcat(path, tree->name);
        strcat(path, ".vtb");
        
        vdb_tree_close(tree);
        vdb_pager_evict_pages(db->pager, name);

        remove_w(path);

        return VDBRC_SUCCESS;
    }

    return VDBRC_ERROR;
}

static void vdb_allocate_dummy_string(struct VdbValue* value) {
    value->as.Str.start = malloc_w(sizeof(char) * 1);
    value->as.Str.len = 1;
    memcpy(value->as.Str.start, "0", 1);
}

static void vdb_assign_column_values(struct VdbValue* data, struct VdbSchema* schema, struct VdbTokenList* cols, struct VdbExprList* values) {
    for (uint32_t i = 0; i < schema->count; i++) {
        
        bool found = false;
        for (int j = 0; j < cols->count; j++) {
            if (strncmp(schema->names[i], cols->tokens[j].lexeme, cols->tokens[j].len) != 0) 
                continue;

            //TODO: passing in NULL for struct VdbRecord* and struct Schema* since values expression shouldn't have identifiers
            //  but holy hell this is ugly - should fix this
            data[i] = vdbexpr_eval(values->exprs[j], NULL, NULL);

            found = true;
            break;

        }

        //user manually inserted null for a string value
        if (found && data[i].type == VDBT_TYPE_NULL && schema->types[i] == VDBT_TYPE_STR) {
            vdb_allocate_dummy_string(&data[i]);
        }

        if (!found) {
            data[i].type = VDBT_TYPE_NULL;

            //automacally insert null value for string
            if (schema->types[i] == VDBT_TYPE_STR) {
                vdb_allocate_dummy_string(&data[i]);
            }
        }

        data[i].block_idx = 0;
        data[i].idxcell_idx = 0;

    }
}

enum VdbReturnCode vdb_insert_new(VDBHANDLE h, const char* name, struct VdbTokenList* cols, struct VdbExprList* values) {
    struct Vdb* db = (struct Vdb*)h;
    struct VdbTree* tree = vdb_treelist_get_tree(db->trees, name);

    struct VdbSchema* schema = vdbtree_meta_read_schema(tree);
    uint32_t key = vdbtree_meta_increment_primary_key_counter(tree);

    //append key to end of values list
    struct VdbToken key_token;
    key_token.type = VDBT_INT;
    char s[64];
    sprintf(s, "%d", key);
    key_token.lexeme = s;
    key_token.len = strlen(s);
    struct VdbExpr* key_expr = vdbexpr_init_literal(key_token);
    vdbexprlist_append_expr(values, key_expr);

    struct VdbValue data[schema->count];
    vdb_assign_column_values(data, schema, cols, values);

    struct VdbRecord* rec = vdbrecord_init(schema->count, data); 
    
    vdb_schema_free(schema);

    vdb_tree_insert_record(tree, rec);
    vdb_record_free(rec);
    vdbexpr_free(key_expr);

    return VDBRC_SUCCESS;
}


/*

bool vdb_delete_record(VDBHANDLE h, const char* name, uint32_t key) {
    struct Vdb* db = (struct Vdb*)h;
    struct VdbTree* tree = vdb_treelist_get_tree(db->trees, name);

    return vdbtree_delete_record(tree, key);
}

void vdb_insert_record(VDBHANDLE h, const char* name, ...) {
    struct Vdb* db = (struct Vdb*)h;
    struct VdbTree* tree = vdb_treelist_get_tree(db->trees, name);

    va_list args;
    va_start(args, name);

    struct VdbSchema* schema = vdbtree_meta_read_schema(tree);
    uint32_t key = vdbtree_meta_increment_primary_key_counter(tree);
    struct VdbRecord* rec = vdb_record_alloc(key, schema, args);
    vdb_schema_free(schema);

    va_end(args);

    vdb_tree_insert_record(tree, rec);
    vdb_record_free(rec);
}

struct VdbRecord* vdb_fetch_record(VDBHANDLE h, const char* name, uint32_t key) {
    struct Vdb* db = (struct Vdb*)h;
    struct VdbTree* tree = vdb_treelist_get_tree(db->trees, name);

    return vdb_tree_fetch_record(tree, key);
}

bool vdb_update_record(VDBHANDLE h, const char* name, uint32_t key, ...) {
    struct Vdb* db = (struct Vdb*)h;
    struct VdbTree* tree = vdb_treelist_get_tree(db->trees, name);

    va_list args;
    va_start(args, key);

    struct VdbSchema* schema = vdbtree_meta_read_schema(tree);
    struct VdbRecord* rec = vdb_record_alloc(key, schema, args);
    vdb_schema_free(schema);

    va_end(args);

    bool result = vdbtree_update_record(tree, rec);
    vdb_record_free(rec);

    return result;
}*/

void vdb_debug_print_tree(VDBHANDLE h, const char* name) {
    struct Vdb* db = (struct Vdb*)h;
    struct VdbTree* tree = vdb_treelist_get_tree(db->trees, name);

    vdbtree_print(tree);
}

struct VdbCursor* vdbcursor_init(VDBHANDLE h, const char* table_name, uint32_t row) {
    struct VdbCursor* cursor = malloc_w(sizeof(struct VdbCursor));
    cursor->db = (struct Vdb*)h;
    cursor->table_name = strdup_w(table_name);

    struct VdbTree* tree = vdb_treelist_get_tree(cursor->db->trees, table_name);
    uint32_t root_idx = vdbtree_meta_read_root(tree);
    cursor->cur_node_idx = vdb_tree_traverse_to(tree, root_idx, row);
    cursor->cur_rec_idx = 0;
    cursor->row_idx = row;

    return cursor;
}

void vdbcursor_free(struct VdbCursor* cursor) {
    free_w(cursor->table_name, sizeof(char) * (strlen(cursor->table_name) + 1)); //include null terminator
    free_w(cursor, sizeof(struct VdbCursor));
}

bool vdbcursor_on_final_record(struct VdbCursor* cursor) {
    struct VdbTree* tree = vdb_treelist_get_tree(cursor->db->trees, cursor->table_name);
    uint32_t highest_key = vdbtree_meta_read_primary_key_counter(tree);
    return cursor->row_idx >= highest_key;
}

void vdbcursor_increment(struct VdbCursor* cursor) {
    struct VdbTree* tree = vdb_treelist_get_tree(cursor->db->trees, cursor->table_name);

    if (cursor->cur_rec_idx + 1 >= vdbtree_leaf_read_record_count(tree, cursor->cur_node_idx)) {
        uint32_t root_idx = vdbtree_meta_read_root(tree);
        cursor->cur_node_idx = vdb_tree_traverse_to(tree, root_idx, cursor->row_idx + 1);
        cursor->cur_rec_idx = 0;
    } else {
        cursor->cur_rec_idx += 1;
    }
    cursor->row_idx++;
}

struct VdbRecord* vdbcursor_read_record(struct VdbCursor* cursor) {
    struct VdbTree* tree = vdb_treelist_get_tree(cursor->db->trees, cursor->table_name);
    struct VdbRecord* rec = vdbtree_leaf_read_record(tree, cursor->cur_node_idx, cursor->cur_rec_idx);

    return rec;
}

void vdbcursor_delete_record(struct VdbCursor* cursor) {
    struct VdbTree* tree = vdb_treelist_get_tree(cursor->db->trees, cursor->table_name);
    vdbtree_leaf_delete_record(tree, cursor->cur_node_idx, cursor->cur_rec_idx);
}

void vdbcursor_update_record(struct VdbCursor* cursor, struct VdbTokenList* attributes, struct VdbExprList* values) {
    struct VdbTree* tree = vdb_treelist_get_tree(cursor->db->trees, cursor->table_name);
    struct VdbValueList* vl = vdbvaluelist_init();

    for (int i = 0; i < values->count; i++) {
        struct VdbValue v = vdbexpr_eval(values->exprs[i], NULL, NULL);
        vdbvaluelist_append_value(vl, v);
    }

    vdbtree_leaf_update_record(tree, cursor->cur_node_idx, cursor->cur_rec_idx, attributes, vl);

    vdbvaluelist_free(vl);
}

bool vdbcursor_apply_selection(struct VdbCursor* cursor, struct VdbRecordSet* rs, struct VdbExpr* selection) {
    struct VdbTree* tree = vdb_treelist_get_tree(cursor->db->trees, cursor->table_name);
    struct VdbSchema* schema = vdbtree_meta_read_schema(tree);

    struct VdbValue v = vdbexpr_eval(selection, rs, schema);
    bool result;
    if (v.type == VDBT_TYPE_NULL) {
        result = false;
    } else if (v.type == VDBT_TYPE_BOOL) {
        result = v.as.Bool;
    } else {
        assert(false && "condition is not boolean expression");
        result = false;
    }

    vdb_schema_free(schema);
    return result;
}

struct VdbRecordSet* vdbcursor_apply_projection(struct VdbCursor* cursor, struct VdbRecordSet* head, struct VdbExprList* projection, bool aggregate) {
    struct VdbTree* tree = vdb_treelist_get_tree(cursor->db->trees, cursor->table_name);
    struct VdbSchema* schema = vdbtree_meta_read_schema(tree);

    struct VdbRecordSet* final = vdbrecordset_init(NULL);
    struct VdbRecordSet* cur = head;

    while (cur) {
        uint32_t max = aggregate ? 1 : cur->count;
        for (uint32_t i = 0; i < max; i++) {
            struct VdbRecord* rec = cur->records[i];
            struct VdbExpr* expr = projection->exprs[0];
            if (expr->type != VDBET_IDENTIFIER || expr->as.identifier.token.type != VDBT_STAR) { //Skip if projection is *
                struct VdbValue* data = malloc_w(sizeof(struct VdbValue) * projection->count);
                for (int i = 0; i < projection->count; i++) {
                    data[i] = vdbexpr_eval(projection->exprs[i], cur, schema);
                }

                free_w(rec->data, sizeof(struct VdbValue) * rec->count);
                rec->data = data;
                rec->count = projection->count;
            }
            vdbrecordset_append_record(final, rec);
        }

        cur = cur->next;
    }

    vdb_schema_free(schema);
    return final;
}

/*
struct VdbIntList* vdbcursor_attrs_to_idxs(struct VdbCursor* cursor, struct VdbExprList* attrs) {
    struct VdbTree* tree = vdb_treelist_get_tree(cursor->db->trees, cursor->table_name);
    struct VdbSchema* schema = vdbtree_meta_read_schema(tree);

    struct VdbIntList* il = vdbintlist_init();
    for (int i = 0; i < attrs->count; i++) {
        for (uint32_t j = 0; j < schema->count; j++) {
            if (strncmp(schema->names[j], attrs->exprs[i]->as.literal.token.lexeme, attrs->exprs[i]->as.literal.token.len) == 0) {
                vdbintlist_append_int(il, j);
            }
        }
    }

    return il;
}*/

struct VdbByteList* vdbcursor_key_from_cols(struct VdbCursor* cursor, struct VdbRecordSet* rs, struct VdbExprList* cols) {
    struct VdbTree* tree = vdb_treelist_get_tree(cursor->db->trees, cursor->table_name);
    struct VdbSchema* schema = vdbtree_meta_read_schema(tree);

    struct VdbByteList* bl = vdbbytelist_init();

    for (int i = 0; i < cols->count; i++) {
        struct VdbValue v = vdbexpr_eval(cols->exprs[i], rs, schema);
        vdbvalue_to_bytes(bl, v);
    }

    vdb_schema_free(schema);
    return bl;
}


