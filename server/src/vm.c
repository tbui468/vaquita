#include <string.h>
#include <assert.h>

#include "vm.h"
#include "util.h"
#include "lexer.h"
#include "binarytree.h"
#include "hashtable.h"
#include "cursor.h"

#define MAX_TAR_SIZE 256
#define MAX_BUF_SIZE 1024

struct VdbServer server;


enum VdbReturnCode vdbvm_drop_table(VDBHANDLE h, const char* name);

struct VdbDatabase* vdbdb_init(const char* name_without_ext) {
    struct VdbDatabase* db = malloc_w(sizeof(struct VdbDatabase));

    db->pager = server.pager;
    db->trees = vdb_treelist_init();
    db->name = strdup_w(name_without_ext);
    mtx_init(&db->lock, mtx_plain);

    return db;
}

void vdbdb_free(struct VdbDatabase* db) {
    mtx_destroy(&db->lock);
    vdb_treelist_free(db->trees);
    free_w(db->name, sizeof(char) * (strlen(db->name) + 1)); //include null terminator
    free_w(db, sizeof(struct VdbDatabase));
}

struct VdbDatabase* vdbvm_open_db(const char* dirname) {
    int len = strlen(dirname) - 4;
    char* buf = malloc_w(sizeof(char) * (len + 1));
    memcpy(buf, dirname, len);
    buf[len] = '\0';

    struct VdbDatabase* db = vdbdb_init(buf);

    free_w(buf, len + 1);

    //error if database doesn't exist
    DIR* d;
    if (!(d = opendir(dirname))) {
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

    return db;
}

void vdbserver_init() {
    server.pager = vdbpager_init();
    server.dbs = vdbdblist_init();
   
    //open current directory
    char dirname[FILENAME_MAX];
    dirname[0] = '\0';
    strcat(dirname, "./");

    DIR* d;
    if (!(d = opendir(dirname))) {
        printf("error opening database\n");
    }

    struct dirent* ent;
    while ((ent = readdir(d)) != NULL) {
        int entry_len = strlen(ent->d_name);
        const char* ext = ".vdb";
        int ext_len = strlen(ext);

        if (entry_len <= ext_len)
            continue;

        if (strncmp(ent->d_name + entry_len - ext_len, ext, ext_len) != 0)
            continue;

        //found entry with .vdb extension
        struct VdbDatabase* db = vdbvm_open_db(ent->d_name);
        vdbdblist_append_db(server.dbs, db);
    }

}

void vdbserver_free() {
    vdbdblist_free(server.dbs);
    vdbpager_free(server.pager);
}

struct VdbDatabaseList *vdbdblist_init() {
    struct VdbDatabaseList *l = malloc_w(sizeof(struct VdbDatabaseList));

    l->capacity = 8;
    l->count = 0;
    l->dbs = malloc_w(sizeof(struct VdbDatabase*) * l->capacity);

    return l;
}

void vdbdblist_free(struct VdbDatabaseList* l) {
    for (int i = 0; i < l->count; i++) {
        vdbdb_free(l->dbs[i]);
    }

    free_w(l->dbs, sizeof(struct VdbDatabase*) * l->capacity);
    free_w(l, sizeof(struct VdbDatabaseList));
}

void vdbdblist_append_db(struct VdbDatabaseList* l, struct VdbDatabase* d) {
    if (l->count + 1 > l->capacity) {
        int old_cap = l->capacity; 
        l->capacity *= 2;
        l->dbs = realloc_w(l->dbs, sizeof(struct VdbDatabase*) * l->capacity, sizeof(struct VdbDatabase*) * old_cap);
    }

    l->dbs[l->count++] = d;
}

struct VdbDatabase* vdbdblist_remove_db(struct VdbDatabaseList* l, const char* name) {
    for (int i = 0; i < l->count; i++) {
        struct VdbDatabase* db = l->dbs[i];
        if (!strncmp(name, db->name, strlen(name))) {
            l->dbs[i] = l->dbs[l->count-1];
            l->count--;
            return db;
        }
    }

    return NULL;
}

VDBHANDLE vdbvm_return_db(const char* name) {
    for (int i = 0; i < server.dbs->count; i++) {
        struct VdbDatabase* db = server.dbs->dbs[i];
        if (!strncmp(name, db->name, strlen(name))) {
            return (VDBHANDLE)db; 
        }
    }

    return NULL;
}

static void vdbvm_output_string(struct VdbByteList* bl, const char* buf, size_t size) {
    uint8_t is_tuple = 0;
    vdbbytelist_append_byte(bl, is_tuple);

    uint8_t type = VDBT_TYPE_STR;
    vdbbytelist_append_byte(bl, type);
    uint32_t len = size;
    vdbbytelist_append_bytes(bl, (uint8_t*)&len, sizeof(uint32_t));
    vdbbytelist_append_bytes(bl, (uint8_t*)buf, size);
}

//used for record insertion - autofills non-specified columns with null
static void vdbvm_assign_column_values(struct VdbValue* data, struct VdbSchema* schema, struct VdbTokenList* cols, struct VdbExprList* values) {
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
            data[i] = vdbstring("0", 1); //insert dummy string
            data[i].type = VDBT_TYPE_NULL;
        }

        if (!found) {
            data[i].type = VDBT_TYPE_NULL;

            //automacally insert null value for string
            if (schema->types[i] == VDBT_TYPE_STR) {
                data[i] = vdbstring("0", 1); //insert dummy string
                data[i].type = VDBT_TYPE_NULL;
            }
        }

    }
}

static bool vdbvm_table_exists(VDBHANDLE h, const char* tab_name) {
    struct VdbDatabase* db = (struct VdbDatabase*)(h);

    for (int i = 0; i < db->trees->count; i++) {
        struct VdbTree* tree = db->trees->trees[i];
        if (strncmp(tree->name, tab_name, strlen(tab_name)) == 0) {
            return true;
        }
    }

    return false;
}

static bool vdbvm_db_exists(const char* db_name) {
    DIR* d;
    if (!(d = opendir("./"))) {
        return true;
    } 

    bool found = false;
    struct dirent* ent;
    while ((ent = readdir(d)) != NULL) {
        int entry_len = strlen(ent->d_name);
        const char* ext = ".vdb";
        int ext_len = strlen(ext);

        if (entry_len <= ext_len)
            continue;

        if (strncmp(ent->d_name + entry_len - ext_len, ext, ext_len) != 0)
            continue;

        if (strncmp(ent->d_name, db_name, strlen(db_name)) == 0) {
            found = true;
            break;
        }
    }

    closedir_w(d);

    return found;
}


enum VdbReturnCode vdbvm_drop_db(const char* name) {

    struct VdbDatabase* db = vdbdblist_remove_db(server.dbs, name);
    for (int i = 0; i < db->trees->count; i++) {
        struct VdbTree* t = db->trees->trees[i];
        vdbvm_drop_table((VDBHANDLE)db, t->name);
    }
    vdbdb_free(db);

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

enum VdbReturnCode vdbvm_drop_table(VDBHANDLE h, const char* name) {
    struct VdbDatabase* db = (struct VdbDatabase*)h;
    struct VdbTree* tree = vdb_treelist_remove_tree(db->trees, name);

    if (tree) {
        char path[FILENAME_MAX];
        path[0] = '\0';
        strcat(path, db->name);
        strcat(path, ".vdb/");
        strcat(path, tree->name);
        strcat(path, ".vtb");
        
        vdbpager_evict_pages(db->pager, tree->f);
        vdb_tree_close(tree);

        remove_w(path);

        return VDBRC_SUCCESS;
    }

    return VDBRC_ERROR;
}

static void vdbvm_show_dbs_executor(struct VdbByteList* output) {
    DIR* d;
    if (!(d = opendir("./"))) {
        const char* msg = "execution error: cannot show databases";
        vdbvm_output_string(output, msg, strlen(msg));
        return;
    } 

    struct VdbRecordSet* final = vdbrecordset_init(NULL);

    struct VdbValue data = vdbstring("databases", strlen("databases")); 
    struct VdbRecord* r = vdbrecord_init(1, &data);
    vdbrecordset_append_record(final, r);

    struct dirent* ent;
    while ((ent = readdir(d)) != NULL) {
        int entry_len = strlen(ent->d_name);
        const char* ext = ".vdb";
        int ext_len = strlen(ext);

        if (entry_len <= ext_len)
            continue;

        if (strncmp(ent->d_name + entry_len - ext_len, ext, ext_len) != 0)
            continue;

        struct VdbValue data = vdbstring(ent->d_name, entry_len - ext_len);
        struct VdbRecord* r = vdbrecord_init(1, &data);
        vdbrecordset_append_record(final, r);
    }

    closedir_w(d);

    vdbrecordset_serialize(final, output);
    vdbrecordset_free(final);
}

static void vdbvm_show_tabs_executor(struct VdbByteList* output, VDBHANDLE* h) {
    struct VdbDatabase* db = (struct VdbDatabase*)(*h);
    struct VdbRecordSet* final = vdbrecordset_init(NULL);

    struct VdbValue data = vdbstring("tables", strlen("tables")); 
    struct VdbRecord* r = vdbrecord_init(1, &data);
    vdbrecordset_append_record(final, r);

    for (int i = 0; i < db->trees->count; i++) {
        struct VdbTree* tree = db->trees->trees[i];

        struct VdbValue data = vdbstring(tree->name, strlen(tree->name));
        struct VdbRecord* r = vdbrecord_init(1, &data);
        vdbrecordset_append_record(final, r);
    }


    vdbrecordset_serialize(final, output);
    vdbrecordset_free(final);

}

static void vdbvm_create_db_executor(struct VdbByteList* output, struct VdbToken target) {
    char db_name[MAX_TAR_SIZE];
    vdbtoken_serialize_lexeme(db_name, target);

    char buf[MAX_BUF_SIZE];

    char dirname[FILENAME_MAX];
    dirname[0] = '\0';
    strcat(dirname, db_name);
    strcat(dirname, ".vdb");

    DIR* d;
    if (!(d = opendir(dirname))) {
        mkdir_w(dirname, 0777);
        struct VdbDatabase* db = vdbvm_open_db(dirname);
        vdbdblist_append_db(server.dbs, db);
    } else {
        closedir_w(d);
        snprintf(buf, MAX_BUF_SIZE, "failed to create database %s", db_name);
        vdbvm_output_string(output, buf, strlen(buf));
        return;
    } 

    snprintf(buf, MAX_BUF_SIZE, "created database %s", db_name);
    vdbvm_output_string(output, buf, strlen(buf));
}

static void vdbvm_create_tab_executor(struct VdbByteList* output, 
                                      VDBHANDLE* h, 
                                      struct VdbToken target, 
                                      struct VdbTokenList* attrs, 
                                      struct VdbTokenList* types, 
                                      int key_idx) {
    struct VdbDatabase* db = (struct VdbDatabase*)(*h);

    char table_name[MAX_TAR_SIZE];
    vdbtoken_serialize_lexeme(table_name, target);
    char buf[MAX_BUF_SIZE];

    if (vdbvm_table_exists(*h, table_name)) {
        snprintf(buf, MAX_BUF_SIZE, "failed to create table %s", table_name);
        vdbvm_output_string(output, buf, strlen(buf));
        return;
    }

    char path[FILENAME_MAX];
    path[0] = '\0';
    strcat(path, db->name);
    strcat(path, ".vdb");
    strcat(path, "/");
    strcat(path, table_name);
    strcat(path, ".vtb");

    struct VdbSchema* schema = vdbschema_alloc(attrs->count, //TODO: this argument is unnecessary if attrs already contains count field
                                               attrs,
                                               types, 
                                               key_idx);

    FILE* f = fopen_w(path, "w+");
    struct VdbTree* tree = vdb_tree_init(table_name, schema, db->pager, f);
    vdb_treelist_append_tree(db->trees, tree);

    snprintf(buf, MAX_BUF_SIZE, "created table %s", table_name);
    vdbvm_output_string(output, buf, strlen(buf));
}

static void vdbvm_if_exists_drop_db_executor(struct VdbByteList* output, struct VdbToken target) {
    char db_name[MAX_TAR_SIZE];
    vdbtoken_serialize_lexeme(db_name, target);

    char buf[MAX_BUF_SIZE];

    if (vdbvm_db_exists(db_name)) {
        if (vdbvm_drop_db(db_name) == VDBRC_SUCCESS) {
            snprintf(buf, MAX_BUF_SIZE, "dropped database %s", db_name);
            vdbvm_output_string(output, buf, strlen(buf));
        } else {
            snprintf(buf, MAX_BUF_SIZE, "failed to drop database %s", db_name);
            vdbvm_output_string(output, buf, strlen(buf));
        }
    }
}

static void vdbvm_if_exists_drop_tab_executor(struct VdbByteList* output, VDBHANDLE* h, struct VdbToken target) {
    char table_name[MAX_TAR_SIZE];
    vdbtoken_serialize_lexeme(table_name, target);
    char buf[MAX_BUF_SIZE];

    if (vdbvm_table_exists(*h, table_name)) {
        if (vdbvm_drop_table(*h, table_name) == VDBRC_SUCCESS) {
            snprintf(buf, MAX_BUF_SIZE, "dropped table %s", table_name);
            vdbvm_output_string(output, buf, strlen(buf));
        } else {
            snprintf(buf, MAX_BUF_SIZE, "failed to drop table %s", table_name);
            vdbvm_output_string(output, buf, strlen(buf));
        }
    }
}

static void vdbvm_drop_db_executor(struct VdbByteList* output, struct VdbToken target) {
    char db_name[MAX_TAR_SIZE];
    vdbtoken_serialize_lexeme(db_name, target);
    char buf[MAX_BUF_SIZE];

    if (vdbvm_drop_db(db_name) == VDBRC_SUCCESS) {
        snprintf(buf, MAX_BUF_SIZE, "dropped database %s", db_name);
        vdbvm_output_string(output, buf, strlen(buf));
    } else {
        snprintf(buf, MAX_BUF_SIZE, "failed to drop database %s", db_name);
        vdbvm_output_string(output, buf, strlen(buf));
    }
}

static void vdbvm_drop_tab_executor(struct VdbByteList* output, VDBHANDLE* h, struct VdbToken target) {
    char table_name[MAX_TAR_SIZE];
    vdbtoken_serialize_lexeme(table_name, target);
    char buf[MAX_BUF_SIZE];

    if (vdbvm_drop_table(*h, table_name) == VDBRC_SUCCESS) {
        snprintf(buf, MAX_BUF_SIZE, "dropped table %s", table_name);
        vdbvm_output_string(output, buf, strlen(buf));
    } else {
        snprintf(buf, MAX_BUF_SIZE, "failed to drop table %s", table_name);
        vdbvm_output_string(output, buf, strlen(buf));
    }
}

static void vdbvm_return_db_executor(struct VdbByteList* output, VDBHANDLE* h, struct VdbToken target) {
    char db_name[MAX_TAR_SIZE];
    vdbtoken_serialize_lexeme(db_name, target);
    char buf[MAX_BUF_SIZE];

    if (vdbvm_db_exists(db_name)) {
        if ((*h = vdbvm_return_db(db_name))) {
            snprintf(buf, MAX_BUF_SIZE, "opened database %s", db_name);
            vdbvm_output_string(output, buf, strlen(buf));
        } else {
            snprintf(buf, MAX_BUF_SIZE, "failed to open database %s", db_name);
            vdbvm_output_string(output, buf, strlen(buf));
        }
    } else {
        snprintf(buf, MAX_BUF_SIZE, "database %s does not exist", db_name);
        vdbvm_output_string(output, buf, strlen(buf));
    }

}

static void vdbvm_close_db_executor(struct VdbByteList* output, VDBHANDLE* h, struct VdbToken target) {
    char db_name[MAX_TAR_SIZE];
    vdbtoken_serialize_lexeme(db_name, target);
    
    char buf[MAX_BUF_SIZE];
    snprintf(buf, MAX_BUF_SIZE, "closed database %s", db_name);
    vdbvm_output_string(output, buf, strlen(buf));

    *h = NULL;
}

static void vdbvm_describe_tab_executor(struct VdbByteList* output, VDBHANDLE* h, struct VdbToken target) {
    struct VdbDatabase *db = (struct VdbDatabase*)(*h);

    char table_name[MAX_TAR_SIZE];
    vdbtoken_serialize_lexeme(table_name, target);
    struct VdbTree* tree = vdb_treelist_get_tree(db->trees, table_name);

    struct VdbRecordSet* final = vdbrecordset_init(NULL);

    struct VdbValue data[3];
    data[0] = vdbstring("field", strlen("field")); 
    data[1] = vdbstring("type", strlen("type")); 
    data[2] = vdbstring("key", strlen("key")); 
    struct VdbRecord* r = vdbrecord_init(3, data);
    vdbrecordset_append_record(final, r);

    for (uint32_t i = 0; i < tree->schema->count; i++) {
        char* type;
        int len;
        switch (tree->schema->types[i]) {
            case VDBT_TYPE_INT:
                type = "int";
                len = 3;
                break;
            case VDBT_TYPE_STR:
                type = "string";
                len = 6;
                break;
            case VDBT_TYPE_FLOAT:
                type = "float";
                len = 5;
                break;
            case VDBT_TYPE_BOOL:
                type = "bool";
                len = 4;
                break;
            default:
                type = "int"; // to silence warning
                len = 0; // to silence warning
                assert(false && "invalid schema data type");
                break;
        }

        struct VdbValue data[3];
        data[0] = vdbstring(tree->schema->names[i], strlen(tree->schema->names[i])); 
        data[1] = vdbstring(type, len); 
        if (i == tree->schema->key_idx) {
            data[2] = vdbstring("pri", 3);
        } else {
            data[2] = vdbstring(" ", 1);
        }
        struct VdbRecord* r = vdbrecord_init(3, data);
        vdbrecordset_append_record(final, r);
    }


    vdbrecordset_serialize(final, output);
    vdbrecordset_free(final);
}

static void vdbvm_insert_executor(struct VdbByteList* output, 
                                  VDBHANDLE* h, 
                                  struct VdbToken target, 
                                  struct VdbTokenList* attrs, 
                                  struct VdbExprList* values) {
    struct VdbDatabase *db = (struct VdbDatabase*)(*h);
    char table_name[MAX_TAR_SIZE];
    vdbtoken_serialize_lexeme(table_name, target);
    int rec_count = values->count / attrs->count;

    struct VdbTree* tree = vdb_treelist_get_tree(db->trees, table_name);

    struct VdbExprList* el = vdbexprlist_init();

    for (int i = 0; i < rec_count; i++) {
        //make record
        int off = i * attrs->count;

        for (int j = off; j < off + attrs->count; j++) {
            vdbexprlist_append_expr(el, values->exprs[j]);
        }

        struct VdbValue data[tree->schema->count];
        vdbvm_assign_column_values(data, tree->schema, attrs, el);

        struct VdbRecord rec;
        rec.count = tree->schema->count;
        rec.data = data;
     
        struct VdbCursor* cursor = vdbcursor_init(tree);
        vdbcursor_seek(cursor, rec.data[tree->schema->key_idx]);
        vdbcursor_insert_record(cursor, &rec);
        vdbcursor_free(cursor);

        //free record
        for (uint32_t i = 0; i < rec.count; i++) {
            struct VdbValue* d = &rec.data[i];
            if (d->type == VDBT_TYPE_STR) {
                free_w(d->as.Str.start, sizeof(char) * d->as.Str.len);
            }
        }

        //reuse expression list
        el->count = 0;
    }

    //not calling vdbexprlist_free(el) since the expressions will be freed when stmt is freed
    free_w(el->exprs, sizeof(struct VdbExpr*) * el->capacity);
    free_w(el, sizeof(struct VdbExprList));

    char buf[MAX_BUF_SIZE];
    snprintf(buf, MAX_BUF_SIZE, "inserted %d record(s) into %s", rec_count, table_name);
    vdbvm_output_string(output, buf, strlen(buf));
}

static void vdbvm_update_executor(struct VdbByteList* output, 
                                  VDBHANDLE* h, 
                                  struct VdbToken target, 
                                  struct VdbTokenList* attrs, 
                                  struct VdbExprList* values, 
                                  struct VdbExpr* selection) {
    struct VdbDatabase *db = (struct VdbDatabase*)(*h);
//    mtx_lock(&db->lock);

    char table_name[MAX_TAR_SIZE];
    vdbtoken_serialize_lexeme(table_name, target);
    struct VdbTree* tree = vdb_treelist_get_tree(db->trees, table_name);
    struct VdbCursor* cursor = vdbcursor_init(tree);

    int updated_count = 0;

    while (!vdbcursor_at_end(cursor)) {
        if (vdbcursor_record_passes_selection(cursor, selection)) {
            vdbcursor_update_record(cursor, attrs, values);
            updated_count++;
        } else {
            struct VdbRecord* rec = vdbcursor_fetch_record(cursor);
            vdbrecord_free(rec);
        }
    }

    char buf[MAX_BUF_SIZE];
    snprintf(buf, MAX_BUF_SIZE, "%d row(s) updated", updated_count);
    vdbvm_output_string(output, buf, strlen(buf));
    vdbcursor_free(cursor);

//    mtx_unlock(&db->lock);
}

static void vdbvm_delete_executor(struct VdbByteList* output, VDBHANDLE* h, struct VdbToken target, struct VdbExpr* selection) {
    struct VdbDatabase *db = (struct VdbDatabase*)(*h);
    char table_name[MAX_TAR_SIZE];
    vdbtoken_serialize_lexeme(table_name, target);
    struct VdbTree* tree = vdb_treelist_get_tree(db->trees, table_name);
    struct VdbCursor* cursor = vdbcursor_init(tree);

    int deleted_count = 0;

    while (!vdbcursor_at_end(cursor)) {
        if (vdbcursor_record_passes_selection(cursor, selection)) {
            vdbcursor_delete_record(cursor);
            deleted_count++;
        } else {
            struct VdbRecord* rec = vdbcursor_fetch_record(cursor);
            vdbrecord_free(rec);
        }
    }

    char buf[MAX_BUF_SIZE];
    snprintf(buf, MAX_BUF_SIZE, "%d row(s) deleted", deleted_count);
    vdbvm_output_string(output, buf, strlen(buf));

    vdbcursor_free(cursor);
}

static void vdbvm_select_executor(struct VdbByteList* output,
                                  VDBHANDLE* h,
                                  struct VdbToken target,
                                  struct VdbExprList* projection,
                                  struct VdbExpr* selection,
                                  struct VdbExprList* grouping,
                                  struct VdbExprList* ordering,
                                  struct VdbExpr* having,
                                  bool order_desc,
                                  bool distinct,
                                  struct VdbExpr* limit) {

    struct VdbDatabase *db = (struct VdbDatabase*)(*h);
    char table_name[MAX_TAR_SIZE];
    vdbtoken_serialize_lexeme(table_name, target);
    struct VdbTree* tree = vdb_treelist_get_tree(db->trees, table_name);
    struct VdbCursor* cursor = vdbcursor_init(tree);

    struct VdbHashTable* distinct_table = vdbhashtable_init();
    struct VdbHashTable* grouping_table = vdbhashtable_init();

    struct VdbRecordSet* head = NULL;

    while (!vdbcursor_at_end(cursor)) {
        struct VdbRecord* rec = vdbcursor_fetch_record(cursor);
        struct VdbRecordSet* rs = vdbrecordset_init(NULL);
        vdbrecordset_append_record(rs, rec);

        //grouping (will apply 'having' clause later)
        if (grouping->count > 0) {
            //not applying selection if 'group by' is used
            //will apply 'having' clause later after aggregates can be applied to entire group
            struct VdbByteList* key = vdbcursor_key_from_cols(cursor, rs, grouping);
            //TODO: freeing recordset rs since it's not used, but kinda messy
            free_w(rs->records, sizeof(struct VdbRecord*) * rs->capacity);
            free_w(rs, sizeof(struct VdbRecordSet));
            vdbhashtable_insert_entry(grouping_table, key, rec);
            continue;
        }

        //does not pass selection criteria
        if (!vdbcursor_apply_selection(cursor, rs, selection)) {
            vdbrecordset_free(rs);
            continue;
        }

        //passes selection criteria
        if (distinct) {
            struct VdbByteList* key = vdbcursor_key_from_cols(cursor, rs, projection);
            if (!vdbhashtable_contains_key(distinct_table, key)) {
                vdbhashtable_insert_entry(distinct_table, key, rec); //using hashtable to check for duplicates bc of 'distinct' keyword

                rs->next = head;
                head = rs;
            }
        } else {
            rs->next = head;
            head = rs;
        }

    }

    //TODO: should not manually use hash table internals like this - error prone.  Should be done with hashtable interface
    if (grouping->count > 0) {
        for (int i = 0; i < VDB_MAX_BUCKETS; i++) {
            struct VdbRecordSet* cur = grouping_table->entries[i];
            while (cur) {
                //eval 'having' clause, and only insert into bt if true
                struct VdbRecordSet* cached_next = cur->next;
                if (vdbcursor_apply_having(cursor, cur, having)) {
                    cur->next = head;
                    head = cur;
                }
                cur = cached_next;
            }
        }
    }

    vdbcursor_sort_linked_list(cursor, &head, ordering, order_desc);

    //apply projections to each recordset in linked-list, and return final single recordset
    struct VdbRecordSet* final = vdbcursor_apply_projection(cursor, head, projection, grouping->count > 0);

    vdbcursor_apply_limit(cursor, final, limit);

    vdbrecordset_serialize(final, output);

    vdbcursor_free(cursor);
    vdbrecordset_free(final);
    vdbhashtable_free(distinct_table);
    vdbhashtable_free(grouping_table);
}

static void vdbvm_exit_executor(struct VdbByteList* output, VDBHANDLE* h) {
    if (*h) {
        struct VdbDatabase *db = (struct VdbDatabase*)(*h);
        char buf[MAX_BUF_SIZE];
        snprintf_w(buf, MAX_BUF_SIZE, "close database %s before exiting", db->name);
        vdbvm_output_string(output, buf, strlen(buf));
    }
}

bool vdbvm_do_execute_stmts(VDBHANDLE* h, struct VdbStmtList* sl, struct VdbByteList* output);
bool vdbvm_execute_stmts(VDBHANDLE* h, struct VdbStmtList* sl, struct VdbByteList* output) {
    /*
    struct VdbDatabase* db = (struct VdbDatabase*)(*h);

    bool locked = false;
    if (*h) {
        mtx_lock(&db->lock);
        locked = true;
    }*/

    bool result = vdbvm_do_execute_stmts(h, sl, output);

    /*
    if (locked) {
        mtx_unlock(&db->lock);
    }*/
    return result;
}

bool vdbvm_do_execute_stmts(VDBHANDLE* h, struct VdbStmtList* sl, struct VdbByteList* output) {
    for (int i = 0; i < sl->count; i++) {
        struct VdbStmt* stmt = &sl->stmts[i];

        switch (stmt->type) {
            case VDBST_SHOW_DBS:
                vdbvm_show_dbs_executor(output);
                break;
            case VDBST_SHOW_TABS:
                vdbvm_show_tabs_executor(output, h);
                break;
            case VDBST_CREATE_DB:
                vdbvm_create_db_executor(output, stmt->target);
                break;
            case VDBST_CREATE_TAB:
                vdbvm_create_tab_executor(output, h, 
                                          stmt->target, 
                                          stmt->as.create.attributes, 
                                          stmt->as.create.types, 
                                          stmt->as.create.key_idx);
                break;
            case VDBST_IF_EXISTS_DROP_DB: 
                vdbvm_if_exists_drop_db_executor(output, stmt->target);
                break;
            case VDBST_IF_EXISTS_DROP_TAB:
                vdbvm_if_exists_drop_tab_executor(output, h, stmt->target);
                break;
            case VDBST_DROP_DB:
                vdbvm_drop_db_executor(output, stmt->target);
                break;
            case VDBST_DROP_TAB:
                vdbvm_drop_tab_executor(output, h, stmt->target);
                break;
            case VDBST_OPEN:
                vdbvm_return_db_executor(output, h, stmt->target);
                break;
            case VDBST_CLOSE:
                vdbvm_close_db_executor(output, h, stmt->target);
                break;
            case VDBST_DESCRIBE:
                vdbvm_describe_tab_executor(output, h, stmt->target);
                break;
            case VDBST_INSERT:
                vdbvm_insert_executor(output, h, stmt->target, 
                                         stmt->as.insert.attributes, 
                                         stmt->as.insert.values);
                break;
            case VDBST_UPDATE:
                vdbvm_update_executor(output, h, stmt->target, 
                                         stmt->as.update.attributes, 
                                         stmt->as.update.values, 
                                         stmt->as.update.selection);
                break;
            case VDBST_DELETE:
                vdbvm_delete_executor(output, h, stmt->target, stmt->as.delete.selection);
                break;
            case VDBST_SELECT:
                vdbvm_select_executor(output, h, stmt->target, 
                                         stmt->as.select.projection, 
                                         stmt->as.select.selection, 
                                         stmt->as.select.grouping,
                                         stmt->as.select.ordering,
                                         stmt->as.select.having,
                                         stmt->as.select.order_desc,
                                         stmt->as.select.distinct,
                                         stmt->as.select.limit);

                break;
            case VDBST_EXIT:
                vdbvm_exit_executor(output, h);
                return true;
            default:
                printf("statement execution not implemented\n");
                break;
        }
    }

    return false;
}
