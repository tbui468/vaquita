#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

#include "vdb.h"
#include "schema.h"
#include "record.h"
#include "util.h"
#include "tree.h"


VDBHANDLE vdb_open_db(const char* name) {
    struct Vdb* db = malloc_w(sizeof(struct Vdb));
    db->pager = vdb_pager_alloc();
    db->trees = vdb_treelist_init();
    db->name = strdup_w(name);

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
        struct VdbTree* tree = vdb_tree_catch(s, f, db->pager);
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

enum VdbReturnCode vdb_show_dbs(char*** dbs, int* count) {
    DIR* d;
    if (!(d = opendir("./"))) {
        return VDBRC_ERROR;
    } 

    int capacity = 8;
    *count = 0;
    *dbs = malloc_w(sizeof(char*) * capacity); 


    struct dirent* ent;
    while ((ent = readdir(d)) != NULL) {
        int entry_len = strlen(ent->d_name);
        const char* ext = ".vdb";
        int ext_len = strlen(ext);

        if (entry_len <= ext_len)
            continue;

        if (strncmp(ent->d_name + entry_len - ext_len, ext, ext_len) != 0)
            continue;

        if (*count + 1 > capacity) {
            capacity *= 2;
            *dbs = realloc_w(*dbs, sizeof(char*) * capacity);
        }

        (*dbs)[*count] = malloc_w(sizeof(char) * (entry_len - ext_len + 1));
        memcpy((*dbs)[*count], ent->d_name, entry_len - ext_len);
        (*dbs)[*count][entry_len - ext_len] = '\0';
        (*count)++;
    }

    closedir_w(d);

    return VDBRC_SUCCESS;
}


enum VdbReturnCode vdb_show_tabs(VDBHANDLE h, char*** tabs, int* count) {
    struct Vdb* db = (struct Vdb*)h;
    *count = db->trees->count;
    *tabs = malloc_w(sizeof(char*) * (*count)); 
    for (uint32_t i = 0; i < db->trees->count; i++) {
        struct VdbTree* tree = db->trees->trees[i];
        (*tabs)[i] = strdup_w(tree->name);
    }

    return VDBRC_SUCCESS;
}

/*
struct VdbSchema {
    enum VdbField* fields;
    char** names;
    uint32_t count;
};*/

enum VdbReturnCode vdb_describe_table(VDBHANDLE h, const char* name, char*** attributes, char*** types, int* count) {
    struct Vdb* db = (struct Vdb*)h;
    struct VdbTree* tree = vdb_treelist_get_tree(db->trees, name);

    struct VdbSchema* schema = vdbtree_meta_read_schema(tree);

    *count = schema->count;
    *attributes = malloc_w(sizeof(char*) * (*count));
    *types = malloc_w(sizeof(char*) * (*count));

    for (int i = 0; i < *count; i++) {
        (*attributes)[i] = strdup_w(schema->names[i]);
        switch (schema->types[i]) {
            case VDBT_TYPE_INT:
                (*types)[i] = malloc_w(sizeof(char) * 4);
                memcpy((*types)[i], "int", 3);
                (*types)[i][3] = '\0';
                break;
            case VDBT_TYPE_STR:
                (*types)[i] = malloc_w(sizeof(char) * 7);
                memcpy((*types)[i], "string", 6);
                (*types)[i][6] = '\0';
                break;
            case VDBT_TYPE_BOOL:
                (*types)[i] = malloc_w(sizeof(char) * 5);
                memcpy((*types)[i], "bool", 4);
                (*types)[i][4] = '\0';
                break;
            default:
                break;
        }
    }

    vdb_schema_free(schema);

    return VDBRC_SUCCESS;
}

bool vdb_close(VDBHANDLE h) {
    struct Vdb* db = (struct Vdb*)h;
    vdb_treelist_free(db->trees);
    vdb_pager_free(db->pager);
    free(db->name);
    free(db);

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

        fclose_w(tree->f);
        vdb_tree_release(tree);
        vdb_pager_evict_pages(db->pager, name);

        remove_w(path);

        return VDBRC_SUCCESS;
    }

    return VDBRC_ERROR;
}

enum VdbReturnCode vdb_insert_new(VDBHANDLE h, const char* name, struct VdbTokenList* attrs, struct VdbTokenList* values) {
    struct Vdb* db = (struct Vdb*)h;
    struct VdbTree* tree = vdb_treelist_get_tree(db->trees, name);

    struct VdbSchema* schema = vdbtree_meta_read_schema(tree); printf("read schema\n");
    uint32_t key = vdbtree_meta_increment_primary_key_counter(tree);
    struct VdbRecord* rec = vdbrecord_alloc(key, schema, attrs, values);printf("alloced record\n");
    vdb_schema_free(schema);

    vdb_tree_insert_record(tree, rec); printf("inserted record\n");
    vdb_record_free(rec);

    return VDBRC_SUCCESS;
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

bool vdb_delete_record(VDBHANDLE h, const char* name, uint32_t key) {
    struct Vdb* db = (struct Vdb*)h;
    struct VdbTree* tree = vdb_treelist_get_tree(db->trees, name);

    return vdbtree_delete_record(tree, key);
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
}

void vdb_debug_print_tree(VDBHANDLE h, const char* name) {
    struct Vdb* db = (struct Vdb*)h;
    struct VdbTree* tree = vdb_treelist_get_tree(db->trees, name);

    vdbtree_print(tree);
}

