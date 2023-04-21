#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

#include "vdb.h"
#include "schema.h"
#include "record.h"
#include "util.h"
#include "tree.h"


struct VdbFileList* vdb_filelist_alloc() {
    struct VdbFileList* fl = malloc_w(sizeof(struct VdbFileList));
    fl->count = 0;
    fl->capacity = 8; 
    fl->files = malloc_w(sizeof(struct VdbFile*) * fl->capacity);
    return fl;
}

void vdb_filelist_append_file(struct VdbFileList* fl, struct VdbFile* f) {
    if (fl->count == fl->capacity) {
        fl->capacity *= 2;
        fl->files = realloc_w(fl->files, sizeof(struct VdbFile*) * fl->capacity);
    }

    fl->files[fl->count++] = f;
}

struct VdbFile* vdb_filelist_get_file(struct VdbFileList* fl, const char* name) {
    for (uint32_t i = 0; i < fl->count; i++) {
        struct VdbFile* f = fl->files[i];
        if (!strncmp(f->name, name, strlen(f->name))) {
            return f;
        }
    }

    return NULL;
}

struct VdbFile* vdb_filelist_remove_file(struct VdbFileList* fl, const char* name) {
    for (uint32_t i = 0; i < fl->count; i++) {
        struct VdbFile* f = fl->files[i];
        if (!strncmp(f->name, name, strlen(f->name))) {
            fl->files[i] = fl->files[fl->count - 1];
            fl->count--;
            return f;
        }
    }

    return NULL;
}

void vdb_filelist_free(struct VdbFileList* fl) {
    for (uint32_t i = 0; i < fl->count; i++) {
        struct VdbFile* f = fl->files[i];
        fclose_w(f->f);
        free(f->name);
        free(f);
    }

    free(fl->files);
    free(fl);
}

VDBHANDLE vdb_open(const char* name) {
    struct Vdb* db = malloc_w(sizeof(struct Vdb));
    db->pager = vdb_pager_alloc();
    db->files = vdb_filelist_alloc();
    db->name = strdup_w(name);

    char dirname[FILENAME_MAX];
    dirname[0] = '\0';
    strcat(dirname, name);
    strcat(dirname, ".vdb");

    //open directory with all table files
    DIR* d;
    if (!(d = opendir(dirname))) {
        mkdir_w(dirname, 0777);
        d = opendir_w(dirname);
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

        struct VdbFile* file = malloc_w(sizeof(struct VdbFile));
        file->f = f;
        file->name = strdup_w(ent->d_name);
        file->name[strlen(ent->d_name)] = '\0';
        vdb_filelist_append_file(db->files, file);
    }

    closedir_w(d);
    
    return (VDBHANDLE)db;
}

void vdb_close(VDBHANDLE h) {
    struct Vdb* db = (struct Vdb*)h;
    free(db->name);
    vdb_pager_free(db->pager);
    vdb_filelist_free(db->files);
    free(db);
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

void vdb_create_table(VDBHANDLE h, const char* name, struct VdbSchema* schema) {
    struct Vdb* db = (struct Vdb*)h;

    //TODO: return false if table with name already exists
    char path[FILENAME_MAX];
    path[0] = '\0';
    strcat(path, db->name);
    strcat(path, ".vdb/");
    strcat(path, name);
    strcat(path, ".vtb");

    struct VdbFile* file = malloc_w(sizeof(struct VdbFile));
    file->f = fopen_w(path, "w+");
    file->name = strdup_w(name);
    vdb_filelist_append_file(db->files, file);

    struct VdbTree* tree = vdb_tree_init(name, schema, db->pager, file->f);
    vdb_tree_release(tree);
}

bool vdb_drop_table(VDBHANDLE h, const char* name) {
    struct Vdb* db = (struct Vdb*)h;
    struct VdbFile* file = vdb_filelist_remove_file(db->files, name);

    if (file) {
        char path[FILENAME_MAX];
        path[0] = '\0';
        strcat(path, db->name);
        strcat(path, ".vdb/");
        strcat(path, file->name);
        strcat(path, ".vtb");

        remove_w(path);

        fclose_w(file->f);
        free(file->name);
        free(file);

        return true;
    }

    return false;
}

void vdb_insert_record(VDBHANDLE h, const char* name, ...) {
    /*
    struct Vdb* db = (struct Vdb*)h;
    struct VdbFile* file = vdb_filelist_get_file(db->files, name);
    struct VdbTree* tree = vdb_tree_catch(file->f, db->pager);

    va_list args;
    va_start(args, name);
    struct VdbRecord* rec = vdb_record_alloc(++tree->pk_counter, tree->schema, args);
    va_end(args);

    vdb_tree_insert_record(tree, rec);
    vdb_tree_release(tree);*/
}

struct VdbRecord* vdb_fetch_record(VDBHANDLE h, const char* name, uint32_t key) {
    /*
    struct Vdb* db = (struct Vdb*)h;
    struct VdbFile* file = vdb_filelist_get_file(db->files, name);
    struct VdbTree* tree = vdb_tree_catch(file->f, db->pager);

    struct VdbRecord* rec = vdb_tree_fetch_record(tree, key);

    if (rec)
        rec = vdb_record_copy(rec);

    return rec;*/
    return NULL;
}

bool vdb_update_record(VDBHANDLE h, const char* name, uint32_t key, ...) {
    /*
    struct Vdb* db = (struct Vdb*)h;
    struct VdbFile* file = vdb_filelist_get_file(db->files, name);
    struct VdbTree* tree = vdb_tree_catch(file->f, db->pager);

    va_list args;
    va_start(args, key);
    struct VdbRecord* rec = vdb_record_alloc(key, tree->schema, args);
    va_end(args);

    return vdb_tree_update_record(tree, rec);*/
    return true;
}

bool vdb_delete_record(VDBHANDLE h, const char* name, uint32_t key) {
    /*
    struct Vdb* db = (struct Vdb*)h;
    struct VdbFile* file = vdb_filelist_get_file(db->files, name);
    struct VdbTree* tree = vdb_tree_catch(file->f, db->pager);

    return vdb_tree_delete_record(tree, key);*/
    return true;
}

void _vdb_debug_print_node(struct VdbNode* node, uint32_t depth) {
    /*
    char spaces[depth * 4 + 1];
    memset(spaces, ' ', sizeof(spaces) - 1);
    spaces[depth * 4] = '\0';
    printf("%s%d", spaces, node->idx);

    if (node->type == VDBN_INTERN) {
        printf("\n");
        for (uint32_t i = 0; i < node->as.intern.nodes->count; i++) {
            struct VdbNode* n = node->as.intern.nodes->nodes[i];
            _vdb_debug_print_node(n, depth + 1);
        }

        _vdb_debug_print_node(node->as.intern.right, depth + 1);
    } else {
        printf(": ");
        for (uint32_t i = 0; i < node->as.leaf.records->count; i++) {
            struct VdbRecord* r = node->as.leaf.records->records[i];
            printf("%d", r->key);
            if (i < node->as.leaf.records->count - 1) {
                printf(", ");
            }
        }
        printf("\n");
    }*/
}

void vdb_debug_print_tree(VDBHANDLE h, const char* name) {
    /*
    struct Vdb* db = (struct Vdb*)h;
    struct VdbFile* file = vdb_filelist_get_file(db->files, name);
    struct VdbTree* tree = vdb_tree_catch(file->f, db->pager);
    _vdb_debug_print_node(tree->root, 0);
    */
}


