#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

#include "vdb.h"
#include "schema.h"
#include "record.h"
#include "util.h"
#include "tree.h"


VDBHANDLE vdb_open(const char* name) {
    struct Vdb* db = malloc_w(sizeof(struct Vdb));
    db->pager = vdb_pager_alloc();
    db->trees = vdb_treelist_init();
    db->name = strdup_w(name);

    char dirname[FILENAME_MAX];
    dirname[0] = '\0';
    strcat(dirname, name);

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

        char* s = malloc_w(sizeof(char) * (entry_len - 3));
        memcpy(s, ent->d_name, entry_len - 4);
        s[entry_len - 3] = '\0';
        struct VdbTree* tree = vdb_tree_catch(s, f, db->pager);
        vdb_treelist_append_tree(db->trees, tree);
    }

    closedir_w(d);
    
    return (VDBHANDLE)db;
}

bool vdb_close(VDBHANDLE h) {
    struct Vdb* db = (struct Vdb*)h;
    vdb_treelist_free(db->trees);
    vdb_pager_free(db->pager);
    free(db->name);
    free(db);

    return true;
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
    strcat(path, "/");
    strcat(path, name);
    strcat(path, ".vtb");

    FILE* f = fopen_w(path, "w+");
    struct VdbTree* tree = vdb_tree_init(name, schema, db->pager, f);
    vdb_treelist_append_tree(db->trees, tree);

    return true;
}

bool vdb_drop_table(VDBHANDLE h, const char* name) {
    struct Vdb* db = (struct Vdb*)h;
    struct VdbTree* tree = vdb_treelist_remove_tree(db->trees, name);

    if (tree) {
        char path[FILENAME_MAX];
        path[0] = '\0';
        strcat(path, db->name);
        strcat(path, "/");
        strcat(path, tree->name);
        strcat(path, ".vtb");

        fclose_w(tree->f);
        vdb_tree_release(tree);
        vdb_pager_evict_pages(db->pager, name);

        remove_w(path);

        return true;
    }

    return false;
}

void vdb_insert_record(VDBHANDLE h, const char* name, ...) {
    struct Vdb* db = (struct Vdb*)h;
    struct VdbTree* tree = vdb_treelist_get_tree(db->trees, name);

    if (tree) {
        printf("found tree\n");
    } else {
        printf("no tree found\n");
    }

    printf("a\n");
    struct VdbChunk meta = vdb_tree_catch_chunk(tree, tree->meta_idx);
    printf("b\n");
    struct VdbChunk root = vdb_tree_catch_chunk(tree, meta.node->as.meta.root_idx);
    printf("c\n");
    struct VdbChunk leaf = vdb_tree_catch_chunk(tree, root.node->as.intern.right_idx);
    vdb_tree_release_chunk(tree, meta); //TODO: why does this invalidate all of root chunk?
    printf("d\n");
    vdb_tree_release_chunk(tree, root);
    printf("e\n");
    vdb_tree_release_chunk(tree, leaf);

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

/*
struct VdbRecord* vdb_fetch_record(VDBHANDLE h, const char* name, uint32_t key) {
    struct Vdb* db = (struct Vdb*)h;
    struct VdbFile* file = vdb_filelist_get_file(db->files, name);
    struct VdbTree* tree = vdb_tree_catch(file->f, db->pager);

    struct VdbRecord* rec = vdb_tree_fetch_record(tree, key);

    if (rec)
        rec = vdb_record_copy(rec);

    return rec;
    return NULL;
}

bool vdb_update_record(VDBHANDLE h, const char* name, uint32_t key, ...) {
    struct Vdb* db = (struct Vdb*)h;
    struct VdbFile* file = vdb_filelist_get_file(db->files, name);
    struct VdbTree* tree = vdb_tree_catch(file->f, db->pager);

    va_list args;
    va_start(args, key);
    struct VdbRecord* rec = vdb_record_alloc(key, tree->schema, args);
    va_end(args);

    return vdb_tree_update_record(tree, rec);
    return true;
}

bool vdb_delete_record(VDBHANDLE h, const char* name, uint32_t key) {
    struct Vdb* db = (struct Vdb*)h;
    struct VdbFile* file = vdb_filelist_get_file(db->files, name);
    struct VdbTree* tree = vdb_tree_catch(file->f, db->pager);

    return vdb_tree_delete_record(tree, key);
    return true;
}

void _vdb_debug_print_node(struct VdbNode* node, uint32_t depth) {
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
    }
}

void vdb_debug_print_tree(VDBHANDLE h, const char* name) {
    struct Vdb* db = (struct Vdb*)h;
    struct VdbFile* file = vdb_filelist_get_file(db->files, name);
    struct VdbTree* tree = vdb_tree_catch(file->f, db->pager);
    _vdb_debug_print_node(tree->root, 0);
}

*/
