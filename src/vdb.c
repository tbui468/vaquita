#include <stdio.h>
#include <time.h>
#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>

#include "vdb.h"
#include "tree.h"
#include "util.h"

bool _vdb_has_ext(const char* name, const char* ext) {
    if (strlen(name) <= strlen(ext))
        return false;

    return strcmp(name + strlen(name) - strlen(ext), ext) == 0;
}

void _vdb_load_table(struct DB* db, const char* pathname) {
    int idx = db->table_count;
    int len = strlen(pathname);

    db->table_files[idx] = fopen_w(pathname, "r+");
    db->table_names[idx] = malloc_w(sizeof(char) * (len + 1));
    memcpy(db->table_names[idx], pathname, len);
    db->table_names[idx][len] = '\0';
    db->table_count++;
}

void _vdb_tablename_to_pathname(struct DB* db, const char* table_name, char* pathname) {
    char dir[FILENAME_MAX];
    memcpy(dir, db->name, strlen(db->name));
    dir[strlen(db->name)] = '\0';
    strcat(dir, ".vdb");

    char tab[FILENAME_MAX];
    memcpy(tab, table_name, strlen(table_name));
    tab[strlen(table_name)] = '\0';
    strcat(tab, ".vtb");

    join_path(pathname, ".", dir, tab, NULL);
}

FILE* _vdb_get_table_file(struct DB* db, const char* table_name) {
    char filename[FILENAME_MAX];
    _vdb_tablename_to_pathname(db, table_name, filename);

    for (uint32_t i = 0; i < db->table_count; i++) {
        if (strcmp(filename, db->table_names[i]) == 0) {
            return db->table_files[i];
        }
    }

    return NULL;
}

VDBHANDLE vdb_create(const char* name) {
    char filename[FILENAME_MAX];
    filename[0] = '\0';
    strcat(filename, name);
    strcat(filename, ".vdb");
    mkdir_w(filename, 0777);

    return vdb_open(name);
}

VDBHANDLE vdb_open(const char* name) {
    char filename[FILENAME_MAX];
    filename[0] ='\0';
    strcat(filename, name);
    strcat(filename, ".vdb");

    struct DB* db = malloc_w(sizeof(struct DB));
    db->pager = pager_init();
    db->name = malloc_w(strlen(name) + 1);
    memcpy(db->name, name, strlen(name));
    db->name[strlen(name)] = '\0';
    db->table_count = 0;

    DIR* d;
    struct dirent* dir;
    d = opendir_w(filename);
    while ((dir = readdir(d)) != NULL) { //not using readdir_w since NULL is acceptable result
        if (_vdb_has_ext(dir->d_name, ".vtb")) {
            _vdb_load_table(db, dir->d_name);
        }
    }
    closedir_w(d);

    return (VDBHANDLE)db;
}

void vdb_close(VDBHANDLE h) {
    struct DB* db = (struct DB*)h;
    free(db->name);
    for (uint32_t i = 0; i < db->table_count; i++) {
        fclose_w(db->table_files[i]);
        free(db->table_names[i]);
    }
    free(db->pager);
    free(db);
}


int vdb_create_table(VDBHANDLE h, const char* table_name, struct VdbSchema* schema) {
    struct DB* db = (struct DB*)h;

    char filename[FILENAME_MAX];
    _vdb_tablename_to_pathname(db, table_name, filename);

    FILE* f = fopen_w(filename, "w+");
    tree_init(f, schema);
    fclose_w(f);

    _vdb_load_table(db, filename);
    return 0;
}


int vdb_drop_table(VDBHANDLE h, const char* table_name) {
    struct DB* db = (struct DB*)h;

    char filename[FILENAME_MAX];
    _vdb_tablename_to_pathname(db, table_name, filename);

    int idx = -1;
    for (uint32_t i = 0; i < db->table_count; i++) {
        if (strcmp(filename, db->table_names[i]) == 0) {
            idx = i;
            break;
        }
    }

    if (idx == -1)
        return -1;

    free(db->table_names[idx]);
    fclose_w(db->table_files[idx]);
    db->table_count--;
    db->table_names[idx] = db->table_names[db->table_count];
    db->table_files[idx] = db->table_files[db->table_count];

    remove_w(filename);

    return 0;
}

int vdb_insert_record(VDBHANDLE h, const char* table_name, struct VdbData* d) {
    struct DB* db = (struct DB*)h;
    //TODO: lock file here
    FILE* f = _vdb_get_table_file(db, table_name);
    tree_insert_record(db->pager, f, d);
    //TODO: unlock file here
    return 0;
}


struct VdbData* vdb_fetch_record(VDBHANDLE h, const char* table, uint32_t key) {
    struct DB* db = (struct DB*)h;
    //TODO: lock file here
    FILE* f = _vdb_get_table_file(db, table);
    struct VdbData* result = tree_fetch_record(db->pager, f, key);
    //TODO: unlock file here
    return result;
}

void vdb_free_data(struct VdbData* data) {
    for (uint32_t i = 0; i < data->count; i++) {
        struct VdbDatum d = data->data[i];
        if (d.type == VDBF_STR) {
            free(d.as.Str);
        }
    }

    free(data->data);
    free(data);
}

void vdb_debug_print_tree(VDBHANDLE h, const char* table) {
    struct DB* db = (struct DB*)h;
    FILE* f = _vdb_get_table_file(db, table);
    debug_print_tree(db->pager, f, 1, 0);
}

void vdb_debug_print_keys(VDBHANDLE h, const char* table, uint32_t block_idx) {
    struct DB* db = (struct DB*)h;
    FILE* f = _vdb_get_table_file(db, table);
    debug_print_keys(db->pager, f, block_idx);
}
