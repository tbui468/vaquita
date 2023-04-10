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

void vdb_free_record(struct VdbRecord* data) {
    for (uint32_t i = 0; i < data->count; i++) {
        struct VdbDatum d = data->data[i];
        if (d.type == VDBF_STR) {
            free(d.as.Str);
        }
    }

    free(data->data);
    free(data);
}

struct VdbRecord vdb_deserialize_record(uint8_t* buf, struct VdbSchema* schema) {
    struct VdbRecord r;
    r.count = schema->count;
    r.data = malloc_w(sizeof(struct VdbDatum) * schema->count);
    int off = 0;
    read_u32(&r.key, buf, &off);

    for (uint32_t i = 0; i < r.count; i++) {
        enum VdbField f = schema->fields[i];
        switch (f) {
            case VDBF_INT:
                r.data[i].type = VDBF_INT;
                r.data[i].as.Int = *((uint64_t*)(buf + off));
                off += sizeof(uint64_t);
                break;
            case VDBF_STR:
                r.data[i].type = VDBF_STR;
                r.data[i].as.Str = malloc_w(sizeof(struct VdbString));
                read_u32(&r.data[i].as.Str->len, buf, &off);
                r.data[i].as.Str->start = malloc_w(sizeof(char) * r.data[i].as.Str->len);
                memcpy(r.data[i].as.Str->start, buf + off, r.data[i].as.Str->len);
                off += r.data[i].as.Str->len;
                break;
            case VDBF_BOOL:
                r.data[i].type = VDBF_BOOL;
                r.data[i].as.Bool = *((bool*)(buf + off));
                off += sizeof(bool);
                break;
        }
    }

    return r;
}

struct VdbRecordSize vdb_rec_size(struct VdbRecord* rec) {
    struct VdbRecordSize rs;
    rs.fixed = sizeof(uint32_t); //key size
    rs.variable = 0;

    for (uint32_t i = 0; i < rec->count; i++) {
        struct VdbDatum* d = &rec->data[i];
        switch (d->type) {
            case VDBF_INT:
                rs.fixed += sizeof(uint64_t);
                break;
            case VDBF_STR:
                rs.fixed += sizeof(uint32_t); //data block offset
                rs.variable += sizeof(uint32_t) * 2; //next ptr and length
                rs.variable += d->as.Str->len; 
                break;
            case VDBF_BOOL:
                rs.fixed += sizeof(bool);
                break;
        }
    }

    return rs;
}


struct VdbRecord* vdb_copy_record(struct VdbRecord* rec) {
    struct VdbRecord* r = malloc_w(sizeof(struct VdbRecord));
    r->count = rec->count;
    r->key = rec->key;
    r->data = malloc_w(sizeof(struct VdbDatum) * r->count);

    for (uint32_t i = 0; i < r->count; i++) {
        struct VdbDatum* d = &rec->data[i];
        switch (d->type) {
            case VDBF_INT:
                r->data[i].type = VDBF_INT;
                r->data[i].as.Int = d->as.Int;
                break;
            case VDBF_STR:
                r->data[i].type = VDBF_STR;
                r->data[i].as.Str = malloc_w(sizeof(struct VdbString));
                r->data[i].as.Str->len = d->as.Str->len;
                r->data[i].as.Str->start = malloc_w(sizeof(char) * r->data[i].as.Str->len);
                memcpy(r->data[i].as.Str->start, d->as.Str->start, r->data[i].as.Str->len);
                break;
            case VDBF_BOOL:
                r->data[i].type = VDBF_BOOL;
                r->data[i].as.Bool = d->as.Bool;
                break;
        }
    }

    return r;
}

struct VdbString* vdb_deserialize_string(uint8_t* buf) {
    struct VdbString* s = malloc_w(sizeof(struct VdbString));

    //first 4 bytes is next pointer
    memcpy(&s->len, buf + sizeof(uint32_t), sizeof(uint32_t));
    memcpy(&s->start, buf + sizeof(uint32_t) * 2, s->len);

    return s; 
}

int vdb_create_table(VDBHANDLE h, const char* table, struct VdbSchema* schema) {
    struct DB* db = (struct DB*)h;

    char filename[FILENAME_MAX];
    _vdb_tablename_to_pathname(db, table, filename);

    FILE* f = fopen_w(filename, "w+");

    struct VdbTree tree = {db->pager, f};

    tree_init(&tree, schema);
    fclose_w(f);

    _vdb_load_table(db, filename);
    return 0;
}

int vdb_drop_table(VDBHANDLE h, const char* table) {
    struct DB* db = (struct DB*)h;

    char filename[FILENAME_MAX];
    _vdb_tablename_to_pathname(db, table, filename);

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

int vdb_insert_record(VDBHANDLE h, const char* table, struct VdbRecord* d) {
    struct DB* db = (struct DB*)h;
    //TODO: lock file here
    struct VdbTree tree =  {db->pager, _vdb_get_table_file(db, table)};
    tree_insert_record(&tree, d);
    //TODO: unlock file here
    return 0;
}


void vdb_debug_print_tree(VDBHANDLE h, const char* table) {
    struct DB* db = (struct DB*)h;
    struct VdbTree t =  {db->pager, _vdb_get_table_file(db, table)};
    debug_print_tree(&t);
}

struct VdbRecord* vdb_fetch_record(VDBHANDLE h, const char* table, uint32_t key) {
    struct DB* db = (struct DB*)h;
    //TODO: lock file here
    struct VdbTree t =  {db->pager, _vdb_get_table_file(db, table)};
    struct VdbRecord* result = tree_fetch_record(&t, key);
    //TODO: unlock file here
    return result;
}

bool vdb_schema_includes_var_len(struct VdbSchema* schema) {
    for (uint32_t i = 0; i < schema->count; i++) {
        if (schema->fields[i] == VDBF_STR)
            return true;
    }
    return false;
}
