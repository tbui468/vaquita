#include <stdio.h>
#include <time.h>
#include <string.h>
#include <stdint.h>
#include <stdlib.h>

#include "vdb.h"
#include "util.h"

#define SECONDS_OFF 0
#define COUNTER_OFF sizeof(uint32_t)
#define FREELIST_OFF sizeof(uint32_t) * 2
#define ENTRYLIST_OFF sizeof(uint32_t) * 3

struct TreeNode {
    uint32_t left; //use this field for freelist
    uint32_t right;
    uint32_t pk;
    uint32_t data_len;
    struct VdbData data;
};

struct DB {
    char* name;
    FILE* root;
    FILE* tables[128];
    uint32_t table_count;
};

void _vdb_init_db(const char* filename) {
    FILE* f = fopen_w(filename, "w");
    write_lock(f, SEEK_SET, 0, 0);

    uint32_t seconds = time(NULL);
    fwrite_w(&seconds, sizeof(uint32_t), 1, f);
    uint32_t counter = 0;
    fwrite_w(&counter, sizeof(uint32_t), 1, f);
    uint32_t freelist = 0;
    fwrite_w(&freelist, sizeof(uint32_t), 1, f);
    uint32_t entrylist = 0;
    fwrite_w(&entrylist, sizeof(uint32_t), 1, f);

    unlock(f, SEEK_SET, 0, 0);
    fclose_w(f);
}

VDBHANDLE vdb_open(const char* name) {
    char filename[FILENAME_MAX];
    memcpy(filename, name, strlen(name));
    memcpy(filename + strlen(name), ".vdb\0", 5);

    FILE* f;
    if (!(f = fopen(filename, "r+"))) { //not using wrapper bc failing is allowed
        _vdb_init_db(filename);
        f = fopen_w(filename, "r+");
    }

    setbuf(f, NULL);

    struct DB* db = malloc_w(sizeof(struct DB));
    db->name = malloc_w(strlen(name) + 1);
    memcpy(db->name, name, strlen(name));
    db->name[strlen(name)] = '\0';
    db->root = f;
    db->table_count = 0;

    fseek_w(db->root, ENTRYLIST_OFF, SEEK_SET);
    uint32_t cur;
    fread_w(&cur, sizeof(uint32_t), 1, db->root);
    while (cur) {
        fseek_w(db->root, cur, SEEK_SET);
        fread_w(&cur, sizeof(uint32_t), 1, db->root);
        uint32_t len;
        fread_w(&len, sizeof(uint32_t), 1, db->root);
        fread_w(filename, sizeof(char), len, db->root);
        filename[len] = '\0';
        db->tables[db->table_count] = fopen_w(filename, "r+");
        setbuf(db->tables[db->table_count], NULL);
        db->table_count++;
    }

    return (VDBHANDLE)db;
}

void vdb_close(VDBHANDLE h) {
    struct DB* db = (struct DB*)h;
    free(db->name);
    fclose_w(db->root);
    for (uint32_t i = 0; i < db->table_count; i++) {
        fclose_w(db->tables[i]);
    }
}

void _vdb_init_table(FILE* f, struct VdbSchema schema) {
    fseek_w(f, 0, SEEK_SET);
    uint32_t seconds = time(NULL);
    fwrite_w(&seconds, sizeof(uint32_t), 1, f);
    uint32_t counter = 0;
    fwrite_w(&counter, sizeof(uint32_t), 1, f);
    uint32_t fc = schema.count;
    fwrite_w(&fc, sizeof(uint32_t), 1, f);
    for (uint32_t i = 0; i < schema.count; i++) {
        uint32_t field = schema.fields[i];
        fwrite_w(&field, sizeof(uint32_t), 1, f);
    }
    uint32_t pk_count = 0;
    fwrite_w(&pk_count, sizeof(uint32_t), 1, f);
    uint32_t freelist = 0;
    fwrite_w(&freelist, sizeof(uint32_t), 1, f);
    uint32_t root_off = 0;
    fwrite_w(&root_off, sizeof(int32_t), 1, f);
}

void _vdb_remove_tab_entry(VDBHANDLE h, uint32_t prev_off, uint32_t entry_off) {
    struct DB* db = (struct DB*)h;

    uint32_t next;
    fseek_w(db->root, entry_off, SEEK_SET);
    fread(&next, sizeof(uint32_t), 1, db->root);
    fseek_w(db->root, prev_off, SEEK_SET);
    fwrite(&next, sizeof(uint32_t), 1, db->root);
}

void _vdb_insert_tab_entry(VDBHANDLE h, uint32_t entry_off, uint32_t head) {
    struct DB* db = (struct DB*)h;

    fseek_w(db->root, head, SEEK_SET);
    uint32_t new_next;
    fread_w(&new_next, sizeof(uint32_t), 1, db->root);
    fseek_w(db->root, entry_off, SEEK_SET);
    fwrite_w(&new_next, sizeof(uint32_t), 1, db->root);

    fseek_w(db->root, head, SEEK_SET);
    fwrite(&entry_off, sizeof(int32_t), 1, db->root);
}

int vdb_create_table(VDBHANDLE h, const char* table_name, struct VdbSchema schema) {
    struct DB* db = (struct DB*)h;

    char filename[FILENAME_MAX];
    memcpy(filename, table_name, strlen(table_name));
    memcpy(filename + strlen(table_name), ".vtb\0", 5);

    FILE* f;
    if ((f = fopen(filename, "r+"))) {
        fclose_w(f);
        return -1; //table already exists!!!
    }

    uint32_t idx = db->table_count;
    db->tables[db->table_count++] = fopen_w(filename, "w+");
    write_lock(db->tables[idx], SEEK_SET, 0, 0);

    _vdb_init_table(db->tables[idx], schema);

    unlock(db->tables[idx], SEEK_SET, 0, 0);

    
    //update root file
    fseek_w(db->root, FREELIST_OFF, SEEK_SET);
    uint32_t cur;
    fread_w(&cur, sizeof(uint32_t), 1, db->root);
    uint32_t prev = FREELIST_OFF;
    while (cur) {
        fseek_w(db->root, cur + sizeof(uint32_t), SEEK_SET);
        uint32_t len;
        fread_w(&len, sizeof(uint32_t), 1, db->root);
        if (len >= strlen(filename)) {
            _vdb_remove_tab_entry(h, prev, cur);
            break;
        }
        prev = cur;
        fseek_w(db->root, cur, SEEK_SET);
        fread_w(&cur, sizeof(uint32_t), 1, db->root);
    }

    if (!cur) {
        fseek_w(db->root, 0, SEEK_END);
        cur = ftell_w(db->root);
    }

    //insert this record at beginning of chain
    uint32_t filelen = strlen(filename);
    fseek_w(db->root, cur + sizeof(uint32_t), SEEK_SET);
    fwrite_w(&filelen, sizeof(int32_t), 1, db->root);
    fwrite_w(filename, sizeof(char), filelen, db->root);

    _vdb_insert_tab_entry(h, cur, ENTRYLIST_OFF);

    return 0;
}

int vdb_drop_table(VDBHANDLE h, const char* table_name) {
    struct DB* db = (struct DB*)h;

    char filename[FILENAME_MAX];
    memcpy(filename, table_name, strlen(table_name));
    memcpy(filename + strlen(table_name), ".vtb\0", 5);

    char currentname[FILENAME_MAX];
    int found = -1;
    for (uint32_t i = 0; i < db->table_count; i++) {
        get_filename(db->tables[i], currentname, FILENAME_MAX);
        if (strcmp(filename, currentname) == 0) {
            fclose_w(db->tables[i]);
            db->tables[i] = db->tables[db->table_count - 1];
            db->table_count--;
            found = 0;
            break;
        } 
    }

    remove_w(filename);

    //remove table from root file
    fseek_w(db->root, ENTRYLIST_OFF, SEEK_SET);
    uint32_t cur;
    fread_w(&cur, sizeof(uint32_t), 1, db->root);
    uint32_t prev = ENTRYLIST_OFF;
    while (cur) {
        fseek_w(db->root, cur + sizeof(uint32_t), SEEK_SET); //skip next field for now
        uint32_t len;
        fread_w(&len, sizeof(uint32_t), 1, db->root);
        char curfilename[len + 1];
        fread_w(curfilename, sizeof(char), len, db->root);
        curfilename[len] = '\0';
        if (strcmp(filename, curfilename) == 0) {
            _vdb_remove_tab_entry(h, prev, cur);
            _vdb_insert_tab_entry(h, cur, FREELIST_OFF);
            break;
        }
        prev = cur;
        fseek_w(db->root, cur, SEEK_SET);
        fread_w(&cur, sizeof(uint32_t), 1, db->root);
    }
    

    return found;
}

bool _vdb_valid_schema(FILE* f, struct VdbData data) {
    fseek_w(f, sizeof(uint32_t) * 2, SEEK_SET);
    uint32_t field_count;
    fread_w(&field_count, sizeof(uint32_t), 1, f);
    if (data.count != field_count)
        return false;

    for (uint32_t i = 0; i < field_count; i++) {
        uint32_t schema_type;
        fread_w(&schema_type, sizeof(uint32_t), 1, f);
        uint32_t data_type = data.data[i].type;
        if (data_type != schema_type)
            return false;
    }

    return true;
}

uint32_t vdb_data_len(struct VdbData data) {
    uint32_t len = 0;
    for (uint32_t i = 0; i < data.count; i++) {
        struct VdbDatum d = data.data[i];
        switch (d.type) {
        case VDBF_INT:
            len += sizeof(uint64_t);
            break;
        case VDBF_STR:
            len += d.as.Str->len;
            break;
        case VDBF_BOOL:
            len += sizeof(bool);
            break;
        default:
            printf("Unrecognized type\n");
            break;
        }
    }
    return len;
}


uint32_t _vdb_read_pk_count(FILE* f) {
    fseek_w(f, sizeof(uint32_t) * 2, SEEK_SET);
    uint32_t field_count;
    fread(&field_count, sizeof(uint32_t), 1, f);
    fseek_w(f, sizeof(uint32_t) * field_count, SEEK_CUR);
    uint32_t pk_count;
    fread(&pk_count, sizeof(uint32_t), 1, f);
    return pk_count;
}

void _vdb_write_pk_count(FILE* f, uint32_t pk_count) {
    fseek_w(f, sizeof(uint32_t) * 2, SEEK_SET);
    uint32_t field_count;
    fread(&field_count, sizeof(uint32_t), 1, f);
    fseek_w(f, sizeof(uint32_t) * field_count, SEEK_CUR);
    fwrite(&pk_count, sizeof(uint32_t), 1, f);
}

uint32_t _vdb_get_free_entry(FILE* f, uint32_t len) {
    fseek_w(f, sizeof(uint32_t) * 2, SEEK_SET);
    uint32_t field_count;
    fread(&field_count, sizeof(uint32_t), 1, f);
    fseek_w(f, sizeof(uint32_t) * (field_count + 1), SEEK_CUR);
    uint32_t cur;
    fread(&cur, sizeof(uint32_t), 1, f);
    uint32_t prev = sizeof(uint32_t) * (3 + field_count); //freelist offset

    while (cur) {
        fseek_w(f, cur + sizeof(uint32_t) * 2, SEEK_SET);
        uint32_t data_len;
        fread_w(&data_len, sizeof(uint32_t), 1, f);
        if (len <= data_len) {
            //remove from freelist
            fseek_w(f, cur, SEEK_SET);
            uint32_t next;
            fread_w(&next, sizeof(uint32_t), 1, f);
            fseek_w(f, prev, SEEK_SET);
            fwrite_w(&next, sizeof(uint32_t), 1, f);
            break;
        }

        prev = cur;
        fseek_w(f, cur, SEEK_SET);
        fread_w(&cur, sizeof(uint32_t), 1, f);
    }

    if (!cur) {
        fseek_w(f, 0, SEEK_END);
        cur = ftell_w(f);
    }

    return cur;
}

void _vdb_write_node(FILE* f, uint32_t off, struct TreeNode n) {
    fseek_w(f, off, SEEK_SET);
    fwrite_w(&n.left, sizeof(uint32_t), 1, f);
    fwrite_w(&n.right, sizeof(uint32_t), 1, f);
    fwrite_w(&n.pk, sizeof(uint32_t), 1, f);
    fwrite_w(&n.data_len, sizeof(uint32_t), 1, f);
    for (uint32_t i = 0; i < n.data.count; i++) {
        struct VdbDatum d = n.data.data[i];
        switch (d.type) {
        case VDBF_INT:
            fwrite_w(&d.as.Int, sizeof(uint64_t), 1, f);
            break;
        case VDBF_STR:
            fwrite_w(&d.as.Str->len, sizeof(uint32_t), 1, f);
            fwrite_w(d.as.Str->start, sizeof(char), d.as.Str->len, f);
            break;
        case VDBF_BOOL:
            fwrite_w(&d.as.Bool, sizeof(bool), 1, f);
            break;
        }
    }
}


void _vdb_insert_node(FILE* f, uint32_t off, struct TreeNode n) {
    fseek_w(f, sizeof(uint32_t) * 2, SEEK_SET);
    uint32_t field_count;
    fread(&field_count, sizeof(uint32_t), 1, f);

    fseek_w(f, sizeof(uint32_t) * (field_count + 2), SEEK_CUR); //root offset
    uint32_t cur;
    fread_w(&cur, sizeof(uint32_t), 1, f);

    if (!cur) { //n will be first node, so write to root offset
        fseek_w(f, sizeof(uint32_t) * (field_count + 5), SEEK_SET); //root offset
        fwrite_w(&off, sizeof(uint32_t), 1, f);
    } else {
        while (true) {
            fseek_w(f, cur, SEEK_SET);
            uint32_t cur_left;
            fread_w(&cur_left, sizeof(uint32_t), 1, f);
            uint32_t cur_right;
            fread_w(&cur_right, sizeof(uint32_t), 1, f);
            uint32_t cur_pk;
            fread_w(&cur_pk, sizeof(uint32_t), 1, f);
            if (n.pk < cur_pk) {
                if (cur_left) {
                    cur = cur_left;
                    continue;
                }
                fseek_w(f, cur, SEEK_SET);
                fwrite_w(&off, sizeof(uint32_t), 1, f);
            } else {
                if (cur_right) {
                    cur = cur_right;
                    continue;
                }
                fseek_w(f, cur + sizeof(uint32_t), SEEK_SET);
                fwrite_w(&off, sizeof(uint32_t), 1, f);
            }

            break;
        }
    }

}

/*
void _vdb_rebalance_tree(FILE* f) {
    //TODO
    //rebalance the tree - rotation 
}*/

int _vdb_get_table_idx(VDBHANDLE h, const char* table) {
    struct DB* db = (struct DB*)h;

    int tabidx = -1;
    char filename[FILENAME_MAX];
    for (uint32_t i = 0; i < db->table_count; i++) {
        get_filename(db->tables[i], filename, FILENAME_MAX);
        filename[strlen(filename) - 4] = '\0';
        if (strcmp(table, filename) == 0) {
            tabidx = i;
            break;
        }
    }

    return tabidx;
}


int vdb_insert_record(VDBHANDLE h, const char* table, struct VdbData data) {
    struct DB* db = (struct DB*)h;

    int tabidx = _vdb_get_table_idx(h, table);

    if (tabidx == -1)
        return tabidx;

    if (!_vdb_valid_schema(db->tables[tabidx], data))
        return -1;

    uint32_t len = vdb_data_len(data);
    uint32_t entry_off = _vdb_get_free_entry(db->tables[tabidx], len);

    uint32_t pk_count = _vdb_read_pk_count(db->tables[tabidx]);
    _vdb_write_pk_count(db->tables[tabidx], pk_count + 1);

    struct TreeNode n = { 0, 0, pk_count, len, data };
    _vdb_insert_node(db->tables[tabidx], entry_off, n);
    _vdb_write_node(db->tables[tabidx], entry_off, n);

    //_vdb_rebalance_tree(db->tables[tabidx]); //TODO: need to rotate to prevent unbalanced tree

    return 0;
}

struct VdbData* vdb_fetch_record(VDBHANDLE h, const char* table, uint32_t key) {
    struct DB* db = (struct DB*)h;

    int tabidx = _vdb_get_table_idx(h, table);

    fseek_w(db->tables[tabidx], sizeof(uint32_t) * 2, SEEK_SET);
    uint32_t field_count;
    fread_w(&field_count, sizeof(uint32_t), 1, db->tables[tabidx]);
    enum VdbField fields[field_count];
    for (uint32_t i = 0; i < field_count; i++) {
        fread_w(fields + i, sizeof(uint32_t), 1, db->tables[tabidx]);
    }

    fseek_w(db->tables[tabidx], sizeof(uint32_t) * 2, SEEK_CUR);
    uint32_t cur;
    fread_w(&cur, sizeof(uint32_t), 1, db->tables[tabidx]);

    while (cur) {
        fseek_w(db->tables[tabidx], cur + sizeof(uint32_t) * 2, SEEK_SET);
        uint32_t pk;
        fread_w(&pk, sizeof(uint32_t), 1, db->tables[tabidx]);

        if (key < pk) {
            fseek_w(db->tables[tabidx], cur, SEEK_SET);
            fread_w(&cur, sizeof(uint32_t), 1, db->tables[tabidx]);
            continue;
        } else if (key > pk) {
            fseek_w(db->tables[tabidx], cur + sizeof(uint32_t), SEEK_SET);
            fread_w(&cur, sizeof(uint32_t), 1, db->tables[tabidx]);
            continue;
        } else {
            fseek_w(db->tables[tabidx], cur + sizeof(uint32_t) * 4, SEEK_SET);
            struct VdbData* data = malloc(sizeof(struct VdbData));
            data->count = field_count;
            data->data = malloc(sizeof(struct VdbDatum) * field_count);

            for (uint32_t i = 0; i < field_count; i++) {
                enum VdbField f = fields[i];
                switch (f) {
                case VDBF_INT:
                    data->data[i].type = VDBF_INT;
                    fread_w(&data->data[i].as.Int, sizeof(uint64_t), 1, db->tables[tabidx]);
                    break;
                case VDBF_STR:
                    data->data[i].type = VDBF_STR;
                    struct VdbString* s = malloc_w(sizeof(struct VdbString));
                    fread_w(&s->len, sizeof(uint32_t), 1, db->tables[tabidx]);
                    s->start = malloc_w(sizeof(char) * s->len);
                    fread_w(s->start, sizeof(char), s->len, db->tables[tabidx]);
                    data->data[i].as.Str = s;
                    break;
                case VDBF_BOOL:
                    data->data[i].type = VDBF_BOOL;
                    fread_w(&data->data[i].as.Bool, sizeof(bool), 1, db->tables[tabidx]);
                    break;
                default:
                    break;
                }
            }

            return data;
        }
    }

    return NULL;
}
