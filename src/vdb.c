#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

#include "vdb.h"
#include "data.h"
#include "util.h"
#include "tree.h"


VDBHANDLE vdb_open(const char* name) {
    struct Vdb* db = malloc_w(sizeof(struct Vdb));
    db->name = strdup(name);
    db->trees = treelist_init();
    
    return (VDBHANDLE)db;
}

void vdb_close(VDBHANDLE h) {
    struct Vdb* db = (struct Vdb*)h;
    free(db->name);
    treelist_free(db->trees);
    free(db);
}

struct VdbSchema* vdb_alloc_schema(int count, ...) {
    va_list args;
    va_start(args, count);
    struct VdbSchema* schema = vdbdata_alloc_schema(count, args);
    va_end(args);
    return schema;
}

void vdb_free_schema(struct VdbSchema* schema) {
    vdbdata_free_schema(schema);
}

void vdb_insert_record(VDBHANDLE h, const char* name, ...) {
    struct Vdb* db = (struct Vdb*)h;
    //find correct tree
    //get schema from that tree

    va_list args;
    va_start(args, name);
    struct VdbRecord* rec = vdbdata_alloc_record(schema, args);
    va_end(args);

    //insert record into tree    
}

void vdb_create_table(VDBHANDLE h, const char* name, struct VdbSchema* schema) {
    struct Vdb* db = (struct Vdb*)h;

    struct VdbTree* tree = tree_init(name, schema);
    treelist_append(db->trees, tree);
}

void vdb_drop_table(VDBHANDLE h, const char* name) {
    struct Vdb* db = (struct Vdb*)h;

    treelist_remove(db->trees, name);
}
