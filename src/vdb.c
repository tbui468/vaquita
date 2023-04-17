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
    struct VdbSchema* schema = vdb_schema_alloc(count, args);
    va_end(args);
    return schema;
}

void vdb_free_schema(struct VdbSchema* schema) {
    vdb_schema_free(schema);
}

void vdb_insert_record(VDBHANDLE h, const char* name, ...) {
    struct Vdb* db = (struct Vdb*)h;

    struct VdbTree* tree = treelist_get_tree(db->trees, name);

    va_list args;
    va_start(args, name);
    struct VdbRecord* rec = vdb_record_alloc(++tree->pk_counter, tree->schema, args);
    va_end(args);

    vdb_tree_insert_record(tree, rec);
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

void _vdb_debug_print_node(struct VdbNode* node, uint32_t depth) {
    char spaces[depth * 2 + 1];
    memset(spaces, ' ', sizeof(spaces) - 1);
    spaces[depth * 2] = '\0';
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

    struct VdbTree* tree = treelist_get_tree(db->trees, name);
    _vdb_debug_print_node(tree->root, 0);
}


