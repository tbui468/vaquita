#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "util.h"
#include "tree.h"

struct VdbTree* tree_init(const char* name, struct VdbSchema* schema) {
    struct VdbTree* tree = malloc_w(sizeof(struct VdbTree));
    tree->name = strdup(name);
    tree->pk_counter = 0;
    tree->node_idx_counter = 0;
    tree->schema = vdb_schema_copy(schema);
    tree->root = NULL;

    return tree;
}

void tree_free(struct VdbTree* tree) {
    free(tree->name);
    vdb_schema_free(tree->schema);
    free(tree);
}

struct VdbNode* _vdb_tree_traverse_to(struct VdbNode* node, uint32_t key) {
    assert(node->type == VDBN_INTERN);

    for (uint32_t i = 0; i < node->as.intern.nodes->count; i++) {
        struct VdbNode* n = node->as.intern.nodes->nodes[i];
        if (n->type == VDBN_LEAF) {
            return n;
        } else if (n->type == VDBN_INTERN) {
            if (key <= n->as.intern.right->idx) {
                return _vdb_tree_traverse_to(n, key);
            }
        }
    }

    if (node->as.intern.right->type == VDBN_LEAF) {
        return node->as.intern.right;
    }

    return _vdb_tree_traverse_to(node->as.intern.right, key);
}

void vdb_tree_insert_record(struct VdbTree* tree, struct VdbRecord* rec) {
    struct VdbNode* root = tree->root;

    struct VdbNode* leaf = _vdb_tree_traverse_to(root, rec->key);
    vdb_recordlist_append_record(leaf->as.leaf.records, rec);
}

struct VdbTreeList* treelist_init() {
    struct VdbTreeList* tl = malloc_w(sizeof(struct VdbTreeList));
    tl->count = 0;
    tl->capacity = 8;
    tl->trees = malloc_w(sizeof(struct VdbTree*) * tl->capacity);

    return tl;
}

void treelist_append(struct VdbTreeList* tl, struct VdbTree* tree) {
    if (tl->count == tl->capacity) {
        tl->capacity *= 2;
        tl->trees = realloc_w(tl->trees, sizeof(struct VdbTree*) * tl->capacity);
    }

    tl->trees[tl->count++] = tree;
}

struct VdbTree* treelist_get_tree(struct VdbTreeList* tl, const char* name) {
    struct VdbTree* t;
    uint32_t i;
    for (i = 0; i < tl->count; i++) {
        t = tl->trees[i];
        if (strncmp(name, t->name, strlen(name)) == 0)
            return t;
    }

    return NULL;
}

void treelist_remove(struct VdbTreeList* tl, const char* name) {
    struct VdbTree* t = NULL;
    uint32_t i;
    for (i = 0; i < tl->count; i++) {
        t = tl->trees[i];
        if (strncmp(name, t->name, strlen(name)) == 0)
            break;
    }

    if (t) {
        tl->trees[i] = tl->trees[--tl->count];
        tree_free(t);
    }
}

void treelist_free(struct VdbTreeList* tl) {
    for (uint32_t i = 0; i < tl->count; i++) {
        struct VdbTree* t = tl->trees[i];
        tree_free(t);
    }

    free(tl->trees);
    free(tl);
}
