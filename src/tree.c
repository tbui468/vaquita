#include <stdlib.h>
#include <string.h>

#include "util.h"
#include "tree.h"
#include "data.h"

struct VdbTree* tree_init(const char* name, struct VdbSchema* schema) {
    struct VdbTree* tree = malloc_w(sizeof(struct VdbTree));
    tree->name = strdup(name);
    tree->schema = vdbdata_copy_schema(schema);

    return tree;
}

void tree_free(struct VdbTree* tree) {
    free(tree->name);
    vdbdata_free_schema(tree->schema);
    free(tree);
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
    //remove tree with given name from treelist
    //free that tree
}

void treelist_free(struct VdbTreeList* tl) {
    for (uint32_t i = 0; i < tl->count; i++) {
        struct VdbTree* t = tl->trees[i];
        tree_free(t);
    }

    free(tl->trees);
    free(tl);
}
