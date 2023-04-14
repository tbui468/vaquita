#ifndef VDB_TREE_H
#define VDB_TREE_H

#include <stdint.h>
#include "data.h"

struct VdbTree {
    char* name;
    struct VdbSchema* schema;
    //root
};

struct VdbTreeList {
    struct VdbTree** trees;
    uint32_t count;
    uint32_t capacity;
};

struct VdbTree* tree_init(const char* name, struct VdbSchema* schema);
void tree_free(struct VdbTree* tree);

struct VdbTreeList* treelist_init();
void treelist_append(struct VdbTreeList* tl, struct VdbTree* tree);
void treelist_remove(struct VdbTreeList* tl, const char* name);
void treelist_free(struct VdbTreeList* tl);


#endif //VDB_TREE_H
