#ifndef VDB_TREE_H
#define VDB_TREE_H

#include <stdint.h>
#include <stdbool.h>
#include "schema.h"
#include "node.h"
#include "record.h"

struct VdbTree {
    char* name;
    struct VdbSchema* schema;
    uint32_t pk_counter;
    uint32_t node_idx_counter;
    struct VdbNode* root;
};

struct VdbTreeList {
    struct VdbTree** trees;
    uint32_t count;
    uint32_t capacity;
};

struct VdbTree* tree_init(const char* name, struct VdbSchema* schema);
void tree_free(struct VdbTree* tree);
void vdb_tree_insert_record(struct VdbTree* tree, struct VdbRecord* rec);
struct VdbRecord* vdb_tree_fetch_record(struct VdbTree* tree, uint32_t key);
bool vdb_tree_update_record(struct VdbTree* tree, struct VdbRecord* rec);

struct VdbTreeList* treelist_init();
void treelist_append(struct VdbTreeList* tl, struct VdbTree* tree);
struct VdbTree* treelist_get_tree(struct VdbTreeList* tl, const char* name);
void treelist_remove(struct VdbTreeList* tl, const char* name);
void treelist_free(struct VdbTreeList* tl);


#endif //VDB_TREE_H
