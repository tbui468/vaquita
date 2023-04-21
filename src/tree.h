#ifndef VDB_TREE_H
#define VDB_TREE_H

#include <stdint.h>
#include <stdbool.h>
#include "schema.h"
#include "node.h"
#include "record.h"
#include "pager.h"

struct VdbChunk {
    struct VdbNode* node;
    struct VdbPage* page;
};

struct VdbChunkList {
    struct VdbChunk* chunks;
    uint32_t count;
    uint32_t capacity;
};

struct VdbTree {
    char* name;
    FILE* f;
    struct VdbPager* pager;
    uint32_t meta_idx;
    struct VdbChunkList* chunks;
};

struct VdbTreeList {
    struct VdbTree** trees;
    uint32_t count;
    uint32_t capacity;
};

struct VdbChunkList* vdb_chunklist_init();
void vdb_chunklist_append_chunk(struct VdbChunkList* cl, struct VdbChunk chunk);
void vdb_chunklist_free(struct VdbChunkList* cl);

struct VdbTree* vdb_tree_init(const char* name, struct VdbSchema* schema, struct VdbPager* pager, FILE* f);
struct VdbTree* vdb_tree_catch(FILE* f, struct VdbPager* pager);
void vdb_tree_release(struct VdbTree* tree);

void vdb_tree_insert_record(struct VdbTree* tree, struct VdbRecord* rec);
struct VdbRecord* vdb_tree_fetch_record(struct VdbTree* tree, uint32_t key);
bool vdb_tree_update_record(struct VdbTree* tree, struct VdbRecord* rec);
bool vdb_tree_delete_record(struct VdbTree* tree, uint32_t key);

struct VdbTreeList* treelist_init();
void treelist_append(struct VdbTreeList* tl, struct VdbTree* tree);
struct VdbTree* treelist_get_tree(struct VdbTreeList* tl, const char* name);
void treelist_remove(struct VdbTreeList* tl, const char* name);
void treelist_free(struct VdbTreeList* tl);


#endif //VDB_TREE_H
