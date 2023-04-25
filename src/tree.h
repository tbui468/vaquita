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

struct VdbPage* vdb_tree_pin_page(struct VdbTree* tree, uint32_t idx);
void vdb_tree_unpin_page(struct VdbTree* tree, struct VdbPage* page);
struct VdbChunk vdb_tree_catch_chunk(struct VdbTree* tree, uint32_t idx); //TODO: replace with one that returns page
void vdb_tree_release_chunk(struct VdbTree* tree, struct VdbChunk chunk); //TODO: replace with one that returns page

struct VdbTree* vdb_tree_init(const char* name, struct VdbSchema* schema, struct VdbPager* pager, FILE* f);
struct VdbTree* vdb_tree_catch(const char* name, FILE* f, struct VdbPager* pager);
void vdb_tree_release(struct VdbTree* tree);


struct VdbPtr vdbtree_intern_read_right_ptr(struct VdbTree* tree, uint32_t idx);
struct VdbPtr vdbtree_intern_read_ptr(struct VdbTree* tree, uint32_t idx, uint32_t ptr_idx);
uint32_t vdbtree_intern_read_ptr_count(struct VdbTree* tree, uint32_t idx);
enum VdbNodeType vdbtree_node_type(struct VdbTree* tree, uint32_t idx);
struct VdbSchema* vdbtree_meta_read_schema(struct VdbTree* tree);
uint32_t vdbtree_meta_increment_primary_key_counter(struct VdbTree* tree);
uint32_t vdbtree_leaf_read_record_key(struct VdbTree* tree, uint32_t leaf_idx, uint32_t rec_idx);

void vdb_tree_insert_record(struct VdbTree* tree, struct VdbRecord* rec);
/*
struct VdbRecord* vdb_tree_fetch_record(struct VdbTree* tree, uint32_t key);
bool vdb_tree_update_record(struct VdbTree* tree, struct VdbRecord* rec);
bool vdb_tree_delete_record(struct VdbTree* tree, uint32_t key);*/

struct VdbTreeList* vdb_treelist_init();
void vdb_treelist_append_tree(struct VdbTreeList* tl, struct VdbTree* tree);
struct VdbTree* vdb_treelist_get_tree(struct VdbTreeList* tl, const char* name);
struct VdbTree* vdb_treelist_remove_tree(struct VdbTreeList* tl, const char* name);
void vdb_treelist_free(struct VdbTreeList* tl);


#endif //VDB_TREE_H
