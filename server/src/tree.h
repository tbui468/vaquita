#ifndef VDB_TREE_H
#define VDB_TREE_H

#include <stdint.h>
#include <stdbool.h>
#include "schema.h"
#include "node.h"
#include "record.h"
#include "pager.h"

struct VdbTree {
    char* name;
    FILE* f;
    struct VdbPager* pager;
    uint32_t meta_idx;
};

struct VdbTreeList {
    struct VdbTree** trees;
    uint32_t count;
    uint32_t capacity;
};

struct VdbPage* vdb_tree_pin_page(struct VdbTree* tree, uint32_t idx);
void vdb_tree_unpin_page(struct VdbTree* tree, struct VdbPage* page);

struct VdbTree* vdb_tree_init(const char* name, struct VdbSchema* schema, struct VdbPager* pager, FILE* f);
struct VdbTree* vdb_tree_open(const char* name, FILE* f, struct VdbPager* pager);
void vdb_tree_close(struct VdbTree* tree);

struct VdbSchema* vdbtree_meta_read_schema(struct VdbTree* tree);
uint32_t vdbtree_meta_increment_primary_key_counter(struct VdbTree* tree);

void vdb_tree_insert_record(struct VdbTree* tree, struct VdbRecord* rec);

struct VdbTreeList* vdb_treelist_init();
void vdb_treelist_append_tree(struct VdbTreeList* tl, struct VdbTree* tree);
struct VdbTree* vdb_treelist_get_tree(struct VdbTreeList* tl, const char* name);
struct VdbTree* vdb_treelist_remove_tree(struct VdbTreeList* tl, const char* name);
void vdb_treelist_free(struct VdbTreeList* tl);

uint32_t vdbtree_leaf_read_record_count(struct VdbTree* tree, uint32_t idx);
struct VdbRecord* vdbtree_leaf_read_record(struct VdbTree* tree, uint32_t idx, uint32_t rec_idx);
void vdbtree_leaf_delete_record(struct VdbTree* tree, uint32_t idx, uint32_t rec_idx);
void vdbtree_leaf_update_record(struct VdbTree* tree, uint32_t idx, uint32_t rec_idx, struct VdbTokenList* attrs, struct VdbValueList* vl);
uint32_t vdbtree_meta_read_primary_key_counter(struct VdbTree* tree);
struct VdbValue vdbtree_leaf_read_record_key(struct VdbTree* tree, uint32_t leaf_idx, uint32_t rec_idx);

uint32_t vdb_tree_traverse_to(struct VdbTree* tree, uint32_t idx, struct VdbValue key);
uint32_t vdbtree_traverse_to_first_leaf(struct VdbTree* tree, uint32_t idx);
uint32_t vdbtree_meta_read_root(struct VdbTree* tree);
uint32_t vdbtree_leaf_read_next_leaf(struct VdbTree* tree, uint32_t idx);

void vdbtree_print(struct VdbTree* tree);

#endif //VDB_TREE_H
