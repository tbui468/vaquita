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
    struct VdbSchema* schema;
};

struct VdbTreeList {
    struct VdbTree** trees;
    int count;
    int capacity;
};

struct VdbPage* vdb_tree_pin_page(struct VdbTree* tree, uint32_t idx);
void vdb_tree_unpin_page(struct VdbTree* tree, struct VdbPage* page);

struct VdbTree* vdb_tree_init(const char* name, struct VdbSchema* schema, struct VdbPager* pager, FILE* f);
struct VdbTree* vdb_tree_open(const char* name, FILE* f, struct VdbPager* pager);
void vdb_tree_close(struct VdbTree* tree);

uint32_t vdbtree_meta_increment_primary_key_counter(struct VdbTree* tree);

struct VdbTreeList* vdb_treelist_init();
void vdb_treelist_append_tree(struct VdbTreeList* tl, struct VdbTree* tree);
struct VdbTree* vdb_treelist_get_tree(struct VdbTreeList* tl, const char* name);
struct VdbTree* vdb_treelist_remove_tree(struct VdbTreeList* tl, const char* name);
void vdb_treelist_free(struct VdbTreeList* tl);

bool vdbtree_leaf_can_fit_record(struct VdbTree* tree, uint32_t idx, struct VdbRecord* rec);
uint32_t vdbtree_leaf_split(struct VdbTree* tree, uint32_t idx, struct VdbValue new_right_key);
uint32_t vdbtree_leaf_read_record_count(struct VdbTree* tree, uint32_t idx);
struct VdbRecord* vdbtree_leaf_read_record(struct VdbTree* tree, uint32_t idx, uint32_t rec_idx);
uint32_t vdbtree_meta_read_primary_key_counter(struct VdbTree* tree);
struct VdbValue vdbtree_leaf_read_record_key(struct VdbTree* tree, uint32_t leaf_idx, uint32_t rec_idx);
void vdbtree_free_datablock_string(struct VdbTree* tree, struct VdbValue* v);

uint32_t vdb_tree_traverse_to(struct VdbTree* tree, uint32_t idx, struct VdbValue key);
uint32_t vdbtree_traverse_to_first_leaf(struct VdbTree* tree, uint32_t idx);
uint32_t vdbtree_meta_read_root(struct VdbTree* tree);
uint32_t vdbtree_leaf_read_next_leaf(struct VdbTree* tree, uint32_t idx);

void vdbtree_serialize_value(struct VdbTree* tree, uint8_t* buf, struct VdbValue* v);
void vdbtree_deserialize_value(struct VdbTree* tree, struct VdbValue* v, uint8_t* buf);

struct VdbRecPtr vdbtree_append_record_to_datablock(struct VdbTree* tree, struct VdbRecord* r);
void vdbtree_write_record_to_datablock(struct VdbTree* tree, struct VdbRecord* r, uint32_t leaf_idx, uint32_t leaf_idxcell_idx);
struct VdbRecord* vdbtree_read_record_from_datablock(struct VdbTree* tree, struct VdbRecPtr* p);
void vdbtree_serialize_recptr(struct VdbTree* tree, uint8_t* buf, struct VdbRecPtr* p);
struct VdbRecPtr vdbtree_deserialize_recptr(struct VdbTree* tree, uint8_t* buf);

void vdbtree_print(struct VdbTree* tree);


#endif //VDB_TREE_H
