#ifndef VDB_TREE_H
#define VDB_TREE_H

#include <stdio.h>
#include "vdb.h"
#include "pager.h"

#define OFFSETS_START 256

struct IndexList {
    uint32_t* indices;
    uint32_t count;
    uint32_t capacity;
};

enum VdbNodeType {
    VDBN_INTERN,
    VDBN_LEAF
};

struct DataCell {
    uint32_t next;
    uint32_t key;
    uint32_t size;
    struct VdbData* data;
};

struct NodeCell {
    uint32_t next;
    uint32_t key;
    uint32_t block_idx;
};

struct TreeMeta {
    uint32_t node_size;
    uint32_t pk_counter;
    struct VdbSchema* schema;
};

struct InternMeta {
    enum VdbNodeType type;
    uint32_t offsets_size;
    uint32_t cells_size;
    uint32_t freelist;    
    struct NodeCell right_ptr;
};

struct LeafMeta {
    enum VdbNodeType type;
    uint32_t offsets_size;
    uint32_t cells_size;
    uint32_t freelist;    
};


void tree_init(FILE* f, struct VdbSchema* schema);
void tree_insert_record(struct VdbPager* pager, FILE* f, struct VdbData* d);
struct VdbData* tree_fetch_record(struct VdbPager* pager, FILE* f, uint32_t key);
void debug_print_nodes(struct VdbPager* pager, FILE* f, uint32_t idx);
void debug_print_keys(struct VdbPager* pager, FILE* f, uint32_t idx);

#endif //VDB_TREE_H
