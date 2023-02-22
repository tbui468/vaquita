#ifndef VDB_TREE_H
#define VDB_TREE_H

#include <stdio.h>
#include "vdb.h"
#include "pager.h"

#define OFFSETS_START 256

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

struct NodeMeta {
    enum VdbNodeType type;
    uint32_t node_size;
    uint32_t offsets_size;
    uint32_t cells_size;
    uint32_t freelist;    
    struct NodeCell right_ptr;
    struct VdbSchema* schema;
};


void tree_init(FILE* f, struct VdbSchema* schema);
void tree_insert_record(struct VdbPager* pager, FILE* f, const char* table_name, struct VdbData* d);
struct VdbData* tree_fetch_record(struct VdbPager* pager, FILE* f, const char* table, uint32_t key);

#endif //VDB_TREE_H
