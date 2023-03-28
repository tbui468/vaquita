#ifndef VDB_TREE_H
#define VDB_TREE_H

#include <stdio.h>
#include "vdb.h"
#include "pager.h"

#define OFFSETS_START 256

struct VdbTree {
    struct VdbPager* pager;
    FILE* f;
};

struct VdbNodeList {
    uint32_t count;
};

struct VdbRecordList {
    uint32_t count;
};

enum VdbNodeType {
    VDBN_META,
    VDBN_INTERN,
    VDBN_LEAF
};

struct VdbNode {
    enum VdbNodeType type;
    uint32_t idx;
    union {
        struct {
            uint32_t pk_counter;
            struct VdbSchema* schema;
        } meta;
        struct {
            struct VdbNodeList* nl;
        } intern;
        struct {
            struct VdbRecordList* rl;
        } leaf;
    } as;
};


void tree_init(struct VdbTree tree, struct VdbSchema* schema);
//void tree_insert_record(struct VdbTree t, struct VdbData* d);
//struct VdbData* tree_fetch_record(struct VdbTree t, uint32_t key);
//void debug_print_tree(struct VdbTree t, uint32_t idx, uint32_t depth);

#endif //VDB_TREE_H
