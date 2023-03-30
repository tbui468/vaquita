#ifndef VDB_TREE_H
#define VDB_TREE_H

#include <stdio.h>
#include "vdb.h"
#include "pager.h"

struct VdbTree {
    struct VdbPager* pager;
    FILE* f;
};

struct VdbNodePtr {
    uint32_t key;
    uint32_t idx;
};

struct VdbNodePtrList {
    uint32_t count;
    uint32_t capacity;
    struct VdbNodePtr* pointers;
};

struct VdbRecordList {
    uint32_t count;
    uint32_t capacity;
    struct VdbRecord* records;
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
            struct VdbNodePtrList* pl;
        } intern;
        struct {
            struct VdbRecordList* rl;
        } leaf;
    } as;
};


void tree_init(struct VdbTree* tree, struct VdbSchema* schema);
void tree_insert_record(struct VdbTree* tree, struct VdbRecord* rec);
//struct VdbData* tree_fetch_record(struct VdbTree t, uint32_t key);
//void debug_print_tree(struct VdbTree t, uint32_t idx, uint32_t depth);

#endif //VDB_TREE_H
