#ifndef VDB_TREE_H
#define VDB_TREE_H

#include <stdio.h>
#include "vdb.h"
#include "pager.h"
#include "node.h"

struct VdbTree {
    struct VdbPager* pager;
    FILE* f;
};

void tree_init(struct VdbTree* tree, struct VdbSchema* schema);
void tree_insert_record(struct VdbTree* tree, struct VdbRecord* rec);
void debug_print_tree(struct VdbTree* tree);
struct VdbRecord* tree_fetch_record(struct VdbTree* t, uint32_t key);

#endif //VDB_TREE_H
