#ifndef VDB_TREE_H
#define VDB_TREE_H

#include <stdio.h>
#include "vdb.h"

enum VdbNodeType {
    VDBN_INTERN,
    VDBN_LEAF
};

struct CellMeta {
    uint32_t next;
    uint32_t size;
};

struct NodeMeta {
    enum VdbNodeType type;
    uint32_t node_size;
    uint32_t offsets_start;
    uint32_t offsets_size;
    uint32_t cells_size;
    uint32_t freelist;    
    struct VdbSchema* schema;
};


void tree_init(FILE* f, struct VdbSchema* schema);


#endif //VDB_TREE_H
