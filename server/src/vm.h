#ifndef VDB_VM_H
#define VDB_VM_H

#include "parser.h"

typedef void* VDBHANDLE;

struct VdbDatabase {
    char* name;
    struct VdbPager* pager;
    struct VdbTreeList* trees;
};

bool vdb_execute(struct VdbStmtList* sl, VDBHANDLE* h, struct VdbByteList* output);

#endif //VDB_VM_H
