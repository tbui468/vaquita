#ifndef VDB_VM_H
#define VDB_VM_H

#include "vdb.h"
#include "parser.h"

bool vdb_execute(struct VdbStmtList* sl, VDBHANDLE* h, struct VdbByteList* output);

#endif //VDB_VM_H
