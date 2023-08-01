#ifndef VDB_VM_H
#define VDB_VM_H

#include <threads.h>

#include "parser.h"

typedef void* VDBHANDLE;

struct VdbDatabase {
    char* name;
    struct VdbPager* pager;
    struct VdbTreeList* trees;
    mtx_t lock;
};

struct VdbDatabaseList {
    struct VdbDatabase** dbs;
    int count;
    int capacity;
};

struct VdbServer {
    struct VdbPager* pager;
    struct VdbDatabaseList* dbs;
};

void vdbserver_init();
void vdbserver_free();

struct VdbDatabaseList *vdbdblist_init();
void vdbdblist_free(struct VdbDatabaseList* l);
void vdbdblist_append_db(struct VdbDatabaseList* l, struct VdbDatabase* d);
void vdbvm_output_string(struct VdbByteList* bl, const char* buf, size_t size);

bool vdbvm_execute_stmts(VDBHANDLE* h, struct VdbStmtList* sl, struct VdbByteList* output);

#endif //VDB_VM_H
