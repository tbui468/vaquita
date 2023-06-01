#ifndef VDB_HASHTABLE_H
#define VDB_HASHTABLE_H

#include "record.h"
#include "binarytree.h"

#define VDB_MAX_BUCKETS 137

struct VdbHashTable {
    struct VdbRecordSet* entries[VDB_MAX_BUCKETS];
};


struct VdbHashTable* vdbhashtable_init();
void vdbhashtable_free(struct VdbHashTable* ht);
bool vdbhashtable_contains_key(struct VdbHashTable* ht, struct VdbByteList* key);
void vdbhashtable_insert_entry(struct VdbHashTable* ht, struct VdbByteList* key, struct VdbRecord* rec);

#endif //VDB_HASHTABLE_H
