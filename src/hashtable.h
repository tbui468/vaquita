#ifndef VDB_HASHTABLE_H
#define VDB_HASHTABLE_H

#include "record.h"

#define VDB_MAX_BUCKETS 137

struct VdbHashTable {
    struct VdbIntList* idxs;
    struct VdbRecordSet* entries[VDB_MAX_BUCKETS];
};


struct VdbHashTable* vdbhashtable_init(struct VdbIntList* idxs);
void vdbhashtable_free(struct VdbHashTable* ht);
bool vdbhashtable_contains_entry(struct VdbHashTable* ht, struct VdbRecord* rec);
void vdbhashtable_insert_entry(struct VdbHashTable* ht, struct VdbRecord* rec);
void vdbhashtable_insert_entry_by_group(struct VdbHashTable* ht, struct VdbRecord* rec, int depth);

#endif //VDB_HASHTABLE_H
