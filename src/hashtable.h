#ifndef VDB_HASHTABLE_H
#define VDB_HASHTABLE_H

#include "record.h"

#define VDB_MAX_BUCKETS 137

enum VdbTableEntryType {
    VDBTET_EMPTY = 0,
    VDBTET_RECSET,
    VDBTET_HTABLE
};

struct VdbHashTableEntry {
    enum VdbTableEntryType type;
    union {
        struct VdbRecordSet* rs;
        struct VdbHashTable* ht;
    } as;
};

struct VdbHashTable {
    struct VdbIntList* idxs;
    struct VdbHashTableEntry entries[VDB_MAX_BUCKETS];
};


struct VdbHashTable* vdbhashtable_init(struct VdbIntList* idxs);
void vdbhashtable_free(struct VdbHashTable* ht);
bool vdbhashtable_contains_entry(struct VdbHashTable* ht, struct VdbRecord* rec);
void vdbhashtable_insert_entry(struct VdbHashTable* ht, struct VdbRecord* rec);

#endif //VDB_HASHTABLE_H
