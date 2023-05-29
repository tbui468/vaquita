#include <assert.h>
#include <string.h>

#include "hashtable.h"


struct VdbHashTable* vdbhashtable_init(struct VdbIntList* idxs) {
    struct VdbHashTable* ht = malloc_w(sizeof(struct VdbHashTable));
    ht->idxs = idxs;
    memset(ht->entries, 0, sizeof(struct VdbRecordSet*) * VDB_MAX_BUCKETS);
    return ht;
}

void vdbhashtable_free(struct VdbHashTable* ht) {
    //TODO: who is reponsible for freeing ht->idxs and all the records/recordsets?
    free_w(ht, sizeof(struct VdbHashTable));
}

//Simple hash algorithm used in key/value store in W. Richard Steven's excellent book
//'Advanced Programming in the Unix Environment'.  Should replace with some
//other algorithm if not sufficient in the future
static uint64_t vdbhashtable_hash(struct VdbByteList* bl) {
    uint32_t hash = 0;

    for (int i = 0; i < bl->count; i++) {
        hash += (i + 1) * bl->values[i];
    }

    return hash;
}

bool vdbhashtable_contains_entry(struct VdbHashTable* ht, struct VdbRecord* rec) {
    struct VdbByteList* bl = vdbrecord_concat_values(rec, ht->idxs);
    uint64_t hash = vdbhashtable_hash(bl);
    uint64_t bucket = hash % VDB_MAX_BUCKETS;

    struct VdbRecordSet* cur = ht->entries[bucket];

    while (cur) {
        struct VdbByteList* other = vdbrecord_concat_values(cur->records[0], ht->idxs);
        if (memcmp(bl->values, other->values, bl->count) == 0) {
            return true;
        }

        cur = cur->next;
    }

    return false;
}

void vdbhashtable_insert_entry(struct VdbHashTable* ht, struct VdbRecord* rec) {
    struct VdbByteList* bl = vdbrecord_concat_values(rec, ht->idxs);
    uint64_t hash = vdbhashtable_hash(bl);
    uint64_t bucket = hash % VDB_MAX_BUCKETS;
    struct VdbRecordSet* cur = ht->entries[bucket];

    while (cur) {
        struct VdbByteList* other = vdbrecord_concat_values(cur->records[0], ht->idxs);
        if (memcmp(bl->values, other->values, bl->count) == 0) {
            vdbrecordset_append_record(cur, rec);
            return;
        }

        cur = cur->next;
    }

    struct VdbRecordSet* head = ht->entries[bucket];
    ht->entries[bucket] = vdbrecordset_init();
    ht->entries[bucket]->next = head;
    vdbrecordset_append_record(ht->entries[bucket], rec);
}

