#include <assert.h>
#include <string.h>

#include "hashtable.h"


struct VdbHashTable* vdbhashtable_init() {
    struct VdbHashTable* ht = malloc_w(sizeof(struct VdbHashTable));
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

bool vdbhashtable_contains_key(struct VdbHashTable* ht, struct VdbByteList* key) {
    uint64_t hash = vdbhashtable_hash(key);
    uint64_t bucket = hash % VDB_MAX_BUCKETS;
    struct VdbRecordSet* cur = ht->entries[bucket];

    while (cur) {
        if (memcmp(cur->key->values, key->values, sizeof(uint8_t) * key->count) == 0) {
            return true;
        }

        cur = cur->next;
    }

    return false;
}

void vdbhashtable_insert_entry(struct VdbHashTable* ht, struct VdbByteList* key, struct VdbRecord* rec) {
    uint64_t hash = vdbhashtable_hash(key);
    uint64_t bucket = hash % VDB_MAX_BUCKETS;
    struct VdbRecordSet* cur = ht->entries[bucket];

    while (cur) {
        if (memcmp(cur->key->values, key->values, sizeof(uint8_t) * key->count) == 0) {
            vdbrecordset_append_record(cur, rec);
            return;
        }

        cur = cur->next;
    }

    struct VdbRecordSet* head = ht->entries[bucket];
    ht->entries[bucket] = vdbrecordset_init(key);
    ht->entries[bucket]->next = head;
    vdbrecordset_append_record(ht->entries[bucket], rec);
}

