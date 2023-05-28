#include <assert.h>
#include <string.h>

#include "hashtable.h"


struct VdbHashTable* vdbhashtable_init(struct VdbIntList* idxs) {
    struct VdbHashTable* ht = malloc_w(sizeof(struct VdbHashTable));
    ht->idxs = idxs;
    memset(ht->entries, 0, sizeof(struct VdbHashTableEntry) * VDB_MAX_BUCKETS);
    return ht;
}

void vdbhashtable_free(struct VdbHashTable* ht) {
    //caller is reponsible for freeing ht->idxs
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

    switch (ht->entries[bucket].type) {
        case VDBTET_EMPTY:
            ht->entries[bucket].type = VDBTET_RECSET;
            ht->entries[bucket].as.rs = vdbrecordset_init();
            vdbrecordset_append_record(ht->entries[bucket].as.rs, rec);
            return false;
        case VDBTET_RECSET: {
            struct VdbRecordSet* rs = ht->entries[bucket].as.rs;
            for (uint32_t i = 0; i < rs->count; i++) {
                struct VdbRecord* r = rs->records[i];
                if (memcmp(bl->values, vdbrecord_concat_values(r, ht->idxs)->values, bl->count) == 0) {
                    return true;
                }
            }

            //not found
            vdbrecordset_append_record(ht->entries[bucket].as.rs, rec);
            return false;
        }
        case VDBTET_HTABLE:
            return vdbhashtable_contains_entry(ht->entries[bucket].as.ht, rec);
        default:
            assert(false && "invalid hash table entry type");
            return false;
    }

}

void vdbhashtable_insert_entry(struct VdbHashTable* ht, struct VdbRecord* rec) {
    struct VdbByteList* bl = vdbrecord_concat_values(rec, ht->idxs);
    uint64_t hash = vdbhashtable_hash(bl);
    uint64_t bucket = hash % VDB_MAX_BUCKETS;

    switch (ht->entries[bucket].type) {
        case VDBTET_EMPTY:
            ht->entries[bucket].type = VDBTET_RECSET;
            ht->entries[bucket].as.rs = vdbrecordset_init();
            vdbrecordset_append_record(ht->entries[bucket].as.rs, rec);
            break;
        case VDBTET_RECSET:
            vdbrecordset_append_record(ht->entries[bucket].as.rs, rec);
            break;
        case VDBTET_HTABLE:
            vdbhashtable_insert_entry(ht->entries[bucket].as.ht, rec);
            break;
        default:
            assert(false && "invalid hash table entry type");
            break;
    }

}
