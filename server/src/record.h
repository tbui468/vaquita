#ifndef VDB_RECORD_H
#define VDB_RECORD_H

#include <stdarg.h>

#include "schema.h"
#include "util.h"

struct VdbRecord {
    struct VdbValue* data;
    int count;
};

struct VdbRecordSet {
    struct VdbRecord** records;
    int count;
    int capacity;
    struct VdbRecordSet* next;
    struct VdbByteList* key;
};


//record
//[next|occupied|data....]

struct VdbRecord* vdbrecord_init(int count, struct VdbValue* data);
void vdbrecord_free(struct VdbRecord* rec);
struct VdbRecord* vdbrecord_copy(struct VdbRecord* rec);
int vdbrecord_serialized_size(struct VdbRecord* rec);
int vdbrecord_fixedlen_size(struct VdbRecord* rec);
int vdbrecord_serialize(uint8_t* buf, struct VdbRecord* rec);
struct VdbRecord* vdbrecord_deserialize(uint8_t* buf, struct VdbSchema* schema);

struct VdbRecordSet* vdbrecordset_init(struct VdbByteList* key);
void vdbrecordset_append_record(struct VdbRecordSet* rs, struct VdbRecord* rec);
void vdbrecordset_free(struct VdbRecordSet* rs);
void vdbrecordset_serialize(struct VdbRecordSet* rs, struct VdbByteList* bl);
struct VdbRecordSet* vdbrecordset_remove_duplicates(struct VdbRecordSet* rs);

#endif //VDB_RECORD_H
