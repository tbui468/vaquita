#ifndef VDB_RECORD_H
#define VDB_RECORD_H

#include <stdarg.h>

#include "schema.h"
#include "util.h"

struct VdbRecord {
    struct VdbValue* data;
    uint32_t count;
};

struct VdbRecordSet {
    struct VdbRecord** records;
    uint32_t count;
    uint32_t capacity;
    struct VdbRecordSet* next;
    struct VdbByteList* key;
};


//record
//[next|occupied|data....]

struct VdbRecord* vdbrecord_init(int count, struct VdbValue* data);
void vdbrecord_free(struct VdbRecord* rec);
struct VdbRecord* vdbrecord_copy(struct VdbRecord* rec);
int vdbrecord_serialized_size(struct VdbRecord* rec, struct VdbSchema* schema);
void vdbrecord_write(uint8_t* buf, struct VdbRecord* rec, struct VdbSchema* schema);
struct VdbRecord* vdbrecord_read(uint8_t* buf, struct VdbSchema* schema);
void vdbrecord_print(struct VdbString* s, struct VdbRecord* record);
void vdbrecord_serialize_to_bytes(struct VdbByteList* bl, struct VdbRecord* r);

struct VdbRecordSet* vdbrecordset_init(struct VdbByteList* key);
void vdbrecordset_append_record(struct VdbRecordSet* rs, struct VdbRecord* rec);
void vdbrecordset_free(struct VdbRecordSet* rs);

#endif //VDB_RECORD_H
