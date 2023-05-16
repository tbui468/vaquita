#ifndef VDB_RECORD_H
#define VDB_RECORD_H

#include <stdarg.h>

#include "schema.h"

struct VdbRecord {
    struct VdbDatum* data;
    uint32_t count;
    uint32_t key;
};

struct VdbRecordSet {
    struct VdbRecord** records;
    uint32_t count;
    uint32_t capacity;
};


//record
//[next|size|data (first is key)....]

struct VdbRecord* vdb_record_alloc(uint32_t key, struct VdbSchema* schema, va_list args);
struct VdbRecord* vdbrecord_alloc(uint32_t key, struct VdbSchema* schema, struct VdbTokenList* attrs, struct VdbTokenList* values);
void vdb_record_free(struct VdbRecord* rec);
struct VdbRecord* vdb_record_copy(struct VdbRecord* rec);
uint32_t vdbrecord_fixedlen_size(struct VdbRecord* rec);
void vdbrecord_write(uint8_t* buf, struct VdbRecord* rec);
bool vdbrecord_has_varlen_data(struct VdbRecord* rec);

struct VdbRecordSet* vdbrecordset_init();
void vdbrecordset_append_record(struct VdbRecordSet* rs, struct VdbRecord* rec);
void vdbrecordset_free(struct VdbRecordSet* rs);

#endif //VDB_RECORD_H
