#ifndef VDB_RECORD_H
#define VDB_RECORD_H

#include <stdarg.h>

#include "schema.h"

struct VdbRecord {
    struct VdbDatum* data;
    uint32_t count;
    uint32_t key;
};

struct VdbRecordList {
    struct VdbRecord** records;
    uint32_t count;
    uint32_t capacity;
};


//record
//[next|size|data (first is key)....]

struct VdbRecord* vdb_record_alloc(uint32_t key, struct VdbSchema* schema, va_list args);
void vdb_record_free(struct VdbRecord* rec);
struct VdbRecord* vdb_record_copy(struct VdbRecord* rec);
uint32_t vdbrecord_size(struct VdbRecord* rec);
void vdbrecord_write(uint8_t* buf, struct VdbRecord* rec);
bool vdbrecord_has_varlen_data(struct VdbRecord* rec);

struct VdbRecordList* vdb_recordlist_alloc();
void vdb_recordlist_append_record(struct VdbRecordList* rl, struct VdbRecord* rec);
struct VdbRecord* vdb_recordlist_get_record(struct VdbRecordList* rl, uint32_t key);
void vdb_recordlist_remove_record(struct VdbRecordList* rl, uint32_t key);
void vdb_recordlist_free(struct VdbRecordList* rl);

#endif //VDB_RECORD_H
