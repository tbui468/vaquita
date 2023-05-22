#ifndef VDB_RECORD_H
#define VDB_RECORD_H

#include <stdarg.h>

#include "schema.h"
//#include "parser.h"

struct VdbRecord {
    struct VdbValue* data;
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

struct VdbRecord* vdbrecord_init(int count, struct VdbValue* data);
void vdb_record_free(struct VdbRecord* rec);
struct VdbRecord* vdb_record_copy(struct VdbRecord* rec);
void vdbrecord_write(uint8_t* buf, struct VdbRecord* rec, struct VdbSchema* schema);
struct VdbRecord* vdbrecord_read(uint8_t* buf, struct VdbSchema* schema);
bool vdbrecord_has_varlen_data(struct VdbRecord* rec);
void vdbrecord_print(struct VdbRecord* record);

struct VdbRecordSet* vdbrecordset_init();
void vdbrecordset_append_record(struct VdbRecordSet* rs, struct VdbRecord* rec);
void vdbrecordset_free(struct VdbRecordSet* rs);

#endif //VDB_RECORD_H
