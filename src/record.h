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
};


//record
//[next|occupied|data....]

struct VdbRecord* vdbrecord_init(int count, struct VdbValue* data);
void vdb_record_free(struct VdbRecord* rec);
struct VdbRecord* vdb_record_copy(struct VdbRecord* rec);
void vdbrecord_write(uint8_t* buf, struct VdbRecord* rec, struct VdbSchema* schema);
struct VdbRecord* vdbrecord_read(uint8_t* buf, struct VdbSchema* schema);
struct VdbValue vdbrecord_read_value_at_idx(uint8_t* buf, struct VdbSchema* schema, uint32_t idx);
void vdbrecord_write_value_at_idx(uint8_t* buf, struct VdbSchema* schema, uint32_t idx, struct VdbValue v);
bool vdbrecord_has_varlen_data(struct VdbRecord* rec);
void vdbrecord_print(struct VdbRecord* record);
int vdbrecord_compare(struct VdbRecord* rec1, struct VdbRecord* rec2, struct VdbIntList* idxs);
struct VdbByteList* vdbrecord_concat_values(struct VdbRecord* rec, struct VdbIntList* idxs);

struct VdbRecordSet* vdbrecordset_init();
void vdbrecordset_append_record(struct VdbRecordSet* rs, struct VdbRecord* rec);
void vdbrecordset_free(struct VdbRecordSet* rs);

#endif //VDB_RECORD_H
