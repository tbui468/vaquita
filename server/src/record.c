#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "record.h"
#include "util.h"

struct VdbRecord* vdbrecord_init(int count, struct VdbValue* data) {
    struct VdbRecord* rec = malloc_w(sizeof(struct VdbRecord));
    rec->count = count;
    rec->data = malloc_w(sizeof(struct VdbValue) * rec->count);
    memcpy(rec->data, data, sizeof(struct VdbValue) * rec->count);

    return rec;
}

void vdbrecord_free(struct VdbRecord* rec) {
    for (uint32_t i = 0; i < rec->count; i++) {
        struct VdbValue* d = &rec->data[i];
        if (d->type == VDBT_TYPE_STR) {
            free_w(d->as.Str.start, sizeof(char) * d->as.Str.len);
        }
    }
    free_w(rec->data, rec->count * sizeof(struct VdbValue));
    free_w(rec, sizeof(struct VdbRecord));
}

struct VdbRecord* vdbrecord_copy(struct VdbRecord* rec) {
    struct VdbRecord* r = malloc_w(sizeof(struct VdbRecord));
    r->count = rec->count; 
    r->data = malloc_w(sizeof(struct VdbValue) * r->count);

    for (uint32_t i = 0; i < rec->count; i++) {
        struct VdbValue* d = &rec->data[i];
        r->data[i].type = d->type;
        switch (d->type) {
            case VDBT_TYPE_INT:
                r->data[i].as.Int = d->as.Int;
                break;
            case VDBT_TYPE_STR:
                r->data[i].as.Str.len = d->as.Str.len;
                r->data[i].as.Str.start = malloc_w(sizeof(char) * d->as.Str.len);
                memcpy(r->data[i].as.Str.start, d->as.Str.start, d->as.Str.len);
                r->data[i].as.Str.block_idx = d->as.Str.block_idx;
                r->data[i].as.Str.idxcell_idx = d->as.Str.idxcell_idx;
                break;
            case VDBT_TYPE_BOOL:
                r->data[i].as.Bool = d->as.Bool;
                break;
            case VDBT_TYPE_NULL:
                break;
            default:
                assert(false && "invalid data type");
                break;
        }
    }

    return r;
}

int vdbrecord_serialized_size(struct VdbRecord* rec) {
    int size = 0;

    for (uint32_t i = 0; i < rec->count; i++) {
        if (rec->data[i].type == VDBT_TYPE_STR) {
            size += vdbvalue_serialized_string_size(rec->data[i]);
        } else {
            size += vdbvalue_serialized_size(rec->data[i]);
        }
    }

    return size;
}

int vdbrecord_fixedlen_size(struct VdbRecord* rec) {
    int size = 0;

    for (uint32_t i = 0; i < rec->count; i++) {
        size += vdbvalue_serialized_size(rec->data[i]);
    }

    return size;
}

int vdbrecord_serialize(uint8_t* buf, struct VdbRecord* rec) {
    int off = 0;

    for (uint32_t i = 0; i < rec->count; i++) {
        off += vdbvalue_serialize(buf + off, rec->data[i]);
    }

    return off;
}

struct VdbRecord* vdbrecord_deserialize(uint8_t* buf, struct VdbSchema* schema) {
    struct VdbRecord* rec = malloc_w(sizeof(struct VdbRecord));
    rec->count = schema->count;
    rec->data = malloc_w(sizeof(struct VdbValue) * rec->count);

    int off = 0;
    for (uint32_t i = 0; i < schema->count; i++) {
        off += vdbvalue_deserialize(&rec->data[i], buf + off);
    }

    return rec;
}

struct VdbRecordSet* vdbrecordset_init(struct VdbByteList* key) {
    struct VdbRecordSet* rs = malloc_w(sizeof(struct VdbRecordSet));
    rs->count = 0;
    rs->capacity = 8; 
    rs->records = malloc_w(sizeof(struct VdbRecord*) * rs->capacity);
    rs->next = NULL;
    rs->key = key;
    return rs;
}

void vdbrecordset_append_record(struct VdbRecordSet* rs, struct VdbRecord* rec) {
    if (rs->count == rs->capacity) {
        uint32_t old_cap = rs->capacity;
        rs->capacity *= 2;
        rs->records = realloc_w(rs->records, sizeof(struct VdbRecord*) * rs->capacity, sizeof(struct VdbRecord*) * old_cap);
    }

    rs->records[rs->count++] = rec;
}

void vdbrecordset_free(struct VdbRecordSet* rs) {
    for (uint32_t i = 0; i < rs->count; i++) {
        struct VdbRecord* rec = rs->records[i];
        vdbrecord_free(rec);
    }

    free_w(rs->records, sizeof(struct VdbRecord*) * rs->capacity);
    free_w(rs, sizeof(struct VdbRecordSet));
    //TODO: Who should free next recordset in linked list ('next' field)
    if (rs->key) vdbbytelist_free(rs->key);
}

void vdbrecordset_serialize(struct VdbRecordSet* rs, struct VdbByteList* bl) {
    uint8_t is_tuple = 1;
    vdbbytelist_append_byte(bl, is_tuple);

    vdbbytelist_append_bytes(bl, (uint8_t*)&rs->count, sizeof(uint32_t));
    if (rs->count > 0) {
        vdbbytelist_append_bytes(bl, (uint8_t*)&rs->records[0]->count, sizeof(uint32_t));
    } else {
        uint32_t zero = 0;
        vdbbytelist_append_bytes(bl, (uint8_t*)&zero, sizeof(uint32_t));
    }

    for (uint32_t i = 0; i < rs->count; i++) {
        struct VdbRecord* r = rs->records[i];
        vdbbytelist_resize(bl, vdbrecord_serialized_size(r));
        for (uint32_t j = 0; j < r->count; j++) {
            struct VdbValue v = r->data[j];

            //vdbvalue_serialize will serialize block_idx/idxcell_idx by default, so
            //need to call vdbvalue_serialize_string to write the actual string
            if (v.type == VDBT_TYPE_STR) {
                bl->count += vdbvalue_serialize_string(bl->values + bl->count, &v);
            } else {
                bl->count += vdbvalue_serialize(bl->values + bl->count, v);
            }
        }
    }
}
