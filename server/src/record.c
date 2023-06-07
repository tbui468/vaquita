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

void vdb_record_free(struct VdbRecord* rec) {
    for (uint32_t i = 0; i < rec->count; i++) {
        struct VdbValue* d = &rec->data[i];
        if (d->type == VDBT_TYPE_STR) {
            free_w(d->as.Str.start, sizeof(char) * d->as.Str.len);
        }
    }
    free_w(rec->data, rec->count * sizeof(struct VdbValue));
    free_w(rec, sizeof(struct VdbRecord));
}

struct VdbRecord* vdb_record_copy(struct VdbRecord* rec) {
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
        r->data[i].block_idx = d->block_idx;
        r->data[i].idxcell_idx = d->idxcell_idx;
    }

    return r;
}

static uint32_t vdbrecord_seek_value(struct VdbSchema* schema, uint32_t idx) {
    uint32_t off = 0;

    for (uint32_t j = 0; j < schema->count; j++) {
        if (j == idx)
            break;

        off += sizeof(bool);

        switch (schema->types[j]) {
            case VDBT_TYPE_INT:
                off += sizeof(int64_t);
                break;
            case VDBT_TYPE_FLOAT:
                off += sizeof(double);
                break;
            case VDBT_TYPE_STR:
                off += sizeof(uint32_t);
                off += sizeof(uint32_t);
                break;
            case VDBT_TYPE_BOOL:
                off += sizeof(bool);
                break;
            default:
                assert(false && "invalid data type");
                break;
        }
    }

    return off;
}

static struct VdbValue vdbrecord_read_value(uint8_t* buf, enum VdbTokenType col_type) {
    struct VdbValue v;
    v.type = col_type;

    int data_off = 0;
    bool is_null = *((bool*)(buf + data_off));
    data_off += sizeof(bool);

    //don't switch data type to null since we need to get correct offset
    switch (col_type) {
        case VDBT_TYPE_INT:
            v.as.Int = *((int64_t*)(buf + data_off));
            data_off += sizeof(int64_t);
            break;
        case VDBT_TYPE_FLOAT:
            v.as.Float = *((double*)(buf + data_off));
            data_off += sizeof(double);
            break;
        case VDBT_TYPE_STR:
            v.block_idx = *((uint32_t*)(buf + data_off));
            data_off += sizeof(uint32_t);
            v.idxcell_idx = *((uint32_t*)(buf + data_off));
            data_off += sizeof(uint32_t);
            break;
        case VDBT_TYPE_BOOL:
            v.as.Bool = *((bool*)(buf + data_off));
            data_off += sizeof(bool);
            break;
        default:
            assert(false && "invalid data type");
            break;
    }

    //switch data type to null
    if (is_null) {
        v.type = VDBT_TYPE_NULL;
    }

    return v;
}

static void vdbrecord_write_value(uint8_t* buf, struct VdbValue* v, enum VdbTokenType col_type) {
    int off = 0;
    *((bool*)(buf + off)) = v->type == VDBT_TYPE_NULL;
    off += sizeof(bool);

    switch (col_type) {
        case VDBT_TYPE_INT:
            *((int64_t*)(buf + off)) = v->as.Int;
            off += sizeof(int64_t);
            break;
        case VDBT_TYPE_FLOAT:
            *((double*)(buf + off)) = v->as.Float;
            off += sizeof(double);
            break;
        case VDBT_TYPE_STR: {
            write_u32(buf, v->block_idx, &off);
            write_u32(buf, v->idxcell_idx, &off);
            break;
        }
        case VDBT_TYPE_BOOL:
            *((bool*)(buf + off)) = v->as.Bool;
            off += sizeof(bool);
            break;
        default:
            assert(false && "invalid data type");
            break;
    }
}

void vdbrecord_write(uint8_t* buf, struct VdbRecord* rec, struct VdbSchema* schema) {
    for (uint32_t i = 0; i < rec->count; i++) {
        int off = vdbrecord_seek_value(schema, i);
        vdbrecord_write_value(buf + off, &rec->data[i], schema->types[i]);
    }
}

struct VdbRecord* vdbrecord_read(uint8_t* buf, struct VdbSchema* schema) {
    struct VdbRecord* rec = malloc_w(sizeof(struct VdbRecord));
    rec->count = schema->count;
    rec->data = malloc_w(sizeof(struct VdbValue) * rec->count);

    for (uint32_t i = 0; i < schema->count; i++) {
        uint32_t off = vdbrecord_seek_value(schema, i);
        rec->data[i] = vdbrecord_read_value(buf + off, schema->types[i]);
    }

    return rec;
}

struct VdbValue vdbrecord_read_value_at_idx(uint8_t* buf, struct VdbSchema* schema, uint32_t idx) {
    uint32_t off = vdbrecord_seek_value(schema, idx);
    return vdbrecord_read_value(buf + off, schema->types[idx]);
}

void vdbrecord_write_value_at_idx(uint8_t* buf, struct VdbSchema* schema, uint32_t idx, struct VdbValue v) {
    uint32_t off = vdbrecord_seek_value(schema, idx);
    vdbrecord_write_value(buf + off, &v, schema->types[idx]);
}

bool vdbrecord_has_varlen_data(struct VdbRecord* rec) {
    for (uint32_t i = 0; i < rec->count; i++) {
        struct VdbValue* d = &rec->data[i];
        if (d->type == VDBT_TYPE_STR) {
            return true;
        }
    } 

    return false;
}

void vdbrecord_print(struct VdbString* s, struct VdbRecord* r) {
    for (uint32_t j = 0; j < r->count; j++) {
        switch (r->data[j].type) {
            case VDBT_TYPE_STR:
                vdbstring_concat(s, "%.*s, ", r->data[j].as.Str.len, r->data[j].as.Str.start); 
                break;
            case VDBT_TYPE_INT:
                vdbstring_concat(s, "%ld, ", r->data[j].as.Int);
                break;
            case VDBT_TYPE_FLOAT:
                vdbstring_concat(s, "%f, ", r->data[j].as.Float);
                break;
            case VDBT_TYPE_BOOL:
                if (r->data[j].as.Bool) {
                    vdbstring_concat(s, "true, ");
                } else {
                    vdbstring_concat(s, "false, ");
                }
                break;
             case VDBT_TYPE_NULL:
                vdbstring_concat(s, "null, ");
                break;
             default:
                assert(false && "token is not valid data type");
                break;
        }
    }
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
        vdb_record_free(rec);
    }

    free_w(rs->records, sizeof(struct VdbRecord*) * rs->capacity);
    free_w(rs, sizeof(struct VdbRecordSet));
    //TODO: Who should free next recordset in linked list ('next' field)
    if (rs->key) vdbbytelist_free(rs->key);
}

