#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "record.h"
#include "util.h"

struct VdbRecord* vdbrecord_init(int count, struct VdbDatum* data) {
    struct VdbRecord* rec = malloc_w(sizeof(struct VdbRecord));
    rec->count = count;
    rec->key = data[0].as.Int; //primary key is first attribute by default
    rec->data = malloc_w(sizeof(struct VdbDatum) * rec->count);
    memcpy(rec->data, data, sizeof(struct VdbDatum) * rec->count);

    return rec;
}

void vdb_record_free(struct VdbRecord* rec) {
    for (uint32_t i = 0; i < rec->count; i++) {
        struct VdbDatum* d = &rec->data[i];
        if (d->type == VDBT_TYPE_STR) {
            free(d->as.Str->start);
            free(d->as.Str);
        }
    }
    free(rec->data);
    free(rec);
}

struct VdbRecord* vdb_record_copy(struct VdbRecord* rec) {
    struct VdbRecord* r = malloc_w(sizeof(struct VdbRecord));
    r->count = rec->count; 
    r->key = rec->key;
    r->data = malloc_w(sizeof(struct VdbDatum) * r->count);

    for (uint32_t i = 0; i < rec->count; i++) {
        struct VdbDatum* d = &rec->data[i];
        r->data[i].type = d->type;
        r->data[i].is_null = d->is_null;
        switch (d->type) {
            case VDBT_TYPE_INT:
                r->data[i].as.Int = d->as.Int;
                break;
            case VDBT_TYPE_STR:
                r->data[i].as.Str = malloc_w(sizeof(struct VdbString));
                r->data[i].as.Str->len = d->as.Str->len;
                r->data[i].as.Str->start = malloc_w(sizeof(char) * d->as.Str->len);
                memcpy(r->data[i].as.Str->start, d->as.Str->start, d->as.Str->len);
                break;
            case VDBT_TYPE_BOOL:
                r->data[i].as.Bool = d->as.Bool;
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

void vdbrecord_write(uint8_t* buf, struct VdbRecord* rec, struct VdbSchema* schema) {
    int off = 0;
    write_u32(buf, rec->key, &off);

    for (uint32_t i = 0; i < rec->count; i++) {
        struct VdbDatum* d = &rec->data[i];
        *((bool*)(buf + off)) = d->is_null;
        off += sizeof(bool);

        switch (schema->types[i]) {
            case VDBT_TYPE_INT:
                *((uint64_t*)(buf + off)) = d->as.Int;
                off += sizeof(uint64_t);
                break;
            case VDBT_TYPE_FLOAT:
                *((double*)(buf + off)) = d->as.Float;
                off += sizeof(double);
                break;
            case VDBT_TYPE_STR: {
                write_u32(buf, d->block_idx, &off);
                write_u32(buf, d->idxcell_idx, &off);
                break;
            }
            case VDBT_TYPE_BOOL:
                *((bool*)(buf + off)) = d->as.Bool;
                off += sizeof(bool);
                break;
            default:
                assert(false && "invalid data type");
                break;
        }
    }
}

struct VdbRecord* vdbrecord_read(uint8_t* buf, struct VdbSchema* schema) {
    uint32_t data_off = 0;
    struct VdbRecord* rec = malloc_w(sizeof(struct VdbRecord));
    rec->count = schema->count;
    rec->key = *((uint32_t*)(buf + data_off));
    data_off += sizeof(uint32_t);
    rec->data = malloc_w(sizeof(struct VdbDatum) * rec->count);

    for (uint32_t i = 0; i < schema->count; i++) {
        enum VdbTokenType type = schema->types[i];
        rec->data[i].type = type;
        rec->data[i].is_null = *((bool*)(buf + data_off));
        data_off += sizeof(bool);
        switch (type) {
            case VDBT_TYPE_INT:
                rec->data[i].as.Int = *((uint64_t*)(buf + data_off));
                data_off += sizeof(uint64_t);
                break;
            case VDBT_TYPE_FLOAT:
                rec->data[i].as.Float = *((double*)(buf + data_off));
                data_off += sizeof(double);
                break;
            case VDBT_TYPE_STR:
                rec->data[i].block_idx = *((uint32_t*)(buf + data_off));
                data_off += sizeof(uint32_t);
                rec->data[i].idxcell_idx = *((uint32_t*)(buf + data_off));
                data_off += sizeof(uint32_t);
                break;
            case VDBT_TYPE_BOOL:
                rec->data[i].as.Bool = *((bool*)(buf + data_off));
                data_off += sizeof(bool);
                break;
            default:
                assert(false && "invalid data type");
                break;
        }
    }

    return rec;
}

bool vdbrecord_has_varlen_data(struct VdbRecord* rec) {
    for (uint32_t i = 0; i < rec->count; i++) {
        struct VdbDatum* d = &rec->data[i];
        if (d->type == VDBT_TYPE_STR) {
            return true;
        }
    } 

    return false;
}

void vdbrecord_print(struct VdbRecord* r) {
    for (uint32_t j = 0; j < r->count; j++) {
        switch (r->data[j].type) {
            case VDBT_TYPE_STR:
                printf("%.*s, ", r->data[j].as.Str->len, r->data[j].as.Str->start); 
                break;
            case VDBT_TYPE_INT:
                printf("%ld, ", r->data[j].as.Int);
                break;
            case VDBT_TYPE_FLOAT:
                printf("%f, ", r->data[j].as.Float);
                break;
            case VDBT_TYPE_BOOL:
                if (r->data[j].as.Bool) {
                    printf("true, ");
                } else {
                    printf("false, ");
                }
                break;
             case VDBT_TYPE_NULL:
                printf("null, ");
                break;
             default:
                assert(false && "token is not valid data type");
                break;
        }
    }
}

struct VdbRecordSet* vdbrecordset_init() {
    struct VdbRecordSet* rs = malloc_w(sizeof(struct VdbRecordSet));
    rs->count = 0;
    rs->capacity = 8; 
    rs->records = malloc_w(sizeof(struct VdbRecord*) * rs->capacity);
    return rs;
}

void vdbrecordset_append_record(struct VdbRecordSet* rs, struct VdbRecord* rec) {
    if (rs->count == rs->capacity) {
        rs->capacity *= 2;
        rs->records = realloc_w(rs->records, sizeof(struct VdbRecord*) * rs->capacity);
    }

    rs->records[rs->count++] = rec;
}

void vdbrecordset_free(struct VdbRecordSet* rs) {
    for (uint32_t i = 0; i < rs->count; i++) {
        struct VdbRecord* rec = rs->records[i];
        vdb_record_free(rec);
    }

    free(rs->records);
    free(rs);
}

