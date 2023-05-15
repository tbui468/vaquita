#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "record.h"
#include "util.h"


struct VdbRecord* vdb_record_alloc(uint32_t key, struct VdbSchema* schema, va_list args) {
    struct VdbRecord* rec = malloc_w(sizeof(struct VdbRecord));
    rec->count = schema->count; 
    rec->key = key;
    rec->data = malloc_w(sizeof(struct VdbDatum) * rec->count);

    for (uint32_t i = 0; i < rec->count; i++) {
        rec->data[i].type = schema->types[i];
        switch (schema->types[i]) {
            case VDBT_TYPE_INT:
                rec->data[i].as.Int = va_arg(args, uint64_t);
                break;
            case VDBT_TYPE_STR: {
                const char* s = va_arg(args, const char*);
                rec->data[i].as.Str = malloc_w(sizeof(struct VdbString));
                rec->data[i].as.Str->start = malloc_w(sizeof(char) * strlen(s));
                rec->data[i].as.Str->len = strlen(s);
                memcpy(rec->data[i].as.Str->start, s, strlen(s));
                break;
            }
            case VDBT_TYPE_BOOL:
                rec->data[i].as.Bool = va_arg(args, int);
                break;
            default:
                assert(false && "invalid data type");
                break;
        }
        rec->data[i].block_idx = 0;
        rec->data[i].idxcell_idx = 0;
    }

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

uint32_t vdbrecord_fixedlen_size(struct VdbRecord* rec) {
    uint32_t size = 0;
    size += sizeof(uint32_t); //key
    for (uint32_t i = 0; i < rec->count; i++) {
        struct VdbDatum* d = &rec->data[i];
        switch (d->type) {
            case VDBT_TYPE_INT:
                size += sizeof(uint64_t);
                break;
            case VDBT_TYPE_STR:
                size += sizeof(uint32_t) * 2;
                break;
            case VDBT_TYPE_BOOL:
                size += sizeof(bool);
                break;
            default:
                assert(false && "invalid data type");
                break;
        }
    }

    return size;
}

void vdbrecord_write(uint8_t* buf, struct VdbRecord* rec) {
    int off = 0;
    write_u32(buf, rec->key, &off);

    for (uint32_t i = 0; i < rec->count; i++) {
        struct VdbDatum* d = &rec->data[i];
        switch (d->type) {
            case VDBT_TYPE_INT:
                *((uint64_t*)(buf + off)) = d->as.Int;
                off += sizeof(uint64_t);
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

bool vdbrecord_has_varlen_data(struct VdbRecord* rec) {
    for (uint32_t i = 0; i < rec->count; i++) {
        struct VdbDatum* d = &rec->data[i];
        if (d->type == VDBT_TYPE_STR) {
            return true;
        }
    } 

    return false;
}

struct VdbRecordList* vdb_recordlist_alloc() {
    struct VdbRecordList* rl = malloc_w(sizeof(struct VdbRecordList));
    rl->count = 0;
    rl->capacity = 8; 
    rl->records = malloc_w(sizeof(struct VdbRecord*) * rl->capacity);
    return rl;
}

void vdb_recordlist_append_record(struct VdbRecordList* rl, struct VdbRecord* rec) {
    if (rl->count == rl->capacity) {
        rl->capacity *= 2;
        rl->records = realloc_w(rl->records, sizeof(struct VdbRecord*) * rl->capacity);
    }

    rl->records[rl->count++] = rec;
}

struct VdbRecord* vdb_recordlist_get_record(struct VdbRecordList* rl, uint32_t key) {
    struct VdbRecord* rec;
    uint32_t i;
    for (i = 0; i < rl->count; i++) {
        rec = rl->records[i];
        if (rec->key == key)
            return rec;
    }

    return NULL;
}

void vdb_recordlist_remove_record(struct VdbRecordList* rl, uint32_t key) {
    struct VdbRecord* rec = NULL;
    uint32_t i;
    for (i = 0; i < rl->count; i++) {
        rec = rl->records[i];
        if (rec->key == key)
            break;
    }

    if (rec) {
        for (uint32_t j = i + 1; j < rl->count; j++) {
            rl->records[j - 1] = rl->records[j];
        }
        rl->count--;
        vdb_record_free(rec);
    }
}

void vdb_recordlist_free(struct VdbRecordList* rl) {
    for (uint32_t i = 0; i < rl->count; i++) {
        struct VdbRecord* rec = rl->records[i];
        vdb_record_free(rec);
    }

    free(rl->records);
    free(rl);
}
