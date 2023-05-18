#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "record.h"
#include "util.h"

struct VdbRecord* vdbrecord_alloc(uint32_t key, struct VdbSchema* schema, struct VdbTokenList* attrs, struct VdbTokenList* values) {
    struct VdbRecord* rec = malloc_w(sizeof(struct VdbRecord));
    rec->count = schema->count; 
    rec->key = key;
    rec->data = malloc_w(sizeof(struct VdbDatum) * rec->count);

    for (uint32_t i = 0; i < rec->count; i++) {
        rec->data[i].type = schema->types[i];
        
        bool found = false;
        for (int j = 0; j < attrs->count; j++) {
            if (strncmp(schema->names[i], attrs->tokens[j].lexeme, attrs->tokens[j].len) != 0) 
                continue;

            rec->data[i].is_null = false;
            switch (schema->types[i]) {
                case VDBT_TYPE_INT: {
                    char n[values->tokens[j].len + 1];
                    memcpy(n, values->tokens[j].lexeme, values->tokens[j].len);
                    n[values->tokens[j].len] = '\0';
                    rec->data[i].as.Int = strtoll(n, NULL, 10);
                    break;
                }
                case VDBT_TYPE_FLOAT: {
                    char n[values->tokens[j].len + 1];
                    memcpy(n, values->tokens[j].lexeme, values->tokens[j].len);
                    n[values->tokens[j].len] = '\0';
                    rec->data[i].as.Float = strtod(n, NULL);
                    break;
                }
                case VDBT_TYPE_STR: {
                    int len = values->tokens[j].len;
                    rec->data[i].as.Str = malloc_w(sizeof(struct VdbString));
                    rec->data[i].as.Str->start = malloc_w(sizeof(char) * len);
                    rec->data[i].as.Str->len = len;
                    memcpy(rec->data[i].as.Str->start, values->tokens[j].lexeme, len);
                    break;
                }
                case VDBT_TYPE_BOOL: {
                    if (strncmp("true", values->tokens[j].lexeme, 4) == 0) {
                        rec->data[i].as.Bool = true;
                    } else {
                        rec->data[i].as.Bool = false;
                    }
                    break;
                }
                default: {
                    assert(false && "invalid data type");
                    break;
                }
            }
            found = true;
            break;

        }

        if (!found) {
            rec->data[i].is_null = true;

            //writing dummy data since writing records to disk expects a non-NULL struct VdbString*
            //maybe not the best solution, but it works for now
            //TODO: should not write data if null - can remove this block when that is implemented
            if (schema->types[i] == VDBT_TYPE_STR) {
                int len = 1;
                rec->data[i].as.Str = malloc_w(sizeof(struct VdbString));
                rec->data[i].as.Str->start = malloc_w(sizeof(char) * len);
                rec->data[i].as.Str->len = len;
                memcpy(rec->data[i].as.Str->start, "0", len);
            }
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

uint32_t vdbrecord_fixedlen_size(struct VdbRecord* rec) {
    uint32_t size = 0;
    size += sizeof(uint32_t); //key
    for (uint32_t i = 0; i < rec->count; i++) {
        struct VdbDatum* d = &rec->data[i];
        size += sizeof(bool); //is_null flag
        switch (d->type) {
            case VDBT_TYPE_INT:
                size += sizeof(uint64_t);
                break;
            case VDBT_TYPE_FLOAT:
                size += sizeof(double);
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
        *((bool*)(buf + off)) = d->is_null;
        off += sizeof(bool);
        switch (d->type) {
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

bool vdbrecord_has_varlen_data(struct VdbRecord* rec) {
    for (uint32_t i = 0; i < rec->count; i++) {
        struct VdbDatum* d = &rec->data[i];
        if (d->type == VDBT_TYPE_STR) {
            return true;
        }
    } 

    return false;
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

