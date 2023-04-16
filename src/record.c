#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

#include "record.h"
#include "util.h"


struct VdbRecord* vdb_record_alloc(uint32_t key, struct VdbSchema* schema, ...) {
    struct VdbRecord* rec = malloc_w(sizeof(struct VdbRecord));
    rec->count = schema->count; 
    rec->key = key;
    rec->data = malloc_w(sizeof(struct VdbDatum) * rec->count);

    va_list ap;
    va_start(ap, schema);

    for (uint32_t i = 0; i < rec->count; i++) {
        switch (schema->fields[i]) {
            case VDBF_INT:
                rec->data[i].type = VDBF_INT;
                rec->data[i].as.Int = va_arg(ap, uint64_t);
                break;
            case VDBF_STR:
                rec->data[i].type = VDBF_STR;
                const char* s = va_arg(ap, const char*);
                rec->data[i].as.Str = malloc_w(sizeof(struct VdbString));
                rec->data[i].as.Str->start = malloc_w(sizeof(char) * strlen(s));
                rec->data[i].as.Str->len = strlen(s);
                memcpy(rec->data[i].as.Str->start, s, strlen(s));
                break;
            case VDBF_BOOL:
                rec->data[i].type = VDBF_BOOL;
                rec->data[i].as.Bool = (bool)va_arg(ap, int);
                break;
        }
    }

    va_end(ap);

    return rec;
}

void vdb_record_free(struct VdbRecord* rec) {
    for (uint32_t i = 0; i < rec->count; i++) {
        struct VdbDatum* d = &rec->data[i];
        if (d->type == VDBF_STR) {
            free(d->as.Str->start);
            free(d->as.Str);
        }
    }
    free(rec->data);
    free(rec);
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
