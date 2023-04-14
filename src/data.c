#include "data.h"
#include "util.h"

#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

struct VdbSchema* vdbdata_alloc_schema(int count, ...) {
    struct VdbSchema* schema = malloc_w(sizeof(struct VdbSchema));
    schema->count = count;
    schema->fields = malloc_w(sizeof(enum VdbField) * count);

    va_list ap;
    va_start(ap, count);

    for (int i = 0; i < count; i++) {
        schema->fields[i] = va_arg(ap, enum VdbField);
    }

    va_end(ap);

    return schema;
}

void vdbdata_free_schema(struct VdbSchema* schema) {
    free(schema->fields);
    free(schema);
}

struct VdbSchema* vdbdata_copy_schema(struct VdbSchema* schema) {
    struct VdbSchema* s = malloc_w(sizeof(struct VdbSchema));
    s->count = schema->count;
    s->fields = malloc_w(sizeof(enum VdbField) * s->count);

    memcpy(s->fields, schema->fields, sizeof(enum VdbField) * s->count);

    return s;
}

struct VdbRecord* vdbdata_alloc_record(struct VdbSchema* schema, ...) {
    struct VdbRecord* rec = malloc_w(sizeof(struct VdbRecord));
    rec->count = schema->count; 
    rec->key = 0;
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
                rec->data[i].as.Str = malloc_w(sizeof(char) * strlen(s));
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

void vdbdata_free_record(struct VdbRecord* rec) {

}

