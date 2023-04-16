#include "schema.h"
#include "util.h"

#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

struct VdbSchema* vdb_schema_alloc(int count, ...) {
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

void vdb_schema_free(struct VdbSchema* schema) {
    free(schema->fields);
    free(schema);
}

struct VdbSchema* vdb_schema_copy(struct VdbSchema* schema) {
    struct VdbSchema* s = malloc_w(sizeof(struct VdbSchema));
    s->count = schema->count;
    s->fields = malloc_w(sizeof(enum VdbField) * s->count);

    memcpy(s->fields, schema->fields, sizeof(enum VdbField) * s->count);

    return s;
}

