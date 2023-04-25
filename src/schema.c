#include "schema.h"
#include "util.h"

#include <stdlib.h>
#include <string.h>

struct VdbSchema* vdb_schema_alloc(int count, va_list args) {
    struct VdbSchema* schema = malloc_w(sizeof(struct VdbSchema));
    schema->count = count;
    schema->fields = malloc_w(sizeof(enum VdbField) * count);

    for (int i = 0; i < count; i++) {
        schema->fields[i] = va_arg(args, int);
    }

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

void vdb_schema_serialize(uint8_t* buf, struct VdbSchema* schema, int* off) {
    write_u32(buf, schema->count, off);
    for (uint32_t i = 0; i < schema->count; i++) {
        enum VdbField f = schema->fields[i];
        write_u32(buf, f, off);
    }
}

struct VdbSchema* vdb_schema_deserialize(uint8_t* buf, int* off) {
    struct VdbSchema* schema = malloc_w(sizeof(struct VdbSchema));
    read_u32(&schema->count, buf, off);
    schema->fields = malloc_w(sizeof(enum VdbField) * schema->count);
    for (uint32_t i = 0; i < schema->count; i++) {
        uint32_t type;
        read_u32(&type, buf, off);
        schema->fields[i] = type;
    }
    return schema;
}