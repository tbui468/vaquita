#include <string.h>
#include <stdlib.h>

#include "tree.h"
#include "util.h"

void _tree_write_metadata(uint8_t* buf, struct NodeMeta* m) {
    int len = 0;

    uint32_t type = m->type;
    memcpy(buf + len, &type, sizeof(uint32_t));
    len += sizeof(uint32_t);
    memcpy(buf + len, &m->node_size, sizeof(uint32_t));
    len += sizeof(uint32_t);
    memcpy(buf + len, &m->offsets_start, sizeof(uint32_t));
    len += sizeof(uint32_t);
    memcpy(buf + len, &m->offsets_size, sizeof(uint32_t));
    len += sizeof(uint32_t);
    memcpy(buf + len, &m->cells_size, sizeof(uint32_t));
    len += sizeof(uint32_t);
    memcpy(buf + len, &m->freelist, sizeof(uint32_t));
    len += sizeof(uint32_t);

    memcpy(buf + len, &m->schema->count, sizeof(uint32_t));
    len += sizeof(uint32_t);

    for (uint32_t i = 0; i < m->schema->count; i++) {
        uint32_t field = m->schema->fields[i];
        memcpy(buf + len, &field, sizeof(uint32_t));
        len += sizeof(uint32_t);
    }
}

void tree_init(FILE* f, struct VdbSchema* schema) {
    struct NodeMeta m;
    m.type = VDBN_INTERN;
    m.node_size = 4096;
    m.offsets_start = 128;
    m.offsets_size = 0;
    m.cells_size = 0;
    m.freelist = 0;
    m.schema = schema;

    uint8_t* buf = calloc_w(4096, sizeof(uint8_t));
    _tree_write_metadata(buf, &m);
    
    fseek_w(f, 0, SEEK_SET);
    fwrite_w(buf, sizeof(uint8_t), 4096, f);

    free(buf);
}

