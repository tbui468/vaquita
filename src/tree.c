#include <string.h>
#include <stdlib.h>
#include <assert.h>

#include "tree.h"
#include "util.h"

void _tree_read_u32(uint32_t* dst, uint8_t* buf, int* off) {
    *dst = *((uint32_t*)(buf + *off));
    *off += sizeof(uint32_t);
}

void _tree_write_u32(uint8_t* dst, uint32_t v, int* off) {
    *((uint32_t*)(dst + *off)) = v;
    *off += sizeof(uint32_t);
}

struct VdbNode _tree_deserialize_meta(uint8_t* buf) {
    struct VdbNode node;
    int off = 0;
    _tree_read_u32(&node.type, buf, &off);
    _tree_read_u32(&node.idx, buf, &off);

    node.as.meta.schema = malloc_w(sizeof(struct VdbSchema));
    _tree_read_u32(&node.as.meta.schema->count, buf, &off);
    node.as.meta.schema->fields = malloc_w(sizeof(enum VdbField) * node.as.meta.schema->count);
    for (uint32_t i = 0; i < node.as.meta.schema->count; i++) {
        uint32_t field;
        _tree_read_u32(&field, buf, &off);
        node.as.meta.schema->fields[i] = field;
    }

    return node;
}

void _tree_serialize_meta(uint8_t* buf, struct VdbNode* node) {
    int off = 0;
    _tree_write_u32(buf, (uint32_t)node->type, &off);
    _tree_write_u32(buf, (uint32_t)node->idx, &off);
    _tree_write_u32(buf, (uint32_t)node->as.meta.schema->count, &off);
    for (uint32_t i = 0; i < node->as.meta.schema->count; i++) {
        uint32_t field = node->as.meta.schema->fields[i];
        _tree_write_u32(buf, field, &off);
    }
}

struct VdbNode _tree_deserialize_intern(uint8_t* buf) {
    struct VdbNode node;
    int off = 0;
    _tree_read_u32(&node.type, buf, &off);
    _tree_read_u32(&node.idx, buf, &off);
    _tree_read_u32(&node.as.intern.nl->count, buf, &off);
    return node;
}

struct VdbNode _tree_deserialize_leaf(uint8_t* buf) {
    struct VdbNode node;
    int off = 0;
    _tree_read_u32(&node.type, buf, &off);
    _tree_read_u32(&node.idx, buf, &off);
    _tree_read_u32(&node.as.leaf.rl->count, buf, &off);
    return node;
}


void tree_init(struct VdbTree* tree, struct VdbSchema* schema) {
    uint32_t idx = pager_allocate_page(tree->f);
    struct VdbPage* page = pager_get_page(tree->pager, tree->f, idx);

    struct VdbNode meta;
    meta.type = VDBN_META;
    meta.idx = idx;
    meta.as.meta.pk_counter = 1;
    meta.as.meta.schema = schema;

    _tree_serialize_meta(page->buf, &meta);
    pager_flush_page(page);
}


struct VdbNode _tree_get_node(struct VdbTree* tree, uint32_t idx) {
    struct VdbPage* page = pager_get_page(tree->pager, tree->f, idx);

    enum VdbNodeType type = (enum VdbNodeType)(*((uint32_t*)page->buf));
    struct VdbNode node;
    switch (type) {
        case VDBN_META:
            node = _tree_deserialize_meta(page->buf);
            break;
        case VDBN_INTERN:
            node = _tree_deserialize_intern(page->buf);
            break;
        case VDBN_LEAF:
            node = _tree_deserialize_leaf(page->buf);
            break;
    }

    return node;
}
