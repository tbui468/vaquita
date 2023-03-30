#include <string.h>
#include <stdlib.h>
#include <assert.h>

#include "tree.h"
#include "util.h"

struct VdbNodePtrList* _pl_alloc() {
    struct VdbNodePtrList* list = malloc_w(sizeof(struct VdbNodePtrList));
    list->count = 0;
    list->capacity = 8;
    list->pointers = malloc_w(sizeof(struct VdbNodePtr) * list->capacity);

    return list;
}

void _pl_append(struct VdbNodePtrList* list, struct VdbNodePtr ptr) {
    if (list->capacity == list->count) {
        list->capacity *= 2;
        list->pointers = realloc_w(list->pointers, sizeof(struct VdbNodePtr) * list->capacity);
    }

    list->pointers[list->count++] = ptr;
}

void _pl_free(struct VdbNodePtrList* list) {
    free(list->pointers);
    free(list);
}

struct VdbRecordList* _rl_alloc() {
    struct VdbRecordList* list = malloc_w(sizeof(struct VdbRecordList));
    list->count = 0;
    list->capacity = 8;
    list->records = malloc_w(sizeof(struct VdbRecord) * list->capacity);

    return list;
}

void _rl_append(struct VdbRecordList* list, struct VdbRecord rec) {
    if (list->capacity == list->count) {
        list->capacity *= 2;
        list->records = realloc_w(list->records, sizeof(struct VdbRecord) * list->capacity);
    }

    list->records[list->count++] = rec;
}

void _rl_free(struct VdbRecordList* list) {
    free(list->records);
    free(list);
}

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
    _tree_read_u32(&node.as.meta.pk_counter, buf, &off);

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
    _tree_write_u32(buf, (uint32_t)node->as.meta.pk_counter, &off);
    _tree_write_u32(buf, (uint32_t)node->as.meta.schema->count, &off);
    for (uint32_t i = 0; i < node->as.meta.schema->count; i++) {
        uint32_t field = node->as.meta.schema->fields[i];
        _tree_write_u32(buf, field, &off);
    }
}

struct VdbNodePtr _tree_deserialize_nodeptr(uint8_t* buf) {
    struct VdbNodePtr ptr;
    int off = 0;
    _tree_read_u32(&ptr.key, buf, &off);
    _tree_read_u32(&ptr.idx, buf, &off);

    return ptr;
}

//[type, idx, count ... offsets ... cells]
struct VdbNode _tree_deserialize_intern(uint8_t* buf) {
    struct VdbNode node;
    int off = 0;
    _tree_read_u32(&node.type, buf, &off);
    _tree_read_u32(&node.idx, buf, &off);
    uint32_t ptr_count;
    _tree_read_u32(&ptr_count, buf, &off);
    node.as.intern.pl = _pl_alloc();

    int idx_off = VDB_OFFSETS_START;
    for (uint32_t i = 0; i < ptr_count; i++) {
        uint32_t idx;
        _tree_read_u32(&idx, buf, &idx_off);
        int ptr_off = (int)idx;
        _pl_append(node.as.intern.pl, _tree_deserialize_nodeptr(buf + ptr_off));
    }

    return node;
}

void _tree_serialize_intern(uint8_t* buf, struct VdbNode* node) {
    int off = 0;
    _tree_write_u32(buf, (uint32_t)node->type, &off);
    _tree_write_u32(buf, (uint32_t)node->idx, &off);
    _tree_write_u32(buf, (uint32_t)node->as.intern.pl->count, &off);

    uint32_t cell_off = VDB_PAGE_SIZE;
    int idx_off = VDB_OFFSETS_START;

    for (uint32_t i = 0; i < node->as.intern.pl->count; i++) {
        struct VdbNodePtr* ptr = &node->as.intern.pl->pointers[i];
        cell_off -= sizeof(uint32_t) * 2;
        _tree_write_u32(buf, cell_off, &idx_off);
        memcpy(buf + cell_off, &ptr->key, sizeof(uint32_t));
        memcpy(buf + cell_off + sizeof(uint32_t), &ptr->idx, sizeof(uint32_t));
    }
}

struct VdbRecord _tree_deserialize_record(uint8_t* buf, struct VdbSchema* schema) {
    struct VdbRecord r;
    r.count = schema->count;
    r.data = malloc_w(sizeof(struct VdbDatum) * schema->count);
    int off = 0;
    _tree_read_u32(&r.key, buf, &off);

    for (uint32_t i = 0; i < r.count; i++) {
        enum VdbField f = schema->fields[i];
        switch (f) {
            case VDBF_INT:
                r.data[i].type = VDBF_INT;
                r.data[i].as.Int = *((uint64_t*)(buf + off));
                off += sizeof(uint64_t);
                break;
            case VDBF_STR:
                r.data[i].type = VDBF_STR;
                r.data[i].as.Str = malloc_w(sizeof(struct VdbString));
                _tree_read_u32(&r.data[i].as.Str->len, buf, &off);
                r.data[i].as.Str->start = malloc_w(sizeof(char) * r.data[i].as.Str->len);
                memcpy(r.data[i].as.Str->start, buf + off, r.data[i].as.Str->len);
                off += r.data[i].as.Str->len;
                break;
            case VDBF_BOOL:
                r.data[i].type = VDBF_BOOL;
                r.data[i].as.Bool = *((bool*)(buf + off));
                off += sizeof(bool);
                break;
        }
    }

    return r;
}

//[type, idx, count ... offsets ... cells]
struct VdbNode _tree_deserialize_leaf(uint8_t* buf, struct VdbSchema* schema) {
    struct VdbNode node;
    int off = 0;
    _tree_read_u32(&node.type, buf, &off);
    _tree_read_u32(&node.idx, buf, &off);

    uint32_t rec_count;
    _tree_read_u32(&rec_count, buf, &off);

    node.as.leaf.rl = _rl_alloc();
    int idx_off = VDB_OFFSETS_START;
   
    for (uint32_t i = 0; i < rec_count; i++) {
        uint32_t rec_off;
        _tree_read_u32(&rec_off, buf, &idx_off);
        _rl_append(node.as.leaf.rl, _tree_deserialize_record(buf + rec_off, schema));
    }

    return node;
}

uint32_t _tree_get_rec_size(struct VdbRecord* rec) {
    uint32_t data_size = sizeof(uint32_t); //key size
    for (uint32_t i = 0; i < rec->count; i++) {
        struct VdbDatum* f = &rec->data[i];
        switch (f->type) {
            case VDBF_INT:
                data_size += sizeof(uint64_t);
                break;
            case VDBF_STR:
                data_size += sizeof(uint32_t) + f->as.Str->len;
                break;
            case VDBF_BOOL:
                data_size += sizeof(bool);
                break;
        }
    }

    return data_size;
}

void _tree_serialize_record(uint8_t* buf, struct VdbRecord* rec) {
    int off = 0;
    _tree_write_u32(buf, rec->key, &off);
    for (uint32_t i = 0; i < rec->count; i++) {
        struct VdbDatum* d = &rec->data[i];
        switch (d->type) {
            case VDBF_INT: {
                memcpy(buf + off, &d->as.Int, sizeof(uint64_t));
                off += sizeof(uint64_t);
                break;
            }
            case VDBF_STR: {
                memcpy(buf + off, &d->as.Str->len, sizeof(uint32_t));
                off += sizeof(uint32_t);
                memcpy(buf + off, d->as.Str->start, d->as.Str->len);
                off += d->as.Str->len; 
                break;
            }
            case VDBF_BOOL: {
                bool b = d->as.Bool;
                memcpy(buf + off, &b, sizeof(bool));
                off += sizeof(bool);
                break;
            }
        }
    }
}

void _tree_serialize_leaf(uint8_t* buf, struct VdbNode* node) {
    int off = 0;
    _tree_write_u32(buf, (uint32_t)node->type, &off);
    _tree_write_u32(buf, (uint32_t)node->idx, &off);
    _tree_write_u32(buf, (uint32_t)node->as.leaf.rl->count, &off);

    off = VDB_OFFSETS_START;
    uint32_t rec_off = VDB_PAGE_SIZE;

    for (uint32_t i = 0; i < node->as.leaf.rl->count; i++) {
        struct VdbRecord* rec = &node->as.leaf.rl->records[i];
        rec_off -= _tree_get_rec_size(rec);
        _tree_write_u32(buf, rec_off, &off);
        _tree_serialize_record(buf + rec_off, rec);
    }
}

void _tree_serialize_node(uint8_t* buf, struct VdbNode* node) {
    enum VdbNodeType type = (enum VdbNodeType)(*((uint32_t*)buf));
    switch (type) {
        case VDBN_META:
            _tree_serialize_meta(buf, node);
            break;
        case VDBN_INTERN:
            _tree_serialize_intern(buf, node);
            break;
        case VDBN_LEAF:
            _tree_serialize_leaf(buf, node);
            break;
    }
}

void _tree_init_meta(struct VdbTree* tree, struct VdbSchema* schema) {
    uint32_t idx = pager_allocate_page(tree->f);
    struct VdbPage* page = pager_get_page(tree->pager, tree->f, idx);

    struct VdbNode meta;
    meta.type = VDBN_META;
    meta.idx = idx;
    meta.as.meta.pk_counter = 0;
    meta.as.meta.schema = schema;

    _tree_serialize_meta(page->buf, &meta);
    pager_flush_page(page);

}

struct VdbNodePtr _tree_init_leaf(struct VdbTree* tree) {
    uint32_t idx = pager_allocate_page(tree->f);
    struct VdbPage* page = pager_get_page(tree->pager, tree->f, idx);

    struct VdbNode leaf;
    leaf.type = VDBN_LEAF;
    leaf.idx = idx;
    leaf.as.leaf.rl = _rl_alloc();

    _tree_serialize_leaf(page->buf, &leaf);
    _rl_free(leaf.as.leaf.rl);
    pager_flush_page(page);

    uint32_t key = 0;
    struct VdbNodePtr ptr = {key, idx};
    return ptr;
}

void _tree_init_root(struct VdbTree* tree) {
    uint32_t idx = pager_allocate_page(tree->f);
    struct VdbPage* page = pager_get_page(tree->pager, tree->f, idx);

    struct VdbNode root;
    root.type = VDBN_INTERN;
    root.idx = idx;
    root.as.intern.pl = _pl_alloc();
    _pl_append(root.as.intern.pl, _tree_init_leaf(tree));

    _tree_serialize_intern(page->buf, &root);

    free(root.as.intern.pl->pointers);
    free(root.as.intern.pl);
    pager_flush_page(page);
}


void tree_init(struct VdbTree* tree, struct VdbSchema* schema) {
    _tree_init_meta(tree, schema);
    _tree_init_root(tree);
}

void _tree_release_node(struct VdbTree* tree, struct VdbNode node) {
    struct VdbPage* page = pager_get_page(tree->pager, tree->f, node.idx);
    _tree_serialize_node(page->buf, &node);
    pager_flush_page(page);

    enum VdbNodeType type = (enum VdbNodeType)(*((uint32_t*)page->buf));
    switch (type) {
        case VDBN_META:
            free(node.as.meta.schema->fields);
            free(node.as.meta.schema);
            break;
        case VDBN_INTERN:
            free(node.as.intern.pl->pointers);
            free(node.as.intern.pl);
            break;
        case VDBN_LEAF:
            free(node.as.leaf.rl->records);
            free(node.as.leaf.rl);
            break;
    }

}

struct VdbNode _tree_catch_node(struct VdbTree* tree, uint32_t idx) {
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
        case VDBN_LEAF: {
            struct VdbNode meta = _tree_catch_node(tree, 0);
            node = _tree_deserialize_leaf(page->buf, meta.as.meta.schema);
            _tree_release_node(tree, meta);
            break;
        }
    }

    return node;
}


void tree_insert_record(struct VdbTree* tree, struct VdbRecord* rec) {
    struct VdbNode meta = _tree_catch_node(tree, 0);    
    struct VdbNode root = _tree_catch_node(tree, 1);    

    uint32_t key = ++meta.as.meta.pk_counter;
    for (uint32_t i = 0; i < root.as.intern.pl->count; i++) {
        struct VdbNodePtr* ptr = &root.as.intern.pl->pointers[i];
        ptr->key = key; //TODO: temp to make sure adding to leaf works - need to traverse tree instead of doing this
        if (key <= ptr->key) {
            struct VdbNode leaf = _tree_catch_node(tree, ptr->idx);
            rec->key = key;
            _rl_append(leaf.as.leaf.rl, *rec);
            _tree_release_node(tree, leaf);
            break;
        }
    }

    _tree_release_node(tree, meta);
    _tree_release_node(tree, root);
}

void _debug_print_node(struct VdbTree* tree, uint32_t idx, int depth) {
    struct VdbNode node = _tree_catch_node(tree, idx);

    switch (node.type) {
        case VDBN_INTERN:
            printf("%*s%d\n", depth * 4, "", node.idx);
            for (uint32_t i = 0; i < node.as.intern.pl->count; i++) {
                struct VdbNodePtr* p = &node.as.intern.pl->pointers[i];
                _debug_print_node(tree, p->idx, depth + 1);
            }
            break;
        case VDBN_LEAF:
            printf("%*s%d: ", depth * 4, "", node.idx);
            for (uint32_t i = 0; i < node.as.leaf.rl->count; i++) {
                struct VdbRecord* r = &node.as.leaf.rl->records[i];
                printf("%d, ", r->key);
            }
            printf("\n");
            break;
        default:
            break;
    }
   
    _tree_release_node(tree, node);
}

void debug_print_tree(struct VdbTree* tree) {
    _debug_print_node(tree, 1, 0);
}

