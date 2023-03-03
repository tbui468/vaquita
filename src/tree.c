#include <string.h>
#include <stdlib.h>
#include <assert.h>

#include "tree.h"
#include "util.h"
#include "pager.h"


void _tree_free_meta(struct NodeMeta* m) {
    free(m->schema->fields);
    free(m->schema);
    free(m);
}

void _tree_read_uint32_t(uint32_t* dst, uint8_t* buf, int* off) {
    *dst = *((uint32_t*)(buf + *off));
    *off += sizeof(uint32_t);
}

void _tree_write_uint32_t(uint8_t* dst, uint32_t v, int* off) {
    *((uint32_t*)(dst + *off)) = v;
    *off += sizeof(uint32_t);
}

struct NodeMeta* _tree_deserialize_meta(uint8_t* buf) {
    struct NodeMeta* m = malloc_w(sizeof(struct NodeMeta));
    struct VdbSchema* s = malloc_w(sizeof(struct VdbSchema));
    m->schema = s;
    int off = 0;

    _tree_read_uint32_t(&m->type, buf, &off);
    _tree_read_uint32_t(&m->node_size, buf, &off);
    _tree_read_uint32_t(&m->offsets_size, buf, &off);
    _tree_read_uint32_t(&m->cells_size, buf, &off);
    _tree_read_uint32_t(&m->freelist, buf, &off);
    _tree_read_uint32_t(&m->parent_idx, buf, &off);
    _tree_read_uint32_t(&m->right_ptr.next, buf, &off);
    _tree_read_uint32_t(&m->right_ptr.key, buf, &off);
    _tree_read_uint32_t(&m->right_ptr.block_idx, buf, &off);
    _tree_read_uint32_t(&m->schema->count, buf, &off);

    m->schema->fields = malloc_w(sizeof(enum VdbField) * m->schema->count);
    for (uint32_t i = 0; i < m->schema->count; i++) {
        uint32_t f;
        _tree_read_uint32_t(&f, buf, &off);
        m->schema->fields[i] = f;
    }

    return m;
}

int _tree_serialize_meta(uint8_t* buf, struct NodeMeta* m) {
    int off = 0;
    _tree_write_uint32_t(buf, (uint32_t)m->type, &off);
    _tree_write_uint32_t(buf, (uint32_t)m->node_size, &off);
    _tree_write_uint32_t(buf, (uint32_t)m->offsets_size, &off);
    _tree_write_uint32_t(buf, (uint32_t)m->cells_size, &off);
    _tree_write_uint32_t(buf, (uint32_t)m->freelist, &off);
    _tree_write_uint32_t(buf, (uint32_t)m->parent_idx, &off);
    _tree_write_uint32_t(buf, (uint32_t)m->right_ptr.next, &off);
    _tree_write_uint32_t(buf, (uint32_t)m->right_ptr.key, &off);
    _tree_write_uint32_t(buf, (uint32_t)m->right_ptr.block_idx, &off);
    _tree_write_uint32_t(buf, (uint32_t)m->schema->count, &off);
    for (uint32_t i = 0; i < m->schema->count; i++) {
        uint32_t field = m->schema->fields[i];
        _tree_write_uint32_t(buf, field, &off);
    }

    return off;
}

struct NodeCell _tree_deserialize_nodecell(uint8_t* buf) {
    struct NodeCell nc;

    int off = 0;
    _tree_read_uint32_t(&nc.next, buf, &off);
    _tree_read_uint32_t(&nc.key, buf, &off);
    _tree_read_uint32_t(&nc.block_idx, buf, &off);

    return nc;
}

void _tree_serialize_nodecell(uint8_t* buf, struct NodeCell* nc) {
    int off = 0;
    _tree_write_uint32_t(buf, nc->next, &off); 
    _tree_write_uint32_t(buf, nc->key, &off); 
    _tree_write_uint32_t(buf, nc->block_idx, &off); 
}

int _tree_serialize_datacell(uint8_t* buf, struct DataCell* dc) {
    struct VdbData* d = dc->data;
    int off = 0;
    _tree_write_uint32_t(buf, dc->next, &off);
    _tree_write_uint32_t(buf, dc->key, &off);
    _tree_write_uint32_t(buf, dc->size, &off);
    for (uint32_t i = 0; i < d->count; i++) {
        struct VdbDatum* f = &d->data[i];
        switch (f->type) {
        case VDBF_INT: {
            memcpy(buf + off, &f->as.Int, sizeof(uint64_t));
            off += sizeof(uint64_t);
            break;
        }
        case VDBF_STR: {
            memcpy(buf + off, &f->as.Str->len, sizeof(uint32_t));
            off += sizeof(uint32_t);
            memcpy(buf + off, f->as.Str->start, f->as.Str->len);
            off += f->as.Str->len; 
            break;
        }
        case VDBF_BOOL: {
            bool b = f->as.Bool;
            memcpy(buf + off, &b, sizeof(bool));
            off += sizeof(bool);
            break;
        }
        }
    }

    return off;
}

struct DataCell* _tree_deserialize_datacell(uint8_t* buf, struct VdbSchema* schema) {
    struct DataCell* dc = malloc_w(sizeof(struct DataCell));
    int off = 0;
    _tree_read_uint32_t(&dc->next, buf, &off);
    _tree_read_uint32_t(&dc->key, buf, &off);
    _tree_read_uint32_t(&dc->size, buf, &off);
    dc->data = malloc_w(sizeof(struct VdbData));
    dc->data->count = schema->count;
    dc->data->data = malloc_w(sizeof(struct VdbDatum) * schema->count);

    for (uint32_t i = 0; i < schema->count; i++) {
        enum VdbField f = schema->fields[i];
        switch (f) {
        case VDBF_INT:
            dc->data->data[i].type = VDBF_INT;
            dc->data->data[i].as.Int = *((uint64_t*)(buf + off));
            off += sizeof(uint64_t);
            break;
        case VDBF_STR:
            dc->data->data[i].type = VDBF_STR;
            dc->data->data[i].as.Str = malloc_w(sizeof(struct VdbString));
            _tree_read_uint32_t(&dc->data->data[i].as.Str->len, buf, &off);
            dc->data->data[i].as.Str->start = malloc_w(sizeof(char) * dc->data->data[i].as.Str->len);
            memcpy(dc->data->data[i].as.Str->start, buf + off, dc->data->data[i].as.Str->len);
            off += dc->data->data[i].as.Str->len;
            break;
        case VDBF_BOOL:
            dc->data->data[i].type = VDBF_BOOL;
            dc->data->data[i].as.Bool = *((bool*)(buf + off));
            off += sizeof(bool);
            break;
        }
    }

    return dc;
}

void _tree_free_datacell(struct DataCell* dc) {
    vdb_free_data(dc->data);
    free(dc);
}

void tree_init(FILE* f, struct VdbSchema* schema) {
    struct NodeMeta m;
    m.type = VDBN_INTERN;
    m.node_size = VDB_PAGE_SIZE;
    m.offsets_size = 0;
    m.cells_size = 0;
    m.freelist = 0;
    m.parent_idx = 0;
    struct NodeCell nc = {0, 1, 1};
    m.right_ptr = nc;
    m.schema = schema;

    //init root
    uint8_t* buf = calloc_w(VDB_PAGE_SIZE, sizeof(uint8_t));
    _tree_serialize_meta(buf, &m);

    fseek_w(f, 0, SEEK_SET);
    fwrite_w(buf, sizeof(uint8_t), VDB_PAGE_SIZE, f);

    free(buf);

    //init first leaf
    struct NodeMeta lm;
    lm.type = VDBN_LEAF;
    lm.node_size = VDB_PAGE_SIZE;
    lm.offsets_size = 0;
    lm.cells_size = 0;
    lm.freelist = 0;
    lm.parent_idx = 0;
    struct NodeCell lnc = {0, 0, 0};
    lm.right_ptr = lnc; //Not used
    lm.schema = schema;

    uint8_t* buf2 = calloc_w(VDB_PAGE_SIZE, sizeof(uint8_t));
    _tree_serialize_meta(buf2, &lm);
    fseek_w(f, VDB_PAGE_SIZE, SEEK_SET);
    fwrite_w(buf2, sizeof(uint8_t), VDB_PAGE_SIZE, f);
    free(buf2);
}

struct VdbPage* _tree_traverse_to_leaf(struct VdbPager* pager, FILE* f, struct VdbPage* node, uint32_t pk) {
    struct NodeMeta* m = _tree_deserialize_meta(node->buf);
    assert(m->type == VDBN_INTERN);

    //TODO: switch to binary search
    int child_idx = -1;
    uint32_t i = OFFSETS_START;
    while (i < OFFSETS_START + m->offsets_size) {
        uint32_t off = *((uint32_t*)&node->buf[i]);
        struct NodeCell nc = _tree_deserialize_nodecell(&node->buf[off]);
        if (i == OFFSETS_START) {
            if (pk <= nc.key) {
                child_idx = nc.block_idx;
                break;
            }
        } else {
            uint32_t prev_off = *((uint32_t*)&node->buf[i - sizeof(uint32_t)]);
            struct NodeCell pnc = _tree_deserialize_nodecell(&node->buf[prev_off]);
            if (pnc.key < pk && pk <= nc.key) {
                child_idx = nc.block_idx;
                break;
            }
        }

        i += sizeof(uint32_t);
    }

    if (child_idx == -1) {
        child_idx = m->right_ptr.block_idx;
    }
    struct VdbPage* child = pager_get_page(pager, f, child_idx);
    struct NodeMeta* cm = _tree_deserialize_meta(child->buf);

    if (cm->type == VDBN_INTERN) {
        child = _tree_traverse_to_leaf(pager, f, child, pk);
    }

    _tree_free_meta(cm); 
    _tree_free_meta(m);
    return child;
}

int _tree_data_size(struct VdbData* d) {
    uint32_t data_size = 0;
    for (uint32_t i = 0; i < d->count; i++) {
        struct VdbDatum* f = &d->data[i];
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

bool _tree_node_is_full(struct VdbPage* node, uint32_t dc_size) {
    struct NodeMeta* lm = _tree_deserialize_meta(node->buf);
    uint32_t current_size = OFFSETS_START + lm->cells_size + lm->offsets_size;
    _tree_free_meta(lm);
    return current_size + dc_size >= VDB_PAGE_SIZE;
}

struct VdbPage* _tree_split_internal(struct VdbPager* pager, FILE* f, struct VdbPage* node) {
    uint32_t left_idx = pager_allocate_page(f);
    uint32_t right_idx = pager_allocate_page(f);

    struct VdbPage* left_internal = pager_get_page(pager, f, left_idx);
    struct VdbPage* right_internal = pager_get_page(pager, f, right_idx);

    struct NodeMeta* nm = _tree_deserialize_meta(node->buf);
    uint32_t offsets_mid = OFFSETS_START + nm->offsets_size / sizeof(uint32_t) / 2 * sizeof(uint32_t);

    //copy data from internal to left/right internal
    struct NodeMeta lm;
    lm.type = VDBN_INTERN;
    lm.node_size = VDB_PAGE_SIZE;
    lm.offsets_size = 0;
    lm.cells_size = 0;
    lm.freelist = 0;
    lm.parent_idx = node->idx;
    uint32_t off_mid = *((uint32_t*)(node->buf + offsets_mid - sizeof(uint32_t)));
    lm.right_ptr = _tree_deserialize_nodecell(node->buf + off_mid);
    lm.schema = nm->schema;

    struct NodeMeta rm;
    rm.type = VDBN_INTERN;
    rm.node_size = VDB_PAGE_SIZE;
    rm.offsets_size = 0;
    rm.cells_size = 0;
    rm.freelist = 0;
    rm.parent_idx = node->idx;
    uint32_t off_end = *((uint32_t*)(node->buf + OFFSETS_START + nm->offsets_size - sizeof(uint32_t)));
    rm.right_ptr = _tree_deserialize_nodecell(node->buf + off_end);
    rm.schema = nm->schema;

    for (uint32_t i = OFFSETS_START; i < offsets_mid - sizeof(uint32_t); i += sizeof(uint32_t)) {
        uint32_t off = *((uint32_t*)(node->buf + i));
        struct NodeCell nc = _tree_deserialize_nodecell(node->buf + off);
        lm.cells_size += sizeof(uint32_t) * 3;
        uint32_t new_off = VDB_PAGE_SIZE - lm.cells_size;
        _tree_serialize_nodecell(left_internal->buf + new_off, &nc);
        memcpy(left_internal->buf + OFFSETS_START + lm.offsets_size, &new_off, sizeof(uint32_t));
        lm.offsets_size += sizeof(uint32_t);
    }

    for (uint32_t i = offsets_mid; i < OFFSETS_START + nm->offsets_size - sizeof(uint32_t); i += sizeof(uint32_t)) {
        uint32_t off = *((uint32_t*)(node->buf + i));
        struct NodeCell nc = _tree_deserialize_nodecell(node->buf + off);
        rm.cells_size += sizeof(uint32_t) * 3;
        uint32_t new_off = VDB_PAGE_SIZE - rm.cells_size;
        _tree_serialize_nodecell(right_internal->buf + new_off, &nc);
        memcpy(right_internal->buf + OFFSETS_START + rm.offsets_size, &new_off, sizeof(uint32_t));
        rm.offsets_size += sizeof(uint32_t);
    }

    memset(node->buf, 0, VDB_PAGE_SIZE);
    nm->offsets_size = 0;
    nm->cells_size = 0;
    struct NodeCell new_right = { 0, rm.right_ptr.key, right_idx };
    nm->right_ptr = new_right;

    nm->cells_size += sizeof(uint32_t) * 3;
    uint32_t left_off = VDB_PAGE_SIZE - nm->cells_size;
    struct NodeCell nc = { 0, lm.right_ptr.key, left_idx };
    _tree_serialize_nodecell(node->buf + left_off, &nc);
    memcpy(node->buf + OFFSETS_START + nm->offsets_size, &left_off, sizeof(uint32_t));
    nm->offsets_size += sizeof(uint32_t);

    _tree_serialize_meta(left_internal->buf, &lm);
    _tree_serialize_meta(right_internal->buf, &rm);
    _tree_serialize_meta(node->buf, nm);

    _tree_free_meta(nm);
    return node;
}

void _tree_split_leaf(struct VdbPager* pager, FILE* f, struct VdbPage* leaf) {
    struct NodeMeta* lm = _tree_deserialize_meta(leaf->buf);
    struct VdbPage* parent = pager_get_page(pager, f, lm->parent_idx);

    uint32_t new_idx = pager_allocate_page(f);
    struct VdbPage* new_leaf = pager_get_page(pager, f, new_idx);

    struct NodeMeta nlm;
    nlm.type = VDBN_LEAF;
    nlm.node_size = VDB_PAGE_SIZE;
    nlm.offsets_size = 0;
    nlm.cells_size = 0;
    nlm.freelist = 0;
    nlm.parent_idx = lm->parent_idx;
    nlm.right_ptr = lm->right_ptr;
    nlm.schema = lm->schema;

    _tree_serialize_meta(new_leaf->buf, &nlm); //TODO: is this necessary?  We should only need to serialize after copying all data over

    uint8_t* buf = malloc_w(VDB_PAGE_SIZE);
    memcpy(buf, leaf->buf, VDB_PAGE_SIZE);
    memset(leaf->buf, 0, VDB_PAGE_SIZE);

    uint32_t prev_offsets_size = lm->offsets_size;

    lm->offsets_size = 0;
    lm->cells_size = 0;
    lm->freelist = 0;

    uint32_t half = prev_offsets_size / sizeof(uint32_t) / 2 * sizeof(uint32_t);
    uint32_t final_key = 0;
    for (uint32_t i = OFFSETS_START; i < OFFSETS_START + half; i += sizeof(uint32_t)) {
        uint32_t off = *((uint32_t*)(buf + i));
        
        struct DataCell* dc = _tree_deserialize_datacell(buf + off, nlm.schema);
        nlm.cells_size += sizeof(uint32_t) * 3 + dc->size;
        _tree_serialize_datacell(new_leaf->buf + VDB_PAGE_SIZE - nlm.cells_size, dc);
        final_key = dc->key;
        _tree_free_datacell(dc);

        uint32_t new_offset = VDB_PAGE_SIZE - nlm.cells_size;
        memcpy(new_leaf->buf + OFFSETS_START + nlm.offsets_size, &new_offset, sizeof(uint32_t));
        nlm.offsets_size += sizeof(uint32_t);
    }
    for (uint32_t i = OFFSETS_START + half; i < OFFSETS_START + prev_offsets_size; i += sizeof(uint32_t)) {
        uint32_t off = *((uint32_t*)(buf + i));
        struct DataCell* dc = _tree_deserialize_datacell(buf + off, lm->schema);
        lm->cells_size += sizeof(uint32_t) * 3 + dc->size;
        _tree_serialize_datacell(leaf->buf + VDB_PAGE_SIZE - lm->cells_size, dc);
        _tree_free_datacell(dc);

        uint32_t new_offset = VDB_PAGE_SIZE - lm->cells_size;
        memcpy(leaf->buf + OFFSETS_START + lm->offsets_size, &new_offset, sizeof(uint32_t));
        lm->offsets_size += sizeof(uint32_t);
    }
    _tree_serialize_meta(leaf->buf, lm);
    _tree_serialize_meta(new_leaf->buf, &nlm);

    free(buf);
    _tree_free_meta(lm);


    uint32_t nc_size = sizeof(uint32_t) * 3;
    if (_tree_node_is_full(parent, nc_size)) {
        parent = _tree_split_internal(pager, f, parent);
        struct NodeMeta* m = _tree_deserialize_meta(parent->buf);
        parent = pager_get_page(pager, f, m->right_ptr.block_idx);
        _tree_free_meta(m);
    }

    struct NodeCell nc;
    nc.next = 0;
    nc.key = final_key;
    nc.block_idx = new_idx;

    struct NodeMeta* pm = _tree_deserialize_meta(parent->buf);
    pm->cells_size += sizeof(uint32_t) * 3;
    uint32_t dc_off = VDB_PAGE_SIZE - pm->cells_size;
    memcpy(parent->buf + OFFSETS_START + pm->offsets_size, &dc_off, sizeof(uint32_t));
    pm->offsets_size += sizeof(uint32_t);
    _tree_serialize_nodecell(parent->buf + dc_off, &nc);
    _tree_serialize_meta(parent->buf, pm);

    _tree_free_meta(pm);
}

void tree_insert_record(struct VdbPager* pager, FILE* f, struct VdbData* d) {
    uint32_t root_idx = 0;
    struct VdbPage* root = pager_get_page(pager, f, root_idx);
    struct NodeMeta* m = _tree_deserialize_meta(root->buf);

    uint32_t pk = m->right_ptr.key;

    struct VdbPage* leaf = _tree_traverse_to_leaf(pager, f, root, pk);
    struct NodeMeta* lm = _tree_deserialize_meta(leaf->buf);

    uint32_t data_size = _tree_data_size(d);
    uint32_t dc_meta_size = sizeof(uint32_t) * 3;
    struct DataCell dc = { 0, pk, data_size, d };

    if (_tree_node_is_full(leaf, dc_meta_size + data_size)) {
        _tree_split_leaf(pager, f, leaf);
        _tree_free_meta(m);
        _tree_free_meta(lm);
        leaf = _tree_traverse_to_leaf(pager, f, root, pk);
        m = _tree_deserialize_meta(root->buf);
        lm = _tree_deserialize_meta(leaf->buf);
    }

    _tree_serialize_datacell(leaf->buf + VDB_PAGE_SIZE - lm->cells_size - data_size - dc_meta_size, &dc);

    //write offset
    uint32_t data_off = VDB_PAGE_SIZE - lm->cells_size - data_size - dc_meta_size;
    memcpy(leaf->buf + OFFSETS_START + lm->offsets_size, &data_off, sizeof(uint32_t));

    lm->offsets_size += sizeof(uint32_t);
    lm->cells_size += data_size + dc_meta_size;
    _tree_serialize_meta(leaf->buf, lm);

    m->right_ptr.key++;
    _tree_serialize_meta(root->buf, m);

    //TODO: flush pages here to test if function works
    //flushing should be a pager function
    fseek_w(f, 0, SEEK_SET);
    fwrite_w(root->buf, VDB_PAGE_SIZE, 1, f);
    fwrite_w(leaf->buf, VDB_PAGE_SIZE, 1, f);

    //    pager_flush(root);
    //    pager_flush(leaf);

    _tree_free_meta(m);
    _tree_free_meta(lm);
}


struct VdbData* tree_fetch_record(struct VdbPager* pager, FILE* f, uint32_t key) {
    uint32_t root_idx = 0;
    struct VdbPage* root = pager_get_page(pager, f, root_idx);
    struct VdbPage* leaf = _tree_traverse_to_leaf(pager, f, root, key);
    struct NodeMeta* lm = _tree_deserialize_meta(leaf->buf);

    struct DataCell* dc = NULL;
    for (uint32_t i = OFFSETS_START; i < OFFSETS_START + lm->offsets_size; i += sizeof(uint32_t)) {
        uint32_t offset = *((uint32_t*)&leaf->buf[i]);
        dc = _tree_deserialize_datacell(leaf->buf + offset, lm->schema);

        if (dc->key == key)
            break;

        _tree_free_datacell(dc);
        dc = NULL;
    }

    if (!dc)
        return NULL;

    struct VdbData* result = dc->data;
    free(dc);
    _tree_free_meta(lm);

    return result;
}

void tree_print_keys(struct VdbPager* pager, FILE* f, uint32_t idx) {
    struct VdbPage* node = pager_get_page(pager, f, idx);    
    struct NodeMeta* m = _tree_deserialize_meta(node->buf);
    printf("Idx: %d, Type: %d, Keys: ", idx, m->type);
    if (m->type == VDBN_LEAF) {
        for (uint32_t i = OFFSETS_START; i < OFFSETS_START + m->offsets_size; i += sizeof(uint32_t)) {
            uint32_t off = *((uint32_t*)(node->buf + i));
            struct DataCell* dc = _tree_deserialize_datacell(node->buf + off, m->schema);
            printf("%d, ", dc->key);
            _tree_free_datacell(dc);
        }
        printf("\n");
    } else {
        for (uint32_t i = OFFSETS_START; i < OFFSETS_START + m->offsets_size; i += sizeof(uint32_t)) {
            uint32_t off = *((uint32_t*)(node->buf + i));
            struct NodeCell nc = _tree_deserialize_nodecell(node->buf + off);
            printf("%d, ", nc.key);
        }
        printf("%d\n", m->right_ptr.key);
    }
}

void tree_print_node(struct VdbPager* pager, FILE* f, uint32_t idx) {
    struct VdbPage* node = pager_get_page(pager, f, idx);    
    struct NodeMeta* m = _tree_deserialize_meta(node->buf);
    printf("Node idx: %d, Free: %d, Type: %d\n", idx, VDB_PAGE_SIZE - OFFSETS_START - m->offsets_size - m->cells_size, m->type);

    if (m->type == VDBN_INTERN) {
        tree_print_node(pager, f, m->right_ptr.block_idx);

        for (uint32_t i = OFFSETS_START; i < OFFSETS_START + m->offsets_size; i += sizeof(uint32_t)) {
            uint32_t off = *((uint32_t*)(node->buf + i));
            struct NodeCell nc = _tree_deserialize_nodecell(node->buf + off);
            tree_print_node(pager, f, nc.block_idx);
        }

    } else {
        /*
        //print out all leaf keys
        for (uint32_t i = OFFSETS_START; i < OFFSETS_START + m->offsets_size; i += sizeof(uint32_t)) {
            uint32_t off = *((uint32_t*)(node->buf + i));
            struct DataCell* dc = _tree_deserialize_datacell(node->buf + off, m->schema);
            printf("%d,", dc->key);
            _tree_free_datacell(dc);
        }
        printf("\n");*/
    }

    _tree_free_meta(m);
}



