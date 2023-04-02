#include <string.h>
#include <stdlib.h>
#include <assert.h>

#include "tree.h"
#include "node.h"
#include "util.h"


void _tree_init_meta(struct VdbTree* tree, struct VdbSchema* schema) {
    uint32_t idx = pager_allocate_page(tree->f);
    struct VdbPage* page = pager_get_page(tree->pager, tree->f, idx);

    struct VdbNode meta;
    meta.type = VDBN_META;
    meta.idx = idx;
    meta.as.meta.pk_counter = 0;
    meta.as.meta.schema = schema;

    node_serialize(page->buf, &meta);
    pager_flush_page(page);

}

struct VdbNodePtr _tree_init_leaf(struct VdbTree* tree) {
    uint32_t idx = pager_allocate_page(tree->f);
    struct VdbPage* page = pager_get_page(tree->pager, tree->f, idx);

    struct VdbNode leaf = node_init_leaf(idx);
    node_serialize(page->buf, &leaf);

    node_free(&leaf);
    pager_flush_page(page);

    uint32_t key = 0;
    struct VdbNodePtr ptr = {key, idx};
    return ptr;
}

void _tree_init_intern(struct VdbTree* tree) {
    uint32_t idx = pager_allocate_page(tree->f); //should be index 1
    struct VdbPage* page = pager_get_page(tree->pager, tree->f, idx);

    struct VdbNode root = node_init_intern(idx);
    node_append_nodeptr(&root, _tree_init_leaf(tree));
    node_serialize(page->buf, &root);

    node_free(&root);
    pager_flush_page(page);
}

void tree_init(struct VdbTree* tree, struct VdbSchema* schema) {
    _tree_init_meta(tree, schema);
    _tree_init_intern(tree);
}

void _tree_release_node(struct VdbTree* tree, struct VdbNode node) {
    struct VdbPage* page = pager_get_page(tree->pager, tree->f, node.idx);
    node_serialize(page->buf, &node);

    pager_flush_page(page);
    node_free(&node);
}

struct VdbNode _tree_catch_node(struct VdbTree* tree, uint32_t idx) {
    struct VdbPage* page = pager_get_page(tree->pager, tree->f, idx);

    enum VdbNodeType type = (enum VdbNodeType)(*((uint32_t*)page->buf));
    struct VdbNode node;
    switch (type) {
        case VDBN_META:
            node = node_deserialize_meta(page->buf);
            break;
        case VDBN_INTERN:
            node = node_deserialize_intern(page->buf);
            break;
        case VDBN_LEAF: {
            struct VdbNode meta = _tree_catch_node(tree, 0);
            node = node_deserialize_leaf(page->buf, meta.as.meta.schema);
            _tree_release_node(tree, meta);
            break;
        }
    }

    return node;
}

void _tree_do_traversal(struct VdbTree* tree, struct VdbNode* node, uint32_t key, struct U32List* idx_list) {
    u32l_append(idx_list, node->idx);

    for (uint32_t i = 0; i < node->as.intern.pl->count; i++) {
        struct VdbNodePtr* ptr = &node->as.intern.pl->pointers[i];
        if (key <= ptr->key) {
            struct VdbNode child = _tree_catch_node(tree, ptr->idx);
            if (child.type == VDBN_LEAF) {
                u32l_append(idx_list, ptr->idx);
            } else {
                _tree_do_traversal(tree, &child, key, idx_list);
            }
            _tree_release_node(tree, child);
            return;
        }
    }

    //if key is larger than right pointer, update right pointer
    struct VdbNodePtr* ptr = &node->as.intern.pl->pointers[node->as.intern.pl->count - 1];
    if (key > ptr->key) {
        ptr->key = key;
        struct VdbNode child = _tree_catch_node(tree, ptr->idx);
        if (child.type == VDBN_LEAF) {
            u32l_append(idx_list, ptr->idx);
        } else {
            _tree_do_traversal(tree, &child, key, idx_list);
        }
        _tree_release_node(tree, child);
    }
}

struct U32List* _tree_traverse_to(struct VdbTree* tree, uint32_t key) {
    struct U32List* idx_list = u32l_alloc();
    struct VdbNode root = _tree_catch_node(tree, 1);
    _tree_do_traversal(tree, &root, key, idx_list);
    _tree_release_node(tree, root);
    return idx_list;
}

void _tree_split_leaf(struct VdbTree* tree, struct U32List* idx_list) {
    struct VdbNode parent = _tree_catch_node(tree, idx_list->values[idx_list->count - 2]);
    
    //new right pointer is necessary
    struct VdbNodePtr* prev_right = &parent.as.intern.pl->pointers[parent.as.intern.pl->count - 1];

    struct VdbNodePtr new_right = _tree_init_leaf(tree);
    new_right.key = prev_right->key;
    prev_right->key--;
    node_append_nodeptr(&parent, new_right);

    _tree_release_node(tree, parent);
}

void tree_insert_record(struct VdbTree* tree, struct VdbRecord* rec) {
    struct VdbNode meta = _tree_catch_node(tree, 0);    

    uint32_t key = ++meta.as.meta.pk_counter;
    struct U32List* idx_list = _tree_traverse_to(tree, key);

    //check if leaf is large enough to hold new rec
    struct VdbNode leaf = _tree_catch_node(tree, idx_list->values[idx_list->count - 1]);
    uint32_t insert_size = vdb_get_rec_size(rec) + sizeof(uint32_t); //record size + index size
    if (node_is_full(&leaf, insert_size)) {
        _tree_release_node(tree, leaf);
        _tree_split_leaf(tree, idx_list);
        u32l_free(idx_list);
        idx_list = _tree_traverse_to(tree, key);

        leaf = _tree_catch_node(tree, idx_list->values[idx_list->count - 1]);
    }

    rec->key = key;
    node_append_record(&leaf, *rec);

    u32l_free(idx_list);
    _tree_release_node(tree, meta);
    _tree_release_node(tree, leaf);
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

