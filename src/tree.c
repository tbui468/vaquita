#include <string.h>
#include <stdlib.h>
#include <assert.h>

#include "tree.h"
#include "node.h"
#include "util.h"


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

uint32_t _tree_init_leaf(struct VdbTree* tree) {
    uint32_t idx = pager_allocate_page(tree->f);
    struct VdbPage* page = pager_get_page(tree->pager, tree->f, idx);

    struct VdbNode leaf = node_init_leaf(idx);
    node_serialize(page->buf, &leaf);

    node_free(&leaf);
    pager_flush_page(page);

    return idx;
}

uint32_t _tree_init_intern(struct VdbTree* tree) {
    uint32_t idx = pager_allocate_page(tree->f);
    struct VdbPage* page = pager_get_page(tree->pager, tree->f, idx);

    struct VdbNode root = node_init_intern(idx);
    node_serialize(page->buf, &root);

    node_free(&root);
    pager_flush_page(page);

    return idx;
}

void tree_init(struct VdbTree* tree, struct VdbSchema* schema) {
    _tree_init_meta(tree, schema);
    uint32_t root_idx = _tree_init_intern(tree);
    uint32_t leaf_idx = _tree_init_leaf(tree);

    struct VdbNode root = _tree_catch_node(tree, root_idx);
    struct VdbNodePtr ptr = {0, leaf_idx};
    node_append_nodeptr(&root, ptr);
    _tree_release_node(tree, root);
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

uint32_t _tree_split_internal(struct VdbTree* tree, struct U32List* idx_list, uint32_t idx) {
    struct VdbNode intern = _tree_catch_node(tree, idx);
    struct VdbNode meta = _tree_catch_node(tree, 0);
    if (node_is_root(&intern)) {
        uint32_t left_idx = _tree_init_intern(tree);
        uint32_t right_idx = _tree_init_intern(tree);

        struct VdbNode left = _tree_catch_node(tree, left_idx);
        struct VdbNode right = _tree_catch_node(tree, right_idx);

        for (uint32_t i = 0; i < intern.as.intern.pl->count; i++) {
            struct VdbNodePtr* ptr = &intern.as.intern.pl->pointers[i];
            node_append_nodeptr(&left, *ptr);
        }

        node_clear_nodeptrs(&intern);
        struct VdbNodePtr left_ptr = {meta.as.meta.pk_counter - 1, left_idx};
        struct VdbNodePtr right_ptr = {meta.as.meta.pk_counter, right_idx};
        node_append_nodeptr(&intern, left_ptr);
        node_append_nodeptr(&intern, right_ptr);

        _tree_release_node(tree, intern);
        _tree_release_node(tree, meta);
        _tree_release_node(tree, left);
        _tree_release_node(tree, right);

        return right_idx;
    }


    _tree_release_node(tree, meta);
    _tree_release_node(tree, intern);

    uint32_t parent_idx = 0;
    for (uint32_t i = 1; i < idx_list->count; i++) {
        if (idx_list->values[i] == idx) {
            parent_idx = idx_list->values[i - 1];
            break;
        }
    }

    struct VdbNode parent = _tree_catch_node(tree, parent_idx);
    if (node_is_full(&parent, node_nodeptr_size())) {

        _tree_release_node(tree, parent);
        uint32_t cached_parent_idx = parent_idx;
        parent_idx = _tree_split_internal(tree, idx_list, parent_idx);
        parent = _tree_catch_node(tree, parent_idx);
        struct VdbNode cached_parent = _tree_catch_node(tree, cached_parent_idx);

        uint32_t new_intern_idx = _tree_init_intern(tree);
        struct VdbNode new_intern = _tree_catch_node(tree, new_intern_idx);

        struct VdbNodePtr* ptr = &cached_parent.as.intern.pl->pointers[cached_parent.as.intern.pl->count - 1];
        struct VdbNodePtr new_ptr = {ptr->key + 1, new_intern_idx};
        node_append_nodeptr(&parent, new_ptr);

        _tree_release_node(tree, cached_parent);
        _tree_release_node(tree, new_intern);
        _tree_release_node(tree, parent);
        return new_intern_idx;
    } else {
        uint32_t new_intern_idx = _tree_init_intern(tree);
        struct VdbNode new_intern = _tree_catch_node(tree, new_intern_idx);

        struct VdbNodePtr* ptr = &parent.as.intern.pl->pointers[parent.as.intern.pl->count - 1];
        struct VdbNodePtr new_ptr = {ptr->key--, new_intern_idx};
        node_append_nodeptr(&parent, new_ptr);

        _tree_release_node(tree, new_intern);
        _tree_release_node(tree, parent);
        return new_intern_idx;
    }

}

uint32_t _tree_split_leaf(struct VdbTree* tree, struct U32List* idx_list) {
    struct VdbNode parent = _tree_catch_node(tree, idx_list->values[idx_list->count - 2]);
    
    struct VdbNodePtr* prev_right = &parent.as.intern.pl->pointers[parent.as.intern.pl->count - 1];

    uint32_t new_right_idx = _tree_init_leaf(tree);
    struct VdbNodePtr new_right = {prev_right->key, new_right_idx};
    prev_right->key--;

    if (node_is_full(&parent, node_nodeptr_size())) {
        _tree_release_node(tree, parent);
        uint32_t new_parent_idx = _tree_split_internal(tree, idx_list, parent.idx);
        parent = _tree_catch_node(tree, new_parent_idx);
    }

    node_append_nodeptr(&parent, new_right);

    _tree_release_node(tree, parent);

    return new_right_idx;
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
        uint32_t new_leaf_idx = _tree_split_leaf(tree, idx_list);
        leaf = _tree_catch_node(tree, new_leaf_idx);
    }

    rec->key = key;
    node_append_record(&leaf, *rec);

    u32l_free(idx_list);
    _tree_release_node(tree, meta);
    _tree_release_node(tree, leaf);
}

//TODO: could probably merge part of this function with _tree_traverse_to since 
//this function is just the top half of that function (we just don't update keys here
void _tree_do_fetch_record(struct VdbTree* tree, struct VdbNode* node, uint32_t key, struct U32List* idx_list) {
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
}

struct VdbRecord* tree_fetch_record(struct VdbTree* tree, uint32_t key) {
    struct U32List* idx_list = u32l_alloc();
    struct VdbNode root = _tree_catch_node(tree, 1);
    _tree_do_fetch_record(tree, &root, key, idx_list);
    _tree_release_node(tree, root);


    if (idx_list->count == 0) {
        u32l_free(idx_list);
        return NULL;
    }

    struct VdbNode node = _tree_catch_node(tree, idx_list->values[idx_list->count - 1]);
    if (node.type == VDBN_INTERN) {
        u32l_free(idx_list);
        _tree_release_node(tree, node);
        return NULL;
    } else {
        struct VdbRecord* result = NULL;
        for (uint32_t i = 0; i < node.as.leaf.rl->count; i++) {
            struct VdbRecord* r = &node.as.leaf.rl->records[i];
            if (r->key == key) {
                result = vdb_copy_record(r);
                break;
            }
        }
        u32l_free(idx_list);
        _tree_release_node(tree, node);
        return result;
    }
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

