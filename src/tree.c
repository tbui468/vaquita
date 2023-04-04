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

//TODO: could have split_internal also return the idx of the p
//      this could solve some problems with not knowing
uint32_t _tree_split_internal(struct VdbTree* tree, struct U32List* idx_list, uint32_t idx) {
    struct VdbNode intern = _tree_catch_node(tree, idx);
    if (node_is_root(&intern)) {
        /*
        printf("yep\n");
        uint32_t left_idx = _tree_init_intern(tree);*/
        uint32_t right_idx = _tree_init_intern(tree);
        //make left and right nodes
        //copy data of current root to left node
        //clear root, and only add left and right nodes (with right node being right pointer)
        //right node also needs access to pk_counter since we need to set the right pointer for that
        //release nodes
        //retraverse tree to update idx_list
        return right_idx;
    }

    /*
    uint32_t parent_idx = 0;
    for (uint32_t i = 1; i < idx_list->count; i++) {
        if (idx_list->values[i] == idx) {
            parent_idx = idx_list->values[i - 1];
            break;
        }
    }

    struct VdbNode parent = _tree_catch_node(tree, parent_idx);
    if (node_is_full(&parent, node_nodeptr_size())) {
        _tree_release_node(tree, intern);
        _tree_release_node(tree, parent);
        _tree_split_internal(tree, idx_list, parent_idx);
        //which idx is the one we are looking at???
    }*/

    //do all the stuff necessary to idx_list
    //free idx_list
    //retraverse tree if necessary and assign idx_list to new
    //assign idx_list pointer to new list created after splitting internal
    return 0; //TODO: just to silence warning for now
}

void _tree_split_leaf(struct VdbTree* tree, struct U32List* idx_list) {
    struct VdbNode parent = _tree_catch_node(tree, idx_list->values[idx_list->count - 2]);
    
    struct VdbNodePtr* prev_right = &parent.as.intern.pl->pointers[parent.as.intern.pl->count - 1];

    uint32_t new_right_idx = _tree_init_leaf(tree);
    struct VdbNodePtr new_right = {prev_right->key, new_right_idx};
    prev_right->key--;

    if (node_is_full(&parent, node_nodeptr_size())) {
        _tree_release_node(tree, parent);
        _tree_split_internal(tree, idx_list, parent.idx);
        parent = _tree_catch_node(tree, idx_list->values[idx_list->count - 2]);
    }

    node_append_nodeptr(&parent, new_right);
    idx_list->values[idx_list->count - 1] = new_right.idx;

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

