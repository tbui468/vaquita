#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "util.h"
#include "tree.h"

struct VdbTree* tree_init(const char* name, struct VdbSchema* schema) {
    struct VdbTree* tree = malloc_w(sizeof(struct VdbTree));
    tree->name = strdup(name);
    tree->pk_counter = 0;
    tree->node_idx_counter = 0;
    tree->schema = vdb_schema_copy(schema);

    //init both root and first leaf (right pointer)
    tree->root = vdb_node_init_intern(++tree->node_idx_counter, NULL);
    tree->root->as.intern.right = vdb_node_init_leaf(++tree->node_idx_counter, tree->root);

    return tree;
}

void tree_free(struct VdbTree* tree) {
    free(tree->name);
    vdb_schema_free(tree->schema);
    free(tree);
}

struct VdbNode* _vdb_tree_traverse_to(struct VdbNode* node, uint32_t key) {
    assert(node->type == VDBN_INTERN);

    //TODO: should do binary search instead of linear
    for (uint32_t i = 0; i < node->as.intern.nodes->count; i++) {
        struct VdbNode* n = node->as.intern.nodes->nodes[i];
        if (n->type == VDBN_LEAF && n->as.leaf.records->count > 0) {
            uint32_t last_rec_key = n->as.leaf.records->records[n->as.leaf.records->count - 1]->key;
            if (key <= last_rec_key)
                return n;
        } else if (n->type == VDBN_INTERN) {
            //get largest key for a given internal node, and compare to key
            struct VdbNode* l = n->as.intern.right;
            while (l->type != VDBN_LEAF) {
                l = l->as.intern.right;
            }
            uint32_t largest_rec_key = l->as.leaf.records->records[l->as.leaf.records->count - 1]->key;
            if (key <= largest_rec_key) {
                return _vdb_tree_traverse_to(n, key);
            }
        }
    }

    if (node->as.intern.right->type == VDBN_LEAF) {
        return node->as.intern.right;
    }

    return _vdb_tree_traverse_to(node->as.intern.right, key);
}

struct VdbNode* _vdb_tree_split_intern(struct VdbTree* tree, struct VdbNode* node) {
    assert(node->type == VDBN_INTERN);

    struct VdbNode* new_intern = NULL;

    if (!node->parent) { //node is root
        struct VdbNode* old_root = node;
        tree->root = vdb_node_init_intern(++tree->node_idx_counter, NULL);
        old_root->parent = tree->root;
        new_intern =  vdb_node_init_intern(++tree->node_idx_counter, tree->root);
        tree->root->as.intern.right = new_intern;
        vdb_nodelist_append_node(tree->root->as.intern.nodes, old_root);
    } else if (!vdb_node_intern_full(node->parent)) {
        new_intern = vdb_node_init_intern(++tree->node_idx_counter, node->parent);
        vdb_nodelist_append_node(node->parent->as.intern.nodes, node->parent->as.intern.right);
        node->parent->as.intern.right = new_intern;
    } else {
        struct VdbNode* new_parent = _vdb_tree_split_intern(tree, node->parent);
        new_intern = vdb_node_init_intern(++tree->node_idx_counter, new_parent);
        new_parent->as.intern.right = new_intern;
    }

    return new_intern;
}

struct VdbNode* _vdb_tree_split_leaf(struct VdbTree* tree, struct VdbNode* node) {
    assert(node->type == VDBN_LEAF);

    struct VdbNode* parent = node->parent;

    if (vdb_node_intern_full(parent)) {
        struct VdbNode* new_intern = _vdb_tree_split_intern(tree, parent);
        new_intern->as.intern.right = vdb_node_init_leaf(++tree->node_idx_counter, new_intern);
        return new_intern->as.intern.right;
    }
    
    vdb_nodelist_append_node(parent->as.intern.nodes, parent->as.intern.right);
    parent->as.intern.right = vdb_node_init_leaf(++tree->node_idx_counter, parent);

    return parent->as.intern.right;
}

void vdb_tree_insert_record(struct VdbTree* tree, struct VdbRecord* rec) {
    struct VdbNode* root = tree->root;
    struct VdbNode* leaf = _vdb_tree_traverse_to(root, rec->key);

    if (vdb_node_leaf_full(leaf, rec)) {
        leaf = _vdb_tree_split_leaf(tree, leaf);
    }

    vdb_recordlist_append_record(leaf->as.leaf.records, rec);
}

struct VdbRecord* vdb_tree_fetch_record(struct VdbTree* tree, uint32_t key) {
    if (key > tree->pk_counter) {
        return NULL;
    }

    struct VdbNode* root = tree->root;
    struct VdbNode* leaf = _vdb_tree_traverse_to(root, key);

    return vdb_recordlist_get_record(leaf->as.leaf.records, key);
}

bool vdb_tree_update_record(struct VdbTree* tree, struct VdbRecord* rec) {
    struct VdbNode* root = tree->root;
    struct VdbNode* leaf = _vdb_tree_traverse_to(root, rec->key);

    if (!leaf) {
        return false;
    }

    for (uint32_t i = 0; i < leaf->as.leaf.records->count; i++) {
        struct VdbRecord* r = leaf->as.leaf.records->records[i];
        if (r->key == rec->key) {
            leaf->as.leaf.records->records[i] = rec;
            vdb_record_free(r);
            return true;
        }
    }
    
    return false;
}

struct VdbTreeList* treelist_init() {
    struct VdbTreeList* tl = malloc_w(sizeof(struct VdbTreeList));
    tl->count = 0;
    tl->capacity = 8;
    tl->trees = malloc_w(sizeof(struct VdbTree*) * tl->capacity);

    return tl;
}

void treelist_append(struct VdbTreeList* tl, struct VdbTree* tree) {
    if (tl->count == tl->capacity) {
        tl->capacity *= 2;
        tl->trees = realloc_w(tl->trees, sizeof(struct VdbTree*) * tl->capacity);
    }

    tl->trees[tl->count++] = tree;
}

struct VdbTree* treelist_get_tree(struct VdbTreeList* tl, const char* name) {
    struct VdbTree* t;
    uint32_t i;
    for (i = 0; i < tl->count; i++) {
        t = tl->trees[i];
        if (strncmp(name, t->name, strlen(name)) == 0)
            return t;
    }

    return NULL;
}

void treelist_remove(struct VdbTreeList* tl, const char* name) {
    struct VdbTree* t = NULL;
    uint32_t i;
    for (i = 0; i < tl->count; i++) {
        t = tl->trees[i];
        if (strncmp(name, t->name, strlen(name)) == 0)
            break;
    }

    if (t) {
        tl->trees[i] = tl->trees[--tl->count];
        tree_free(t);
    }
}

void treelist_free(struct VdbTreeList* tl) {
    for (uint32_t i = 0; i < tl->count; i++) {
        struct VdbTree* t = tl->trees[i];
        tree_free(t);
    }

    free(tl->trees);
    free(tl);
}
