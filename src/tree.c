#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "util.h"
#include "tree.h"

void _vdb_tree_release_node(struct VdbTree* tree, struct VdbNode* node);

struct VdbNode* _vdb_tree_init_node(struct VdbTree* tree, struct VdbNode* parent, enum VdbNodeType type) {
    uint32_t idx = vdb_pager_fresh_page(tree->f);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->f, idx);
    page->dirty = true;
    switch (type) {
        case VDBN_INTERN:
            return vdb_node_init_intern(idx, parent, page);
        case VDBN_LEAF:
            return vdb_node_init_leaf(idx, parent, page);
        case VDBN_DATA:
            return vdb_node_init_data(idx, parent, page);
    }
}

struct VdbTree* vdb_tree_init(const char* name, struct VdbSchema* schema, struct VdbPager* pager, FILE* f) {
    struct VdbTree* tree = malloc_w(sizeof(struct VdbTree));
    tree->name = strdup(name);
    tree->pk_counter = 0;
    tree->node_idx_counter = 0;
    tree->schema = vdb_schema_copy(schema);
    tree->pager = pager;
    tree->f = f;
    tree->dirty = true;

    uint32_t header_idx = vdb_pager_fresh_page(f);
    struct VdbPage* page = vdb_pager_pin_page(pager, f, header_idx);
    tree->page = page;
    page->dirty = true;

    tree->root = _vdb_tree_init_node(tree, NULL, VDBN_INTERN);
    tree->root->as.intern.right = _vdb_tree_init_node(tree, tree->root, VDBN_LEAF);

    _vdb_tree_release_node(tree, tree->root->as.intern.right);
    _vdb_tree_release_node(tree, tree->root);

    return tree;
}

struct VdbTree* vdb_tree_catch(FILE* f, struct VdbPager* pager) {
    return NULL;
}

struct VdbNode* _vdb_tree_catch_node(struct VdbTree* tree, uint32_t idx) {
    //TODO: assert that idx is valid

    struct VdbNode* node = malloc_w(sizeof(struct VdbNode));
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->f, idx); 
    vdb_node_deserialize(node, page->buf);

    node->page = page;
    node->dirty = false;

    return node;
}

void _vdb_tree_release_node(struct VdbTree* tree, struct VdbNode* node) {
    if (node->dirty) {
        node->page->dirty = true;
        vdb_node_serialize(node->page->buf, node);
    }

    vdb_pager_unpin_page(node->page);

    vdb_node_free(node);
}

void vdb_tree_release(struct VdbTree* tree) {
    if (tree->dirty) {
        tree->page->dirty = true;
        int off = 0;
        write_u32(tree->page->buf, tree->pk_counter, &off);
        write_u32(tree->page->buf, 5, &off);
        write_u32(tree->page->buf, 1, &off); //TODO: change root in tree into root_idx (since we will use indices rather than pointers now)
    }

    vdb_pager_unpin_page(tree->page);

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
        tree->root = _vdb_tree_init_node(tree, NULL, VDBN_INTERN);
        old_root->parent = tree->root;
        new_intern = _vdb_tree_init_node(tree, tree->root, VDBN_INTERN);
        tree->root->as.intern.right = new_intern;
        vdb_nodelist_append_node(tree->root->as.intern.nodes, old_root);
    } else if (!vdb_node_intern_full(node->parent)) {
        new_intern = _vdb_tree_init_node(tree, node->parent, VDBN_INTERN);
        vdb_nodelist_append_node(node->parent->as.intern.nodes, node->parent->as.intern.right);
        node->parent->as.intern.right = new_intern;
    } else {
        struct VdbNode* new_parent = _vdb_tree_split_intern(tree, node->parent);
        new_intern = _vdb_tree_init_node(tree, new_parent, VDBN_INTERN);
        new_parent->as.intern.right = new_intern;
    }

    return new_intern;
}

struct VdbNode* _vdb_tree_split_leaf(struct VdbTree* tree, struct VdbNode* node) {
    assert(node->type == VDBN_LEAF);

    struct VdbNode* parent = node->parent;

    if (vdb_node_intern_full(parent)) {
        struct VdbNode* new_intern = _vdb_tree_split_intern(tree, parent);
        new_intern->as.intern.right = _vdb_tree_init_node(tree, new_intern, VDBN_LEAF);
        return new_intern->as.intern.right;
    }
    
    vdb_nodelist_append_node(parent->as.intern.nodes, parent->as.intern.right);
    parent->as.intern.right = _vdb_tree_init_node(tree, parent, VDBN_LEAF);

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

bool vdb_tree_delete_record(struct VdbTree* tree, uint32_t key) {
    struct VdbNode* root = tree->root;
    struct VdbNode* leaf = _vdb_tree_traverse_to(root, key);

    if (!leaf) {
        return false;
    }

    //TODO: need a way to reuse deleted records
    for (uint32_t i = 0; i < leaf->as.leaf.records->count; i++) {
        struct VdbRecord* r = leaf->as.leaf.records->records[i];
        if (r->key == key) {
            r->key = 0;
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
        vdb_tree_release(t);
    }
}

void treelist_free(struct VdbTreeList* tl) {
    for (uint32_t i = 0; i < tl->count; i++) {
        struct VdbTree* t = tl->trees[i];
        vdb_tree_release(t);
    }

    free(tl->trees);
    free(tl);
}
