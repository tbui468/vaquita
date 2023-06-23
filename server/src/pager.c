#include <stdbool.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include "pager.h"
#include "util.h"


static void vdbpager_flush_page(struct VdbPage* p);

struct VdbPager* vdbpager_init() {
    struct VdbPager* pager = malloc_w(sizeof(struct VdbPager));
    pager->count = 0;
    pager->capacity = 4096;
    pager->pages = malloc_w(sizeof(struct VdbPage) * pager->capacity);
    return pager;
}

void vdbpager_free(struct VdbPager* pager) {
    for (uint32_t i = 0; i < pager->count; i++) {
        struct VdbPage* p = &pager->pages[i];
        if (p->dirty) {
            vdbpager_flush_page(p);
        }
        free_w(p->name, sizeof(char) * (strlen(p->name) + 1)); //also free null terminator
    }
    free_w(pager->pages, sizeof(struct VdbPage) * pager->capacity);
    free_w(pager, sizeof(struct VdbPager));
}

uint32_t vdbpager_fresh_page(FILE* f) {
    static uint8_t buf[VDB_PAGE_SIZE] = {0};

    fseek_w(f, 0, SEEK_END);
    uint32_t idx = ftell_w(f) / VDB_PAGE_SIZE;
    fwrite_w(buf, sizeof(uint8_t), VDB_PAGE_SIZE, f);

    return idx;
}

struct VdbPage* vdbpager_load_page(struct VdbPager* pager, char* name, FILE* f, uint32_t idx) {
    if (pager->count >= pager->capacity) {
        int old_cap = pager->capacity;
        pager->capacity *= 2;
        pager->pages = realloc_w(pager->pages, sizeof(struct VdbPage) * pager->capacity, sizeof(struct VdbPage) * old_cap);
    }

    struct VdbPage* p = &pager->pages[pager->count++];

    p->dirty = false;
    p->pin_count = 0;
    p->idx = idx;
    fseek_w(f, p->idx * VDB_PAGE_SIZE, SEEK_SET);
    fread_w(p->buf, sizeof(uint8_t), VDB_PAGE_SIZE, f);
    p->f = f;
    p->name = strdup_w(name);

    return p;
}

static void vdbpager_flush_page(struct VdbPage* p) {
    fseek_w(p->f, p->idx * VDB_PAGE_SIZE, SEEK_SET);
    fwrite_w(p->buf, sizeof(uint8_t), VDB_PAGE_SIZE, p->f);
    p->dirty = false;
}

struct VdbPage* vdbpager_pin_page(struct VdbPager* pager, char* name, FILE* f, uint32_t idx) {
    struct VdbPage* page = NULL;
    for (uint32_t i = 0; i < pager->count; i++) {
        struct VdbPage* p = &pager->pages[i];
        if (p->idx == idx && !strncmp(p->name, name, strlen(name))) {
            page = p;
            break;
        }
    }

    //not cached, so read from disk
    if (!page) {
        //TODO: evict here if necessary before loading page.  Only evict a page if pin_count == 0
        /*if (pager->pages->count >= 32) {
            for (uint32_t i = 0; i < pager->pages->count; i++) {
                struct VdbPage* p = pager->pages->pages[i];
                if (p->pin_count == 0) {
                    _vdb_pager_flush_page(p);
                    _vdb_pager_free_page(p);
                    pager->pages->pages[i] = _vdb_pager_load_page(name, f, idx);
                    page = pager->pages->pages[i];
                    break;
                }
            }
        } else*/ {
            page = vdbpager_load_page(pager, name, f, idx);
        }
    }

    page->pin_count++;

    return page;
}

void vdbpager_unpin_page(struct VdbPage* page) {
    page->pin_count--;
}

void vdbpager_evict_pages(struct VdbPager* pager, const char* name) {
    for (uint32_t i = 0; i < pager->count; i++) {
        struct VdbPage* p = &pager->pages[i];
        if (!strncmp(name, p->name, strlen(name))) {
            free_w(p->name, sizeof(char) * (strlen(p->name) + 1));
            memcpy(pager->pages + i, pager->pages + pager->count - 1, sizeof(struct VdbPage));
            pager->count--;
            i--;
        }
    }
}
