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

struct VdbPage* vdbpager_load_page(struct VdbPager* pager, FILE* f, uint32_t idx) {
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

    return p;
}

static void vdbpager_flush_page(struct VdbPage* p) {
    fseek_w(p->f, p->idx * VDB_PAGE_SIZE, SEEK_SET);
    fwrite_w(p->buf, sizeof(uint8_t), VDB_PAGE_SIZE, p->f);
    p->dirty = false;
}

static struct VdbPage* vdbpager_evict_first_unpinned(struct VdbPager* pager, FILE* f, uint32_t idx) {
    uint32_t i; 
    for (i = 0; i < pager->count; i++) {
        struct VdbPage* p = &pager->pages[i];
        if (p->pin_count == 0) {
            if (p->dirty) {
                vdbpager_flush_page(p);
            }

            p->dirty = false;
            p->pin_count = 0;
            p->idx = idx;
            fseek_w(f, p->idx * VDB_PAGE_SIZE, SEEK_SET);
            fread_w(p->buf, sizeof(uint8_t), VDB_PAGE_SIZE, f);
            p->f = f;
            return p;
        }
    }
    
    printf("buffer pool doesn't have enough space!!! Bug!!!\n");
    return NULL;
}

struct VdbPage* vdbpager_pin_page(struct VdbPager* pager, FILE* f, uint32_t idx) {
    struct VdbPage* page = NULL;
    for (uint32_t i = 0; i < pager->count; i++) {
        struct VdbPage* p = &pager->pages[i];
        if (p->idx == idx && fileno(f) == fileno(p->f)) {
            page = p;
            break;
        }
    }

    const int MAX_PAGES = 8; //TODO: replace with larger number (artificially small to test eviction)

    //not cached, so read from disk
    if (!page) {
        if (pager->count >= MAX_PAGES) {
            //TODO: replace with LRU or other eviction algorithm later
            page = vdbpager_evict_first_unpinned(pager, f, idx);
        } else {
            page = vdbpager_load_page(pager, f, idx);
        }
    }

    page->pin_count++;

    return page;
}

void vdbpager_unpin_page(struct VdbPage* page, bool dirty) {
    page->pin_count--;
    if (dirty) {
        page->dirty = true;
    }
}

void vdbpager_evict_pages(struct VdbPager* pager, FILE* f) {
    for (uint32_t i = 0; i < pager->count; i++) {
        struct VdbPage* p = &pager->pages[i];
        if (fileno(p->f) == fileno(f)) {
            memcpy(pager->pages + i, pager->pages + pager->count - 1, sizeof(struct VdbPage));
            pager->count--;
            i--;
        }
    }

}
