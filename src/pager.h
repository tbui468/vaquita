#ifndef VDB_PAGER_H
#define VDB_PAGER_H

#include <stdio.h>
#include <stdint.h>

#define VDB_PAGE_SIZE 256
#define VDB_PAGE_HDR_SIZE 128

struct VdbPage {
    bool dirty;
    uint32_t idx;
    uint32_t pin_count;
    uint8_t* buf;
    FILE* f;
    char* name;
};

struct VdbPageList {
    struct VdbPage** pages;
    uint32_t count;
    uint32_t capacity;
};

struct VdbPager {
    struct VdbPageList* pages;
};

struct VdbPager* vdb_pager_alloc();
void vdb_pager_free(struct VdbPager* pager);

uint32_t vdb_pager_fresh_page(FILE* f);
struct VdbPage* vdb_pager_pin_page(struct VdbPager* pager, char* name, FILE* f, uint32_t idx);
void vdb_pager_unpin_page(struct VdbPage* page);
void vdb_pager_evict_pages(struct VdbPager* pager, const char* name);

#endif //VDB_PAGER_H
