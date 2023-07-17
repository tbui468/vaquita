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
    uint8_t buf[VDB_PAGE_SIZE];
    FILE* f;
    char* name;
};

struct VdbPager {
    struct VdbPage* pages;
    uint32_t count;
    uint32_t capacity;
};

struct VdbPager* vdbpager_init();
void vdbpager_free(struct VdbPager* pager);

uint32_t vdbpager_fresh_page(FILE* f);
struct VdbPage* vdbpager_pin_page(struct VdbPager* pager, char* name, FILE* f, uint32_t idx);
void vdbpager_unpin_page(struct VdbPage* page, bool dirty);
void vdbpager_evict_pages(struct VdbPager* pager, const char* name);

#endif //VDB_PAGER_H
