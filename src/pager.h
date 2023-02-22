#ifndef VDB_PAGER_H
#define VDB_PAGER_H

#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>

#define VDB_PAGE_SIZE 4096

struct VdbPager {
    struct VdbPage* pages; 
    uint32_t page_count;
    FILE* table_files[128];
    char* table_names[128];
    uint32_t table_count;
};

struct VdbPage {
    char* table;
    uint32_t idx;
    uint8_t* buf;
    bool dirty;
    struct VdbPage* next;
};

struct VdbPager* pager_init();
struct VdbPage* pager_get_page(struct VdbPager* pager, FILE* f, const char* table, uint32_t idx);
void pager_write_page(struct VdbPage* p, uint8_t* buf, int off, int len);


#endif //VDB_PAGER_H
