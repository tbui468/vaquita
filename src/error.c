#include <stdarg.h>
#include <stdlib.h>

#include "util.h"
#include "error.h"

struct VdbErrorList* vdberrorlist_init() {
    struct VdbErrorList* el = malloc_w(sizeof(struct VdbErrorList));
    el->count = 0;
    el->capacity = 8;
    el->errors = malloc_w(sizeof(struct VdbError) * el->capacity);

    return el;
}

void vdberrorlist_free(struct VdbErrorList* el) {
    for (int i = 0; i < el->count; i++) {
        free(el->errors[i].msg);
    }
    free(el->errors);
    free(el);
}

void vdberrorlist_append_error(struct VdbErrorList* el, int line, const char* fmt, ...) {
    static int MSG_MAX = 64;
    if (el->count + 1 > el->capacity) {
        int old_cap = el->capacity;
        el->capacity *= 2;
        el->errors = realloc_w(el->errors, sizeof(struct VdbError) * el->capacity, sizeof(struct VdbError) * old_cap);
    }

    struct VdbError e;
    e.line = line;
    e.msg = malloc_w(sizeof(char) * MSG_MAX);

    va_list ap;
    va_start(ap, fmt);
    vsnprintf_w(e.msg, MSG_MAX, fmt, ap);
    va_end(ap);


    el->errors[el->count++] = e;
}
