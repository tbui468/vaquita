#ifndef VDB_ERROR_H
#define VDB_ERROR_H


enum VdbReturnCode {
    VDBRC_SUCCESS,
    VDBRC_ERROR
};

struct VdbError {
    int line;
    char* msg;
};

struct VdbErrorList {
    struct VdbError* errors;
    int count;
    int capacity;
};

struct VdbErrorList* vdberrorlist_init();
void vdberrorlist_free(struct VdbErrorList* el);
void vdberrorlist_append_error(struct VdbErrorList* el, int line, const char* fmt, ...);

#endif //VDB_ERROR_H
