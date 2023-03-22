#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "vdb.h"


struct VdbData* make_data(uint64_t b, char* c, bool a) {
    struct VdbData* d = malloc(sizeof(struct VdbData));
    struct VdbString* str = malloc(sizeof(struct VdbString));
    str->start = c;
    str->len = strlen(c);

    struct VdbDatum* data = malloc(sizeof(struct VdbDatum) * 3);
    data[0].type = VDBF_INT;
    data[0].as.Int = b;
    data[1].type = VDBF_STR;
    data[1].as.Str = str;
    data[2].type = VDBF_BOOL;
    data[2].as.Bool = a;
    d->data = data;
    d->count = 3;
    return d;
}

void print_data_and_free(struct VdbData* result) {
    printf("%ld, %.*s, %d\n", result->data[0].as.Int, result->data[1].as.Str->len, result->data[1].as.Str->start, result->data[2].as.Bool);
    vdb_free_data(result);
}

int main(int argc, char** argv) {
    argc = argc;
    argv = argv;

    VDBHANDLE h = vdb_create("test");

    enum VdbField fields[] = { VDBF_INT, VDBF_STR, VDBF_BOOL };
    struct VdbSchema s = { fields, 3 };
    if (vdb_create_table(h, "students", &s) != 0)
        printf("failed to create table\n");

    if (vdb_create_table(h, "teachers", &s) != 0)
        printf("failed to create table\n");

    if (vdb_drop_table(h, "teachers") != 0)
        printf("failed to drop table\n");

    for (int i = 1; i <= 62; i++) { //TODO: Adding a 56th record requires splitting interal node (root in this case)
        struct VdbData* d = make_data(i, "dogs", i % 2 == 0);
        if (vdb_insert_record(h, "students", d) != 0)
            printf("failed to insert record\n");
        vdb_free_data(d);
    }

    struct VdbData* result;
    if ((result = vdb_fetch_record(h, "students", 1)))
        print_data_and_free(result);
    else
        printf("No record found\n");

    if ((result = vdb_fetch_record(h, "students", 24)))
        print_data_and_free(result);
    else
        printf("No record found\n");

    if ((result = vdb_fetch_record(h, "students", 40)))
        print_data_and_free(result);
    else
        printf("No record found\n");

    if ((result = vdb_fetch_record(h, "students", 41)))
        print_data_and_free(result);
    else
        printf("No record found\n");

    if ((result = vdb_fetch_record(h, "students", 51)))
        print_data_and_free(result);
    else
        printf("No record found\n");

    if ((result = vdb_fetch_record(h, "students", 62)))
        print_data_and_free(result);
    else
        printf("No record found\n");

    //vdb_debug_print_tree(h, "students");
    vdb_debug_print_keys(h, "students", 1);
    vdb_debug_print_keys(h, "students", 20);
    vdb_debug_print_keys(h, "students", 21);

/*
    if ((result = vdb_fetch_record(h, "students", 2)))
        print_data(result);
    if ((result = vdb_fetch_record(h, "students", 3)))
        print_data(result);

    if (vdb_delete_record(h, "students", 0) != 0)
        printf("failed to delete record\n");
    if (vdb_delete_record(h, "students", 2) != 0)
        printf("failed to delete record\n");

    if ((result = vdb_fetch_record(h, "students", 0)))
        print_data(result);
    if ((result = vdb_fetch_record(h, "students", 1)))
        print_data(result);
    if ((result = vdb_fetch_record(h, "students", 2)))
        print_data(result);*/

    vdb_close(h);

    return 0;
}
