#include <stdio.h>
#include <stdbool.h>
#include <string.h>

#include "vdb.h"

int main(int argc, char** argv) {
    argc = argc;
    argv = argv;

    VDBHANDLE h;
    if ((h = vdb_open("school"))) {
        printf("Opened 'school' database\n");    
    } else {
        printf("Failed to open 'school' database\n");
    }


    struct VdbSchema* schema = vdb_alloc_schema(3, VDBF_INT, VDBF_STR, VDBF_BOOL);

    if (vdb_create_table(h, "students", schema)) {
        printf("Created 'students' table\n");
    } else {
        printf("Failed to create 'students' table\n");
    }

    vdb_free_schema(schema);

    char word[100];
    memset(word, 'x', sizeof(char) * 100);
    word[99] = '\0';
    
    const char* words[] = {word, "dogs", "turtles"};
    for (int i = 1; i <= 15; i++) {
        vdb_insert_record(h, "students", i * 2, words[i % 3], i % 2 == 0);
        printf("inserted record\n");
    }

    vdb_debug_print_tree(h, "students");
    //vdb_update_record(h, "students", 100, 0, "lions", true);
    //vdb_delete_record(h, "students", 2);
    //vdb_delete_record(h, "students", 3);

    int keys[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

    for (int i = 0; i < 10; i++) {
        struct VdbRecord* r = vdb_fetch_record(h, "students", keys[i]);
        if (r) {
            printf("key %d: %ld, %.*s, %d\n", r->key, r->data[0].as.Int, r->data[1].as.Str->len, r->data[1].as.Str->start, r->data[2].as.Bool);
        } else {
            printf("not found!\n");
        }
    }

/*
    if (vdb_drop_table(h, "students")) {
        printf("Dropped 'students' table\n");
    } else {
        printf("Failed to drop 'students' table\n");
    }*/

    if (vdb_close(h)) {
        printf("Closed 'school' database\n");
    } else {
        printf("Failed to close 'school' database\n");
    }

    return 0;
}
