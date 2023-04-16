set(Sources
    main.c
    vdb.c
    util.c
    schema.c
    tree.c
    node.c
    record.c
    )

set(Headers
    vdb.h
    util.h
    schema.h
    tree.h
    node.h
    record.h
    )

add_executable(
    vdb
    ${Headers}
    ${Sources}
    )
