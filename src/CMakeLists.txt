set(Sources
    main.c
    vdb.c
    util.c
    schema.c
    tree.c
    node.c
    record.c
    pager.c
    )

set(Headers
    vdb.h
    util.h
    schema.h
    tree.h
    node.h
    record.h
    pager.h
    )

add_executable(
    vdb
    ${Headers}
    ${Sources}
    )
