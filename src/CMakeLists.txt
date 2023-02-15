set(Sources
    main.c
    vdb.c
    util.c
    tree.c
    )

set(Headers
    vdb.h
    util.h
    tree.h
    )

add_executable(
    vdb
    ${Headers}
    ${Sources}
    )
