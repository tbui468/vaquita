set(Sources
    main.c
    vdb.c
    util.c
    )

set(Headers
    vdb.h
    util.h
    )

add_executable(
    vdb
    ${Headers}
    ${Sources}
    )
