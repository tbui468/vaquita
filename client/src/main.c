#include <stdio.h>
#include "client.h"

int main() {
    connect_and_request("localhost", "3333");
    return 0;
}
