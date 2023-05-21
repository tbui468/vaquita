import subprocess

def test(filename):
    cmd = "./../build/src/vdb ./" + filename + ".sql"
    #cmd += " > /dev/null"
    cmd += " > result.txt"

    return_code = subprocess.call(cmd, shell=True)

    output_matches = False
    with open(filename + ".expected", "r") as expected:
        with open("result.txt", "r") as result:
            if expected.read() == result.read():
                output_matches = True

    if (return_code == 0 and output_matches):
        print(filename.ljust(20, " "), "passed")
    else:
        print(filename.ljust(20, " "), "failed")

test("create_drop_db")
test("open_close_db")
test("show_dbs")
test("create_drop_table")
test("insert_records")
test("describe_table")
test("select_all")
test("select_projection")
#test("insert_records_with_nulls")
