import subprocess

def test(filename):
    cmd = "./../build/src/vdb ./" + filename
    cmd += " > /dev/null"

    cp = subprocess.call(cmd, shell=True)
    if (cp == 0):
        print(filename.ljust(20, " "), "passed")
    else:
        print(filename.ljust(20, " "), "failed")

test("create_db.sql")
test("test.sql")
test("test2.sql")
test("sol.sql")
