import subprocess
import os

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
        print(filename.ljust(30, " "), "passed")
    else:
        print(filename.ljust(30, " "), "failed")

files = os.listdir('./')

for f in files:
    if f.endswith('.sql'):
        test(f[:-4])

