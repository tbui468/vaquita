import subprocess
import sys

def move_test(filename):
    cmd = "mv " + filename + ".sql ./../test/" + filename + ".sql"
    return_code = subprocess.call(cmd, shell=True)

    cmd = "mv " + filename + ".expected ./../test/" + filename + ".expected"
    return_code = subprocess.call(cmd, shell=True)


if __name__ == "__main__":
    move_test(sys.argv[1])
