import subprocess
import sys

def generate_expected(filename):
    cmd = "./../build/src/vdb ./" + filename
    cmd += " > " + filename[:-4] + ".expected"

    return_code = subprocess.call(cmd, shell=True)


if __name__ == "__main__":
    generate_expected(sys.argv[1])
