import subprocess
import os

GREEN = '\033[32m'
RED = '\033[31m'
ENDC = '\033[0m'

#server must be started first before tests can be run

def test(filename):
    cmd = "./../client/build/src/vdbclient ./" + filename + ".sql"
    cmd += " > result.txt"

    return_code = subprocess.call(cmd, shell=True)

    output_matches = False
    with open(filename + ".expected", "r") as expected:
        with open("result.txt", "r") as result:
            if expected.read() == result.read():
                output_matches = True

    if (return_code == 0 and output_matches):
        print(filename.ljust(30, " "), GREEN + "passed" + ENDC)
        return True
    else:
        print(filename.ljust(30, " "), RED + "failed" + ENDC)
        return False

files = os.listdir('./')

passed = 0
failed = 0
names = []

for f in files:
    if f.endswith('.sql'):
        names.append(f)

names.sort()

for n in names:
    if test(n[:-4]):
        passed +=1 
    else:
        failed += 1


print("")
print(GREEN + "passed: " + str(passed) + "  " + RED + "failed: " + str(failed) + ENDC + "  of " + str(len(names)) + " tests")
print("")
