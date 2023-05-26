import subprocess
import os

GREEN = '\033[32m'
RED = '\033[31m'
ENDC = '\033[0m'

names = []
results = {}

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

    names.append(filename)
    if (return_code == 0 and output_matches):
        results[filename] = True
        #print(filename.ljust(30, " "), GREEN + "passed" + ENDC)
        return True
    else:
        results[filename] = False
        #print(filename.ljust(30, " "), RED + "failed" + ENDC)
        return False

files = os.listdir('./')

total = 0
passed = 0
failed = 0

for f in files:
    if f.endswith('.sql'):
        total += 1
        if test(f[:-4]):
            passed +=1 
        else:
            failed += 1

names.sort()
for n in names:
    if results[n]:
        print(n.ljust(30, " "), GREEN + "passed" + ENDC)
    else:
        print(n.ljust(30, " "), RED + "failed" + ENDC)

print("")
print(GREEN + "passed: " + str(passed) + "  " + RED + "failed: " + str(failed) + ENDC + "  of " + str(total) + " tests")
print("")
