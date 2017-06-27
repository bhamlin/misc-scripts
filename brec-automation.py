#!/usr/bin/python3

from subprocess import call
import os, os.path
import sys

debug = True

files = sys.argv[1:]

appRoot = "/usr/local/brec/automation"
jarFile = "Automation.jar"
configRoot = "/etc/brec/automation"

if len(files) > 0:
    if debug: print(os.getcwd())
    os.chdir(configRoot)
    if debug: print(os.getcwd())
    found = []
    cmd = []
    for file in files:
        if os.path.exists(file):
            found.append(file)
        else:
            for ext in ("config", "properties", "info"):
                fname = "{}.{}".format(file, ext)
                if os.path.exists(fname):
                    found.append(fname)
                    break
    if debug: print("Found: {}".format(found))
    cmd.append("/usr/bin/java")
    cmd.append("-jar")
    cmd.append("{}/{}".format(appRoot, jarFile))
    cmd.extend(found)
    if debug: print("Command: " + str(cmd))
    call(cmd)
