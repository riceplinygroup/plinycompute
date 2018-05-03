#!/usr/bin/env python3

import sys
import hashlib
import os


def write_out_hash(text):

    # if the size of the arguments is not three we are not doing anything
    if len(sys.argv) != 4:
        return

    # hash file name
    hash_file = '%s/.__optimizer_codes_hash' % sys.argv[3]

    # get the hash
    m = hashlib.sha256()
    m.update(text.encode('utf-8'))
    m = m.digest()

    with open(hash_file, 'wb') as file:
        file.write(m)


def check_if_changed(text):

    # if the size of the arguments is not three we are not doing anything
    if len(sys.argv) != 4:
        return False

    # hash file name
    hash_file = '%s/.__optimizer_codes_hash' % sys.argv[3]

    # check if hash file exits
    if os.path.isfile(hash_file):

        # if so open it
        with open(hash_file, "rb") as file:
            hash = file.read()
    else:
        hash = ""

    # get the hash
    m = hashlib.sha256()
    m.update(text.encode('utf-8'))
    m = m.digest()

    # return true if they are equal
    return hash != m


with open(sys.argv[1]) as f:

    # read the entire file
    content = f.read()

    # check if the content has changed if it has then we need to regenerate the .cc file containing the prolog
    if check_if_changed(content):

        print(sys.argv[2])

        # do all the escapings
        content = content.replace("\\", "\\\\")
        content = content.replace("\"", "\\\"")
        content = "\"" + content.replace("\n", "\\n\"\n\"") + "\""
        content = content.replace("\t", "\\t")

        # this thing is the prefix of the generated file
        prefix = """#include <PrologOptimizer.h>\n/**\n * This file is created from the PrologToTCAP.pl so it can be embedded into pdb\n */\nconst std::string pdb::PrologOptimizer::prologToTCAPScript = """

        # this thing is the sufix of the generated file
        sufix = ";"

        # write out the file
        f = open(sys.argv[2], "w+")
        f.write(prefix + content + sufix)
        f.close()

        # write out the hash
        write_out_hash(content)
