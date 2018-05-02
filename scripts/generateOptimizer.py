import sys

with open(sys.argv[1]) as f:

    # read the entire file
    content = f.read()

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
