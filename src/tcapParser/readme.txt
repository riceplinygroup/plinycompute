################
TcapParser Module
################

1.) Overview

    The TcapParser module provides classes to parse a string in the TCAP language and produce a parse tree
    as directed by the TCAP grammar.

    This module uses the following definition of the TCAP grammar:

        https://github.com/riceplinygroup/pdb/wiki/TCAP-Grammar/92941259851167a0c54700a7196552007ed7002f

        (Nov 14, 2016)

2.) Principle Function

    The principle function of interest for this module is:

         shared_ptr<SafeResult<TranslationUnit>> parseTcap(const string &source);

    as found in TcapParser.h


    An example of its usage would be:

        using std::shared_ptr;
        using std::string;
        using pdb::SafeResult;
        using pdb_detail:TranslationUnit;
        using pdb_detail:parseTcap;

        string program = "@exec \"exec1\"\n"
                         "A(student) = load \"(databaseName, inputSetName)\"";

        shared_ptr<SafeResult<TranslationUnit>> parseTreeResult = parseTcap(program);

        parseTreeResult->apply(
                [&](TranslationUnit parseTree)
                {
                   // process the parse tree
                },
                [&](string errorMsg)
                {
                   // report an error
                }

3.) Design Notes

    The names of classes in this module correspond to the non-terminals defined in the grammar specification.

    For example:

    load_op -> LoadOperaton.h
    translation_unit -> TranslationUnit.h
    statement -> TcapStatement.h


    When parsing the input string, no context sensitive analysis is performed.