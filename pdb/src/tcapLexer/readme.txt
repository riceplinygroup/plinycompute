################
TcapLexer Module
################

1.) Overview

    The TcapLexer module provides classes to lex a string in the TCAP language and produce a stream of tokens defined
    by the TCAP grammar.

    This module uses the following definition of the TCAP grammar:

        https://github.com/riceplinygroup/pdb/wiki/TCAP-Grammar/92941259851167a0c54700a7196552007ed7002f

        (Nov 14, 2016)

2.) Principle Function

    The principle function of interest for this module is:

         TcapTokenStream lexTcap(const string &source);

    as found in TcapLexer.h


    An example of its usage would be:

         using pdb_detail::lexTcap;
         using pdb_detail::TcapTokenStream;

         TcapTokenStream result = lexTcap("E(student) = filter D by isExamGreater retain student");

3.) Design Notes

    The implementation of

        TcapTokenStream lexTcap(const string &source)

    in TcapLexer.cc is a hand rolled lexer. It was authored in a moment of frustration with the lack of documentation
    surrounding the use of the ANTLR lexer generator with a C++ output target.  The only justification for hand writing
    the lexer instead of using a lexer generator is that at 246 lines it appeared quicker to write the lexer by hand
    than guess the correct usage of ANLTR without corresponding documentation or learn Flex.

    However, if the TCAP grammar grows in the future or an future author has knowledge of the Flex tool (or similar),
    replacing the current implemetnation with a machine generated implementation would be welcome.