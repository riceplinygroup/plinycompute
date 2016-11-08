/*****************************************************************************
 *                                                                           *
 *  Copyright 2018 Rice University                                           *
 *                                                                           *
 *  Licensed under the Apache License, Version 2.0 (the "License");          *
 *  you may not use this file except in compliance with the License.         *
 *  You may obtain a copy of the License at                                  *
 *                                                                           *
 *      http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                           *
 *  Unless required by applicable law or agreed to in writing, software      *
 *  distributed under the License is distributed on an "AS IS" BASIS,        *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *  See the License for the specific language governing permissions and      *
 *  limitations under the License.                                           *
 *                                                                           *
 *****************************************************************************/

#include "LexTcapTests.h"
#include "TcapLexer.h"

using pdb_detail::TcapToken;
using pdb_detail::TcapTokenStream;
using pdb_detail::lexTcap;

namespace pdb_tests
{
    void testLexTcap1(UnitTest &qunit)
    {
     TcapTokenStream result = lexTcap(
                "@exec \"exec1\"\n"
                "A(student) = load \"(databaseName, inputSetName)\"\n"
                "B(student, examAverage) = apply \"avgExams\" to A[student] retain all\n"
                "C(student, examAverage, hwAverage) = apply \"getHwAvg\" to B[student] retain all\n"
                "D(student, isExamGreater) = C[examAverage] > C[hwAverage] retain student\n"
                "E(student) = filter D by isExamGreater retain student\n"
                "F(name) = apply \"getName\" to E[student] retain none"
                "store F \"(databaseName, outputSetName)\"");


        int tokenIndex = 0;

        QUNIT_IS_EQUAL("@", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::AT_SIGN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("exec", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("\"exec1\"", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::STRING_LITERAL_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("A", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("(", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::LPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("student", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(")", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::RPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("=", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::EQ_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("load", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::LOAD_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("\"(databaseName, inputSetName)\"", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::STRING_LITERAL_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("B", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("(", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::LPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("student", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(",", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::COMMA_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("examAverage", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(")", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::RPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("=", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::EQ_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("apply", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::APPLY_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("\"avgExams\"", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::STRING_LITERAL_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("to", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::TO_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("A", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("[", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::LBRACKET_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("student", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("]", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::RBRACKET_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("retain", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::RETAIN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("all", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::ALL_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("C", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("(", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::LPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("student", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(",", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::COMMA_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("examAverage", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(",", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::COMMA_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("hwAverage", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(")", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::RPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("=", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::EQ_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("apply", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::APPLY_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("\"getHwAvg\"", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::STRING_LITERAL_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("to", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::TO_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("B", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("[", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::LBRACKET_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("student", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("]", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::RBRACKET_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("retain", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::RETAIN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("all", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::ALL_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("D", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("(", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::LPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("student", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(",", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::COMMA_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("isExamGreater", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(")", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::RPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("=", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::EQ_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("C", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("[", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::LBRACKET_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("examAverage", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("]", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::RBRACKET_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(">", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::GREATER_THAN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("C", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("[", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::LBRACKET_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("hwAverage", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("]", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::RBRACKET_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("retain", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::RETAIN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("student", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("E", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("(", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::LPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("student", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(")", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::RPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("=", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::EQ_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("filter", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::FILTER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("D", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("by", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::BY_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("isExamGreater", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("retain", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::RETAIN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("student", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("F", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("(", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::LPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("name", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(")", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::RPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("=", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::EQ_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("apply", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::APPLY_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("\"getName\"", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::STRING_LITERAL_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("to", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::TO_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("E", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("[", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::LBRACKET_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("student", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("]", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::RBRACKET_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("retain", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::RETAIN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("none", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::NONE_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("store", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::STORE_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("F", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("\"(databaseName, outputSetName)\"", result.peek().lexeme);
        QUNIT_IS_EQUAL(TcapTokenType::STRING_LITERAL_TYPE, result.advance().tokenType);


        QUNIT_IS_FALSE(result.hasNext());


    }
}