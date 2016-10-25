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

using pdb_detail::Lexeme;
using pdb_detail::TokenStream;
using pdb_detail::lexTcap;

namespace pdb_tests
{
    void testLexTcap1(UnitTest &qunit)
    {
       TokenStream result = lexTcap(
                "@exec \"exec1\"\n"
                "A(student) = load \"(databaseName, inputSetName)\"\n"
                "B(student, examAverage) = apply \"avgExams\" to A[student] retain all\n"
                "C(student, examAverage, hwAverage) = apply \"getHwAvg\" to B[student] retain all\n"
                "D(student, isExamGreater) = C[examAverage] > C[hwAverage] retain student\n"
                "E(student) = filter D by isExamGreater retain student\n"
                "F(name) = apply \"getName\" to E[student] retain none"
                "store F \"(databaseName, outputSetName)\"");


        int tokenIndex = 0;

        QUNIT_IS_EQUAL("@", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::AT_SIGN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("exec", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("\"exec1\"", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::STRING_LITERAL_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("A", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("(", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::LPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("student", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(")", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::RPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("=", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::EQ_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("load", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::LOAD_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("\"(databaseName, inputSetName)\"", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::STRING_LITERAL_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("B", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("(", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::LPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("student", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(",", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::COMMA_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("examAverage", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(")", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::RPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("=", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::EQ_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("apply", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::APPLY_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("\"avgExams\"", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::STRING_LITERAL_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("to", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::TO_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("A", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("[", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::LBRACKET_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("student", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("]", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::RBRACKET_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("retain", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::RETAIN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("all", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::ALL_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("C", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("(", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::LPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("student", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(",", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::COMMA_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("examAverage", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(",", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::COMMA_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("hwAverage", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(")", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::RPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("=", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::EQ_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("apply", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::APPLY_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("\"getHwAvg\"", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::STRING_LITERAL_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("to", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::TO_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("B", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("[", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::LBRACKET_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("student", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("]", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::RBRACKET_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("retain", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::RETAIN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("all", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::ALL_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("D", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("(", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::LPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("student", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(",", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::COMMA_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("isExamGreater", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(")", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::RPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("=", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::EQ_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("C", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("[", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::LBRACKET_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("examAverage", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("]", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::RBRACKET_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(">", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::GREATER_THAN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("C", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("[", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::LBRACKET_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("hwAverage", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("]", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::RBRACKET_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("retain", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::RETAIN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("student", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("E", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("(", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::LPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("student", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(")", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::RPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("=", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::EQ_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("filter", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::FILTER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("D", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("by", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::BY_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("isExamGreater", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("retain", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::RETAIN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("student", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("F", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("(", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::LPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("name", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(")", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::RPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("=", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::EQ_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("apply", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::APPLY_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("\"getName\"", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::STRING_LITERAL_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("to", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::TO_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("E", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("[", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::LBRACKET_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("student", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("]", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::RBRACKET_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("retain", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::RETAIN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("none", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::NONE_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("store", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::STORE_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("F", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("\"(databaseName, outputSetName)\"", result.peek().getToken());
        QUNIT_IS_EQUAL(Lexeme::STRING_LITERAL_TYPE, result.advance().tokenType);


        QUNIT_IS_FALSE(result.hasNext());


    }
}