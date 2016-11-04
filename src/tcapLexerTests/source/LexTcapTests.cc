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

using pdb_detail::Token;
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

        QUNIT_IS_EQUAL("@", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::AT_SIGN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("exec", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("\"exec1\"", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::STRING_LITERAL_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("A", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("(", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::LPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("student", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(")", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::RPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("=", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::EQ_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("load", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::LOAD_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("\"(databaseName, inputSetName)\"", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::STRING_LITERAL_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("B", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("(", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::LPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("student", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(",", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::COMMA_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("examAverage", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(")", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::RPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("=", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::EQ_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("apply", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::APPLY_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("\"avgExams\"", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::STRING_LITERAL_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("to", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::TO_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("A", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("[", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::LBRACKET_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("student", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("]", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::RBRACKET_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("retain", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::RETAIN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("all", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::ALL_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("C", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("(", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::LPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("student", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(",", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::COMMA_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("examAverage", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(",", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::COMMA_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("hwAverage", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(")", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::RPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("=", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::EQ_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("apply", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::APPLY_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("\"getHwAvg\"", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::STRING_LITERAL_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("to", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::TO_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("B", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("[", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::LBRACKET_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("student", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("]", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::RBRACKET_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("retain", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::RETAIN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("all", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::ALL_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("D", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("(", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::LPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("student", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(",", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::COMMA_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("isExamGreater", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(")", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::RPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("=", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::EQ_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("C", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("[", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::LBRACKET_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("examAverage", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("]", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::RBRACKET_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(">", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::GREATER_THAN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("C", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("[", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::LBRACKET_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("hwAverage", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("]", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::RBRACKET_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("retain", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::RETAIN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("student", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("E", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("(", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::LPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("student", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(")", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::RPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("=", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::EQ_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("filter", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::FILTER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("D", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("by", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::BY_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("isExamGreater", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("retain", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::RETAIN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("student", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("F", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("(", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::LPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("name", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL(")", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::RPAREN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("=", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::EQ_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("apply", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::APPLY_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("\"getName\"", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::STRING_LITERAL_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("to", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::TO_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("E", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("[", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::LBRACKET_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("student", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("]", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::RBRACKET_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("retain", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::RETAIN_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("none", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::NONE_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("store", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::STORE_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("F", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::IDENTIFIER_TYPE, result.advance().tokenType);

        QUNIT_IS_EQUAL("\"(databaseName, outputSetName)\"", result.peek().value);
        QUNIT_IS_EQUAL(TokenType::STRING_LITERAL_TYPE, result.advance().tokenType);


        QUNIT_IS_FALSE(result.hasNext());


    }
}