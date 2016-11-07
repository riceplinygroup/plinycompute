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
//
// Created by barnett on 10/25/16.
//

#include "TcapParser.h"

#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "ApplyOperation.h"
#include "FilterOperation.h"
#include "LoadOperation.h"
#include "GreaterThanOp.h"
#include "HoistOperation.h"
#include "RetainAllClause.h"
#include "RetainExplicitClause.h"
#include "RetainNoneClause.h"
#include "StoreOperation.h"
#include "TcapAttribute.h"
#include "TcapIdentifier.h"
#include "TcapLexer.h"

using std::function;
using std::make_shared;
using std::shared_ptr;
using std::string;
using std::vector;

using pdb_detail::TokenStream;
using pdb_detail::lexTcap;

namespace pdb_detail
{
    shared_ptr<Token> consumeNext(TokenStream &tokenStream, int expectedType1, int expectedType2)
    {
        if(!tokenStream.hasNext())
            throw "token stream empty";

        Token next = tokenStream.advance();

        if(next.tokenType == expectedType1 || next.tokenType == expectedType2)
            return make_shared<Token>(next);

        throw "neither type found";

    }

    shared_ptr<Token> consumeNext(TokenStream &tokenStream, int expectedType)
    {
        return consumeNext(tokenStream, expectedType, expectedType);
    }


    shared_ptr<TcapIdentifier> makeIdentifier(TokenStream &tokens)
    {
        shared_ptr<Token> ident = consumeNext(tokens, TokenType::IDENTIFIER_TYPE);

        return make_shared<TcapIdentifier>(ident->lexeme);
    }

    shared_ptr<vector<TcapIdentifier>> makeIdentList(TokenStream &tokens)
    {
        shared_ptr<TcapIdentifier> ident = makeIdentifier(tokens);

        shared_ptr<vector<TcapIdentifier>> idents = make_shared<vector<TcapIdentifier>>();

        idents->push_back(*ident.get());

        while(tokens.hasNext() && tokens.peek().tokenType == TokenType::COMMA_TYPE)
        {
            tokens.advance();

            ident = makeIdentifier(tokens);

            idents->push_back(*ident.get());
        }

        return idents;
    }

    shared_ptr<RetainClause> makeRetainClause(TokenStream &tokens)
    {
        shared_ptr<Token> retain = consumeNext(tokens, TokenType::RETAIN_TYPE);

        if (tokens.peek().tokenType == TokenType::ALL_TYPE)
        {
            tokens.advance();
            return make_shared<RetainAllClause>();
        }

        if (tokens.peek().tokenType == TokenType::NONE_TYPE)
        {
            tokens.advance();
            return make_shared<RetainNoneClause>();
        }

        shared_ptr<vector<TcapIdentifier>> columns = makeIdentList(tokens);

        return make_shared<RetainExplicitClause>(columns);
    }


    shared_ptr<ApplyOperation> makeApplyOperation(TokenStream &tokens)
    {
        consumeNext(tokens, TokenType::APPLY_TYPE);

        shared_ptr<Token> applyTargetType = consumeNext(tokens, TokenType::FUNC_TYPE, TokenType::METHOD_TYPE);

        ApplyOperationType applyType = applyTargetType->tokenType == TokenType::FUNC_TYPE ? ApplyOperationType::func : ApplyOperationType::method;

        shared_ptr<Token> applyTarget = consumeNext(tokens, TokenType::STRING_LITERAL_TYPE);

        shared_ptr<Token> to = consumeNext(tokens, TokenType::TO_TYPE);

        shared_ptr<TcapIdentifier> inputTableName = makeIdentifier(tokens);

        consumeNext(tokens, TokenType::LBRACKET_TYPE);

        shared_ptr<vector<TcapIdentifier>> inputTableInputColumnNames = makeIdentList(tokens);

        consumeNext(tokens, TokenType::RBRACKET_TYPE);

        shared_ptr<RetainClause> retainClause = makeRetainClause(tokens);

        return make_shared<ApplyOperation>(applyTarget->lexeme, applyType, *inputTableName.get(), inputTableInputColumnNames, retainClause);


    }

    shared_ptr<HoistOperation> makeHoistOperation(TokenStream &tokens)
    {
        consumeNext(tokens, TokenType::HOIST_TYPE);

        shared_ptr<Token> hoistTarget = consumeNext(tokens, TokenType::STRING_LITERAL_TYPE);

        consumeNext(tokens, TokenType::FROM_TYPE);

        shared_ptr<TcapIdentifier> inputTableName = makeIdentifier(tokens);

        consumeNext(tokens, TokenType::LBRACKET_TYPE);

        shared_ptr<vector<TcapIdentifier>> inputTableInputColumnNames = makeIdentList(tokens);

        consumeNext(tokens, TokenType::RBRACKET_TYPE);

        shared_ptr<RetainClause> retainClause = makeRetainClause(tokens);

        return make_shared<HoistOperation>(hoistTarget->lexeme, *inputTableName.get(), inputTableInputColumnNames, retainClause);


    }

    shared_ptr<LoadOperation> makeLoadOperation(TokenStream &tokens)
    {
        consumeNext(tokens, TokenType::LOAD_TYPE);

        shared_ptr<Token> externSourceString = consumeNext(tokens, TokenType::STRING_LITERAL_TYPE);

        return make_shared<LoadOperation>(externSourceString->lexeme);

    }

    shared_ptr<StoreOperation> makeStoreOperation(TokenStream &tokens)
    {
        consumeNext(tokens, TokenType::STORE_TYPE);

        shared_ptr<TcapIdentifier> outputTable = makeIdentifier(tokens);

        consumeNext(tokens, TokenType::LBRACKET_TYPE);

        shared_ptr<vector<TcapIdentifier>> columnsToStore = makeIdentList(tokens);

        consumeNext(tokens, TokenType::RBRACKET_TYPE);

        shared_ptr<Token> externSourceString = consumeNext(tokens, TokenType::STRING_LITERAL_TYPE);

        return make_shared<StoreOperation>(*outputTable.get(), columnsToStore, externSourceString->lexeme);

    }

    shared_ptr<FilterOperation> makeFilterOperation(TokenStream &tokens)
    {
        consumeNext(tokens, TokenType::FILTER_TYPE);

        shared_ptr<TcapIdentifier> inputTableName = makeIdentifier(tokens);

        consumeNext(tokens, TokenType::BY_TYPE);

        shared_ptr<TcapIdentifier> filterColumnName = makeIdentifier(tokens);

        shared_ptr<RetainClause> retain = makeRetainClause(tokens);

        return make_shared<FilterOperation>(*inputTableName.get(), *filterColumnName.get(), retain);

    }



    shared_ptr<TableExpression> makeBinaryOperation(TokenStream &tokens)
    {
        shared_ptr<TcapIdentifier> lhsTableName = makeIdentifier(tokens);

        shared_ptr<Token> lbracket = consumeNext(tokens, TokenType::LBRACKET_TYPE);

        shared_ptr<TcapIdentifier> lhsColumnName = makeIdentifier(tokens);

        shared_ptr<Token> rbracket = consumeNext(tokens, TokenType::RBRACKET_TYPE);

        shared_ptr<Token> op = consumeNext(tokens, TokenType::GREATER_THAN_TYPE);

        shared_ptr<TcapIdentifier> rhsTableName = makeIdentifier(tokens);

        lbracket = consumeNext(tokens, TokenType::LBRACKET_TYPE);

        shared_ptr<TcapIdentifier> rhsColumnName = makeIdentifier(tokens);

        rbracket = consumeNext(tokens, TokenType::RBRACKET_TYPE);

        shared_ptr<RetainClause> retain = makeRetainClause(tokens);

        if(op->tokenType == TokenType::GREATER_THAN_TYPE)
            return make_shared<GreaterThanOp>(*lhsTableName.get(), *lhsColumnName.get(), *rhsTableName.get(), *rhsColumnName.get(), retain);

        throw "Unknown operator type";
    }

    shared_ptr<TableExpression> makeTableExpression(TokenStream &tokens)
    {
        switch(tokens.peek().tokenType)
        {
            case (TokenType::APPLY_TYPE):
                return makeApplyOperation(tokens);
            case (TokenType::HOIST_TYPE):
                return makeHoistOperation(tokens);
            case (TokenType::LOAD_TYPE):
                return makeLoadOperation(tokens);
            case(TokenType::FILTER_TYPE):
                return makeFilterOperation(tokens);
            default:
                return makeBinaryOperation(tokens);
        }
    }

    shared_ptr<TableAssignment> makeTableAssignment(TokenStream &tokens)
    {
        shared_ptr<TcapIdentifier> tableName = makeIdentifier(tokens);

        consumeNext(tokens, TokenType::LPAREN_TYPE);

        shared_ptr<vector<TcapIdentifier>> columnNames = makeIdentList(tokens);

        consumeNext(tokens, TokenType::RPAREN_TYPE);

        consumeNext(tokens, TokenType::EQ_TYPE);

        shared_ptr<TableExpression> tableExp = makeTableExpression(tokens);

        return make_shared<TableAssignment>(*tableName.get(), columnNames, tableExp);
    }

    shared_ptr<TcapAttribute> makeAttribute(TokenStream &tokens)
    {
        consumeNext(tokens, TokenType::AT_SIGN_TYPE);

        shared_ptr<TcapIdentifier> name = makeIdentifier(tokens);

        shared_ptr<Token> value = consumeNext(tokens, TokenType::STRING_LITERAL_TYPE);

        return make_shared<TcapAttribute>(*name.get(), make_shared<string>(value->lexeme));

    }

    shared_ptr<TcapStatement> makeStatement(TokenStream &tokens)
    {
        shared_ptr<vector<TcapAttribute>> attributes = make_shared<vector<TcapAttribute>>();

        while(tokens.hasNext() && tokens.peek().tokenType == TokenType::AT_SIGN_TYPE)
        {
            shared_ptr<TcapAttribute> attribute = makeAttribute(tokens);

            attributes->push_back(*attribute.get());
        }

        shared_ptr<TcapStatement> stmt;
        switch (tokens.peek().tokenType)
        {
            case TokenType::STORE_TYPE:
                stmt = makeStoreOperation(tokens);
                break;
            default:
                stmt = makeTableAssignment(tokens);
                break;

        }

        stmt->attributes = attributes;

        return stmt;

    }

    shared_ptr<TranslationUnit> parseTcap(const string &source)
    {
        TokenStream tokens = lexTcap(source);

        shared_ptr<TranslationUnit> unit = make_shared<TranslationUnit>();

        while(tokens.hasNext())
        {
            shared_ptr<TcapStatement> stmt;
            try
            {
                stmt = makeStatement(tokens);
            }
            catch(const string& msg)
            {
                return nullptr;
            }

            unit->statements->push_back(stmt);
        }

        return unit;
    }
}