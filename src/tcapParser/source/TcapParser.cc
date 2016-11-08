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


namespace pdb_detail
{
    /**
     * Advances the given tokenStream one token and returns that token if it's type is either
     * expectedType1 or expectedType2.
     *
     * If the token from the stream is not of type expectedType1 or expectedType2 a string
     * exception is thrown.
     *
     * If the given tokenStream does not have a next token, a string exception is thrown.
     *
     * @param tokenStream
     * @param expectedType1
     * @param expectedType2
     * @return
     */
    TcapTokenPtr consumeNext(TcapTokenStream &tokenStream, TcapTokenType expectedType1,
                                      TcapTokenType expectedType2)
    {
        if(!tokenStream.hasNext())
            throw "token stream empty";

        TcapToken next = tokenStream.advance();

        if(next.tokenType == expectedType1 || next.tokenType == expectedType2)
            return make_shared<TcapToken>(next);

        throw "neither type found";

    }

    TcapTokenPtr consumeNext(TcapTokenStream &tokenStream, TcapTokenType expectedType)
    {
        return consumeNext(tokenStream, expectedType, expectedType);
    }


    shared_ptr<TcapIdentifier> makeIdentifier(TcapTokenStream &tokens)
    {
        shared_ptr<TcapToken> ident = consumeNext(tokens, TcapTokenType::IDENTIFIER_TYPE);

        return make_shared<TcapIdentifier>(ident->lexeme);
    }

    shared_ptr<vector<TcapIdentifier>> makeIdentList(TcapTokenStream &tokens)
    {
        shared_ptr<TcapIdentifier> ident = makeIdentifier(tokens);

        shared_ptr<vector<TcapIdentifier>> idents = make_shared<vector<TcapIdentifier>>();

        idents->push_back(*ident.get());

        while(tokens.hasNext() && tokens.peek().tokenType == TcapTokenType::COMMA_TYPE)
        {
            tokens.advance();

            ident = makeIdentifier(tokens);

            idents->push_back(*ident.get());
        }

        return idents;
    }

    shared_ptr<RetainClause> makeRetainClause(TcapTokenStream &tokens)
    {
        shared_ptr<TcapToken> retain = consumeNext(tokens, TcapTokenType::RETAIN_TYPE);

        if (tokens.peek().tokenType == TcapTokenType::ALL_TYPE)
        {
            tokens.advance();
            return make_shared<RetainAllClause>();
        }

        if (tokens.peek().tokenType == TcapTokenType::NONE_TYPE)
        {
            tokens.advance();
            return make_shared<RetainNoneClause>();
        }

        shared_ptr<vector<TcapIdentifier>> columns = makeIdentList(tokens);

        return make_shared<RetainExplicitClause>(columns);
    }


    shared_ptr<ApplyOperation> makeApplyOperation(TcapTokenStream &tokens)
    {
        consumeNext(tokens, TcapTokenType::APPLY_TYPE);

        shared_ptr<TcapToken> applyTargetType = consumeNext(tokens, TcapTokenType::FUNC_TYPE, TcapTokenType::METHOD_TYPE);

        ApplyOperationType applyType = applyTargetType->tokenType == TcapTokenType::FUNC_TYPE ? ApplyOperationType::func : ApplyOperationType::method;

        shared_ptr<TcapToken> applyTarget = consumeNext(tokens, TcapTokenType::STRING_LITERAL_TYPE);

        shared_ptr<TcapToken> to = consumeNext(tokens, TcapTokenType::TO_TYPE);

        shared_ptr<TcapIdentifier> inputTableName = makeIdentifier(tokens);

        consumeNext(tokens, TcapTokenType::LBRACKET_TYPE);

        shared_ptr<vector<TcapIdentifier>> inputTableInputColumnNames = makeIdentList(tokens);

        consumeNext(tokens, TcapTokenType::RBRACKET_TYPE);

        shared_ptr<RetainClause> retainClause = makeRetainClause(tokens);

        return make_shared<ApplyOperation>(applyTarget->lexeme, applyType, *inputTableName.get(), inputTableInputColumnNames, retainClause);


    }

    shared_ptr<HoistOperation> makeHoistOperation(TcapTokenStream &tokens)
    {
        consumeNext(tokens, TcapTokenType::HOIST_TYPE);

        shared_ptr<TcapToken> hoistTarget = consumeNext(tokens, TcapTokenType::STRING_LITERAL_TYPE);

        consumeNext(tokens, TcapTokenType::FROM_TYPE);

        shared_ptr<TcapIdentifier> inputTableName = makeIdentifier(tokens);

        consumeNext(tokens, TcapTokenType::LBRACKET_TYPE);

        shared_ptr<vector<TcapIdentifier>> inputTableInputColumnNames = makeIdentList(tokens);

        consumeNext(tokens, TcapTokenType::RBRACKET_TYPE);

        shared_ptr<RetainClause> retainClause = makeRetainClause(tokens);

        return make_shared<HoistOperation>(hoistTarget->lexeme, *inputTableName.get(), inputTableInputColumnNames, retainClause);


    }

    shared_ptr<LoadOperation> makeLoadOperation(TcapTokenStream &tokens)
    {
        consumeNext(tokens, TcapTokenType::LOAD_TYPE);

        shared_ptr<TcapToken> externSourceString = consumeNext(tokens, TcapTokenType::STRING_LITERAL_TYPE);

        return make_shared<LoadOperation>(externSourceString->lexeme);

    }

    shared_ptr<StoreOperation> makeStoreOperation(TcapTokenStream &tokens)
    {
        consumeNext(tokens, TcapTokenType::STORE_TYPE);

        shared_ptr<TcapIdentifier> outputTable = makeIdentifier(tokens);

        consumeNext(tokens, TcapTokenType::LBRACKET_TYPE);

        shared_ptr<vector<TcapIdentifier>> columnsToStore = makeIdentList(tokens);

        consumeNext(tokens, TcapTokenType::RBRACKET_TYPE);

        shared_ptr<TcapToken> externSourceString = consumeNext(tokens, TcapTokenType::STRING_LITERAL_TYPE);

        return make_shared<StoreOperation>(*outputTable.get(), columnsToStore, externSourceString->lexeme);

    }

    shared_ptr<FilterOperation> makeFilterOperation(TcapTokenStream &tokens)
    {
        consumeNext(tokens, TcapTokenType::FILTER_TYPE);

        shared_ptr<TcapIdentifier> inputTableName = makeIdentifier(tokens);

        consumeNext(tokens, TcapTokenType::BY_TYPE);

        shared_ptr<TcapIdentifier> filterColumnName = makeIdentifier(tokens);

        shared_ptr<RetainClause> retain = makeRetainClause(tokens);

        return make_shared<FilterOperation>(*inputTableName.get(), *filterColumnName.get(), retain);

    }



    shared_ptr<TableExpression> makeBinaryOperation(TcapTokenStream &tokens)
    {
        shared_ptr<TcapIdentifier> lhsTableName = makeIdentifier(tokens);

        shared_ptr<TcapToken> lbracket = consumeNext(tokens, TcapTokenType::LBRACKET_TYPE);

        shared_ptr<TcapIdentifier> lhsColumnName = makeIdentifier(tokens);

        shared_ptr<TcapToken> rbracket = consumeNext(tokens, TcapTokenType::RBRACKET_TYPE);

        shared_ptr<TcapToken> op = consumeNext(tokens, TcapTokenType::GREATER_THAN_TYPE);

        shared_ptr<TcapIdentifier> rhsTableName = makeIdentifier(tokens);

        lbracket = consumeNext(tokens, TcapTokenType::LBRACKET_TYPE);

        shared_ptr<TcapIdentifier> rhsColumnName = makeIdentifier(tokens);

        rbracket = consumeNext(tokens, TcapTokenType::RBRACKET_TYPE);

        shared_ptr<RetainClause> retain = makeRetainClause(tokens);

        if(op->tokenType == TcapTokenType::GREATER_THAN_TYPE)
            return make_shared<GreaterThanOp>(*lhsTableName.get(), *lhsColumnName.get(), *rhsTableName.get(), *rhsColumnName.get(), retain);

        throw "Unknown operator type";
    }

    shared_ptr<TableExpression> makeTableExpression(TcapTokenStream &tokens)
    {
        switch(tokens.peek().tokenType)
        {
            case (TcapTokenType::APPLY_TYPE):
                return makeApplyOperation(tokens);
            case (TcapTokenType::HOIST_TYPE):
                return makeHoistOperation(tokens);
            case (TcapTokenType::LOAD_TYPE):
                return makeLoadOperation(tokens);
            case(TcapTokenType::FILTER_TYPE):
                return makeFilterOperation(tokens);
            default:
                return makeBinaryOperation(tokens);
        }
    }

    shared_ptr<TableAssignment> makeTableAssignment(TcapTokenStream &tokens)
    {
        shared_ptr<TcapIdentifier> tableName = makeIdentifier(tokens);

        consumeNext(tokens, TcapTokenType::LPAREN_TYPE);

        shared_ptr<vector<TcapIdentifier>> columnNames = makeIdentList(tokens);

        consumeNext(tokens, TcapTokenType::RPAREN_TYPE);

        consumeNext(tokens, TcapTokenType::EQ_TYPE);

        shared_ptr<TableExpression> tableExp = makeTableExpression(tokens);

        return make_shared<TableAssignment>(*tableName.get(), columnNames, tableExp);
    }

    shared_ptr<TcapAttribute> makeAttribute(TcapTokenStream &tokens)
    {
        consumeNext(tokens, TcapTokenType::AT_SIGN_TYPE);

        shared_ptr<TcapIdentifier> name = makeIdentifier(tokens);

        shared_ptr<TcapToken> value = consumeNext(tokens, TcapTokenType::STRING_LITERAL_TYPE);

        return make_shared<TcapAttribute>(*name.get(), make_shared<string>(value->lexeme));

    }

    shared_ptr<TcapStatement> makeStatement(TcapTokenStream &tokens)
    {
        shared_ptr<vector<TcapAttribute>> attributes = make_shared<vector<TcapAttribute>>();

        while(tokens.hasNext() && tokens.peek().tokenType == TcapTokenType::AT_SIGN_TYPE)
        {
            shared_ptr<TcapAttribute> attribute = makeAttribute(tokens);

            attributes->push_back(*attribute.get());
        }

        shared_ptr<TcapStatement> stmt;
        switch (tokens.peek().tokenType)
        {
            case TcapTokenType::STORE_TYPE:
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
        TcapTokenStream tokens = lexTcap(source);

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