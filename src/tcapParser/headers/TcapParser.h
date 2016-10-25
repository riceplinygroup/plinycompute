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
// Created by barnett on 10/12/16.
//

#ifndef PDB_TCAPPARSER_TCAPPARSER_H
#define PDB_TCAPPARSER_TCAPPARSER_H

#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

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
    shared_ptr<Lexeme> consumeNext(TokenStream &tokenStream, int expectedType1, int expectedType2)
    {
        if(!tokenStream.hasNext())
            return nullptr;

        Lexeme next = tokenStream.advance();

        if(next.tokenType == expectedType1 || next.tokenType == expectedType2)
            return make_shared<Lexeme>(next);

        return nullptr;

    }

    shared_ptr<Lexeme> consumeNext(TokenStream &tokenStream, int expectedType)
    {
        return consumeNext(tokenStream, expectedType, expectedType);
    }

    class Identifier
    {

    public:

        Identifier(string contents) : contents(make_shared<string>(contents))
        {
        }

        Identifier(shared_ptr<string> contents) : contents(contents)
        {

        }

        shared_ptr<string> contents;

    };

    class MemoStatement;

    class TableAssignment;

    class StoreOperation;

    class Attribute
    {
    public:

        Identifier name;

        shared_ptr<string> value;

        Attribute(Identifier name,  shared_ptr<string> value) : name(name), value(value)
        {

        }



    };

    class Statement
    {
    public:

        shared_ptr<vector<Attribute>> attributes;

        Statement() : attributes(make_shared<vector<Attribute>>())
        {

        }

        virtual void execute(function<void(MemoStatement&)> forMemo,
                             function<void(TableAssignment&)> forTableAssignment, function<void(StoreOperation&)> forStore) = 0;
    };

    class StringLiteral
    {

    public:

        StringLiteral(string contents) : contents(make_shared<string>(contents))
        {
        }

        StringLiteral(shared_ptr<string> contents) : contents(contents)
        {

        }

        shared_ptr<string> contents;

    };



    class RetainAllClause;

    class RetainExplicitClause;

    class RetainNoneClause;

    class RetainClause
    {
    public:

        virtual bool isAll() = 0;

        virtual bool isNone() = 0;

        virtual void execute(function<void(RetainAllClause&)> forAll, function<void(RetainExplicitClause&)> forExplicit, function<void(RetainNoneClause&)> forNone) = 0;
    };

    class RetainAllClause : public RetainClause
    {
        bool isAll() override
        {
            return true;
        }

        bool isNone() override
        {
            return false;
        }

        void execute(function<void(RetainAllClause&)> forAll, function<void(RetainExplicitClause&)>, function<void(RetainNoneClause&)> forNone) override
        {
            forAll(*this);
        }
    };

    class RetainNoneClause : public RetainClause
    {
        bool isAll() override
        {
            return false;
        }

        bool isNone() override
        {
            return true;
        }

        void execute(function<void(RetainAllClause&)> forAll, function<void(RetainExplicitClause&)>, function<void(RetainNoneClause&)> forNone) override
        {
            forNone(*this);
        }
    };

    class RetainExplicitClause : public RetainClause
    {
    public:

        shared_ptr<vector<Identifier>> columns;

        RetainExplicitClause(shared_ptr<vector<Identifier>> columns) : columns(columns)
        {
        }

        bool isAll() override
        {
            return false;
        }

        bool isNone() override
        {
            return false;
        }

        void execute(function<void(RetainAllClause&)>, function<void(RetainExplicitClause&)> forExplicit, function<void(RetainNoneClause&)> forNone) override
        {
            forExplicit(*this);
        }
    };

    class ApplyOperation;

    class LoadOperation;

    class FilterOperation;

    class HoistOperation;

    class BinaryOperation;

    class TableExpression
    {
    public:

        virtual void execute(function<void(LoadOperation&)> forLoad, function<void(ApplyOperation&)> forApply,
                             function<void(FilterOperation&)> forFilter, function<void(HoistOperation&)> forHoist,
                             function<void(BinaryOperation&)> forBinaryOp) = 0;
    };

    enum ApplyOperationType
    {
        func, method
    };

    class ApplyOperation : public TableExpression
    {
    public:

        shared_ptr<string> applyTarget;

        ApplyOperationType applyType;

        Identifier inputTable;

        shared_ptr<vector<Identifier>> inputTableColumnNames;

        shared_ptr<RetainClause> retain;

        ApplyOperation(string applyTarget, ApplyOperationType applyType, Identifier inputTable, shared_ptr<vector<Identifier>> inputTableColumnNames,
                       shared_ptr<RetainClause> retain)
                : ApplyOperation(make_shared<string>(applyTarget), applyType, inputTable, inputTableColumnNames, retain)
        {
        }

        ApplyOperation(shared_ptr<string> applyTarget, ApplyOperationType applyType, Identifier inputTable,
                       shared_ptr<vector<Identifier>> inputTableColumnNames, shared_ptr<RetainClause> retain)
                : applyTarget(applyTarget), applyType(applyType),  inputTable(inputTable), inputTableColumnNames(inputTableColumnNames),
                  retain(retain)
        {
        }

        void execute(function<void(LoadOperation&)>, function<void(ApplyOperation&)> forApply, function<void(FilterOperation&)>,
                     function<void(HoistOperation&)>, function<void(BinaryOperation&)>) override
        {
            return forApply(*this);
        }
    };

    class LoadOperation : public TableExpression
    {
    public:

        LoadOperation(shared_ptr<string> source) : source(source)
        {

        }

        LoadOperation(string source) : source(make_shared<string>(source))
        {

        }

        shared_ptr<string> source;

        void execute(function<void(LoadOperation&)> forLoad, function<void(ApplyOperation&)>,
                     function<void(FilterOperation&)>,function<void(HoistOperation&)> forHoist,
                     function<void(BinaryOperation&)> forBinaryOp) override
        {
            return forLoad(*this);
        }

    };

    class StoreOperation : public Statement
    {
    public:

        shared_ptr<string> destination;

        Identifier outputTable;

        StoreOperation(Identifier outputTable, shared_ptr<string> destination) : outputTable(outputTable),  destination(destination)
        {

        }

        StoreOperation(Identifier outputTable, string destination) : outputTable(outputTable), destination(make_shared<string>(destination))
        {

        }

        void execute(function<void(MemoStatement&)>, function<void(TableAssignment&)>, function<void(StoreOperation&)> forStore) override
        {
            forStore(*this);
        }
    };

    class FilterOperation : public TableExpression
    {
    public:

        Identifier inputTableName;

        Identifier filterColumnName;

        shared_ptr<RetainClause> retain;

        FilterOperation(Identifier inputTableName, Identifier filterColumnName, shared_ptr<RetainClause> retain)
                : inputTableName(inputTableName), filterColumnName(filterColumnName), retain(retain)
        {

        }

        void execute(function<void(LoadOperation&)>, function<void(ApplyOperation&)>,
                     function<void(FilterOperation&)> forFilter, function<void(HoistOperation&)> forHoist,
                     function<void(BinaryOperation&)> forBinaryOp) override
        {
            return forFilter(*this);
        }
    };

    shared_ptr<Identifier> makeIdentifier(TokenStream &tokens)
    {
        shared_ptr<Lexeme> ident = consumeNext(tokens, Lexeme::IDENTIFIER_TYPE);

        if(ident == nullptr)
            return nullptr;

        return make_shared<Identifier>(ident->getToken());
    }

    shared_ptr<vector<Identifier>> makeIdentList(TokenStream &tokens)
    {
        shared_ptr<Identifier> ident = makeIdentifier(tokens);

        if(ident == nullptr)
            return nullptr;

        shared_ptr<vector<Identifier>> idents = make_shared<vector<Identifier>>();

        idents->push_back(*ident.get());


        while(tokens.hasNext() && tokens.peek().tokenType == Lexeme::COMMA_TYPE)
        {
            tokens.advance();

            ident = makeIdentifier(tokens);

            if(ident == nullptr)
                return nullptr;

            idents->push_back(*ident.get());
        }

        return idents;
    }


    class MemoStatement : public Statement
    {
    public:

        MemoStatement(StringLiteral stringLiteral) : stringLiteral(stringLiteral)
        {
        }

        StringLiteral stringLiteral;

        void execute(function<void(MemoStatement&)> forMemo, function<void(TableAssignment&)>, function<void(StoreOperation&)>) override
        {
            forMemo(*this);
        }

    };

    class TableAssignment : public Statement
    {
    public:

        Identifier tableName;

        shared_ptr<vector<Identifier>> columnNames;

        shared_ptr<TableExpression> value;

        TableAssignment(Identifier tableName, shared_ptr<vector<Identifier>> columnNames,  shared_ptr<TableExpression> value)
                : tableName(tableName), columnNames(columnNames), value(value)
        {
        }

        virtual void execute(function<void(MemoStatement&)> forMemo,
                             function<void(TableAssignment&)> forTableAssignment, function<void(StoreOperation&)> forStore) override {
            forTableAssignment(*this);
        }

    };

    class TranslationUnit
    {

    public:

        shared_ptr<vector<shared_ptr<Statement>>> const statements = make_shared<vector<shared_ptr<Statement>>>();
    };


    shared_ptr<RetainClause> makeRetainClause(TokenStream &tokens)
    {
        shared_ptr<Lexeme> retain = consumeNext(tokens, Lexeme::RETAIN_TYPE);

        if (retain == nullptr)
            return nullptr;

        if (tokens.peek().tokenType == Lexeme::ALL_TYPE)
        {
            tokens.advance();
            return make_shared<RetainAllClause>();
        }

        if (tokens.peek().tokenType == Lexeme::NONE_TYPE)
        {
            tokens.advance();
            return make_shared<RetainNoneClause>();
        }


        shared_ptr<vector<Identifier>> columns = makeIdentList(tokens);

        if(columns == nullptr)
            return nullptr;

        return make_shared<RetainExplicitClause>(columns);

        return nullptr;
    }

    class HoistOperation : public TableExpression
    {
    public:

        shared_ptr<string> hoistTarget;

        Identifier inputTable;

        shared_ptr<vector<Identifier>> inputTableColumnNames;

        shared_ptr<RetainClause> retain;

        HoistOperation(string hoistTarget, Identifier inputTable, shared_ptr<vector<Identifier>> inputTableColumnNames,
                       shared_ptr<RetainClause> retain)
                : HoistOperation(make_shared<string>(hoistTarget), inputTable, inputTableColumnNames, retain)
        {
        }

        HoistOperation(shared_ptr<string> hoistTarget, Identifier inputTable,
                       shared_ptr<vector<Identifier>> inputTableColumnNames, shared_ptr<RetainClause> retain)
                : hoistTarget(hoistTarget), inputTable(inputTable), inputTableColumnNames(inputTableColumnNames),
                  retain(retain)
        {
        }

        void execute(function<void(LoadOperation&)>, function<void(ApplyOperation&)>, function<void(FilterOperation&)>,
                     function<void(HoistOperation&)> forHoist, function<void(BinaryOperation&)> forBinaryOp) override
        {
            return forHoist(*this);
        }
    };

    shared_ptr<ApplyOperation> makeApplyOperation(TokenStream &tokens)
    {
        shared_ptr<Lexeme> apply = consumeNext(tokens, Lexeme::APPLY_TYPE);

        if(apply == nullptr)
            return nullptr;

        shared_ptr<Lexeme> applyTargetType = consumeNext(tokens, Lexeme::FUNC_TYPE, Lexeme::METHOD_TYPE);

        if(applyTargetType == nullptr)
            return nullptr;

        ApplyOperationType applyType = applyTargetType->tokenType == Lexeme::FUNC_TYPE ? ApplyOperationType::func : ApplyOperationType::method;


        shared_ptr<Lexeme> applyTarget = consumeNext(tokens, Lexeme::STRING_LITERAL_TYPE);

        if(applyTarget == nullptr)
            return nullptr;

        shared_ptr<Lexeme> to = consumeNext(tokens, Lexeme::TO_TYPE);

        if(to == nullptr)
            return nullptr;

        shared_ptr<Identifier> inputTableName = makeIdentifier(tokens);

        if(inputTableName == nullptr)
            return nullptr;

        shared_ptr<Lexeme> lbracket = consumeNext(tokens, Lexeme::LBRACKET_TYPE);

        if(lbracket == nullptr)
            return nullptr;

        shared_ptr<vector<Identifier>> inputTableInputColumnNames = makeIdentList(tokens);

        if(inputTableInputColumnNames == nullptr)
            return nullptr;

        shared_ptr<Lexeme> rbracket = consumeNext(tokens, Lexeme::RBRACKET_TYPE);

        if(rbracket == nullptr)
            return nullptr;

        shared_ptr<RetainClause> retainClause = makeRetainClause(tokens);

        return make_shared<ApplyOperation>(applyTarget->getToken(), applyType, *inputTableName.get(), inputTableInputColumnNames, retainClause);


    }

    shared_ptr<HoistOperation> makeHoistOperation(TokenStream &tokens)
    {
        shared_ptr<Lexeme> hoist = consumeNext(tokens, Lexeme::HOIST_TYPE);

        if(hoist == nullptr)
            return nullptr;

        shared_ptr<Lexeme> hoistTarget = consumeNext(tokens, Lexeme::STRING_LITERAL_TYPE);

        if(hoistTarget == nullptr)
            return nullptr;

        shared_ptr<Lexeme> from = consumeNext(tokens, Lexeme::FROM_TYPE);

        if(from == nullptr)
            return nullptr;

        shared_ptr<Identifier> inputTableName = makeIdentifier(tokens);

        if(inputTableName == nullptr)
            return nullptr;

        shared_ptr<Lexeme> lbracket = consumeNext(tokens, Lexeme::LBRACKET_TYPE);

        if(lbracket == nullptr)
            return nullptr;

        shared_ptr<vector<Identifier>> inputTableInputColumnNames = makeIdentList(tokens);

        if(inputTableInputColumnNames == nullptr)
            return nullptr;

        shared_ptr<Lexeme> rbracket = consumeNext(tokens, Lexeme::RBRACKET_TYPE);

        if(rbracket == nullptr)
            return nullptr;

        shared_ptr<RetainClause> retainClause = makeRetainClause(tokens);

        return make_shared<HoistOperation>(hoistTarget->getToken(), *inputTableName.get(), inputTableInputColumnNames, retainClause);


    }

    shared_ptr<LoadOperation> makeLoadOperation(TokenStream &tokens)
    {
        shared_ptr<Lexeme> load = consumeNext(tokens, Lexeme::LOAD_TYPE);

        if(load == nullptr)
            return nullptr;

        shared_ptr<Lexeme> externSourceString = consumeNext(tokens, Lexeme::STRING_LITERAL_TYPE);

        if(externSourceString == nullptr)
            return nullptr;

        return make_shared<LoadOperation>(externSourceString->getToken());

    }

    shared_ptr<StoreOperation> makeStoreOperation(TokenStream &tokens)
    {
        shared_ptr<Lexeme> store = consumeNext(tokens, Lexeme::STORE_TYPE);

        if(store == nullptr)
            return nullptr;

        shared_ptr<Identifier> outputTable = makeIdentifier(tokens);

        if(outputTable == nullptr)
            return nullptr;

        shared_ptr<Lexeme> externSourceString = consumeNext(tokens, Lexeme::STRING_LITERAL_TYPE);

        if(externSourceString == nullptr)
            return nullptr;

        return make_shared<StoreOperation>(*outputTable.get(), externSourceString->getToken());

    }

    shared_ptr<FilterOperation> makeFilterOperation(TokenStream &tokens)
    {
        shared_ptr<Lexeme> filter = consumeNext(tokens, Lexeme::FILTER_TYPE);

        if(filter == nullptr)
            return nullptr;

        shared_ptr<Identifier> inputTableName = makeIdentifier(tokens);

        if(inputTableName == nullptr)
            return nullptr;

        shared_ptr<Lexeme> by = consumeNext(tokens, Lexeme::BY_TYPE);

        if(by == nullptr)
            return nullptr;


        shared_ptr<Identifier> filterColumnName = makeIdentifier(tokens);

        if(filterColumnName == nullptr)
            return nullptr;

        shared_ptr<RetainClause> retain = makeRetainClause(tokens);

        if(retain == nullptr)
            return nullptr;

        return make_shared<FilterOperation>(*inputTableName.get(), *filterColumnName.get(), retain);

    }

    class GreaterThanOp;

    class BinaryOperation : public TableExpression
    {
    public:

        void execute(function<void(LoadOperation&)>, function<void(ApplyOperation&)>,
                     function<void(FilterOperation&)>, function<void(HoistOperation&)>,
                     function<void(BinaryOperation&)> forBinaryOp)
        {
            forBinaryOp(*this);
        }

        virtual void execute(function<void(GreaterThanOp&)> forGreaterThan) = 0;
    };

    class GreaterThanOp : public BinaryOperation
    {
    public:

        Identifier lhsTableName;

        Identifier lhsColumnName;

        Identifier rhsTableName;

        Identifier rhsColumnName;

        shared_ptr<RetainClause> retain;

        GreaterThanOp(Identifier lhsTableName, Identifier lhsColumnName, Identifier rhsTableName, Identifier rhsColumnName, shared_ptr<RetainClause> retain)
                : lhsTableName(lhsTableName), lhsColumnName(lhsColumnName), rhsTableName(rhsTableName), rhsColumnName(rhsColumnName), retain(retain)
        {

        }

        void execute(function<void(GreaterThanOp&)> forGreaterThan) override
        {
            forGreaterThan(*this);
        }

    };

    shared_ptr<TableExpression> makeBinaryOperation(TokenStream &tokens)
    {
        shared_ptr<Identifier> lhsTableName = makeIdentifier(tokens);

        if(lhsTableName == nullptr)
            return nullptr;

        shared_ptr<Lexeme> lbracket = consumeNext(tokens, Lexeme::LBRACKET_TYPE);

        if(lbracket == nullptr)
            return nullptr;

        shared_ptr<Identifier> lhsColumnName = makeIdentifier(tokens);

        if(lhsColumnName == nullptr)
            return nullptr;

        shared_ptr<Lexeme> rbracket = consumeNext(tokens, Lexeme::RBRACKET_TYPE);

        if(rbracket == nullptr)
            return nullptr;


        shared_ptr<Lexeme> op = consumeNext(tokens, Lexeme::GREATER_THAN_TYPE);

        if(op == nullptr)
            return nullptr;

        shared_ptr<Identifier> rhsTableName = makeIdentifier(tokens);

        if(rhsTableName == nullptr)
            return nullptr;

        lbracket = consumeNext(tokens, Lexeme::LBRACKET_TYPE);

        if(lbracket == nullptr)
            return nullptr;

        shared_ptr<Identifier> rhsColumnName = makeIdentifier(tokens);

        if(rhsColumnName == nullptr)
            return nullptr;

        rbracket = consumeNext(tokens, Lexeme::RBRACKET_TYPE);

        if(rbracket == nullptr)
            return nullptr;

        shared_ptr<RetainClause> retain = makeRetainClause(tokens);

        if(retain == nullptr)
            return nullptr;

        if(op->tokenType == Lexeme::GREATER_THAN_TYPE)
            return make_shared<GreaterThanOp>(*lhsTableName.get(), *lhsColumnName.get(), *rhsTableName.get(), *rhsColumnName.get(), retain);


        return nullptr;
    }

    shared_ptr<TableExpression> makeTableExpression(TokenStream &tokens)
    {
        switch(tokens.peek().tokenType)
        {
            case (Lexeme::APPLY_TYPE):
                return makeApplyOperation(tokens);
            case (Lexeme::HOIST_TYPE):
                return makeHoistOperation(tokens);
            case (Lexeme::LOAD_TYPE):
                return makeLoadOperation(tokens);
            case(Lexeme::FILTER_TYPE):
                return makeFilterOperation(tokens);
            default:
                return makeBinaryOperation(tokens);
        }
    }

    shared_ptr<TableAssignment> makeTableAssignment(TokenStream &tokens)
    {

        shared_ptr<Identifier> tableName = makeIdentifier(tokens);

        if(tableName == nullptr)
            return nullptr;

        shared_ptr<Lexeme> lparen = consumeNext(tokens, Lexeme::LPAREN_TYPE);

        if(lparen == nullptr)
            return nullptr;


        shared_ptr<vector<Identifier>> columnNames = makeIdentList(tokens);

        if(columnNames == nullptr)
            return nullptr;

        shared_ptr<Lexeme> rparen = consumeNext(tokens, Lexeme::RPAREN_TYPE);

        if(rparen == nullptr)
            return nullptr;

        shared_ptr<Lexeme> eq = consumeNext(tokens, Lexeme::EQ_TYPE);

        if(eq == nullptr)
            return nullptr;

        shared_ptr<TableExpression> tableExp = makeTableExpression(tokens);

        if(tableExp == nullptr)
            return nullptr;

        return make_shared<TableAssignment>(*tableName.get(), columnNames, tableExp);
    }

    shared_ptr<Attribute> makeAttribute(TokenStream &tokens)
    {
        shared_ptr<Lexeme> at = consumeNext(tokens, Lexeme::AT_SIGN_TYPE);

        if(at == nullptr)
            return nullptr;

        shared_ptr<Identifier> name = makeIdentifier(tokens);

        if(name == nullptr)
            return nullptr;

        shared_ptr<Lexeme> value = consumeNext(tokens, Lexeme::STRING_LITERAL_TYPE);

        if(value == nullptr)
            return nullptr;

        return make_shared<Attribute>(*name.get(), make_shared<string>(value->getToken()));


    }

    shared_ptr<Statement> makeStatement(TokenStream &tokens)
    {
        shared_ptr<vector<Attribute>> attributes = make_shared<vector<Attribute>>();

        while(tokens.hasNext() && tokens.peek().tokenType == Lexeme::AT_SIGN_TYPE)
        {
            shared_ptr<Attribute> attribute = makeAttribute(tokens);

            if(attribute == nullptr)
                return nullptr;

            attributes->push_back(*attribute.get());
        }

        shared_ptr<Statement> stmt;
        switch (tokens.peek().tokenType)
        {

            case Lexeme::UNKNOWN_TYPE:
                return nullptr;
            case Lexeme::STORE_TYPE:
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
        //tokens.printTypes();

        shared_ptr<TranslationUnit> unit = make_shared<TranslationUnit>();
        while(tokens.hasNext())
        {
            shared_ptr<Statement> stmt = makeStatement(tokens);
            if(stmt == nullptr)
                return nullptr;
            unit->statements->push_back(stmt);
        }

       return unit;
    }
}

#endif //PDB_TCAPPARSER_TCAPPARSER_H
