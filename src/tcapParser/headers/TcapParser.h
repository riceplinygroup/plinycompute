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
    shared_ptr<Token> consumeNext(TokenStream &tokenStream, int expectedType1, int expectedType2);

    shared_ptr<Token> consumeNext(TokenStream &tokenStream, int expectedType);

    class Identifier
    {

    public:

        Identifier(string contents);

        Identifier(shared_ptr<string> contents);

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

        Attribute(Identifier name,  shared_ptr<string> value);
    };

    class Statement
    {
    public:

        shared_ptr<vector<Attribute>> attributes;

        Statement();

        virtual void execute(function<void(TableAssignment&)> forTableAssignment, function<void(StoreOperation&)> forStore) = 0;
    };

    class StringLiteral
    {

    public:

        StringLiteral(string contents);

        StringLiteral(shared_ptr<string> contents);

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
        bool isAll();

        bool isNone();

        void execute(function<void(RetainAllClause&)> forAll, function<void(RetainExplicitClause&)>, function<void(RetainNoneClause&)> forNone);
    };

    class RetainNoneClause : public RetainClause
    {
        bool isAll();

        bool isNone() ;

        void execute(function<void(RetainAllClause&)> forAll, function<void(RetainExplicitClause&)>, function<void(RetainNoneClause&)> forNone);
    };

    class RetainExplicitClause : public RetainClause
    {
    public:

        shared_ptr<vector<Identifier>> columns;

        RetainExplicitClause(Identifier column);

        RetainExplicitClause(shared_ptr<vector<Identifier>> columns);

        bool isAll();

        bool isNone();

        void execute(function<void(RetainAllClause&)>, function<void(RetainExplicitClause&)> forExplicit, function<void(RetainNoneClause&)> forNone);
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
                       shared_ptr<RetainClause> retain);

        ApplyOperation(shared_ptr<string> applyTarget, ApplyOperationType applyType, Identifier inputTable,
                       shared_ptr<vector<Identifier>> inputTableColumnNames, shared_ptr<RetainClause> retain);

        void execute(function<void(LoadOperation&)>, function<void(ApplyOperation&)> forApply, function<void(FilterOperation&)>,
                     function<void(HoistOperation&)>, function<void(BinaryOperation&)>);
    };

    class LoadOperation : public TableExpression
    {
    public:

        LoadOperation(shared_ptr<string> source) ;

        LoadOperation(string source);

        shared_ptr<string> source;

        void execute(function<void(LoadOperation&)> forLoad, function<void(ApplyOperation&)>,
                     function<void(FilterOperation&)>,function<void(HoistOperation&)> forHoist,
                     function<void(BinaryOperation&)> forBinaryOp) override;

    };

    class StoreOperation : public Statement
    {
    public:

        Identifier outputTable;

        shared_ptr<vector<Identifier>> columnsToStore;

        shared_ptr<string> destination;

        StoreOperation(Identifier outputTable,  shared_ptr<vector<Identifier>> columnsToStore, shared_ptr<string> destination);

        StoreOperation(Identifier outputTable, shared_ptr<vector<Identifier>> columnsToStore, string destination);

        void execute(function<void(TableAssignment&)>, function<void(StoreOperation&)> forStore);
    };

    class FilterOperation : public TableExpression
    {
    public:

        Identifier inputTableName;

        Identifier filterColumnName;

        shared_ptr<RetainClause> retain;

        FilterOperation(Identifier inputTableName, Identifier filterColumnName, shared_ptr<RetainClause> retain);

        void execute(function<void(LoadOperation&)>, function<void(ApplyOperation&)>,
                     function<void(FilterOperation&)> forFilter, function<void(HoistOperation&)> forHoist,
                     function<void(BinaryOperation&)> forBinaryOp);
    };

    shared_ptr<Identifier> makeIdentifier(TokenStream &tokens);

    shared_ptr<vector<Identifier>> makeIdentList(TokenStream &tokens);



    class TableAssignment : public Statement
    {
    public:

        Identifier tableName;

        shared_ptr<vector<Identifier>> columnNames;

        shared_ptr<TableExpression> value;

        TableAssignment(Identifier tableName, shared_ptr<vector<Identifier>> columnNames,  shared_ptr<TableExpression> value);

        virtual void execute(function<void(TableAssignment&)> forTableAssignment, function<void(StoreOperation&)> forStore);

    };

    class TranslationUnit
    {

    public:

        shared_ptr<vector<shared_ptr<Statement>>> const statements = make_shared<vector<shared_ptr<Statement>>>();
    };


    shared_ptr<RetainClause> makeRetainClause(TokenStream &tokens);

    class HoistOperation : public TableExpression
    {
    public:

        shared_ptr<string> hoistTarget;

        Identifier inputTable;

        shared_ptr<vector<Identifier>> inputTableColumnNames;

        shared_ptr<RetainClause> retain;

        HoistOperation(string hoistTarget, Identifier inputTable, shared_ptr<vector<Identifier>> inputTableColumnNames,
                       shared_ptr<RetainClause> retain);

        HoistOperation(shared_ptr<string> hoistTarget, Identifier inputTable,
                       shared_ptr<vector<Identifier>> inputTableColumnNames, shared_ptr<RetainClause> retain);

        void execute(function<void(LoadOperation&)>, function<void(ApplyOperation&)>, function<void(FilterOperation&)>,
                     function<void(HoistOperation&)> forHoist, function<void(BinaryOperation&)> forBinaryOp);
    };

    shared_ptr<ApplyOperation> makeApplyOperation(TokenStream &tokens);

    shared_ptr<HoistOperation> makeHoistOperation(TokenStream &tokens);

    shared_ptr<LoadOperation> makeLoadOperation(TokenStream &tokens);

    shared_ptr<StoreOperation> makeStoreOperation(TokenStream &tokens);

    shared_ptr<FilterOperation> makeFilterOperation(TokenStream &tokens);

    class GreaterThanOp;

    class BinaryOperation : public TableExpression
    {
    public:

        void execute(function<void(LoadOperation&)>, function<void(ApplyOperation&)>,
                     function<void(FilterOperation&)>, function<void(HoistOperation&)>,
                     function<void(BinaryOperation&)> forBinaryOp);

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

        GreaterThanOp(Identifier lhsTableName, Identifier lhsColumnName, Identifier rhsTableName, Identifier rhsColumnName, shared_ptr<RetainClause> retain);

        void execute(function<void(GreaterThanOp&)> forGreaterThan);

    };

    shared_ptr<TableExpression> makeBinaryOperation(TokenStream &tokens);

    shared_ptr<TableExpression> makeTableExpression(TokenStream &tokens);

    shared_ptr<TableAssignment> makeTableAssignment(TokenStream &tokens);

    shared_ptr<Attribute> makeAttribute(TokenStream &tokens);

    shared_ptr<Statement> makeStatement(TokenStream &tokens);

    shared_ptr<TranslationUnit> parseTcap(const string &source);
}

#endif //PDB_TCAPPARSER_TCAPPARSER_H
