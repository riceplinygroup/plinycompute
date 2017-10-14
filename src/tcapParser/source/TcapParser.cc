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


using pdb::SafeResultFailure;
using pdb::SafeResult;
using pdb::SafeResultSuccess;


namespace pdb_detail {
/**
 * Advances the given tokenStream one token and returns that token if it's type is either
 * expectedType1 or expectedType2.
 *
 * If the token from the stream is not of type expectedType1 or expectedType2 a string
 * exception is thrown.
 *
 * If the given tokenStream does not have a next token, a string exception is thrown.
 *
 * @param tokenStream the stream from which to retrieve the next token
 * @param expectedType1 one of two possible expected types for the next token in the stream
 * @param expectedType2 one of two possible expected types for the next token in the stream
 * @return the next token in the stream
 */
TcapToken consumeNext(TcapTokenStream& tokenStream,
                      TcapTokenType expectedType1,
                      TcapTokenType expectedType2) {
    if (!tokenStream.hasNext())
        throw "token stream empty";

    TcapToken next = tokenStream.advance();

    if (next.tokenType == expectedType1 || next.tokenType == expectedType2)
        return next;

    throw "neither type found";
}

/**
 * Advances the given tokenStream one token and returns that token if it's type is expectedType.
 *
 * If the token from the stream is not of type expectedType a string exception is thrown.
 *
 * If the given tokenStream does not have a next token, a string exception is thrown.
 *
 * @param tokenStream the stream from which to retrieve the next token
 * @param expectedType the expected typee for the next token in the stream
 * @return the next token in the stream
 */
TcapToken consumeNext(TcapTokenStream& tokenStream, TcapTokenType expectedType) {
    return consumeNext(tokenStream, expectedType, expectedType);
}


/**
 * Consumes the next token in the stream (assumed IDENTIFIER_TYPE) and creates a TcapIdentifier from
 * the symbol.
 *
 * Assumes that a next token is avaialbe in the stream and that it is of type
 * TcapTokenType::IDENTIFIER_TYPE.
 * If either of these assumptions are untrue, a string exception is generated.
 *
 * @param tokens the stream of tokens to advance.
 * @return the TcapIdentifier version of the next token in the stream.
 */
TcapIdentifier makeIdentifier(TcapTokenStream& tokens) {
    TcapToken ident = consumeNext(tokens, TcapTokenType::IDENTIFIER_TYPE);

    return TcapIdentifier(ident.lexeme);
}

/**
 * Consumes the next tokens in the stream of the form:
 *
 * Identifier Comma Identifier Comma Identifier Comma ... Identifier XXXXXX
 *
 * until a comma token is not observed after the last consumed identifier or the stream is
 * excausted.
 *
 * If the first token read from the stream is not an identifier token, a string exception is
 * generated.
 * If any token following a comma is not an identifier, a string exception is generated.
 * If the given stream is empty, a string exception is generated.
 *
 * @param tokens the stream to read from
 * @return the identifiers consumed from he stream in the same order observed
 */
shared_ptr<vector<TcapIdentifier>> makeIdentList(TcapTokenStream& tokens) {
    shared_ptr<vector<TcapIdentifier>> identsAccum = make_shared<vector<TcapIdentifier>>();

    TcapIdentifier firstIdent = makeIdentifier(tokens);
    identsAccum->push_back(firstIdent);

    while (tokens.hasNext() && tokens.peek().tokenType == TcapTokenType::COMMA_TYPE) {
        tokens.advance();  // consume the comma we just observed

        TcapIdentifier ident = makeIdentifier(tokens);

        identsAccum->push_back(ident);
    }

    return identsAccum;
}

/**
 * Consumes the next tokens in the stream that form  a retain clause and return a representation of
 * that clause.
 *
 * Assumes that the next token in the stream is of type TcapTokenType::RETAIN_TYPE and consumes it.
 * If the next
 * token of the stream is not of type TcapTokenType::RETAIN_TYPE or the stream is empty, a string
 * exception
 * is generated.
 *
 * Aftewords, there are three potential cases depending on the token following
 * TcapTokenType::RETAIN_TYPE.
 *
 * 1.) The next token is of type TcapTokenType::ALL_TYPE.  In this case the token is consumed and
 *     a pointer to a RetainAllClause instance is returned.
 *
 * 2.) The next token is of type TcapTokenType::NONE_TYPE. In this case the token is consumed and a
 *     pointer to a RetainNoneClause instance is returned.
 *
 * 3.) The next tokens of the stream are assumed to form an identifier list, are consumed as such,
 *     and returned via a (pointer to) RetainExplicitClause instance.
 *
 * If the next token or tokens does not conform to any of these expected paterns, a string exception
 * is generated.
 *
 * @param tokens the stream to read the retain clause from
 * @return the retain clause
 */
// n.b. need to return a pointer here because RetainClause has virtual methods
RetainClausePtr makeRetainClause(TcapTokenStream& tokens) {
    consumeNext(tokens,
                TcapTokenType::RETAIN_TYPE);  // consume the "retain" token assumed to be next

    if (tokens.peek().tokenType == TcapTokenType::ALL_TYPE) {
        tokens.advance();  // consume the "all" token we just observed
        return make_shared<RetainAllClause>();
    }

    if (tokens.peek().tokenType == TcapTokenType::NONE_TYPE) {
        tokens.advance();  // consume the "none" token we just observed
        return make_shared<RetainNoneClause>();
    }

    shared_ptr<vector<TcapIdentifier>> columns = makeIdentList(tokens);

    return RetainClausePtr(new RetainExplicitClause(columns));
}


/**
 * Consumes the next tokens in the stream that form an ApplyOperation and return a representation of
 * that
 * operation.
 *
 * If the given stream is empty, a string exception is generated.
 *
 * If the given stream does not start with tokens that form an ApplyOperation, a string exception is
 * generated.
 *
 * @param tokens the token stream from which to read the tokens to form an ApplyOperation
 * @return a shared pointer to the created ApplyOperation
 */
// n.b. we return pointer here because the caller will return this results as TableExpressionPtr
shared_ptr<ApplyOperation> makeApplyOperation(TcapTokenStream& tokens) {
    consumeNext(tokens, TcapTokenType::APPLY_TYPE);

    TcapToken applyTargetType =
        consumeNext(tokens, TcapTokenType::FUNC_TYPE, TcapTokenType::METHOD_TYPE);

    ApplyOperationType applyType = applyTargetType.tokenType == TcapTokenType::FUNC_TYPE
        ? ApplyOperationType::func
        : ApplyOperationType::method;

    TcapToken applyTarget = consumeNext(tokens, TcapTokenType::STRING_LITERAL_TYPE);

    consumeNext(tokens, TcapTokenType::TO_TYPE);

    TcapIdentifier inputTableName = makeIdentifier(tokens);

    consumeNext(tokens, TcapTokenType::LBRACKET_TYPE);

    shared_ptr<vector<TcapIdentifier>> inputTableInputColumnNames = makeIdentList(tokens);

    consumeNext(tokens, TcapTokenType::RBRACKET_TYPE);

    RetainClausePtr retainClause = makeRetainClause(tokens);

    ApplyOperation* op = new ApplyOperation(
        applyTarget.lexeme, applyType, inputTableName, inputTableInputColumnNames, retainClause);
    return shared_ptr<ApplyOperation>(op);
}

/**
 * Consumes the next tokens in the stream that form a HoistOperation and return a representation of
 * that
 * operation.
 *
 * If the given stream is empty, a string exception is generated.
 *
 * If the given stream does not start with tokens that form a HoistOperation, a string exception is
 * generated.
 *
 * @param tokens the token stream from which to read the tokens to form an HoistOperation
 * @return a shared pointer to the created HoistOperation
 */
// n.b. we return pointer here because the caller will return this results as TableExpressionPtr
HoistOperationPtr makeHoistOperation(TcapTokenStream& tokens) {
    consumeNext(tokens, TcapTokenType::HOIST_TYPE);

    TcapToken hoistTarget = consumeNext(tokens, TcapTokenType::STRING_LITERAL_TYPE);

    consumeNext(tokens, TcapTokenType::FROM_TYPE);

    TcapIdentifier inputTableName = makeIdentifier(tokens);

    consumeNext(tokens, TcapTokenType::LBRACKET_TYPE);

    TcapIdentifier inputTableInputColumnName = makeIdentifier(tokens);

    consumeNext(tokens, TcapTokenType::RBRACKET_TYPE);

    RetainClausePtr retainClause = makeRetainClause(tokens);

    return shared_ptr<HoistOperation>(new HoistOperation(
        hoistTarget.lexeme, inputTableName, inputTableInputColumnName, retainClause));
}

/**
 * Consumes the next tokens in the stream that form a LoadOperation and return a representation of
 * that
 * operation.
 *
 * If the given stream is empty, a string exception is generated.
 *
 * If the given stream does not start with tokens that form a LoadOperation, a string exception is
 * generated.
 *
 * @param tokens the token stream from which to read the tokens to form an LoadOperation
 * @return a shared pointer to the created LoadOperation
 */
// n.b. we return pointer here because the caller will return this results as TableExpressionPtr
LoadOperationPtr makeLoadOperation(TcapTokenStream& tokens) {
    consumeNext(tokens, TcapTokenType::LOAD_TYPE);

    TcapToken externSourceString = consumeNext(tokens, TcapTokenType::STRING_LITERAL_TYPE);

    return make_shared<LoadOperation>(externSourceString.lexeme);
}

/**
 * Consumes the next tokens in the stream that form a StoreOperation and return a representation of
 * that
 * operation.
 *
 * If the given stream is empty, a string exception is generated.
 *
 * If the given stream does not start with tokens that form a StoreOperation, a string exception is
 * generated.
 *
 * @param the attributes of the statement
 * @param tokens the token stream from which to read the tokens to form an StoreOperation
 * @return a shared pointer to the created StoreOperation
 */
// n.b. we return pointer here because the caller will return this results as TcapStatementPtr
StoreOperationPtr makeStoreOperation(TcapTokenStream& tokens,
                                     shared_ptr<vector<TcapAttribute>> attributes) {
    consumeNext(tokens, TcapTokenType::STORE_TYPE);

    TcapIdentifier outputTable = makeIdentifier(tokens);

    consumeNext(tokens, TcapTokenType::LBRACKET_TYPE);

    shared_ptr<vector<TcapIdentifier>> columnsToStore = makeIdentList(tokens);

    consumeNext(tokens, TcapTokenType::RBRACKET_TYPE);

    TcapToken externSourceString = consumeNext(tokens, TcapTokenType::STRING_LITERAL_TYPE);

    return StoreOperationPtr(
        new StoreOperation(attributes, outputTable, columnsToStore, externSourceString.lexeme));
}

/**
 * Consumes the next tokens in the stream that form a FilterOperation and return a representation of
 * that
 * operation.
 *
 * If the given stream is empty, a string exception is generated.
 *
 * If the given stream does not start with tokens that form a FilterOperation, a string exception is
 * generated.
 *
 * @param tokens the token stream from which to read the tokens to form an FilterOperation
 * @return a shared pointer to the created FilterOperation
 */
// n.b. we return pointer here because the caller will return this results as TableExpressionPtr
FilterOperationPtr makeFilterOperation(TcapTokenStream& tokens) {
    consumeNext(tokens, TcapTokenType::FILTER_TYPE);

    TcapIdentifier inputTableName = makeIdentifier(tokens);

    consumeNext(tokens, TcapTokenType::BY_TYPE);

    TcapIdentifier filterColumnName = makeIdentifier(tokens);

    RetainClausePtr retain = makeRetainClause(tokens);

    return FilterOperationPtr(new FilterOperation(inputTableName, filterColumnName, retain));
}

/**
 * Consumes the next tokens in the stream that form a BinaryOperation and return a representation of
 * that
 * operation.
 *
 * A BinaryOperation may be one of the following (only one currently exists, but will grow in the
 * future):
 *
 *     GreaterThanOp
 *
 *
 * If the given stream is empty, a string exception is generated.
 *
 * If the given stream does not start with tokens that form any type of BinaryOperation,
 * a string exception is generated.
 *
 * @param tokens the token stream from which to read the tokens to form a BinaryOperation
 * @return a shared pointer to the created BinaryOperation
 */
BinaryOperationPtr makeBinaryOperation(TcapTokenStream& tokens) {
    TcapIdentifier lhsTableName = makeIdentifier(tokens);

    consumeNext(tokens, TcapTokenType::LBRACKET_TYPE);

    TcapIdentifier lhsColumnName = makeIdentifier(tokens);

    consumeNext(tokens, TcapTokenType::RBRACKET_TYPE);

    TcapToken op = consumeNext(tokens, TcapTokenType::GREATER_THAN_TYPE);

    TcapIdentifier rhsTableName = makeIdentifier(tokens);

    consumeNext(tokens, TcapTokenType::LBRACKET_TYPE);

    TcapIdentifier rhsColumnName = makeIdentifier(tokens);

    consumeNext(tokens, TcapTokenType::RBRACKET_TYPE);

    RetainClausePtr retain = makeRetainClause(tokens);

    if (op.tokenType == TcapTokenType::GREATER_THAN_TYPE)
        return shared_ptr<GreaterThanOp>(
            new GreaterThanOp(lhsTableName, lhsColumnName, rhsTableName, rhsColumnName, retain));

    throw "Unknown operator type";
}

/**
 * Consumes the next tokens in the stream that form a TableExpression and return a representation of
 * that
 * expression.
 *
 * A TableExpression may be one of the following:
 *
 *     ApplyOperation
 *     FilterOperation
 *     HoistOperation
 *     LoadOperation
 *     BinaryOperation
 *
 * If the given stream is empty, a string exception is generated.
 *
 * If the given stream does not start with tokens that form any type of TableExpression,
 * a string exception is generated.
 *
 * @param tokens the token stream from which to read the tokens to form a TableExpression
 * @return a shared pointer to the created TableExpression
 */
TableExpressionPtr makeTableExpression(TcapTokenStream& tokens) {
    switch (tokens.peek().tokenType) {
        case (TcapTokenType::APPLY_TYPE):
            return makeApplyOperation(tokens);
        case (TcapTokenType::FILTER_TYPE):
            return makeFilterOperation(tokens);
        case (TcapTokenType::HOIST_TYPE):
            return makeHoistOperation(tokens);
        case (TcapTokenType::LOAD_TYPE):
            return makeLoadOperation(tokens);
        default:
            return makeBinaryOperation(tokens);  // only other possible case
    }
}

/**
 * Consumes the next tokens in the stream that form a TableAssignment and return a representation of
 * that
 * assignment.
 *
 * If the given stream is empty, a string exception is generated.
 *
 * If the given stream does not start with tokens that form a TableAssignment, a string exception is
 * generated.
 *
 * @param attributes the attributes of the statement
 * @param tokens the token stream from which to read the tokens to form an TableAssignment
 * @return a shared pointer to the created TableAssignment
 */
TableAssignmentPtr makeTableAssignment(TcapTokenStream& tokens,
                                       shared_ptr<vector<TcapAttribute>> attributes) {
    TcapIdentifier tableName = makeIdentifier(tokens);

    consumeNext(tokens, TcapTokenType::LPAREN_TYPE);

    shared_ptr<vector<TcapIdentifier>> columnNames = makeIdentList(tokens);

    consumeNext(tokens, TcapTokenType::RPAREN_TYPE);

    consumeNext(tokens, TcapTokenType::EQ_TYPE);

    shared_ptr<TableExpression> tableExp = makeTableExpression(tokens);

    return make_shared<TableAssignment>(attributes, tableName, columnNames, tableExp);
}

/**
 * Consumes the next tokens in the stream that form a TcapAttribute and return a representation of
 * that
 * attribute.
 *
 * If the given stream is empty, a string exception is generated.
 *
 * If the given stream does not start with tokens that form a TcapAttribute, a string exception is
 * generated.
 *
 * @param tokens the token stream from which to read the tokens to form an TcapAttribute
 * @return the created TcapAttribute
 */
TcapAttribute makeAttribute(TcapTokenStream& tokens) {
    consumeNext(tokens, TcapTokenType::AT_SIGN_TYPE);

    TcapIdentifier name = makeIdentifier(tokens);

    TcapToken value = consumeNext(tokens, TcapTokenType::STRING_LITERAL_TYPE);

    return TcapAttribute(name, value.lexeme);
}

/**
 * Consumes the next tokens in the stream that form a TcapStatement and return a representation of
 * that
 * statement including any preceeding TcapAttributes for the statement.
 *
 * A TcapStatement may be one of the following:
 *
 *     StoreOperation
 *     TableAssignment
 *
 * If the given stream is empty, a string exception is generated.
 *
 * If the given stream does not start with tokens that form any type of TcapStatement,
 * a string exception is generated.
 *
 * @param tokens the token stream from which to read the tokens to form a TcapStatement
 * @return a shared pointer to the created TcapStatement
 */
// n.b. returning pointer because TcapStatement has a virtual method
TcapStatementPtr makeStatement(TcapTokenStream& tokens) {
    /*
     * Consume all attributes that precede the statement.
     */
    shared_ptr<vector<TcapAttribute>> attributes = make_shared<vector<TcapAttribute>>();
    {

        while (tokens.hasNext() && tokens.peek().tokenType == TcapTokenType::AT_SIGN_TYPE) {
            TcapAttribute attribute = makeAttribute(tokens);

            attributes->push_back(attribute);
        }
    }

    /*
     * Parse and return the statement.
     */
    TcapStatementPtr stmt;
    switch (tokens.peek().tokenType) {
        case TcapTokenType::STORE_TYPE:
            stmt = makeStoreOperation(tokens, attributes);
            break;
        default:
            stmt = makeTableAssignment(tokens, attributes);
            break;
    }

    return stmt;
}

// contract from .h
shared_ptr<SafeResult<TranslationUnit>> parseTcap(const string& source) {
    TcapTokenStream tokens = lexTcap(source);

    shared_ptr<vector<TcapStatementPtr>> statements = make_shared<vector<TcapStatementPtr>>();

    while (tokens.hasNext()) {
        shared_ptr<TcapStatement> stmt;
        try {
            stmt = makeStatement(tokens);
        } catch (const string& msg) {
            return make_shared<SafeResultFailure<TranslationUnit>>(msg);
        }

        statements->push_back(stmt);
    }

    return make_shared<SafeResultSuccess<TranslationUnit>>(TranslationUnit(statements));
}
}