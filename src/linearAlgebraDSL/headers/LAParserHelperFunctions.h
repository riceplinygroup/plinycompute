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
#ifndef LA_IR_HEADERS_H
#define LA_IR_HEADERS_H

// by Binhang, June 2017

#ifdef __cplusplus
extern "C" {
#endif

struct LAExpressionNode;
struct LAIdentifierNode;
struct LAInitializerNode;
struct LAPrimaryExpressionNode;
struct LAPostfixExpressionNode;
struct LAMultiplicativeExpressionNode;
struct LAAdditiveExpressionNode;
struct LAStatementNode;
struct LAStatementsList;


struct LAIdentifierNode* makeIdentifier(char* name);

struct LAInitializerNode* makeZerosInitializer(int rs, int cs, int rn, int cn);

struct LAInitializerNode* makeOnesInitializer(int rs, int cs, int rn, int cn);

struct LAInitializerNode* makeIdentityInitializer(int size, int num);

struct LAInitializerNode* makeLoadInitializer(int rs, int cs, int rn, int cn, char* path);

struct LAPrimaryExpressionNode* makePrimaryExpressionFromIdentifier(
    struct LAIdentifierNode* identifierPointer);

struct LAPrimaryExpressionNode* makePrimaryExpressionFromInitializer(
    struct LAInitializerNode* initializerPointer);

struct LAPrimaryExpressionNode* makePrimaryExpressionFromExpression(
    const char* flag, struct LAExpressionNode* expPointer);

struct LAPrimaryExpressionNode* makePrimaryExpressionFromExpressionDuplicate(
    const char* flag, struct LAExpressionNode* expPointer, int size, int num);

struct LAPostfixExpressionNode* makePostfixExpressionFromPrimaryExpression(
    const char* op, struct LAPrimaryExpressionNode* primaryExpPointer);

struct LAMultiplicativeExpressionNode* makeMultiplicativeExpressionFromPostfixExpressionSingle(
    struct LAPostfixExpressionNode* postfixExpPointer);

struct LAMultiplicativeExpressionNode* makeMultiplicativeExpressionFromPostfixExpressionDouble(
    const char* op,
    struct LAMultiplicativeExpressionNode* leftExp,
    struct LAPostfixExpressionNode* rightExp);

struct LAAdditiveExpressionNode* makeAdditiveExpressionFromMultiplicativeExpressionSingle(
    struct LAMultiplicativeExpressionNode* multiExp);

struct LAAdditiveExpressionNode* makeAdditiveExpressionFromMultiplicativeExpressionDouble(
    const char* op,
    struct LAAdditiveExpressionNode* leftExp,
    struct LAMultiplicativeExpressionNode* rightExp);

struct LAStatementNode* makeStatement(struct LAIdentifierNode* identifier,
                                      struct LAExpressionNode* exp);

struct LAStatementsList* makeStatementList(struct LAStatementNode* statement);

struct LAStatementsList* appendStatementList(struct LAStatementsList* list,
                                             struct LAStatementNode* statement);


#ifdef __cplusplus
}
#endif

#endif