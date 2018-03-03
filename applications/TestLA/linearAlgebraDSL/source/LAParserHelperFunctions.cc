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
#ifndef LA_IR_HEADERS_CC
#define LA_IR_HEADERS_CC

// by Binhang, June 2017

#include <iostream>
#include <memory>

#include "LAParserHelperFunctions.h"
#include "LADimension.h"
#include "LAExpressionNode.h"
#include "LAIdentifierNode.h"
#include "LAInitializerNode.h"
#include "LAPrimaryExpressionNode.h"
#include "LAPostfixExpressionNode.h"
#include "LAMultiplicativeExpressionNode.h"
#include "LAAdditiveExpressionNode.h"
#include "LAStatementNode.h"
#include "LAStatementsList.h"

bool makePrintFlag = true;


struct LAIdentifierNode* makeIdentifier(char* name) {
    if (makePrintFlag) {
        std::cout << "Make Identifier:" << name << std::endl;
    }
    LAIdentifierNodePtr returnVal = std::make_shared<LAIdentifierNode>(name);
    returnVal->setShared(returnVal);
    free(name);
    return returnVal.get();
}


struct LAInitializerNode* makeZerosInitializer(int rs, int cs, int rn, int cn) {
    if (makePrintFlag) {
        std::cout << "Make zeros(" << rs << "," << cs << "," << rn << "," << cn << ")" << std::endl;
    }
    LAInitializerNodePtr returnVal = std::make_shared<LAInitializerNode>("zeros", rs, cs, rn, cn);
    returnVal->setShared(returnVal);
    return returnVal.get();
}


struct LAInitializerNode* makeOnesInitializer(int rs, int cs, int rn, int cn) {
    if (makePrintFlag) {
        std::cout << "Make ones(" << rs << "," << cs << "," << rn << "," << cn << ")" << std::endl;
    }
    LAInitializerNodePtr returnVal = std::make_shared<LAInitializerNode>("ones", rs, cs, rn, cn);
    returnVal->setShared(returnVal);
    return returnVal.get();
}


struct LAInitializerNode* makeIdentityInitializer(int size, int num) {
    if (makePrintFlag) {
        std::cout << "Make identity(" << size << "," << num << ")" << std::endl;
    }
    LAInitializerNodePtr returnVal =
        std::make_shared<LAInitializerNode>("identity", size, size, num, num);
    returnVal->setShared(returnVal);
    return returnVal.get();
}


struct LAInitializerNode* makeLoadInitializer(int rs, int cs, int rn, int cn, char* path) {
    if (makePrintFlag) {
        std::cout << "Make load(" << rs << "," << cs << "," << rn << "," << cn << "," << path << ")"
                  << std::endl;
    }
    LAInitializerNodePtr returnVal = std::make_shared<LAInitializerNode>("load", rs, cs, rn, cn);
    returnVal->setLoadPath(path);
    returnVal->setShared(returnVal);
    free(path);
    return returnVal.get();
}


struct LAPrimaryExpressionNode* makePrimaryExpressionFromIdentifier(
    struct LAIdentifierNode* identifierPointer) {
    if (makePrintFlag) {
        std::cout << "Make PrimaryExpression from Identifier" << std::endl;
    }
    LAPrimaryExpressionNodePtr returnVal = std::make_shared<LAPrimaryExpressionNode>();
    returnVal->setShared(returnVal);
    // This may be buggy
    LAIdentifierNodePtr identifierPtr(identifierPointer);
    returnVal->setIdentifier(identifierPtr);
    identifierPtr.reset();
    return returnVal.get();
}


struct LAPrimaryExpressionNode* makePrimaryExpressionFromInitializer(
    struct LAInitializerNode* initializerPointer) {
    if (makePrintFlag) {
        std::cout << "Make PrimaryExpression from Initializer" << std::endl;
    }
    LAPrimaryExpressionNodePtr returnVal = std::make_shared<LAPrimaryExpressionNode>();
    returnVal->setShared(returnVal);
    // This may be buggy
    LAInitializerNodePtr initializerPtr(initializerPointer);
    returnVal->setInitializer(initializerPtr);
    initializerPtr.reset();
    return returnVal.get();
}


struct LAPrimaryExpressionNode* makePrimaryExpressionFromExpression(
    const char* flag, struct LAExpressionNode* expPointer) {
    if (makePrintFlag) {
        std::cout << "Make PrimaryExpression from Expression (" << flag << ")" << std::endl;
    }
    LAPrimaryExpressionNodePtr returnVal = std::make_shared<LAPrimaryExpressionNode>();
    returnVal->setShared(returnVal);
    // This may be buggy
    LAExpressionNodePtr expPtr(expPointer);
    returnVal->setChild(flag, expPtr);
    expPtr.reset();
    return returnVal.get();
}

struct LAPrimaryExpressionNode* makePrimaryExpressionFromExpressionDuplicate(
    const char* flag, struct LAExpressionNode* expPointer, int size, int num) {
    if (makePrintFlag) {
        std::cout << "Make PrimaryExpression from Expression (" << flag << ")" << std::endl;
    }
    LAPrimaryExpressionNodePtr returnVal = std::make_shared<LAPrimaryExpressionNode>();
    returnVal->setShared(returnVal);
    // This may be buggy
    LAExpressionNodePtr expPtr(expPointer);
    returnVal->setChild(flag, expPtr);
    expPtr.reset();
    if (strcmp(flag, "duplicateRow") == 0) {
        LADimension dupliDim(size, 0, num, 0);
        returnVal->setDuplicateDim(dupliDim);
    } else if (strcmp(flag, "duplicateCol") == 0) {
        LADimension dupliDim(0, size, 0, num);
        returnVal->setDuplicateDim(dupliDim);
    } else {
        std::cerr << "Invalid flag: " << flag << std::endl;
    }
    return returnVal.get();
}


struct LAPostfixExpressionNode* makePostfixExpressionFromPrimaryExpression(
    const char* op, struct LAPrimaryExpressionNode* primaryExpPointer) {
    if (makePrintFlag) {
        std::cout << "Make PostfixExpression from PrimaryExpression (" << op << ")" << std::endl;
    }
    LAPostfixExpressionNodePtr returnVal = std::make_shared<LAPostfixExpressionNode>();
    returnVal->setShared(returnVal);
    // This may be buggy
    LAPrimaryExpressionNodePtr primaryExpPtr(primaryExpPointer);
    returnVal->setChild(op, primaryExpPtr);
    primaryExpPtr.reset();
    return returnVal.get();
}


struct LAMultiplicativeExpressionNode* makeMultiplicativeExpressionFromPostfixExpressionSingle(
    struct LAPostfixExpressionNode* postfixExpPointer) {
    if (makePrintFlag) {
        std::cout << "Make MultiplicativeExpression from PostfixExpression (single)" << std::endl;
    }
    LAMultiplicativeExpressionNodePtr returnVal =
        std::make_shared<LAMultiplicativeExpressionNode>("none");
    returnVal->setShared(returnVal);
    // This may be buggy
    LAPostfixExpressionNodePtr postfixExpPtr(postfixExpPointer);
    returnVal->setRightChild(postfixExpPtr);
    postfixExpPtr.reset();
    return returnVal.get();
}


struct LAMultiplicativeExpressionNode* makeMultiplicativeExpressionFromPostfixExpressionDouble(
    const char* op,
    struct LAMultiplicativeExpressionNode* leftExp,
    struct LAPostfixExpressionNode* rightExp) {
    if (makePrintFlag) {
        std::cout << "Make MultiplicativeExpression from PostfixExpression (double:" << op << ")"
                  << std::endl;
    }
    LAMultiplicativeExpressionNodePtr returnVal =
        std::make_shared<LAMultiplicativeExpressionNode>(op);
    returnVal->setShared(returnVal);
    // This may be buggy
    LAMultiplicativeExpressionNodePtr leftExpPtr(leftExp);
    returnVal->setLeftChild(leftExpPtr);
    leftExpPtr.reset();
    LAPostfixExpressionNodePtr rightExpPtr(rightExp);
    returnVal->setRightChild(rightExpPtr);
    rightExpPtr.reset();
    return returnVal.get();
}


struct LAAdditiveExpressionNode* makeAdditiveExpressionFromMultiplicativeExpressionSingle(
    struct LAMultiplicativeExpressionNode* multiExp) {
    if (makePrintFlag) {
        std::cout << "Make AdditiveExpression from MultiplicativeExpression (single)" << std::endl;
    }
    LAAdditiveExpressionNodePtr returnVal = std::make_shared<LAAdditiveExpressionNode>("none");
    returnVal->setShared(returnVal);
    // This may be buggy
    LAMultiplicativeExpressionNodePtr multiExpPtr(multiExp);
    returnVal->setRightChild(multiExpPtr);
    multiExpPtr.reset();
    return returnVal.get();
}


struct LAAdditiveExpressionNode* makeAdditiveExpressionFromMultiplicativeExpressionDouble(
    const char* op,
    struct LAAdditiveExpressionNode* leftExp,
    struct LAMultiplicativeExpressionNode* rightExp) {
    if (makePrintFlag) {
        std::cout << "Make AdditiveExpression from MultiplicativeExpression (double:" << op << ")"
                  << std::endl;
    }
    LAAdditiveExpressionNodePtr returnVal = std::make_shared<LAAdditiveExpressionNode>(op);
    returnVal->setShared(returnVal);
    // This may be buggy
    LAAdditiveExpressionNodePtr leftExpPtr(leftExp);
    returnVal->setLeftChild(leftExpPtr);
    leftExpPtr.reset();
    LAMultiplicativeExpressionNodePtr rightExpPtr(rightExp);
    returnVal->setRightChild(rightExpPtr);
    rightExpPtr.reset();
    return returnVal.get();
}


struct LAStatementNode* makeStatement(struct LAIdentifierNode* identifier,
                                      struct LAExpressionNode* exp) {
    if (makePrintFlag) {
        std::cout << "Make statement" << std::endl;
    }
    LAStatementNodePtr returnVal = std::make_shared<LAStatementNode>();
    returnVal->setShared(returnVal);
    // This may be buggy
    LAIdentifierNodePtr identifierPtr(identifier);
    returnVal->setLeftIdentifier(identifierPtr);
    identifierPtr.reset();
    LAExpressionNodePtr expPtr(exp);
    returnVal->setRightExpression(expPtr);
    expPtr.reset();
    return returnVal.get();
}


struct LAStatementsList* makeStatementList(struct LAStatementNode* statement) {
    struct LAStatementsList* returnVal = new LAStatementsList();
    returnVal->addStatement(LAStatementNodePtr(statement));
    return returnVal;
}


struct LAStatementsList* appendStatementList(struct LAStatementsList* list,
                                             struct LAStatementNode* statement) {
    if (makePrintFlag) {
        std::cout << "Append statement, list pre-size:" << list->size() << std::endl;
    }
    list->addStatement(LAStatementNodePtr(statement));
    return list;
}


#endif