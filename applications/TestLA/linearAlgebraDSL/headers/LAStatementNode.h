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
#ifndef LA_STATEMENT_NODE_H
#define LA_STATEMENT_NODE_H

#include "LAASTNode.h"
#include "LAIdentifierNode.h"
#include "LAExpressionNode.h"
#include "LAIdentifierNode.h"
#include "LAInitializerNode.h"
#include "LAPrimaryExpressionNode.h"
#include "LAPostfixExpressionNode.h"
#include "LAMultiplicativeExpressionNode.h"
#include "LAAdditiveExpressionNode.h"

// by Binhang, June 2017

struct LAStatementNode;
typedef std::shared_ptr<struct LAStatementNode> LAStatementNodePtr;


struct LAStatementNode : public LAASTNode {

private:
    LAIdentifierNodePtr identifier = NULL;

    LAExpressionNodePtr expression = NULL;

    LAStatementNodePtr me = NULL;

    bool printQueryResult = true;


public:
    LAStatementNode() : LAASTNode(LA_ASTNODE_TYPE_STATEMENT) {}

    std::string toString() final {
        return identifier->toString() + " = " + expression->toString();
    }

    void setShared(LAStatementNodePtr meIn) {
        me = meIn;
    }

    void setLeftIdentifier(LAIdentifierNodePtr i) {
        identifier = i;
    }

    void setRightExpression(LAExpressionNodePtr e) {
        expression = e;
    }

    void evaluateQuery(LAPDBInstance& instance);
};

#endif