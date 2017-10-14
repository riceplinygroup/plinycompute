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
#ifndef LA_POSTFIXEXPRESSION_NODE_H
#define LA_POSTFIXEXPRESSION_NODE_H

// by Binhang, June 2017

#include "LAASTNode.h"
#include "LAExpressionNode.h"
#include "LAPrimaryExpressionNode.h"


struct LAPostfixExpressionNode;
typedef std::shared_ptr<struct LAPostfixExpressionNode> LAPostfixExpressionNodePtr;


struct LAPostfixExpressionNode : public LAExpressionNode {

private:
    std::string postOperator;
    LAPrimaryExpressionNodePtr child = NULL;
    LAPostfixExpressionNodePtr me = NULL;
    pdb::Handle<pdb::Computation> query;
    LADimension dim;

public:
    LAPostfixExpressionNode() : LAExpressionNode(LA_ASTNODE_TYPE_POSTFIXEXPRESSION) {}

    std::string toString() final {
        if (postOperator.compare("none") == 0) {
            return child->toString();
        } else if (postOperator.compare("transpose") == 0) {
            return child->toString() + "^T ";
        } else if (postOperator.compare("inverse") == 0) {
            return child->toString() + "^-1 ";
        } else {
            return "PostfixExpression invalid operator: " + postOperator;
        }
    }

    pdb::Handle<pdb::Computation>& evaluate(LAPDBInstance& instance) final;


    void setShared(LAPostfixExpressionNodePtr meIn) {
        me = meIn;
    }

    void setChild(const char* op, LAPrimaryExpressionNodePtr cptr) {
        postOperator = op;
        child = cptr;
    }

    bool isSyntaxSugarInitializer() {
        if (postOperator.compare("none") == 0) {
            return child->isSyntaxSugarInitializer();
        } else {
            return false;
        }
    }

    LADimension getDimension() {
        return dim;
    }

    void setDimension(LADimension other) {
        dim = other;
    }
};

#endif