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
#ifndef LA_ADDITIVEEXPRESSION_NODE_H
#define LA_ADDITIVEEXPRESSION_NODE_H

// by Binhang, June 2017

#include "LAASTNode.h"
#include "LAExpressionNode.h"
#include "LAMultiplicativeExpressionNode.h"


struct LAAdditiveExpressionNode;
typedef std::shared_ptr<struct LAAdditiveExpressionNode> LAAdditiveExpressionNodePtr;


struct LAAdditiveExpressionNode : public LAExpressionNode {

private:
    std::string addOperator;
    LAAdditiveExpressionNodePtr leftChild = NULL;
    LAMultiplicativeExpressionNodePtr rightChild = NULL;
    LAAdditiveExpressionNodePtr me = NULL;
    pdb::Handle<pdb::Computation> query;
    LADimension dim;

public:
    LAAdditiveExpressionNode(const char* op)
        : LAExpressionNode(LA_ASTNODE_TYPE_ADDITIVEEXPRESSION) {
        addOperator = op;
    }

    std::string toString() final {
        if (addOperator.compare("none") == 0) {
            return rightChild->toString();
        } else if (addOperator.compare("add") == 0) {
            return leftChild->toString() + " + " + rightChild->toString();
        } else if (addOperator.compare("substract") == 0) {
            return leftChild->toString() + " - " + rightChild->toString();
        } else {
            return "AdditiveExpression invalid operator: " + addOperator;
        }
    }

    pdb::Handle<pdb::Computation>& evaluate(LAPDBInstance& instance) final;

    void setShared(LAAdditiveExpressionNodePtr meIn) {
        me = meIn;
    }

    void setLeftChild(LAAdditiveExpressionNodePtr lptr) {
        leftChild = lptr;
    }

    void setRightChild(LAMultiplicativeExpressionNodePtr rptr) {
        rightChild = rptr;
    }

    bool isSyntaxSugarInitializer() {
        if (addOperator.compare("none") == 0) {
            return rightChild->isSyntaxSugarInitializer();
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