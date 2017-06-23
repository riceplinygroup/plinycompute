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
#ifndef LA_MULTIPLICATIVEEXPRESSION_NODE_H
#define LA_MULTIPLICATIVEEXPRESSION_NODE_H



#include "LAASTNode.h"
#include "LAExpressionNode.h"
#include "LAPostfixExpressionNode.h"


struct LAMultiplicativeExpressionNode;
typedef std::shared_ptr<struct LAMultiplicativeExpressionNode> LAMultiplicativeExpressionNodePtr;


struct LAMultiplicativeExpressionNode : public LAExpressionNode {

private:
	std::string multiOperator;
	LAMultiplicativeExpressionNodePtr leftChild = NULL;
	LAPostfixExpressionNodePtr rightChild = NULL;
	LAMultiplicativeExpressionNodePtr me = NULL;
	pdb::Handle<pdb::Computation> query1;
	pdb::Handle<pdb::Computation> query2;
	LADimension dim;

public:
	LAMultiplicativeExpressionNode(const char * op):LAExpressionNode(LA_ASTNODE_TYPE_MULTIPLICATIVEEXPRESSION){
		multiOperator = op;
	}

	std::string toString() final{
		if(multiOperator.compare("none")==0){
			return rightChild->toString();
		}
		else if(multiOperator.compare("multiply")==0){
			return leftChild->toString() + " %*% " + rightChild->toString();
		}
		else if(multiOperator.compare("transpose_multiply")==0){
			return leftChild->toString() + " '* " + rightChild->toString();
		}
		else if(multiOperator.compare("scale_multiply")==0){
			return leftChild->toString() + " * " + rightChild->toString();
		}
		else{
			return "MultiplicativeExpression invalid operator: " + multiOperator;
		}
	}

	pdb::Handle<pdb::Computation> evaluate(LAPDBInstance& instance) final;

	void setShared(LAMultiplicativeExpressionNodePtr meIn){
		me = meIn;
	}

	void setLeftChild(LAMultiplicativeExpressionNodePtr lptr){
		leftChild = lptr;
	}

	void setRightChild(LAPostfixExpressionNodePtr rptr){
		rightChild = rptr;
	}

	bool isSyntaxSugarInitializer(){
		if(multiOperator.compare("none")==0){
			return rightChild->isSyntaxSugarInitializer();
		}
		else{
			return false;
		}
	}

	LADimension getDimension(){
		return dim;
	}

	void setDimension(LADimension other){
		dim = other;
	}
};

#endif