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
#ifndef LA_PRIMARYEXPRESSION_NODE_H
#define LA_PRIMARYEXPRESSION_NODE_H

//by Binhang, June 2017

#include "LAASTNode.h"
#include "LAExpressionNode.h"
#include "LAIdentifierNode.h"
#include "LAInitializerNode.h"



struct LAPrimaryExpressionNode;
typedef std::shared_ptr<struct LAPrimaryExpressionNode> LAPrimaryExpressionNodePtr;


struct LAPrimaryExpressionNode : public LAExpressionNode {

private:
	std::string flag;
	LAIdentifierNodePtr identifer = NULL;
	LAInitializerNodePtr initializer = NULL;
	LAExpressionNodePtr child = NULL;
	LAPrimaryExpressionNodePtr me = NULL;
	pdb::Handle<pdb::Computation> query;
	LADimension dim;
	LADimension duplicateDim;

public:
	LAPrimaryExpressionNode():LAExpressionNode(LA_ASTNODE_TYPE_PRIMARYEXPRESSION){};

	std::string toString() final{
		if(flag.compare("identifer")==0){
			return identifer->toString();
		}
		else if(flag.compare("initializer")==0){
			return initializer->toString();
		}
		else if(flag.compare("recursive")==0){
			return "("+child->toString()+")";
		}
		else if(flag.compare("max")==0 || flag.compare("min")==0 
			 || flag.compare("rowMax")==0 || flag.compare("rowMin")==0
			 || flag.compare("colMin")==0 || flag.compare("colMax")==0
			 || flag.compare("rowSum")==0 || flag.compare("colSum")==0){
			return flag+"(" + child->toString() + ")"; 
		}
		else if(flag.compare("duplicateCol")==0){
			return flag+"(" + child->toString() + "," + std::to_string(duplicateDim.blockRowSize) + "," + std::to_string(duplicateDim.blockRowNum) +")";
		}
		else if(flag.compare("duplicateRow")==0){
			return flag+"(" + child->toString() + "," + std::to_string(duplicateDim.blockColSize) + "," + std::to_string(duplicateDim.blockColNum) +")";
		}
		else{
			return "PrimaryExpression invalid flag: " + flag;
		}
	}

	pdb::Handle<pdb::Computation>& evaluate(LAPDBInstance& instance) final;

	void setShared(LAPrimaryExpressionNodePtr meIn){
		me = meIn;
	}

	void setIdentifier(LAIdentifierNodePtr idptr){
		flag = "identifer";
		identifer = idptr;
	}

	void setInitializer(LAInitializerNodePtr inptr){
		flag = "initializer";
		initializer = inptr;
	}

	void setChild(const char* f, LAExpressionNodePtr cptr){
		flag = f;
		child = cptr;
	}

	bool isSyntaxSugarInitializer(){
		if(flag.compare("initializer")==0){
			return initializer->isSyntaxSugarInitializer();
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

	void setDuplicateDim(LADimension other){
		duplicateDim = other;
	}
};

#endif