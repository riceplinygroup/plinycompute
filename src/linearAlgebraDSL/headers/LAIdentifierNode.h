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
#ifndef LA_IDENTIFIER_NODE_H
#define LA_IDENTIFIER_NODE_H

#include "LAASTNode.h"
#include "LAExpressionNode.h"


struct LAIdentifierNode;
typedef std::shared_ptr<struct LAIdentifierNode> LAIdentifierNodePtr;


struct LAIdentifierNode : public LAExpressionNode{
private:
	std::string name;
	bool linked;
	pdb::Handle<pdb::Computation> scanSet;
	LAIdentifierNodePtr me;

protected:
	pdb::Handle<pdb::Computation> evaluate() final;
	

public:
	LAIdentifierNode(char * tag):LAExpressionNode(LA_ASTNODE_TYPE_IDENTIFIER){
		name = tag;
		linked = false;
	}

	std::string toString() final{
		return name;
	}

	void setScanSet(pdb::Handle<pdb::Computation> setMe){
		scanSet = setMe;
	}

	void setShared(LAIdentifierNodePtr meIn){
		me = meIn;
	}
};

#endif