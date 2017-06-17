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
#ifndef LA_EXPRESSION_NODE_H
#define LA_EXPRESSION_NODE_H

#include "LAASTNode.h"
#include "Query.h"
#include "Lambda.h"
#include "LAPDBInstance.h"
#include "LADimension.h"


struct LAExpressionNode;
typedef std::shared_ptr<struct LAExpressionNode> LAExpressionNodePtr;


struct LAExpressionNode : public LAASTNode {
protected:
	LAExpressionNode(int t): LAASTNode(t) {}

public:
	virtual pdb::Handle<pdb::Computation> evaluate(LAPDBInstance& instance) = 0;
	virtual std::string toString() = 0;
	virtual bool isSyntaxSugarInitializer() = 0;
	virtual LADimension getDimension() = 0;
};

#endif