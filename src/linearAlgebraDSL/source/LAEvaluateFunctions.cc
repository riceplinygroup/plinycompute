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
#ifndef LA_EVALUATE_CC
#define LA_EVALUATE_CC

#include "LAIdentifierNode.h"
#include "LAInitializerNode.h"
#include "LAPrimaryExpressionNode.h"
#include "LAPostfixExpressionNode.h"
#include "LAMultiplicativeExpressionNode.h"
#include "LAAdditiveExpressionNode.h"
#include "LAStatementNode.h"



pdb::Handle<pdb::Computation> LAInitializerNode :: evaluate(){
	if(scanSet.isNullPtr()){
		std::cerr << "LAInitializerNode " << method << " scanSet did not set!" << std::endl;
		exit(1);
	}
	return scanSet;
}


pdb::Handle<pdb::Computation> LAIdentifierNode :: evaluate(){
	if(scanSet.isNullPtr()){
		std::cerr << "LAIdentifierNode " << name << " scanSet did not set!" << std::endl;
		exit(1);
	}
	return scanSet;
}


pdb::Handle<pdb::Computation> LAPrimaryExpressionNode :: evaluate(){
	return query;
}


pdb::Handle<pdb::Computation> LAPostfixExpressionNode :: evaluate(){
	return query;
}


pdb::Handle<pdb::Computation> LAMultiplicativeExpressionNode :: evaluate(){
	return query;
}


pdb::Handle<pdb::Computation> LAAdditiveExpressionNode :: evaluate(){
	return query;
}


void LAStatementNode :: evaluateQuery(){

}
#endif