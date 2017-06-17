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


#include "LAMaxElementOutputType.h"
#include "LAMaxElementValueType.h"
#include "LAMinElementOutputType.h"
#include "LAMinElementValueType.h"
#include "LAScanMatrixBlockSet.h"
#include "LASillyAddJoin.h"
#include "LASillyColMaxAggregate.h"
#include "LASillyColMinAggregate.h"
#include "LASillyMaxElementAggregate.h"
#include "LASillyMinElementAggregate.h"
#include "LASillyMultiply1Join.h"
#include "LASillyMultiply2Aggregate.h"
#include "LASillyRowMaxAggregate.h"
#include "LASillyRowMinAggregate.h"
#include "LASillySubstractJoin.h"
#include "LASillyTransposeMultiply1Join.h"
#include "LASillyTransposeSelection.h"
#include "LAWriteMatrixBlockSet.h"
#include "LAWriteMaxElementSet.h"
#include "LAWriteMinElementSet.h"
#include "MatrixBlock.h"
#include "MatrixData.h"
#include "MatrixMeta.h"


/*
 *	This does not consider if the block cannot fit in a single page, will be fixed soon.  
 */
pdb::Handle<pdb::Computation> LAInitializerNode :: evaluate(LAPDBInstance& instance){
	std::string setName = "LA_"+method+"_"+std::to_string(instance.getDispatchCount());
	instance.increaseDispatchCount();
	 // now, create a new set in LA_db
    if (!instance.getStorageClient().createSet<MatrixBlock> ("LA_db", setName, instance.instanceErrMsg())) {
     	std:: cout << "Not able to create set: " + instance.instanceErrMsg();
        exit (-1);
    } else {
        std:: cout << "Created set: " << setName << std::endl;
    }
	//Dispatch the set;
	pdb :: makeObjectAllocatorBlock(instance.getBlockSize() * 1024 * 1024, true);
    pdb::Handle<pdb::Vector<pdb::Handle<MatrixBlock>>> storeMatrix = pdb::makeObject<pdb::Vector<pdb::Handle<MatrixBlock>>>();
	if(method.compare("zeros")==0){
		for (int i = 0; i < dim.blockRowNum; i++) {
            for (int j = 0; j < dim.blockColNum; j++){
                pdb :: Handle <MatrixBlock> myData = pdb::makeObject<MatrixBlock>(i,j,dim.blockRowSize,dim.blockColSize);
                for(int ii = 0; ii < dim.blockRowSize; ii++){
                    for(int jj=0; jj < dim.blockColSize; jj++){
                        (*(myData->getRawDataHandle()))[ii*dim.blockColSize+jj] = 0;
                    }
                }
                storeMatrix->push_back (myData);
            }
        }
	}
	else if(method.compare("ones")==0){
		for (int i = 0; i < dim.blockRowNum; i++) {
            for (int j = 0; j < dim.blockColNum; j++){
                pdb :: Handle <MatrixBlock> myData = pdb::makeObject<MatrixBlock>(i,j,dim.blockRowSize,dim.blockColSize);
                for(int ii = 0; ii < dim.blockRowSize; ii++){
                    for(int jj=0; jj < dim.blockColSize; jj++){
                        (*(myData->getRawDataHandle()))[ii*dim.blockColSize+jj] = 1;
                    }
                }
                storeMatrix->push_back (myData);
            }
        }
	}
	else if(method.compare("identity")==0){
		for (int i = 0; i < dim.blockRowNum; i++) {
            for (int j = 0; j < dim.blockColNum; j++){
                pdb :: Handle <MatrixBlock> myData = pdb::makeObject<MatrixBlock>(i,j,dim.blockRowSize,dim.blockColSize);
                for(int ii = 0; ii < dim.blockRowSize; ii++){
                    for(int jj=0; jj < dim.blockColSize; jj++){
                        (*(myData->getRawDataHandle()))[ii*dim.blockColSize+jj] = (i==j && ii==jj)?1:0;
                    }
                }
                storeMatrix->push_back (myData);
            }
        }	
	}
	else if(method.compare("load")==0){
		//Load to be added soon.
	}
	else{
		std::cerr << "LAInitializerNode <" << method << "> method invalid!" << std::endl;
		exit(1);
	}
	
	
	if (!instance.getDispatchClient().sendData<MatrixBlock>(std::pair<std::string, std::string>(setName, "LA_db"), storeMatrix, instance.instanceErrMsg())) {
        std :: cerr << "Failed to send data to dispatcher server" << std :: endl;
        exit(1);
    }    
    instance.getStorageClient().flushData(instance.instanceErrMsg());
    scanSet = makeObject<LAScanMatrixBlockSet>("LA_db", setName);
    instance.addToCachedSet(setName);
	if(scanSet.isNullPtr()){
		std::cerr << "LAInitializerNode " << method << " scanSet did not set!" << std::endl;
		exit(1);
	}
	return scanSet;
}


pdb::Handle<pdb::Computation> LAIdentifierNode :: evaluate(LAPDBInstance& instance){
	scanSet = instance.findScanSet(name);
	if(scanSet.isNullPtr()){
		std::cerr << "LAIdentifierNode " << name << " scanSet did not set yet!" << std::endl;
		exit(1);
	}
	setDimension(instance.findDimension(name));
	return scanSet;
}


pdb::Handle<pdb::Computation> LAPrimaryExpressionNode :: evaluate(LAPDBInstance& instance){
	if(flag.compare("identifer")==0){
		query = identifer->evaluate(instance);
		setDimension(identifer->getDimension());
	}
	else if(flag.compare("initializer")==0){
		query = initializer->evaluate(instance);
		setDimension(initializer->getDimension());
	}
	else if(flag.compare("recursive")==0){
		query = child->evaluate(instance);
		setDimension(child->getDimension());
	}
	else if(flag.compare("max")==0){
		//query = makeObject<SillyGroupBy>();
		query = makeObject<LASillyMaxElementAggregate>();
		query->setInput(child->evaluate(instance));
		setDimension(LADimension(1,1,1,1));	
	}
	else if(flag.compare("min")==0){
		query = makeObject<LASillyMinElementAggregate>();
		query->setInput(child->evaluate(instance));
		setDimension(LADimension(1,1,1,1));
	} 
	else if(flag.compare("rowMax")==0){
		query = makeObject<LASillyRowMaxAggregate>();
		query->setInput(child->evaluate(instance));
		setDimension(LADimension(child->getDimension().blockRowSize,1,child->getDimension().blockRowNum,1));
	}
	else if(flag.compare("rowMin")==0){
		query = makeObject<LASillyRowMinAggregate>();
		query->setInput(child->evaluate(instance));
		setDimension(LADimension(child->getDimension().blockRowSize,1,child->getDimension().blockRowNum,1));
	}
	else if(flag.compare("colMax")==0){
		query = makeObject<LASillyColMaxAggregate>();
		query->setInput(child->evaluate(instance));
		setDimension(LADimension(1,child->getDimension().blockColSize,1,child->getDimension().blockColNum));
	}
	else if(flag.compare("colMin")==0){
		query = makeObject<LASillyColMinAggregate>();
		query->setInput(child->evaluate(instance));
		setDimension(LADimension(1,child->getDimension().blockColSize,1,child->getDimension().blockColNum));
	}
	else{
		std::cerr << "PostfixExpression invalid flag: " + flag << std::endl;
		exit(1);
	}
	return query;
}


pdb::Handle<pdb::Computation> LAPostfixExpressionNode :: evaluate(LAPDBInstance& instance){
	if(postOperator.compare("none")==0){
		query = child->evaluate(instance);
		setDimension(child->getDimension());
	}
	else if(postOperator.compare("transpose")==0){
		query = makeObject<LASillyTransposeSelection>();
		query->setInput(child->evaluate(instance));
		setDimension(child->getDimension().transpose());
	}
	else if(postOperator.compare("inverse")==0){
		std::cerr <<  "Inverse is not supported." << std::endl;
		exit(1);
	}
	else{
		std::cerr <<  "PostfixExpression invalid operator: " + postOperator << std::endl;
		exit(1);
	}
	return query;
}


pdb::Handle<pdb::Computation> LAMultiplicativeExpressionNode :: evaluate(LAPDBInstance& instance){
	if(multiOperator.compare("none")==0){
		query2 = rightChild->evaluate(instance);
		setDimension(rightChild->getDimension());
	}
	else if(multiOperator.compare("multiply")==0){
		LADimension dimLeft = leftChild->getDimension();
		LADimension dimRight = rightChild->getDimension();
		if(dimLeft.blockColSize != dimRight.blockRowSize || dimLeft.blockColNum != dimRight.blockRowNum){
			std::cerr << "Multiply operator dimension not match: " << leftChild->toString() <<"," << rightChild->toString()<<std::endl;
			exit(1); 
		}
		query1 = makeObject<LASillyMultiply1Join>();
		query1->setInput(0,leftChild->evaluate(instance));
		query1->setInput(1,rightChild->evaluate(instance));
		query2 = makeObject<LASillyMultiply2Aggregate>();
		query2->setInput(query1);
		LADimension dimNew(dimLeft.blockRowSize,dimRight.blockColSize,dimLeft.blockRowNum,dimRight.blockColNum);
		setDimension(dimNew);
	}
	else if(multiOperator.compare("transpose_multiply")==0){
		LADimension dimLeft = leftChild->getDimension();
		LADimension dimRight = rightChild->getDimension();
		if(dimLeft.blockRowSize != dimRight.blockRowSize || dimLeft.blockRowNum != dimRight.blockRowNum){
			std::cerr << "Transpose Multiply operator dimension not match: " << leftChild->toString() <<"," << rightChild->toString()<<std::endl;
			exit(1); 
		}
		query1 = makeObject<LASillyTransposeMultiply1Join>();
		query1->setInput(0,leftChild->evaluate(instance));
		query1->setInput(1,rightChild->evaluate(instance));
		query2 = makeObject<LASillyMultiply2Aggregate>();
		query2->setInput(query1);
		LADimension dimNew(dimLeft.blockColSize,dimRight.blockColSize,dimLeft.blockColNum,dimRight.blockColNum);
		setDimension(dimNew);
	}
	else{
		std::cerr << "MultiplicativeExpression invalid operator: " + multiOperator << std::endl;
		exit(1);
	}
	return query2;
}


pdb::Handle<pdb::Computation> LAAdditiveExpressionNode :: evaluate(LAPDBInstance& instance){
	if(addOperator.compare("none")==0){
		query = rightChild->evaluate(instance);
		setDimension(rightChild->getDimension());
	}
	else if(addOperator.compare("add")==0){
		LADimension dimLeft = leftChild->getDimension();
		LADimension dimRight = rightChild->getDimension();
		if(dimLeft != dimRight){
			std::cerr << "Add operator dimension not match: " << leftChild->toString() <<"," << rightChild->toString()<<std::endl;
			exit(1); 
		}
		query = makeObject<LASillyAddJoin>();
		query->setInput(0,leftChild->evaluate(instance));
		query->setInput(1,rightChild->evaluate(instance));
		setDimension(dimLeft);
	}
	else if(addOperator.compare("substract")==0){
		LADimension dimLeft = leftChild->getDimension();
		LADimension dimRight = rightChild->getDimension();
		if(dimLeft != dimRight){
			std::cerr << "Substract operator dimension not match: " << leftChild->toString() <<"," << rightChild->toString()<<std::endl;
			exit(1); 
		}
		query = makeObject<LASillySubstractJoin>();
		query->setInput(0,leftChild->evaluate(instance));
		query->setInput(1,rightChild->evaluate(instance));
		setDimension(dimLeft);
	}
	else{
		std::cerr << "AdditiveExpression invalid operator: " + addOperator << std::endl;
		exit(1);
	}
	return query;
}

/*
 *	Potential problem: 
 *	(1)	Dispatched initializer sets are not removed!
 *	(2) Max/Min element does not considered yet! (check type for 2 operand operators)
 */
void LAStatementNode :: evaluateQuery(LAPDBInstance& instance){
	//Remove the previous set in DB
	if(instance.existsScanSet(identifier->toString())){//Rename a variable
		std::cout << "Redefine variable " << identifier->toString() << std::endl;
		pdb::Handle<pdb::Computation> previousScan = instance.findScanSet(identifier->toString());
		if(!instance.getStorageClient().removeSet(previousScan->getDatabaseName(),previousScan->getSetName(),instance.instanceErrMsg())){
			std::cout << "Not able to remove set: " + instance.instanceErrMsg() << std::endl;
            exit (-1);
		}
		else{
			std::cout << "Previously linked set " + previousScan->getSetName() + " removed" <<std::endl; 
		}
		instance.deleteFromCachedSet(previousScan->getSetName());
	}

	if(expression->isSyntaxSugarInitializer()){
		std::cout << "Initialize variable " << identifier->toString() << std::endl;
		pdb::Handle<pdb::Computation> initializerScan = expression->evaluate(instance);		
		identifier->setScanSet(initializerScan);
		std::cout << "Initialized scanSet: " << identifier->getScanSet()->getSetName() << std::endl;
		identifier->setDimension(expression->getDimension());
		instance.addToIdentifierComputationMap(identifier->toString(),initializerScan);
		instance.addToIdentifierDimensionMap(identifier->toString(),identifier->getDimension());
	}
	else{
		pdb::Handle<pdb::Computation> statementQuery = expression->evaluate(instance);
		std::cout << "Query output type: "<< statementQuery->getOutputType() << std::endl;
		Handle<Computation> writeSet;
		std::string outputSetName = "LA_computation_result_"+identifier->toString();
		if(statementQuery->getOutputType().compare("MatrixBlock")==0){
			if (!instance.getStorageClient().createSet<MatrixBlock> ("LA_db", outputSetName, instance.instanceErrMsg())) {
            	std::cout << "Not able to create set: " + instance.instanceErrMsg() << std::endl;
            	exit (-1);
        	} 
			writeSet = makeObject<LAWriteMatrixBlockSet>("LA_db", outputSetName);
		}
		else if(statementQuery->getOutputType().compare("LAMaxElementOutputType")==0){
			if (!instance.getStorageClient().createSet<LAMaxElementOutputType> ("LA_db", outputSetName, instance.instanceErrMsg())) {
            	std::cout << "Not able to create set: " + instance.instanceErrMsg() << std::endl;
            	exit (-1);
        	} 
			writeSet = makeObject<LAWriteMaxElementSet>("LA_db", outputSetName);
		}
		else if(statementQuery->getOutputType().compare("LAMinElementOutputType")==0){
			if (!instance.getStorageClient().createSet<LAMinElementOutputType> ("LA_db", outputSetName, instance.instanceErrMsg())) {
            	std::cout << "Not able to create set: " + instance.instanceErrMsg() << std::endl;
            	exit (-1);
        	} 
			writeSet = makeObject<LAWriteMinElementSet>("LA_db", outputSetName);
		}
		else{
			std::cerr << "Invalid query output type!" << std::endl;
			exit(1);
		}
		writeSet->setInput(statementQuery);
		auto begin = std :: chrono :: high_resolution_clock :: now();
		if (!instance.getQueryClient().executeComputations(instance.instanceErrMsg(), writeSet)) {
        	std :: cout << "Query failed. Message was: " << instance.instanceErrMsg() << "\n";
        	exit(1);;
    	}
   	 	std :: cout << std :: endl;
   	 	auto end = std::chrono::high_resolution_clock::now();

   	 	instance.addToCachedSet(outputSetName);
   	 	pdb::Handle<pdb::Computation> newScanSet = makeObject<LAScanMatrixBlockSet>("LA_db", outputSetName);
   	 	identifier->setScanSet(newScanSet);
		std::cout << "Updated scanSet: " << identifier->getScanSet()->getSetName() << std::endl;
		identifier->setDimension(expression->getDimension());
		instance.addToIdentifierComputationMap(identifier->toString(),newScanSet);
		instance.addToIdentifierDimensionMap(identifier->toString(),identifier->getDimension());

		if(printQueryResult){
			std :: cout << "To print result..." << std :: endl;
			if(statementQuery->getOutputType().compare("MatrixBlock")==0){
				SetIterator <MatrixBlock> output = instance.getQueryClient().getSetIterator<MatrixBlock> ("LA_db", outputSetName);
        		std :: cout << "Output Matrix:" << std::endl;
        		int count = 0;
        		for (auto a : output) {
            		count ++;
            		std :: cout << count << ":";
            		a->print();
            		std :: cout << std::endl;
        		}
        		std :: cout << "Matrix output block nums:" << count << "\n";
			}
			else if(statementQuery->getOutputType().compare("LAMaxElementOutputType")==0){
				SetIterator <LAMaxElementOutputType> result = instance.getQueryClient().getSetIterator <LAMaxElementOutputType> ("LA_db", outputSetName);
        		std :: cout << "Max Element query results: "<< std :: endl;
        		int countOut = 0;
        		for (auto a : result) {
            		countOut ++;
            		std :: cout << countOut << ":";
            		a->print();
            		std :: cout << std::endl;
        		}
        		std :: cout << "Max Element output count:" << countOut << "\n";
			}
			else if(statementQuery->getOutputType().compare("LAMinElementOutputType")==0){
				SetIterator <LAMinElementOutputType> result = instance.getQueryClient().getSetIterator <LAMinElementOutputType> ("LA06_db", "LA_min_set");
        		std :: cout << "Minimal Element query results: "<< std :: endl;
        		int countOut = 0;
        		for (auto a : result) {
            		countOut ++;
            		std :: cout << countOut << ":";
            		a->print();
            		std :: cout << std::endl;
        		}
        		std :: cout << "Minimal Element output count:" << countOut << "\n";
			}
			else{
				std::cerr << "Invalid query output type!" << std::endl;
				exit(1);
			}
			std::cout << "Time Duration: " << std::chrono::duration_cast<std::chrono::duration<float>>(end-begin).count() << " secs." << std::endl;
		}
	}
}
#endif