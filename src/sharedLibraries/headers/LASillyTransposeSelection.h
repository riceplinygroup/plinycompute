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
#ifndef SILLY_LA_TRANSPOSE_SELECT_H
#define SILLY_LA_TRANSPOSE_SELECT_H

//by Binhang, May 2017

#include "Lambda.h"
#include "LambdaCreationFunctions.h"
#include "SelectionComp.h"
//#include "BuiltInMatrixBlock.h"
#include "MatrixBlock.h"

//LA libraries:
#include <eigen3/Eigen/Dense>

using namespace pdb;

class LASillyTransposeSelection : public SelectionComp <MatrixBlock, MatrixBlock> {

public:

	ENABLE_DEEP_COPY

	LASillyTransposeSelection () {}

	Lambda <bool> getSelection (Handle <MatrixBlock> checkMe) override {
		return makeLambda (checkMe, [] (Handle<MatrixBlock> & checkMe) {return true;});
	}


	Lambda <Handle <MatrixBlock>> getProjection (Handle <MatrixBlock> checkMe) override {
        return makeLambda (checkMe, [] (Handle<MatrixBlock> & checkMe) {
        	if (MatrixBlock::librayCode==EIGEN_CODE){
            	std::cout <<"Test Eigen" << std::endl;
            	std::cout <<"Current Matrix :"<< std::endl;
            	checkMe->print();
            	int rowNums = checkMe->getRowNums();
            	int colNums = checkMe->getColNums();
            	int blockRowIndex = checkMe->getBlockRowIndex();
            	int blockColIndex = checkMe->getBlockColIndex();
            	Eigen::Map<Eigen::Matrix<double,Eigen::Dynamic,Eigen::Dynamic,Eigen::RowMajor> > currentMatrix(checkMe->getRawDataHandle()->c_ptr(),rowNums,colNums);
            	
            	
            	//std::cout <<"Test Safe ?" << std::endl;
            	//std::cout << "Eigen matrix:\n" << currentMatrix << std::endl; 
            	pdb::Handle<MatrixBlock> resultMatrixBlock = pdb::makeObject<MatrixBlock>(blockColIndex,blockRowIndex,colNums,rowNums); 
            	
            	//std::cout <<"Test Safe ??" << std::endl;
            	Eigen::Map<Eigen::Matrix<double,Eigen::Dynamic,Eigen::Dynamic,Eigen::RowMajor> > transposeMatrix(resultMatrixBlock->getRawDataHandle()->c_ptr(),colNums,rowNums);
            	//std::cout <<"Test Safe ???" << std::endl;
            	transposeMatrix = currentMatrix.transpose();
            	//std::cout << "Eigen matrix:\n" << transposeMatrix << std::endl; 
            	std::cout <<"Transposed Matrix :"<< std::endl;
            	resultMatrixBlock->print();
				
            	return resultMatrixBlock;
        	}
        	else{
        		std::cerr << "Wrong librayCode!" << std::endl;
            	return checkMe;
        	}
        });
	}
};


#endif
