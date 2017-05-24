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
#ifndef BUILTIN_MATRIX_BLOCK_H
#define BUILTIN_MATRIX_BLOCK_H

//#ifndef MAX_BLOCK_SIZE
//    #define MAX_BLOCK_SIZE 10000
//#endif

#ifndef EIGEN_CODE
    #define EIGEN_CODE 0
#endif


//  PRELOAD %MatrixBlock%

//by Binhang, May 2017, this should be replaced by a sharedLibrary soon.

#include "Object.h"
#include "PDBVector.h"
#include "PDBString.h"
#include "Handle.h"
#include <vector>

//LA libraries:
#include <eigen3/Eigen/Dense>


namespace pdb {

class MatrixBlock : public Object {

private:
    int blockRowIndex;
    int blockColIndex;
    int rowNums;
    int colNums;
    Handle<Vector <double>> rawData;

public:
    
    const static int librayCode = EIGEN_CODE;

	ENABLE_DEEP_COPY

    ~MatrixBlock () { }
    MatrixBlock () {}

    void print () {
        std :: cout << "Block: (" << blockRowIndex <<","<< blockColIndex << "), size: (" << rowNums <<","<<colNums<<"), length:" << rawData->size();
        for(int i=0;i<rawData->size();i++){
            if(i%colNums==0){
                std::cout << std::endl;
            }
            std::cout << (*rawData)[i] <<" ";
        }
        std::cout << std::endl;
    }

	int getBlockRowIndex () {
		return blockRowIndex;
	}

    int getBlockColIndex () {
        return blockColIndex;
    }

	int getRowNums() {
        return rowNums;
    }

    int getColNums() {
        return colNums;
    }
        
    Handle<Vector <double>>& getRawDataHandle(){
        return rawData;
    }


    MatrixBlock(int blockRowIndexIn, int blockColIndexIn, int rowNumsIn, int colNumsIn) {
        blockRowIndex = blockRowIndexIn;
        blockColIndex = blockColIndexIn;
        rowNums = rowNumsIn;
        colNums = colNumsIn;
        rawData = makeObject<Vector<double> >(rowNums*colNums, rowNums*colNums);
        std::cout << "BuiltIn MatrixBlock constructor RawData size:" << rawData->size() << std::endl;
    }
};
}

#endif
