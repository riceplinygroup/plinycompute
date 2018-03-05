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
#ifndef LA_SINGLE_MATRIX_H
#define LA_SINGLE_MATRIX_H

#include "Object.h"
#include "Handle.h"
#include "MatrixBlock.h"
#include <eigen3/Eigen/Dense>

// By Binhang, June 2017
// This is only used by a temporal solution of matrix inverse.


using namespace pdb;
class SingleMatrix : public Object {
private:
    int key = 1;  // Any constant should work here!
    MatrixBlock myValue;

public:
    ENABLE_DEEP_COPY

    ~SingleMatrix() {}
    SingleMatrix() {}

    SingleMatrix(MatrixBlock block) : key(1), myValue(block) {}

    int& getKey() {
        return key;
    }

    MatrixBlock& getValue() {
        return myValue;
    }

    void print() {
        std::cout << "Single matrix" << std::endl;
        myValue.print();
    }

    pdb::Handle<SingleMatrix> getInverse() {
        if (MatrixBlock::librayCode == EIGEN_CODE) {
            // std::cout <<"Test Eigen" << std::endl;
            // std::cout <<"Current Matrix :"<< std::endl;
            // this->print();
            int rowNums = myValue.getRowNums();
            int colNums = myValue.getColNums();
            int blockRowIndex = myValue.getBlockRowIndex();
            int blockColIndex = myValue.getBlockColIndex();
            int totalRows = myValue.getTotalRowNums();
            int totalCols = myValue.getTotalColNums();
            Eigen::Map<Eigen::Matrix<double, Eigen::Dynamic, Eigen::Dynamic, Eigen::RowMajor>>
                currentMatrix(myValue.getRawDataHandle()->c_ptr(), rowNums, colNums);

            // std::cout <<"Test Safe ?" << std::endl;
            // std::cout << "Eigen matrix:\n" << currentMatrix << std::endl;
            pdb::Handle<MatrixBlock> resultMatrixBlock = pdb::makeObject<MatrixBlock>(
                blockColIndex, blockRowIndex, colNums, rowNums, totalRows, totalCols);

            // std::cout <<"Test Safe ??" << std::endl;
            Eigen::Map<Eigen::Matrix<double, Eigen::Dynamic, Eigen::Dynamic, Eigen::RowMajor>>
                inverseMatrix(resultMatrixBlock->getRawDataHandle()->c_ptr(), colNums, rowNums);
            // std::cout <<"Test Safe ???" << std::endl;
            inverseMatrix = currentMatrix.inverse();
            // std::cout << "Eigen matrix:\n" << inverseMatrix << std::endl;
            // std::cout <<"Inverse Matrix :"<< std::endl;
            // resultMatrixBlock->print();

            pdb::Handle<SingleMatrix> result = pdb::makeObject<SingleMatrix>(*resultMatrixBlock);
            return result;
        } else {
            std::cerr << "Wrong librayCode!" << std::endl;
            exit(1);
        }
    }
};


#endif