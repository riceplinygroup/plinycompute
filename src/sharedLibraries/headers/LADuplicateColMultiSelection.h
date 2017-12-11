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

#ifndef LA_DUPLICATE_COL_MULTISELECTION_H
#define LA_DUPLICATE_COL_MULTISELECTION_H

// by Binhang, June 2017


#include "Lambda.h"
#include "LambdaCreationFunctions.h"
#include "MultiSelectionComp.h"
#include "MatrixBlock.h"
#include "LADimension.h"


using namespace pdb;
class LADuplicateColMultiSelection : public MultiSelectionComp<MatrixBlock, MatrixBlock> {

public:
    ENABLE_DEEP_COPY

    LADuplicateColMultiSelection() {}

    LADuplicateColMultiSelection(LADimension dim) : targetDim(dim) {}

    Lambda<bool> getSelection(Handle<MatrixBlock> checkMe) override {
        return makeLambda(checkMe, [](Handle<MatrixBlock>& checkMe) { return true; });
    }

    Lambda<Vector<Handle<MatrixBlock>>> getProjection(Handle<MatrixBlock> checkMe) override {
        return makeLambda(
            checkMe, [&](Handle<MatrixBlock>& checkMe) { return this->duplicateCol(checkMe); });
    }

private:
    LADimension targetDim;

    Vector<Handle<MatrixBlock>> duplicateCol(Handle<MatrixBlock> checkMe) {
        // checkMe->print();
        // std::cout<<std::endl;
        Vector<Handle<MatrixBlock>> result;
        for (int i = 0; i < targetDim.blockColNum; i++) {
            int rowNums = checkMe->getRowNums();
            int colNums = targetDim.blockColSize;
            int blockRowIndex = checkMe->getBlockRowIndex();
            int blockColIndex = i;
            int totalRows = checkMe->getTotalRowNums();
            int totalCols = targetDim.blockColNum * targetDim.blockColSize;
            Handle<MatrixBlock> resultMatrixBlock = pdb::makeObject<MatrixBlock>(
                blockRowIndex, blockColIndex, rowNums, colNums, totalRows, totalCols);
            for (int ii = 0; ii < rowNums; ii++) {
                for (int jj = 0; jj < colNums; jj++) {
                    (*(resultMatrixBlock->getRawDataHandle()))[ii * colNums + jj] =
                        (*(checkMe->getRawDataHandle()))[ii];
                }
            }
            // resultMatrixBlock->print();
            // std::cout<<std::endl;
            result.push_back(resultMatrixBlock);
        }
        return result;
    }
};


#endif
