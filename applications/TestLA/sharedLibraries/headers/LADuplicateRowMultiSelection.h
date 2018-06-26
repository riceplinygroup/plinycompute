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

#ifndef LA_DUPLICATE_ROW_MULTISELECTION_H
#define LA_DUPLICATE_ROW_MULTISELECTION_H

// by Binhang, June 2017


#include "Lambda.h"
#include "LambdaCreationFunctions.h"
#include "MultiSelectionComp.h"
#include "MatrixBlock.h"
#include "LADimension.h"


using namespace pdb;
class LADuplicateRowMultiSelection : public MultiSelectionComp<MatrixBlock, MatrixBlock> {

public:
    ENABLE_DEEP_COPY

    LADuplicateRowMultiSelection() {}

    LADuplicateRowMultiSelection(LADimension dim) : targetDim(dim) {}

    Lambda<bool> getSelection(Handle<MatrixBlock> checkMe) override {
        return makeLambda(checkMe, [](Handle<MatrixBlock>& checkMe) { return true; });
    }

    Lambda<Vector<Handle<MatrixBlock>>> getProjection(Handle<MatrixBlock> checkMe) override {
        return makeLambda(
            checkMe, [&](Handle<MatrixBlock>& checkMe) { return this->duplicateRow(checkMe); });
    }

private:
    LADimension targetDim;

            Vector<Handle <MatrixBlock>> duplicateRow(Handle <MatrixBlock> checkMe){
                Vector<Handle <MatrixBlock> > result;
                for(int i = 0; i < targetDim.blockRowNum; i++){
                        int rowNums = targetDim.blockRowSize;
            int colNums = checkMe->getColNums();
            int blockRowIndex = i;
            int blockColIndex = checkMe->getBlockColIndex();
            int totalRows = targetDim.blockRowNum * targetDim.blockRowSize;
            int totalCols = checkMe->getTotalColNums();
                        Handle<MatrixBlock> resultMatrixBlock = pdb::makeObject<MatrixBlock>(blockRowIndex,blockColIndex,rowNums,colNums,totalRows,totalCols);
                        for(int ii = 0; ii < rowNums; ii++){
                                for(int jj = 0; jj < colNums; jj++){
                                        (*(resultMatrixBlock->getRawDataHandle()))[ii*colNums+jj] = (*(checkMe->getRawDataHandle()))[jj];
                                }
                        }
                        result.push_back(resultMatrixBlock);
                }
                return result;
        }
};


#endif
