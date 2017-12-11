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
#ifndef LA_INVERSE3_MULTISELECT_H
#define LA_INVERSE3_MULTISELECT_H

// by Binhang, June 2017

#include "Lambda.h"
#include "LambdaCreationFunctions.h"
#include "LASingleMatrix.h"
#include "MultiSelectionComp.h"
#include "MatrixBlock.h"
#include "LADimension.h"


class LAInverse3MultiSelection : public MultiSelectionComp<MatrixBlock, SingleMatrix> {

public:
    ENABLE_DEEP_COPY

    LAInverse3MultiSelection() {}

    LAInverse3MultiSelection(LADimension dim) : targetDim(dim) {}

    Lambda<bool> getSelection(Handle<SingleMatrix> checkMe) override {
        return makeLambda(checkMe, [](Handle<SingleMatrix>& checkMe) { return true; });
    }

    Lambda<Vector<Handle<MatrixBlock>>> getProjection(Handle<SingleMatrix> checkMe) override {
        return makeLambda(checkMe,
                          [&](Handle<SingleMatrix>& checkMe) { return this->split(checkMe); });
    }

private:
    LADimension targetDim;

    Vector<Handle<MatrixBlock>> split(Handle<SingleMatrix> checkMe) {
        MatrixBlock largeM = checkMe->getValue();
        // std::cout<<"Split Function" <<std::endl;
        // largeM.print();
        int totalRows = targetDim.blockRowSize * targetDim.blockRowNum;
        int totalCols = targetDim.blockColSize * targetDim.blockColNum;
        // std::cout<<totalRows<<", "<<totalCols<<std::endl;
        Vector<Handle<MatrixBlock>> result;
        for (int i = 0; i < targetDim.blockRowNum; i++) {
            for (int j = 0; j < targetDim.blockColNum; j++) {
                Handle<MatrixBlock> currentBlock = makeObject<MatrixBlock>(
                    i, j, targetDim.blockRowSize, targetDim.blockColSize, totalRows, totalCols);
                for (int ii = 0; ii < targetDim.blockRowSize; ii++) {
                    for (int jj = 0; jj < targetDim.blockColSize; jj++) {
                        int finalRowIndex = i * targetDim.blockRowSize + ii;
                        int finalColIndex = j * targetDim.blockColSize + jj;
                        // std::cout<<"("<<finalRowIndex<<","<<finalColIndex<<")"<<endl;
                        (*(currentBlock->getRawDataHandle()))[ii * targetDim.blockColSize + jj] =
                            (*(largeM
                                   .getRawDataHandle()))[finalRowIndex * totalCols + finalColIndex];
                    }
                }
                // currentBlock->print();
                result.push_back(currentBlock);
            }
        }
        return result;
    }
};

#endif