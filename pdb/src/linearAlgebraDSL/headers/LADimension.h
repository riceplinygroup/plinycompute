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
#ifndef LA_DIMENSION_H
#define LA_DIMENSION_H

// by Binhang, June 2017

typedef struct LADimension {
    int blockRowSize;  // Size for each block
    int blockColSize;
    int blockRowNum;  // Number of blocks
    int blockColNum;

    LADimension() : blockRowSize(0), blockColSize(0), blockRowNum(0), blockColNum(0) {}
    LADimension(int rs, int cs, int rn, int cn)
        : blockRowSize(rs), blockColSize(cs), blockRowNum(rn), blockColNum(cn) {}

    LADimension transpose() {
        LADimension T;
        T.blockRowSize = this->blockColSize;
        T.blockColSize = this->blockRowSize;
        T.blockRowNum = this->blockColNum;
        T.blockColNum = this->blockRowNum;
        return T;
    }

    LADimension& operator=(const LADimension& other) {
        blockRowSize = other.blockRowSize;
        blockColSize = other.blockColSize;
        blockRowNum = other.blockRowNum;
        blockColNum = other.blockColNum;
        return *this;
    }

    bool operator==(const LADimension& other) {
        return blockRowSize == other.blockRowSize && blockColSize == other.blockColSize &&
            blockRowNum == other.blockRowNum && blockColNum == other.blockColNum;
    }

    bool operator!=(const LADimension& other) {
        return blockRowSize != other.blockRowSize || blockColSize != other.blockColSize ||
            blockRowNum != other.blockRowNum || blockColNum != other.blockColNum;
    }
} LADimension;


#endif