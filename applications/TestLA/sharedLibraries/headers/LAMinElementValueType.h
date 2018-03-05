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
#ifndef LA_MIN_ELEMENT_VALUE_H
#define LA_MIN_ELEMENT_VALUE_H

#include "Object.h"
#include "Handle.h"


// By Binhang, May 2017

// This class is designed for finding the maximal element by aggregation;
using namespace pdb;

class LAMinElementValueType : public Object {

private:
    double value = 100000;

    int rowIndex = -1;
    int colIndex = -1;

public:
    ENABLE_DEEP_COPY

    ~LAMinElementValueType() {}
    LAMinElementValueType() {}

    void setValue(double v) {
        value = v;
    }

    void setRowIndex(int rI) {
        rowIndex = rI;
    }

    void setColIndex(int cI) {
        colIndex = cI;
    }

    double getValue() {
        return value;
    }

    int getRowIndex() {
        return rowIndex;
    }

    int getColIndex() {
        return colIndex;
    }

    LAMinElementValueType& operator+(LAMinElementValueType& other) {
        if (value > other.value) {
            value = other.value;
            rowIndex = other.rowIndex;
            colIndex = other.colIndex;
        }
        return *this;
    }
};

#endif