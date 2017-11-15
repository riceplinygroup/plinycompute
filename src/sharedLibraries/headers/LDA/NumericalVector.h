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
#ifndef NUMERICAL_VECTOR_H
#define NUMERICAL_VECTOR_H

#include "Object.h"
#include "Handle.h"
#include "PDBVector.h"

using namespace pdb;

/*
 * This class is used for aggregations over vectors of numerical types (ints, doubles, unsigned,
 * etc.).
 * The benefit of using this class is that if the vector has only a single non-zero entry, the
 * NumericalVector
 * class stores it sparsely and efficiently.  This means that one can very efficiently aggregate
 * vectors
 * tht have only a single non-empty entry at initialization
 */

template <class NumericalType>
class NumericalVector {

    // the number of slots
    unsigned numDims = 0;

    // a single value will be stored here
    unsigned dim = 0;
    NumericalType value = 0;

    // if there is more than one value, it is put here
    Handle<Vector<NumericalType>> myData;

public:
    NumericalVector(Handle<Vector<NumericalType>> initValue) : myData(initValue) {
        numDims = myData->size();
    }

    NumericalVector(unsigned numDims, unsigned dim, NumericalType value)
        : numDims(numDims), dim(dim), value(value) {}

    NumericalVector() {}

    // add two numerical vectors
    NumericalVector<NumericalType>& operator+=(NumericalVector<NumericalType>& addMe) {

        // the guy we are adding in is sparse
        if (addMe.myData == nullptr) {

            // see if we are sparse as well
            if (myData == nullptr) {

                // see if we record the same dim
                if (dim == addMe.dim) {
                    value += addMe.value;
                } else {

                    getVector();
                    (*myData)[addMe.dim] = addMe.value;
                }

                // we are not sparse
            } else {
                (*myData)[addMe.dim] += addMe.value;
            }

            // the guy we are adding in is NOT sparse
        } else {

            // see if we are sparse
            if (myData == nullptr) {

                // we need to initialize our data
                getVector();

                // now add him in
                NumericalType* data = myData->c_ptr();
                NumericalType* hisData = addMe.myData->c_ptr();
                for (int i = 0; i < numDims; i++) {
                    data[i] += hisData[i];
                }

                // we are not sparse
            } else {
                NumericalType* data = myData->c_ptr();
                NumericalType* hisData = addMe.myData->c_ptr();
                for (int i = 0; i < numDims; i++) {
                    data[i] += hisData[i];
                }
            }
        }

        return *this;
    }

    Vector<NumericalType>& getVector() {
        if (myData == nullptr) {
            // we need to initialize our data
            myData = makeObject<Vector<NumericalType>>(numDims, numDims);
            myData->fill(0);
            (*myData)[dim] = value;
        }
        return *myData;
    }
};

#endif
