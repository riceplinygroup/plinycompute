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

#ifndef GMM_SAMPLE_SELECTION_H
#define GMM_SAMPLE_SELECTION_H

#include "Lambda.h"
#include "LambdaCreationFunctions.h"
#include "SelectionComp.h"
#include "DoubleVector.h"
#include <ctime>
#include <cstdlib>

using namespace pdb;
class GmmSampleSelection : public SelectionComp<DoubleVector, DoubleVector> {

private:
    double fraction;

public:
    ENABLE_DEEP_COPY

    GmmSampleSelection() {}

    GmmSampleSelection(double inputFraction) {
        this->fraction = inputFraction;
    }

    // srand has already been invoked in server
    Lambda<bool> getSelection(Handle<DoubleVector> checkMe) override {
        return makeLambda(checkMe, [&](Handle<DoubleVector>& checkMe) {

            double myVal = (double)rand() / (double)RAND_MAX;
            bool ifSample = (myVal <= (this->fraction));
            if (ifSample)
                return true;
            else
                return false;
        });
    }


    Lambda<Handle<DoubleVector>> getProjection(Handle<DoubleVector> checkMe) override {
        return makeLambda(checkMe, [](Handle<DoubleVector>& checkMe) {
            std::cout << "I am selected!!";
            checkMe->print();
            return checkMe;
        });
    }
};


#endif
