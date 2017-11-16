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

#ifndef K_MEANS_SAMPLE_SELECTION_H
#define K_MEANS_SAMPLE_SELECTION_H

#include "Lambda.h"
#include "LambdaCreationFunctions.h"
#include "SelectionComp.h"
#include "KMeansDoubleVector.h"
#include "PDBVector.h"
#include "PDBString.h"
#include <cstdlib>

/* This class assigns random values to the input KMeansDoubleVector */ 
using namespace pdb;
class KMeansSampleSelection : public SelectionComp<KMeansDoubleVector, KMeansDoubleVector> {

private:
    double fraction;

public:
    ENABLE_DEEP_COPY

    KMeansSampleSelection() {}

    KMeansSampleSelection(double inputFraction) {
        this->fraction = inputFraction;
    }

    /* 
     * Assign random values to checkMe
     * srand has already been invoked in server
     */
    Lambda<bool> getSelection(Handle<KMeansDoubleVector> checkMe) override {
        return makeLambda(checkMe, [&](Handle<KMeansDoubleVector>& checkMe) {
            double myVal = (double)rand() / (double)RAND_MAX;
            bool ifSample = (myVal <= (this->fraction));
            if (ifSample)
                return true;
            else
                return false;
        });
    }

    Lambda<Handle<KMeansDoubleVector>> getProjection(Handle<KMeansDoubleVector> checkMe) override {
        return makeLambda(checkMe, [](Handle<KMeansDoubleVector>& checkMe) { return checkMe; });
    }
};


#endif
