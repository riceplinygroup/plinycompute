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
#include "DoubleVector.h"
#include "PDBVector.h"
#include "PDBString.h"
#include <random>

using namespace pdb;
class KMeansSampleSelection : public SelectionComp <DoubleVector, DoubleVector> {

private:
        double fraction;
	std::uniform_real_distribution<> unif;
	std::mt19937 gen;

public:

	ENABLE_DEEP_COPY

	KMeansSampleSelection () {}

	KMeansSampleSelection (double inputFraction) : unif(0,1) {
		this->fraction = inputFraction;
		std::random_device rd;
		std::mt19937 gen(rd());
	}

	Lambda <bool> getSelection (Handle <DoubleVector> checkMe) override {
		return makeLambda (checkMe, [&] (Handle<DoubleVector> & checkMe) {
			
		//	std::random_device rd;
		//	std::mt19937 gen(rd());
			double myVal = this->unif(this->gen);
			bool ifSample = (myVal <= (this->fraction));
	//		std :: cout << "The sampled value: " << myVal << std :: endl;
			if (ifSample)
				return true;
			else
				return false;
                });
	}

	Lambda <Handle <DoubleVector>> getProjection (Handle <DoubleVector> checkMe) override {
		return makeLambda (checkMe, [] (Handle<DoubleVector> & checkMe) {return checkMe;});
	}
};


#endif
