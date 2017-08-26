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
#ifndef GMM_NEW_COMP_H
#define GMM_NEW_COMP_H

//by Tania, August 2017

#include "Object.h"
#include "Handle.h"
#include "DoubleVector.h"

using namespace pdb;

class GmmNewComp : public Object {

private:
    DoubleVector sumR; //Sum of r
    Vector<DoubleVector> weightedX; //Sum of r*x
    Vector<DoubleVector> weightedX2; //Sum of r*(x^2)

public:

    ENABLE_DEEP_COPY

    GmmNewComp () {}

    GmmNewComp (DoubleVector sumR,
    		Vector<DoubleVector> weightedX, Vector<DoubleVector> weightedX2) {
        this->sumR = sumR;
        this->weightedX = weightedX;
        this->weightedX2 = weightedX2;
    }

    ~GmmNewComp () {
    }

    double getR(int index) {
        return this->sumR.getDouble(index);
    }

    DoubleVector& getSumR() {
    	return this->sumR;
    }

    void setSumR(DoubleVector& sumR) {
        this->sumR = sumR;
    }

    DoubleVector& getWeightedX(int index) {
		return this->weightedX[index];
	}

    void setWeightedX(DoubleVector& sumR) {
           this->sumR = sumR;
    }

    DoubleVector& getWeightedX2(int index) {
    	return this->weightedX2[index];
    }


    GmmNewComp& operator + (GmmNewComp &other) {

		this->sumR = this->sumR + other.getSumR();

		for (int i = 0; i < sumR.size; i++) {
			this->weightedX[i] = this->weightedX[i] + other.getWeightedX(i);
			this->weightedX2[i] = this->weightedX2[i] + other.getWeightedX2(i);
		}

		Handle<GmmNewComp> result = makeObject<GmmNewComp>
				(this->sumR, this->weightedX, this->weightedX2);

		return *result;

    }

};

#endif
