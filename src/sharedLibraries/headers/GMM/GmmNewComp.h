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
	double logLikelihood;
    DoubleVector sumR; //Sum of r
    Vector<DoubleVector> weightedX; //Sum of r*x
    Vector<DoubleVector> weightedX2; //Sum of r*(x^2)

public:

    ENABLE_DEEP_COPY

    GmmNewComp () {}

    GmmNewComp (double logLikelihood, DoubleVector sumR,
    		Vector<DoubleVector> weightedX, Vector<DoubleVector> weightedX2) {
    	this->logLikelihood = logLikelihood;
        this->sumR = sumR;
        this->weightedX = weightedX;
        this->weightedX2 = weightedX2;
    }

    ~GmmNewComp () {
    }

    double getLogLikelihood() {
        return this->logLikelihood;
    }

    double getR(int index) {
        return this->sumR.getDouble(index);
    }

    DoubleVector& getSumR() {
    	return this->sumR;
    }

    DoubleVector& getWeightedX(int index) {
		return this->weightedX[index];
    }

    DoubleVector& getWeightedX2(int index) {
    	return this->weightedX2[index];
    }


    GmmNewComp& operator + (GmmNewComp &other) {

    	this->logLikelihood += other.logLikelihood;
		this->sumR = this->sumR + other.getSumR();

		for (int i = 0; i < sumR.size; i++) {
			/*double a = this->weightedX[i].getDouble(0) + other.getWeightedX(i).getDouble(0);

			if (a != a) {
				std::cout << "****************************************************" << std::endl;
				std::cout << "**********NAN!!! " << std::endl;
				this->weightedX[i].print();
				other.getWeightedX(i).print();
				exit(-1);
			}*/

			//std::cout << "operator + ! this.weightedX2 " << i << std::endl; this->weightedX2[i].print();
			//std::cout << "operator + ! other.weightedX2 " << i << std::endl; other.weightedX2[i].print();


			this->weightedX[i] = this->weightedX[i] + other.getWeightedX(i);
			this->weightedX2[i] = this->weightedX2[i] + other.getWeightedX2(i);

			//std::cout << "operator + ! total.weightedX2 " << i << std::endl; this->weightedX2[i].print();


		}

		Handle<GmmNewComp> result = makeObject<GmmNewComp>
				(this->logLikelihood, this->sumR, this->weightedX, this->weightedX2);

		return *result;

    }

};

#endif
