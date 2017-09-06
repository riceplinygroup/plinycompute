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
#ifndef GMM_AGGREGATE_OUTPUT_H
#define GMM_AGGREGATE_OUTPUT_H

#include "Object.h"
#include "Handle.h"
#include "GmmNewComp.h"
#include "GmmModel.h"
#include "DoubleVector.h"


// By Tania, August 2017

using namespace pdb;

class GmmAggregateOutputType : public Object {

private:
    int key = 1;

    Handle<GmmModel> model;				//GMM model with means, covars and weights
    Handle<DoubleVector> datapoint;		//Datapoint to be processed
    Handle<GmmNewComp> newComp;			//newComp contains the partial results of
    									//processing datapoint for current model
public:

    ENABLE_DEEP_COPY

    GmmAggregateOutputType () {
    }

    GmmAggregateOutputType (Handle<GmmModel> model, Handle<DoubleVector> datapoint) {
    	this->model = model;
    	this->datapoint = datapoint;
    	this->newComp = nullptr;
    }

    GmmAggregateOutputType(const GmmAggregateOutputType & other) {
    	this->model = other.model;
    	this->datapoint = other.datapoint;
    	this->newComp = other.newComp;
    }

	int &getKey(){
		return key;
	}

	GmmAggregateOutputType &getValue(){
		return (*this);
	}

	//Lazy evaluation -> if newComp == null, the data point has not been processed yet
	//In that case, we process newComp and later perform the overload +

	GmmAggregateOutputType& operator + (GmmAggregateOutputType &other) {

		std::cout << "Entering GmmAggregateOutputType operator+" << std::endl;

		if (this->newComp == nullptr) {
			std::cout << "		Creating left GmmNewComp" << std::endl;
			this->newComp = model->createNewComp(this->datapoint);
		}
		else {
			std::cout << "		*************************** left Already created!!" << std::endl;
		}

		if (other.newComp == nullptr) {
			std::cout << "		Creating right GmmNewComp" << std::endl;
			other.newComp = model->createNewComp(other.datapoint);
		}
		else {
			std::cout << "		*************************** right Already created!!" << std::endl;
		}

		//this->newComp = *(this->newComp) + *(other.newComp);

		addNewCompToThis(other.newComp);

		//Return left object with the resulting sum

		//GmmAggregateOutputType output = GmmAggregateOutputType(*this);
		//return output;

		std::cout << "Exiting GmmAggregateOutputType operator+" << std::endl;


		return (*this);
	}

	void addNewCompToThis(Handle<GmmNewComp> other) {
		std::cout << "	Entering addNewCompToThis" << std::endl;
		newComp->setSumR(newComp->getSumR() + other->getSumR());

		/////////////////////////////////////////////////
		double a = newComp->getWeightedX(0).getDouble(0) + other->getWeightedX(0).getDouble(0);

		if (a != a) {
			std::cout << "****************************************************" << std::endl;
			std::cout << "**********NAN!!! " << std::endl;
			newComp->getWeightedX(0).print();
			other->getWeightedX(0).print();
			exit(-1);
		}
		/////////////////////////////////////////////////

		for (int i = 0; i < model->getNumK(); i++) {
			newComp->setWeightedX(i, newComp->getWeightedX(i) + other->getWeightedX(i));
			newComp->setWeightedX2(i, newComp->getWeightedX2(i) + other->getWeightedX2(i));
		}
		std::cout << "	Exiting addNewCompToThis" << std::endl;

	}

	GmmNewComp getNewComp(){
		return *(this->newComp);
	}


    ~GmmAggregateOutputType () {
    }

};

#endif
