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

#ifndef COMPUTATION_H
#define COMPUTATION_H

#include "Object.h"
#include "Lambda.h"
#include "ComputeSource.h"
#include "ComputeSink.h"
#include "InputTupleSetSpecifier.h"
#include "PDBString.h"
#include <map>

namespace pdb {

class ComputePlan;

// all nodes in a user-supplied computation are descended from this
class Computation : public Object {

public:

	// this is implemented by the actual computation object... as the name implies, it is used
	// to extract the lambdas from the computation
	virtual void extractLambdas (std :: map <std :: string, GenericLambdaObjectPtr> &returnVal) {}

	// if this particular computation can be used as a compute source in a pipeline, this
	// method will return the compute source object associated with the computation...
	//
	// In the general case, this method accepts the logical plan that this guy is a part of,
	// as well as the actual TupleSpec that this guy is supposed to produce, and then returns 
	// a pointer to a ComputeSource object that can actually produce TupleSet objects corresponding
	// to that particular TupleSpec
	virtual ComputeSourcePtr getComputeSource (TupleSpec &produceMe, ComputePlan &plan) {return nullptr;}

	// likewise, if this particular computation can be used as a compute sink in a pipeline, this
	// method will return the compute sink object associated with the computation.  It requires the
	// TupleSpec that should be processed, as well as the projection of that TupleSpec that will
	// be put into the sink
	virtual ComputeSinkPtr getComputeSink (TupleSpec &consumeMe, TupleSpec &projection, ComputePlan &plan) {
             return nullptr;
}

        virtual ComputeSinkPtr getComputeSink (TupleSpec &consumeMe, TupleSpec &whichAttsToOpOn, TupleSpec &projection, ComputePlan &plan) {
             return getComputeSink(consumeMe, projection, plan);
        }

	// returns the type of this Computation
	virtual std :: string getComputationType () = 0;

        //JiaNote: below function returns a TCAP string for this Computation
        virtual std :: string toTCAPString (std :: vector<InputTupleSetSpecifier> inputTupleSets, int computationLabel, std :: string& outputTupleSetName, std :: vector<std :: string>& outputColumnNames, std :: string& addedOutputColumnName) = 0;

        //JiaNote: below functions are added to construct a query graph
        //Those functions are borrowed from Chris' QueryBase class

        // gets the name of the i^th input type...
        virtual std :: string getIthInputType (int i) = 0;

        bool hasInput()
        {
            return !inputs.isNullPtr();
        }

        // get a handle to the i^th input to this query, which is also a query
        Handle<Computation> & getIthInput (int i) const {
             return (*inputs)[i];
        }

        // get the number of inputs to this query type
        virtual int getNumInputs() = 0;

        // gets the output type of this query as a string
        virtual std :: string getOutputType () = 0;


        // set the first slot, by default
        bool setInput (Handle<Computation> toMe) {
             return setInput (0, toMe);
        }

        // sets the i^th input to be the output of a specific query... returns
        // true if this is OK, false if it is not
        bool setInput (int whichSlot, Handle <Computation> toMe) {
             
             // set the array of inputs if it is a nullptr
             if (inputs == nullptr) {
                 inputs = makeObject <Vector<Handle<Computation>>> (getNumInputs());
                 for (int i = 0; i < getNumInputs(); i++) {
                      inputs->push_back(nullptr);
                 } 
             }

             // if we are adding this query to a valid slot
             if (whichSlot < getNumInputs ()) {
                 
                 //make sure the output type of the guy we are accepting meets the input type
                 if (getIthInputType (whichSlot) != toMe->getOutputType()) {
                      std :: cout << "Cannot set output of query node with output of type " << toMe->getOutputType () << " to be the input";
                      std :: cout << " of a query with input type " << getIthInputType (whichSlot) << ".\n";
                      return false;
                 }
                 (*inputs)[whichSlot] = toMe;

             } else {

                 return false;

             }
             return true;
        }


        //JiaNote: below methods are added to facilitate analyzing the query graph

       
        //whether the node has been traversed or not
        bool isTraversed () { 

            return traversed; 

        }

        //set the node to have been traversed
        void setTraversed (bool traversed) {

            this->traversed = traversed;

        }

        //get output TupleSet name if the node has been traversed already
        std :: string getOutputTupleSetName() {

            if (traversed == true) {
                return outputTupleSetName;
            }
            return ""; 
        }

        //set output TupleSet name. This method should be invoked by the TCAP string generation method
        void setOutputTupleSetName (std :: string outputTupleSetName) {
             this->outputTupleSetName = outputTupleSetName;
        }

        //get output column name to apply if the node has been traversed already
        std :: string getOutputColumnToApply() {

            if (traversed == true) {
                return outputColumnToApply;
            }
            return "";
        }

        //set output column name to apply. This method should be invoked by the TCAP string generation method
        void setOutputColumnToApply (std :: string outputColumnToApply) {
            this->outputColumnToApply = outputColumnToApply;
        }

        //set user set for output when necessary (e.g. results need to be materialized)
        //by default it do nothing, subclasses shall override this function to handle the case when results need to be materialized.
        virtual void setOutput (std :: string dbName, std :: string setName) {}

        virtual std :: string getDatabaseName() { return ""; }

        virtual std :: string getSetName() { return ""; }

        virtual bool needsMaterializeOutput () { return false; }

        virtual void setBatchSize(int batchSize) {}

private:

        //JiaNote: added to construct query graph

        Handle <Vector <Handle <Computation>>> inputs = nullptr;

        bool traversed = false;

        String outputTupleSetName = "";

        String outputColumnToApply = "";

};

}

#endif
