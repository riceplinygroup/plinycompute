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

#ifndef JOIN_COMP
#define JOIN_COMP

#include "Computation.h"
#include "JoinTests.h"
#include "ComputePlan.h"
#include "JoinTuple.h"
#include "JoinCompBase.h"
#include "MultiInputsBase.h"

namespace pdb {

// used to parameterize joins that are run as part of a pipeline
class JoinArg : public ComputeInfo {

public:

	// this is the compute plan that we are part of
	ComputePlan &plan;

	// the location of the hash table
	void *pageWhereHashTableIs;

	JoinArg (ComputePlan &plan, void *pageWhereHashTableIs) : plan (plan), pageWhereHashTableIs (pageWhereHashTableIs) {}

	~JoinArg () {}
};


template <typename Out, typename In1, typename In2, typename ...Rest>
class JoinComp  : public JoinCompBase {

private:

       //this is used to pass to lambda tree to update pipeline information for each input
       MultiInputsBase * multiInputsBase = new MultiInputsBase();

public:

        MultiInputsBase * getMultiInputBase () {
            return multiInputsBase;
        }

        virtual ~ JoinComp() {
            free(multiInputsBase);
        }

        void analyzeInputSets(std :: vector < std :: string> inputNames) {
            //Step 1. setup all input names (the column name corresponding to input in tuple set)
            for (int i = 0; i < inputNames.size(); i++) {
                this->multiInputsBase->setNameForIthInput(i, inputNames[i]);
            }

            //Step 2. analyze selectionLambda to find all inputs in predicates
            Lambda<bool> selectionLambda = callGetSelection(*this);
            std :: vector <std :: string> inputsInPredicates = selectionLambda.getAllInputs(this->multiInputsBase);
            for (int i = 0; i < inputsInPredicates.size(); i++) {
                this->multiInputsBase->addInputNameToPredicates(inputsInPredicates[i]);
            }
            //Step 3. analyze projectionLambda to find all inputs in projection
            Lambda<Handle<Out>> projectionLambda = callGetProjection(*this);
            std :: vector <std :: string> inputsInProjection = projectionLambda.getAllInputs(this->multiInputsBase);
            for (int i = 0; i < inputsInProjection.size(); i++) {
                this->multiInputsBase->addInputNameToProjection(inputsInProjection[i]);
            }

        }


	// the computation returned by this method is called to see if a data item should be returned in the output set
	virtual Lambda <bool> getSelection (Handle <In1> in1, Handle <In2> in2, Handle <Rest> ...otherArgs) = 0;

	// the computation returned by this method is called to produce output tuples from this method
	virtual Lambda <Handle <Out>> getProjection (Handle <In1> in1, Handle <In2> in2, Handle <Rest> ...otherArgs) = 0;

	// calls getProjection and getSelection to extract the lambdas
	void extractLambdas (std :: map <std :: string, GenericLambdaObjectPtr> &returnVal) override {
		int suffix = 0;
		Lambda <bool> selectionLambda = callGetSelection (*this);
		Lambda <Handle <Out>> projectionLambda = callGetProjection (*this);
		selectionLambda.toMap (returnVal, suffix);
		projectionLambda.toMap (returnVal, suffix);
	}

	// return the output type
	std :: string getOutputType () override {
		return getTypeName <Out> ();
	}

	// count the number of inputs
        int getNumInputs() final {
		const int extras = sizeof...(Rest);
		return extras + 2;
        }

	template <typename First, typename ...Others> 
	typename std :: enable_if<sizeof ...(Others) == 0, std :: string> :: type getIthInputType (int i) {
		if (i == 0) {
			return getTypeName <First> ();	
		} else {
			std :: cout << "Asked for an input type that didn't exist!";
			exit (1);
		}
	}

	// helper function to get a particular intput type
	template <typename First, typename ...Others> 
	typename std :: enable_if<sizeof ...(Others) != 0, std :: string> :: type getIthInputType (int i) {
		if (i == 0) {
			return getTypeName <First> ();	
		} else {
			return getIthInputType <Others...> (i - 1);
		}
	}
	
	// from the interface: get the i^th input type
	std :: string getIthInputType (int i) final {
		return getIthInputType <In1, In2, Rest...> (i);
	}
	
	// this gets a compute sink
	ComputeSinkPtr getComputeSink (TupleSpec &consumeMe, TupleSpec &attsToOpOn, TupleSpec &projection, ComputePlan &plan) override {
		
		// loop through each of the attributes that we are supposed to accept, and for each of them, find the type
		std :: vector <std :: string> typeList;
		AtomicComputationPtr producer = plan.getPlan ()->getComputations ().getProducingAtomicComputation (consumeMe.getSetName ());
		std :: cout << "consumeMe was: " << consumeMe << "\n";
		std :: cout << "attsToOpOn was: " << attsToOpOn << "\n";
		std :: cout << "projection was: " << projection << "\n";
		for (auto &a : projection.getAtts ()) {

			// find the identity of the producing computation
			std :: cout << "finding the source of " << projection.getSetName () << "." << a << "\n"; 
			std :: pair <std :: string, std :: string> res = producer->findSource (a, plan.getPlan ()->getComputations ());	
			std :: cout << "got " << res.first << " " << res.second << "\n";

			// and find its type... in the first case, there is not a particular lambda that we need to ask for
			if (res.second == "") {
				typeList.push_back ("pdb::Handle<"+plan.getPlan ()->getNode (res.first).getComputation ().getOutputType ()+">");
			} else {
				typeList.push_back ("pdb::Handle<"+plan.getPlan ()->getNode (res.first).getLambda (res.second)->getOutputType ()+">");
			} 
		}

		for (auto &aa : typeList) {
			std :: cout << "Got type " << aa << "\n";
		}

		// now we get the correct join tuple, that will allow us to pack tuples from the join in a hash table
		std :: vector <int> whereEveryoneGoes;
		JoinTuplePtr correctJoinTuple = findCorrectJoinTuple <In1, In2, Rest...> (typeList, whereEveryoneGoes);
		
		for (auto &aa : whereEveryoneGoes) {
			std :: cout << aa << " ";
		}
		std :: cout << "\n";

		return correctJoinTuple->getSink (consumeMe, attsToOpOn, projection, whereEveryoneGoes);
	}

	// this is a join computation
        std :: string getComputationType () override {
                return std :: string ("JoinComp");
        }

        //JiaNote: Returning a TCAP string for this Join computation
        virtual std :: string toTCAPString (std :: vector<InputTupleSetSpecifier> inputTupleSets, int computationLabel, std :: string& outputTupleSetName, std :: vector<std :: string>& outputColumnNames, std :: string& addedOutputColumnName) override {

                std :: cout << "to get TCAPString for Computation: JoinComp_" << computationLabel << " with " << inputTupleSets.size() << "inputs" << std :: endl;
                if (inputTupleSets.size()  == getNumInputs()) {
                    std :: string tcapString = "";
                    multiInputsBase->setNumInputs(this->getNumInputs());
                    std :: vector < std :: string > inputNames;
                    //update tupleset name for input sets                    
                    for (unsigned int i = 0; i < inputTupleSets.size(); i++) {
                       this->multiInputsBase->setTupleSetNameForIthInput(i, inputTupleSets[i].getTupleSetName());
                       this->multiInputsBase->setInputColumnsForIthInput(i, inputTupleSets[i].getColumnNamesToKeep());
                       this->multiInputsBase->setInputColumnsToApplyForIthInput(i, inputTupleSets[i].getColumnNamesToApply());
                       inputNames.push_back(inputTupleSets[i].getColumnNamesToApply()[0]);
                    }
                    analyzeInputSets(inputNames);
                    Lambda <bool> selectionLambda = callGetSelection (*this);
                    std :: string inputTupleSetName = "";
                    std :: vector<std :: string> inputColumnNames;
                    std :: vector<std :: string> inputColumnsToApply;
                    std :: vector<std :: string> childrenLambdaNames;
                    int lambdaLabel = 0;
                    std :: string myLambdaName;
                    MultiInputsBase * multiInputsComp = this->getMultiInputBase();
                    tcapString += selectionLambda.toTCAPString (inputTupleSetName, inputColumnNames, inputColumnsToApply, childrenLambdaNames, lambdaLabel, "JoinComp", computationLabel, outputTupleSetName, outputColumnNames, addedOutputColumnName, myLambdaName, false, multiInputsComp, true);
                    

                    //std :: cout << tcapString << std :: endl;
                    std :: vector <std :: string> inputsInProjection = multiInputsComp->getInputsInProjection ();
                    tcapString += "\n/* run Join projection on ( " + inputsInProjection[0];
                    for (unsigned int i = 1; i < inputsInProjection.size(); i++) {
                        tcapString += " "+inputsInProjection[i];
                    }
                    tcapString += " )*/\n";
                    Lambda <Handle <Out>> projectionLambda = callGetProjection (*this);
                    inputTupleSetName = outputTupleSetName;
                    inputColumnNames.clear();
                    for (unsigned int i = 0; i < outputColumnNames.size(); i++) {
                        inputColumnNames.push_back(outputColumnNames[i]);
                    }
                    inputColumnsToApply.clear();
                    childrenLambdaNames.clear();
                    tcapString += projectionLambda.toTCAPString (inputTupleSetName, inputColumnNames, inputColumnsToApply, childrenLambdaNames, lambdaLabel, "JoinComp", computationLabel, outputTupleSetName, outputColumnNames, addedOutputColumnName, myLambdaName, true, multiInputsComp, false);
                    //std :: cout << tcapString << std :: endl;
                    //std :: cout << "JoinComp: outputTupleSetName: " << outputTupleSetName << std :: endl;
                    //std :: cout << "JoinComp: addedOutputColumnName: " << addedOutputColumnName << std :: endl;
                    return tcapString;
                     
                } else {
                    std :: cout << "ERROR: inputTupleSet size is " << inputTupleSets.size() << " and not equivalent with Join's inputs " << getNumInputs() << std :: endl;
                    return "";
                }
        }
 

	// gets an execute that can run a scan join... needToSwapAtts is true if the atts that are currently stored in the hash table need to
	// come SECOND in the output tuple sets... hashedInputSchema tells us the schema for the attributes that are currently stored in the
	// hash table... pipelinedInputSchema tells us the schema for the attributes that will be coming through the pipeline... 
	// pipelinedAttsToOperateOn is the identity of the hash attribute... pipelinedAttsToIncludeInOutput tells us the set of attributes
	// that are coming through the pipeline that we actually have to write to the output stream
        ComputeExecutorPtr getExecutor (bool needToSwapAtts, TupleSpec &hashedInputSchema, TupleSpec &pipelinedInputSchema,
                TupleSpec &pipelinedAttsToOperateOn, TupleSpec &pipelinedAttsToIncludeInOutput, ComputeInfoPtr arg) override {

		// get the argument to the join
		JoinArg &joinArg = *((JoinArg *) arg.get ());

		std :: cout << "pipelinedInputSchema is " << pipelinedInputSchema << "\n";
		std :: cout << "pipelinedAttsToOperateOn is " << pipelinedAttsToOperateOn << "\n";
		std :: cout << "pipelinedAttsToIncludeInOutput is " << pipelinedAttsToIncludeInOutput << "\n";
		std :: cout << "From the join arg, got " << (size_t) joinArg.pageWhereHashTableIs << "\n";

		// loop through each of the attributes that we are supposed to accept, and for each of them, find the type
		std :: vector <std :: string> typeList;
		AtomicComputationPtr producer = joinArg.plan.getPlan ()->getComputations ().getProducingAtomicComputation (hashedInputSchema.getSetName ());
		for (auto &a : (hashedInputSchema.getAtts ())) {

			// find the identity of the producing computation
			std :: cout << "finding the source of " << hashedInputSchema.getSetName () << "." << a << "\n"; 
			std :: pair <std :: string, std :: string> res = producer->findSource (a, joinArg.plan.getPlan ()->getComputations ());	

			// and find its type... in the first case, there is not a particular lambda that we need to ask for
			if (res.second == "") {
				typeList.push_back ("pdb::Handle<"+joinArg.plan.getPlan ()->getNode (res.first).getComputation ().getOutputType ()+">");
			} else {
				typeList.push_back ("pdb::Handle<"+joinArg.plan.getPlan ()->getNode (res.first).getLambda (res.second)->getOutputType ()+">");
			}
		}

		for (auto &aa : typeList) {
			std :: cout << "Got type " << aa << "\n";
		}

		// now we get the correct join tuple, that will allow us to pack tuples from the join in a hash table
		std :: vector <int> whereEveryoneGoes;
		JoinTuplePtr correctJoinTuple = findCorrectJoinTuple <In1, In2, Rest...> (typeList, whereEveryoneGoes);
		
		std :: cout << "whereEveryoneGoes was: ";
		for (auto &a : whereEveryoneGoes) {
			std :: cout << a << " ";
		}
		std :: cout << "\n";

		// and return the correct probing code
		return correctJoinTuple->getProber (joinArg.pageWhereHashTableIs, whereEveryoneGoes,
			pipelinedInputSchema, pipelinedAttsToOperateOn, pipelinedAttsToIncludeInOutput, needToSwapAtts);
        }

        ComputeExecutorPtr getExecutor (bool needToSwapAtts, TupleSpec &hashedInputSchema, TupleSpec &pipelinedInputSchema, 
		TupleSpec &pipelinedAttsToOperateOn, TupleSpec &pipelinedAttsToIncludeInOutput) override {
		std :: cout << "Currently, no pipelined version of the join doesn't take an arg.\n";
		exit (1);
	}

};

}

#endif
