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

#ifndef COMPUTE_PLAN_CC
#define COMPUTE_PLAN_CC

#include "ComputePlan.h"
#include "FilterExecutor.h"
#include "HashOneExecutor.h"
#include "FlattenExecutor.h"
#include "AtomicComputationClasses.h"
#include "EqualsLambda.h"
#include "JoinCompBase.h"
#include "Lexer.h"
#include "Parser.h"

extern int yydebug;

namespace pdb {

inline ComputePlan :: ComputePlan () {}

inline LogicalPlanPtr ComputePlan :: getPlan () {

	// if we already have the plan, then just return it
	if (myPlan != nullptr)
		return myPlan;

	// get the string to compile
	std :: string myLogicalPlan = TCAPComputation;
	myLogicalPlan.push_back ('\0');

	// where the result of the parse goes
	AtomicComputationList *myResult;

	// now, do the compilation
        yyscan_t scanner;
        LexerExtra extra { "" };
        yylex_init_extra (&extra, &scanner);
        const YY_BUFFER_STATE buffer { yy_scan_string (myLogicalPlan.data(), scanner) };
        const int parseFailed { yyparse (scanner, &myResult) };
        yy_delete_buffer (buffer, scanner);
        yylex_destroy (scanner);

	// if it didn't parse, get outta here
	if (parseFailed) {
                std :: cout << "Parse error when compiling TCAP: " << extra.errorMessage;
		exit (1);
        }

	// this is the logical plan to return
	myPlan = std :: make_shared <LogicalPlan> (*myResult, allComputations);
	delete myResult;

	// and now we are outta here
	return myPlan;
}

inline void ComputePlan :: nullifyPlanPointer () {
	myPlan = nullptr;
}

// this does a DFS, trying to find a list of computations that lead to the specified computation
inline bool recurse (LogicalPlanPtr myPlan, std :: vector <AtomicComputationPtr> &listSoFar, std :: string &targetTupleSetName) {

	// see if the guy at the end of the list is indeed the target
	if (listSoFar.back ()->getOutputName () == targetTupleSetName) {

		// in this case, we have the complete list of computations
		return true;
	}

	// get all of the guys who consume the dude on the end of the list
	std :: vector <AtomicComputationPtr> &nextOnes = myPlan->getComputations ().getConsumingAtomicComputations (listSoFar.back ()->getOutputName ());

	// and try to put each of the next computations on the end of the list, and recursively search
	for (auto &a : nextOnes) {

		// see if the next computation was on the path to the target
		listSoFar.push_back (a);
		if (recurse (myPlan, listSoFar, targetTupleSetName)) {

			// it was!  So we are done
			return true;
		}

		// we couldn't find the target
		listSoFar.pop_back ();
	}

	// if we made it here, we could not find the target
	return false;
}


inline std :: string ComputePlan :: getProducingComputationName(std :: string sourceTupleSetName) {

        if (myPlan == nullptr) {
                getPlan();
        }

        AtomicComputationList &allComps = myPlan->getComputations();

        return allComps.getProducingAtomicComputation (sourceTupleSetName)->getComputationName();


}


//JiaNote: implemented following method to provide merger for broadcast join
inline SinkMergerPtr ComputePlan :: getMerger (std :: string sourceTupleSetName, std :: string targetTupleSetName, std :: string targetComputationName) {

        if (targetComputationName.find("JoinComp") == std :: string :: npos) {
               return nullptr;
        }

        // build the plan if it is not already done
        if (myPlan == nullptr)
                getPlan ();

        // get all of the computations
        AtomicComputationList &allComps = myPlan->getComputations ();

        //std :: cout << "print computations:" << std :: endl;
        //std :: cout << allComps << std :: endl;


        // and get the schema for the output TupleSet objects that it is supposed to produce
        TupleSpec &targetSpec = allComps.getProducingAtomicComputation (targetTupleSetName)->getOutput ();
        //std :: cout << "The target is " << targetSpec << "\n";

        // and get the projection for this guy
        std :: vector <AtomicComputationPtr> &consumers = allComps.getConsumingAtomicComputations (targetSpec.getSetName ());
        //JiaNote: change the reference into a new variable based on Chris' Join code
        //TupleSpec &targetProjection = targetSpec;
        TupleSpec targetProjection;
        TupleSpec targetAttsToOpOn;
        for (auto &a : consumers) {
                if (a->getComputationName () == targetComputationName) {

                        //std :: cout << "targetComputationName was " << targetComputationName << "\n";

                        // we found the consuming computation
                        if (targetSpec == a->getInput ()) {
                                targetProjection = a->getProjection ();
                                targetAttsToOpOn = a->getInput();
                                break;
                        }

                        // the only way that the input to this guy does not match targetSpec is if he is a join, which has two inputs
                        if (a->getAtomicComputationType () != std :: string ("JoinSets")) {
                                std :: cout << "This is bad... is the target computation name correct??";
                                std :: cout << "Didn't find a JoinSets, target was " << targetSpec.getSetName () << "\n";
                                exit (1);
                        }

                        // get the join and make sure it matches
                        ApplyJoin *myGuy = (ApplyJoin *) a.get ();
                        if (!(myGuy->getRightInput () == targetSpec)) {
                                std :: cout << "This is bad... is the target computation name correct??";
                                std :: cout << "Find a JoinSets, target was " << targetSpec.getSetName () << "\n";
                                exit (1);
                        }

                        //std :: cout << "Building sink for: " << targetSpec << " " << myGuy->getRightProjection () << " " << myGuy->getRightInput () << "\n";
                        targetProjection = myGuy->getRightProjection ();
                        targetAttsToOpOn = myGuy->getRightInput ();
                        //std :: cout << "Building sink for: " << targetSpec << " " << targetAttsToOpOn << " " << targetProjection << "\n";
                }
        }

        // now we have the list of computations, and so it is time to get the sink merger
        SinkMergerPtr sinkMerger = myPlan->getNode (targetComputationName).getComputation ().getSinkMerger (targetSpec, targetAttsToOpOn, targetProjection, *this);

        return sinkMerger;

}


//JiaNote: add a new buildPipeline method to avoid ambiguity
inline PipelinePtr ComputePlan :: buildPipeline (std :: vector <std :: string> buildTheseTupleSets, std :: string targetComputationName,
        std :: function <std :: pair <void *, size_t> ()> getPage, std :: function <void (void *)> discardTempPage,
        std :: function <void (void *)> writeBackPage) {

        std :: map <std :: string, ComputeInfoPtr> params;
        return buildPipeline (buildTheseTupleSets, targetComputationName, getPage, discardTempPage, writeBackPage, params);
}



inline PipelinePtr ComputePlan :: buildPipeline (std :: string sourceTupleSetName, std :: string targetTupleSetName,
        std :: string targetComputationName,
        std :: function <std :: pair <void *, size_t> ()> getPage, std :: function <void (void *)> discardTempPage,
        std :: function <void (void *)> writeBackPage) {

        std :: map <std :: string, ComputeInfoPtr> params;
        return buildPipeline (sourceTupleSetName, targetTupleSetName, targetComputationName, getPage, discardTempPage, writeBackPage, params);
}


//JiaNote: add below method to make sure the pipeline to build is unique, and no ambiguity.
inline PipelinePtr ComputePlan :: buildPipeline (std :: vector<std :: string> buildTheseTupleSets, std :: string targetComputationName, std :: function <std :: pair <void *, size_t> ()> getPage, std :: function <void (void *)> discardTempPage,
        std :: function <void (void *)> writeBackPage, std :: map <std :: string, ComputeInfoPtr> &params) {


        // build the plan if it is not already done
        if (myPlan == nullptr)
                getPlan ();

        // get all of the computations
        AtomicComputationList &allComps = myPlan->getComputations ();

        //std :: cout << "print computations:" << std :: endl;
        //std :: cout << allComps << std :: endl;

        //to get compute source
        // now we get the name of the actual computation object that corresponds to the producer of this tuple set
        size_t numTupleSets = buildTheseTupleSets.size();
        if (numTupleSets == 0) {
            std :: cout << "ERROR: there is no tuple sets to build pipeline" << std :: endl;
            return nullptr;
        }


        std :: string sourceTupleSetName = buildTheseTupleSets[0];

        std :: string producerName = allComps.getProducingAtomicComputation (sourceTupleSetName)->getComputationName ();
        //std :: cout << "producerName = " << producerName << std :: endl;

        // and get the schema for the output TupleSet objects that it is supposed to produce
        TupleSpec &origSpec = allComps.getProducingAtomicComputation (sourceTupleSetName)->getOutput ();

        // now we are going to ask that particular node for the compute source
        ComputeSourcePtr computeSource = myPlan->getNode (producerName).getComputation ().getComputeSource (origSpec, *this);


        //to get compute sink
        std :: string targetTupleSetName = buildTheseTupleSets[numTupleSets-1];
        TupleSpec & targetSpec = allComps.getProducingAtomicComputation (targetTupleSetName)->getOutput ();
        TupleSpec targetProjection;
        TupleSpec targetAttsToOpOn;

        //std :: cout << "targetComputationName was " << targetComputationName << "\n";
        if (targetComputationName.find("SelectionComp") == std :: string :: npos) {

            // and get the schema for the output TupleSet objects that it is supposed to produce
            if ((allComps.getConsumingAtomicComputations(targetTupleSetName)).size() > 1) {
                std :: cout << "ERROR: target tuple set in pipeline should have only one consumer" << std :: endl;
                return nullptr;
            }
            //std :: cout << "The target is " << targetSpec << "\n";

            //JiaNote: change the reference into a new variable based on Chris' Join code
            //TupleSpec &targetProjection = targetSpec;

            auto a = (allComps.getConsumingAtomicComputations(targetTupleSetName))[0];

            // we found the consuming computation
            if (targetSpec == a->getInput ()) {
                targetProjection = a->getProjection ();

                //added following to merge join code
                if(targetComputationName.find("JoinComp") == std :: string :: npos) {
                    targetSpec = targetProjection;
                }

                targetAttsToOpOn = a->getInput();
            
            }

            // the only way that the input to this guy does not match targetSpec is if he is a join, which has two inputs
            else if (a->getAtomicComputationType () != std :: string ("JoinSets")) {
                std :: cout << "This is bad... is the target computation name correct??";
                std :: cout << "Didn't find a JoinSets, target was " << targetSpec.getSetName () << "\n";
                exit (1);
            }
            else {
                // get the join and make sure it matches
                ApplyJoin *myGuy = (ApplyJoin *) a.get ();
                if (!(myGuy->getRightInput () == targetSpec)) {
                    std :: cout << "This is bad... is the target computation name correct??";
                    std :: cout << "Find a JoinSets, target was " << targetSpec.getSetName () << "\n";
                    exit (1);
                }
                else {
                    //std :: cout << "Building sink for: " << targetSpec << " " << myGuy->getRightProjection () << " " << myGuy->getRightInput () << "\n";
                    targetProjection = myGuy->getRightProjection ();
                    targetAttsToOpOn = myGuy->getRightInput ();
                    //std :: cout << "Building sink for: " << targetSpec << " " << targetAttsToOpOn << " " << targetProjection << "\n";
                }
            }
        } else {
            targetProjection = targetSpec;
            targetAttsToOpOn = targetSpec;
        }
        // now we have the list of computations, and so it is time to build the pipeline... start by building a compute sink
        ComputeSinkPtr computeSink = myPlan->getNode (targetComputationName).getComputation ().getComputeSink (targetSpec, targetAttsToOpOn, targetProjection, *this);

        // make the pipeline
        PipelinePtr returnVal = std :: make_shared <Pipeline> (getPage, discardTempPage, writeBackPage, computeSource, computeSink);

        // add the operations to the pipeline
        AtomicComputationPtr lastOne = myPlan->getComputations ().getProducingAtomicComputation (sourceTupleSetName);

        for (int i = 1; i < buildTheseTupleSets.size(); i++) {

                //std :: cout << "the " << i << "-th TupleSetName is " << buildTheseTupleSets[i] << std :: endl;
          
                AtomicComputationPtr a = myPlan->getComputations().getProducingAtomicComputation(buildTheseTupleSets[i]);

                if (a == nullptr) {

                    std :: cout << "ERROR: We can't get producing computation and stop building" << std :: endl;
                    return nullptr;

                }


                // if we have a filter, then just go ahead and create it
                if (a->getAtomicComputationType () == "Filter") {
                        //std :: cout << "Adding: " << a->getProjection () << " + filter [" << a->getInput () << "] => " << a->getOutput () << "\n";
                        if (params.count(a->getOutput ().getSetName ()) == 0)    {
                            returnVal->addStage (std :: make_shared <FilterExecutor> (lastOne->getOutput (), a->getInput (), a->getProjection ()));
                        } else {

                            returnVal->addStage (std :: make_shared <FilterExecutor> (lastOne->getOutput (), a->getInput (),
                                        a->getProjection (), params[a->getOutput ().getSetName ()]));

                        }
                // if we had an apply, go ahead and find it and add it to the pipeline
                } else if (a->getAtomicComputationType () == "Apply") {
                        //std :: cout << "Adding: " << a->getProjection () << " + apply [" << a->getInput () << "] => " << a->getOutput () << "\n";

                        // if we have an available parameter, send it
                        if (params.count (a->getOutput ().getSetName ()) == 0)    {
                                returnVal->addStage (myPlan->getNode (a->getComputationName ()).getLambda (
                                        ((ApplyLambda *) a.get ())->getLambdaToApply ())->getExecutor (
                                        lastOne->getOutput (), a->getInput (), a->getProjection ()));
                        }
                        else  {
                                returnVal->addStage (myPlan->getNode (a->getComputationName ()).getLambda (
                                        ((ApplyLambda *) a.get ())->getLambdaToApply ())->getExecutor (
                                        lastOne->getOutput (), a->getInput (), a->getProjection (), params[a->getOutput ().getSetName ()]));
                        }

                } else if (a->getAtomicComputationType () == "HashLeft") {
                        //std :: cout << "Adding: " << a->getProjection () << " + hashleft [" << a->getInput () << "] => " << a->getOutput () << "\n";

                        // if we have an available parameter, send it
                        if (params.count (a->getOutput ().getSetName ()) == 0)
                                returnVal->addStage (myPlan->getNode (a->getComputationName ()).getLambda (
                                        ((HashLeft *) a.get ())->getLambdaToApply ())->getLeftHasher (
                                        lastOne->getOutput (), a->getInput (), a->getProjection ()));
                        else
                                returnVal->addStage (myPlan->getNode (a->getComputationName ()).getLambda (
                                        ((HashLeft *) a.get ())->getLambdaToApply ())->getLeftHasher (
                                        lastOne->getOutput (), a->getInput (), a->getProjection (), params[a->getOutput ().getSetName ()]));

                } else if (a->getAtomicComputationType () == "HashRight") {
                        //std :: cout << "Adding: " << a->getProjection () << " + hashright [" << a->getInput () << "] => " << a->getOutput () << "\n";

                        // if we have an available parameter, send it
                        if (params.count (a->getOutput ().getSetName ()) == 0)
                                returnVal->addStage (myPlan->getNode (a->getComputationName ()).getLambda (
                                        ((HashLeft *) a.get ())->getLambdaToApply ())->getRightHasher (
                                        lastOne->getOutput (), a->getInput (), a->getProjection ()));
                        else
                                returnVal->addStage (myPlan->getNode (a->getComputationName ()).getLambda (
                                        ((HashLeft *) a.get ())->getLambdaToApply ())->getRightHasher (
                                        lastOne->getOutput (), a->getInput (), a->getProjection (), params[a->getOutput ().getSetName ()]));

                } else if (a->getAtomicComputationType () == "HashOne") {
                        //std :: cout << "Adding: " << a->getProjection () << " + hashone [" << a->getInput () << "] => " << a->getOutput () << "\n";
                        if (params.count(a->getOutput ().getSetName ()) == 0)    {
                            returnVal->addStage (std :: make_shared <HashOneExecutor> (lastOne->getOutput (), a->getInput (), a->getProjection ()));
                        } else {

                            returnVal->addStage (std :: make_shared <HashOneExecutor> (lastOne->getOutput (), a->getInput (),
                                        a->getProjection (), params[a->getOutput ().getSetName ()]));

                        }
                } else if (a->getAtomicComputationType() == "Flatten") {
                        //std :: cout << "Adding: " << a->getProjection () << " + flatten [" << a->getInput () << "] => " << a->getOutput () << "\n";
                        if (params.count(a->getOutput ().getSetName ()) == 0)    {
                            returnVal->addStage (std :: make_shared <FlattenExecutor> (lastOne->getOutput (), a->getInput (), a->getProjection ()));
                        } else {

                            returnVal->addStage (std :: make_shared <FlattenExecutor> (lastOne->getOutput (), a->getInput (),
                                        a->getProjection (), params[a->getOutput ().getSetName ()]));

                        }

                } else if (a->getAtomicComputationType () == "JoinSets") {
                        //std :: cout << "Adding: " << a->getProjection () << " + join [" << a->getInput () << "] => " << a->getOutput () << "\n";

                        // join is weird, because there are two inputs...
                        JoinCompBase &myComp = (JoinCompBase &) myPlan->getNode (a->getComputationName ()).getComputation ();
                        ApplyJoin *myJoin = (ApplyJoin *) (a.get ());

                        // check if we are pipelinining the right input
                        if (lastOne->getOutput ().getSetName () == myJoin->getRightInput ().getSetName ()) {

                                //std :: cout << "We are pipelining the right input...\n";

                                // if we are pipelining the right input, then we don't need to switch left and right inputs
                                if (params.count (a->getOutput ().getSetName ()) == 0) {
                                        returnVal->addStage (myComp.getExecutor (true, myJoin->getProjection (),
                                                lastOne->getOutput (), myJoin->getRightInput (), myJoin->getRightProjection ()));
                                } else {
                                        returnVal->addStage (myComp.getExecutor (true, myJoin->getProjection (),
                                                lastOne->getOutput (), myJoin->getRightInput (), myJoin->getRightProjection (),
                                                params[a->getOutput ().getSetName ()]));
                                }

                        } else {

                                //std :: cout << "We are pipelining the left input...\n";

                                // if we are pipelining the right input, then we don't need to switch left and right inputs
                                if (params.count (a->getOutput ().getSetName ()) == 0) {
                                        returnVal->addStage (myComp.getExecutor (false, myJoin->getRightProjection (),
                                                lastOne->getOutput (), myJoin->getInput (), myJoin->getProjection ()));
                                } else { 
                                        returnVal->addStage (myComp.getExecutor (false, myJoin->getRightProjection (),
                                                lastOne->getOutput (), myJoin->getInput (), myJoin->getProjection (),
                                                params[a->getOutput ().getSetName ()]));
                                }
                        }



                } else {
                        std :: cout << "This is bad... found an unexpected computation type (" << a->getComputationName () << ") inside of a pipeline.\n";
                }

                lastOne = a;

        }
        //std :: cout << "Sink: " << targetSpec << " [" << targetProjection << "]\n";
        return returnVal;
}

inline PipelinePtr ComputePlan :: buildPipeline (std :: string sourceTupleSetName, std :: string targetTupleSetName, 
	std :: string targetComputationName,
	std :: function <std :: pair <void *, size_t> ()> getPage, std :: function <void (void *)> discardTempPage, 
	std :: function <void (void *)> writeBackPage, std :: map <std :: string, ComputeInfoPtr> &params) {

	// build the plan if it is not already done
	if (myPlan == nullptr)
		getPlan ();
		
	// get all of the computations
	AtomicComputationList &allComps = myPlan->getComputations ();

        //std :: cout << "print computations:" << std :: endl;
        //std :: cout << allComps << std :: endl;

	// now we get the name of the actual computation object that corresponds to the producer of this tuple set
	std :: string producerName = allComps.getProducingAtomicComputation (sourceTupleSetName)->getComputationName ();

        //std :: cout << "producerName = " << producerName << std :: endl;

	// and get the schema for the output TupleSet objects that it is supposed to produce
	TupleSpec &origSpec = allComps.getProducingAtomicComputation (sourceTupleSetName)->getOutput ();

	// now we are going to ask that particular node for the compute source
	ComputeSourcePtr computeSource = myPlan->getNode (producerName).getComputation ().getComputeSource (origSpec, *this);

	//std :: cout << "\nBUILDING PIPELINE\n";
	//std :: cout << "Source: " << origSpec << "\n";
	// now we have to do a DFS.  This vector will store all of the computations we've found so far
	std :: vector <AtomicComputationPtr> listSoFar;

	// and this list stores the computations that we still need to process
	std :: vector <AtomicComputationPtr> &nextOnes = myPlan->getComputations ().getConsumingAtomicComputations (origSpec.getSetName ());

	// now, see if each of the next guys can get us to the target tuple set
	bool gotIt = false;
	for (auto &a : nextOnes) {
		listSoFar.push_back (a);

		// see if the next computation was on the path to the target
		if (recurse (myPlan, listSoFar, targetTupleSetName)) {
			gotIt = true;
			break;
		}

		// we couldn't find the target
		listSoFar.pop_back ();
	}

	// see if we could not find a path
	if (!gotIt) {
		std :: cerr << "This is bad.  Could not find a path from source computation to sink computation.\n";
		exit (1);
	}
		
	// and get the schema for the output TupleSet objects that it is supposed to produce
	TupleSpec &targetSpec = allComps.getProducingAtomicComputation (targetTupleSetName)->getOutput ();
        //std :: cout << "The target is " << targetSpec << "\n";


	// and get the projection for this guy
	std :: vector <AtomicComputationPtr> &consumers = allComps.getConsumingAtomicComputations (targetSpec.getSetName ());
        //JiaNote: change the reference into a new variable based on Chris' Join code
	//TupleSpec &targetProjection = targetSpec;
        TupleSpec targetProjection;
        TupleSpec targetAttsToOpOn;
	for (auto &a : consumers) {
		if (a->getComputationName () == targetComputationName) {

                        //std :: cout << "targetComputationName was " << targetComputationName << "\n";

			// we found the consuming computation
			if (targetSpec == a->getInput ()) {
				targetProjection = a->getProjection ();

                                //added following to merge join code
                                if(targetComputationName.find("JoinComp") == std :: string :: npos) {
                                    targetSpec = targetProjection;
                                }

                                targetAttsToOpOn = a->getInput();
				break;
			}	

			// the only way that the input to this guy does not match targetSpec is if he is a join, which has two inputs
			if (a->getAtomicComputationType () != std :: string ("JoinSets")) {
				std :: cout << "This is bad... is the target computation name correct??";
                                std :: cout << "Didn't find a JoinSets, target was " << targetSpec.getSetName () << "\n";
				exit (1);
			}

			// get the join and make sure it matches
			ApplyJoin *myGuy = (ApplyJoin *) a.get ();
			if (!(myGuy->getRightInput () == targetSpec)) {
				std :: cout << "This is bad... is the target computation name correct??";
                                std :: cout << "Find a JoinSets, target was " << targetSpec.getSetName () << "\n";
				exit (1);
			}
			
                        //std :: cout << "Building sink for: " << targetSpec << " " << myGuy->getRightProjection () << " " << myGuy->getRightInput () << "\n";
                        targetProjection = myGuy->getRightProjection ();
                        targetAttsToOpOn = myGuy->getRightInput ();
                        //std :: cout << "Building sink for: " << targetSpec << " " << targetAttsToOpOn << " " << targetProjection << "\n";
		}
	}
	
	// now we have the list of computations, and so it is time to build the pipeline... start by building a compute sink
	ComputeSinkPtr computeSink = myPlan->getNode (targetComputationName).getComputation ().getComputeSink (targetSpec, targetAttsToOpOn, targetProjection, *this);
		
	// make the pipeline
	PipelinePtr returnVal = std :: make_shared <Pipeline> (getPage, discardTempPage, writeBackPage, computeSource, computeSink); 
	
	// add the operations to the pipeline
	AtomicComputationPtr lastOne = myPlan->getComputations ().getProducingAtomicComputation (sourceTupleSetName);
	for (auto &a : listSoFar) {

		// if we have a filter, then just go ahead and create it
		if (a->getAtomicComputationType () == "Filter") {
    			//std :: cout << "Adding: " << a->getProjection () << " + filter [" << a->getInput () << "] => " << a->getOutput () << "\n";
                        if (params.count(a->getOutput ().getSetName ()) == 0)    {
			    returnVal->addStage (std :: make_shared <FilterExecutor> (lastOne->getOutput (), a->getInput (), a->getProjection ()));
                        } else {

                            returnVal->addStage (std :: make_shared <FilterExecutor> (lastOne->getOutput (), a->getInput (),
                                        a->getProjection (), params[a->getOutput ().getSetName ()]));

                        }
		// if we had an apply, go ahead and find it and add it to the pipeline
		} else if (a->getAtomicComputationType () == "Apply") {
			//std :: cout << "Adding: " << a->getProjection () << " + apply [" << a->getInput () << "] => " << a->getOutput () << "\n";

                        // if we have an available parameter, send it
                        if (params.count (a->getOutput ().getSetName ()) == 0)    {
                                returnVal->addStage (myPlan->getNode (a->getComputationName ()).getLambda (
                                        ((ApplyLambda *) a.get ())->getLambdaToApply ())->getExecutor (
                                        lastOne->getOutput (), a->getInput (), a->getProjection ()));
                        }
                        else  {
                                returnVal->addStage (myPlan->getNode (a->getComputationName ()).getLambda (
                                        ((ApplyLambda *) a.get ())->getLambdaToApply ())->getExecutor (
                                        lastOne->getOutput (), a->getInput (), a->getProjection (), params[a->getOutput ().getSetName ()]));
                        }

		} else if (a->getAtomicComputationType () == "HashLeft") {
                        //std :: cout << "Adding: " << a->getProjection () << " + hashleft [" << a->getInput () << "] => " << a->getOutput () << "\n";

                        // if we have an available parameter, send it
                        if (params.count (a->getOutput ().getSetName ()) == 0)
                                returnVal->addStage (myPlan->getNode (a->getComputationName ()).getLambda (
                                        ((HashLeft *) a.get ())->getLambdaToApply ())->getLeftHasher (
                                        lastOne->getOutput (), a->getInput (), a->getProjection ()));
                        else
                                returnVal->addStage (myPlan->getNode (a->getComputationName ()).getLambda (
                                        ((HashLeft *) a.get ())->getLambdaToApply ())->getLeftHasher (
                                        lastOne->getOutput (), a->getInput (), a->getProjection (), params[a->getOutput ().getSetName ()]));

                } else if (a->getAtomicComputationType () == "HashRight") {
                        //std :: cout << "Adding: " << a->getProjection () << " + hashright [" << a->getInput () << "] => " << a->getOutput () << "\n";

                        // if we have an available parameter, send it
                        if (params.count (a->getOutput ().getSetName ()) == 0)
                                returnVal->addStage (myPlan->getNode (a->getComputationName ()).getLambda (
                                        ((HashLeft *) a.get ())->getLambdaToApply ())->getRightHasher (
                                        lastOne->getOutput (), a->getInput (), a->getProjection ()));
                        else
                                returnVal->addStage (myPlan->getNode (a->getComputationName ()).getLambda (
                                        ((HashLeft *) a.get ())->getLambdaToApply ())->getRightHasher (
                                        lastOne->getOutput (), a->getInput (), a->getProjection (), params[a->getOutput ().getSetName ()]));

                } else if (a->getAtomicComputationType () == "HashOne") {
                        //std :: cout << "Adding: " << a->getProjection () << " + hashone [" << a->getInput () << "] => " << a->getOutput () << "\n";
                        if (params.count(a->getOutput ().getSetName ()) == 0)    {
                            returnVal->addStage (std :: make_shared <HashOneExecutor> (lastOne->getOutput (), a->getInput (), a->getProjection ()));
                        } else {

                            returnVal->addStage (std :: make_shared <HashOneExecutor> (lastOne->getOutput (), a->getInput (),
                                        a->getProjection (), params[a->getOutput ().getSetName ()]));

                        }
                 
                } else if (a->getAtomicComputationType () == "Flatten") {
                        //std :: cout << "Adding: " << a->getProjection () << " + flatten [" << a->getInput () << "] => " << a->getOutput () << "\n";
                        if (params.count(a->getOutput ().getSetName ()) == 0)    {
                            returnVal->addStage (std :: make_shared <FlattenExecutor> (lastOne->getOutput (), a->getInput (), a->getProjection ()));
                        } else {

                            returnVal->addStage (std :: make_shared <FlattenExecutor> (lastOne->getOutput (), a->getInput (),
                                        a->getProjection (), params[a->getOutput ().getSetName ()]));

                        }
                } else if (a->getAtomicComputationType () == "JoinSets") { 
                        //std :: cout << "Adding: " << a->getProjection () << " + join [" << a->getInput () << "] => " << a->getOutput () << "\n";

                        // join is weird, because there are two inputs...
                        JoinCompBase &myComp = (JoinCompBase &) myPlan->getNode (a->getComputationName ()).getComputation ();
                        ApplyJoin *myJoin = (ApplyJoin *) (a.get ());

                        // check if we are pipelinining the right input
                        if (lastOne->getOutput ().getSetName () == myJoin->getRightInput ().getSetName ()) {

                                //std :: cout << "We are pipelining the right input...\n";

                                // if we are pipelining the right input, then we don't need to switch left and right inputs
                                if (params.count (a->getOutput ().getSetName ()) == 0) {
                                        returnVal->addStage (myComp.getExecutor (true, myJoin->getProjection (),
                                                lastOne->getOutput (), myJoin->getRightInput (), myJoin->getRightProjection ()));
                                } else {
                                        returnVal->addStage (myComp.getExecutor (true, myJoin->getProjection (),
                                                lastOne->getOutput (), myJoin->getRightInput (), myJoin->getRightProjection (),
                                                params[a->getOutput ().getSetName ()]));
                                }

                        } else {

                                //std :: cout << "We are pipelining the left input...\n";

                                // if we are pipelining the right input, then we don't need to switch left and right inputs
                                if (params.count (a->getOutput ().getSetName ()) == 0) {
                                        returnVal->addStage (myComp.getExecutor (false, myJoin->getRightProjection (),
                                                lastOne->getOutput (), myJoin->getInput (), myJoin->getProjection ()));
                                } else {
                                        returnVal->addStage (myComp.getExecutor (false, myJoin->getRightProjection (),
                                                lastOne->getOutput (), myJoin->getInput (), myJoin->getProjection (),
                                                params[a->getOutput ().getSetName ()]));
                                }
                        }



                } else {
			std :: cout << "This is bad... found an unexpected computation type (" << a->getComputationName () << ") inside of a pipeline.\n";
		}

		lastOne = a;
	}
	
	//std :: cout << "Sink: " << targetSpec << " [" << targetProjection << "]\n";
	return returnVal;
}

inline ComputePlan :: ComputePlan (String &TCAPComputation, Vector <Handle <Computation>> &allComputations) :
	TCAPComputation (TCAPComputation), allComputations (allComputations) {}


}

#endif


