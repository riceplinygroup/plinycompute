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

#include "Handle.h"
#include "Lambda.h"
#include "Supervisor.h"
#include "Employee.h"
#include "LambdaCreationFunctions.h"
#include "UseTemporaryAllocationBlock.h"
#include "Pipeline.h"
#include "SetWriter.h"
#include "SelectionComp.h"
#include "AggregateComp.h"
#include "JoinComp.h"
#include "ScanSet.h"
#include "VectorSink.h"
#include "HashSink.h"
#include "MapTupleSetIterator.h"
#include "VectorTupleSetIterator.h"
#include "ComputePlan.h"
#include "StringIntPair.h"

// to run the aggregate, the system first passes each through the hash operation...
// then the system
using namespace pdb;

class SillyWrite : public SetWriter<double> {

public:
    ENABLE_DEEP_COPY

    // eventually, this method should be moved into a class that works with the system to
    // iterate through pages that are pulled from disk/RAM by the system... a programmer
    // should not provide this particular method
    ComputeSinkPtr getComputeSink(TupleSpec& consumeMe,
                                  TupleSpec& whichAttsToOpOn,
                                  TupleSpec& projection,
                                  ComputePlan& plan) override {
        return std::make_shared<VectorSink<double>>(consumeMe, projection);
    }
};

// this plan has three tables: A (a: int), B (a: int, c: String), C (c: String), D (int)
// it first joins A with B, and then joins the result with C
class SillyJoin : public JoinComp<String, int, StringIntPair, String, int> {

public:
    ENABLE_DEEP_COPY

    Lambda<bool> getSelection(Handle<int> in1,
                              Handle<StringIntPair> in2,
                              Handle<String> in3,
                              Handle<int> in4) override {
        return (makeLambdaFromSelf(in1) == makeLambdaFromMember(in2, myInt)) &&
            ((makeLambdaFromMember(in2, myString) == makeLambdaFromSelf(in3) &&
              makeLambdaFromSelf(in1) == makeLambdaFromSelf(in4)));
    }

    Lambda<Handle<String>> getProjection(Handle<int> in1,
                                         Handle<StringIntPair> in2,
                                         Handle<String> in3,
                                         Handle<int> in4) override {
        return makeLambda(in1,
                          in2,
                          in3,
                          in4,
                          [](Handle<int>& in1,
                             Handle<StringIntPair>& in2,
                             Handle<String>& in3,
                             Handle<int>& in4) {
                              std::ostringstream oss;
                              oss << "Got int " << *in1 << " and StringIntPair (" << in2->myInt
                                  << ", '" << *(in2->myString) << "') and String '" << *in3
                                  << "' and int " << *in4;
                              Handle<String> res = makeObject<String>(oss.str());
                              return res;
                          });
    }
};

class SillyReadOfA : public ScanSet<int> {

    ENABLE_DEEP_COPY

    // eventually, this method should be moved into a class that works with the system to
    // iterate through pages that are pulled from disk/RAM by the system... a programmer
    // should not provide this particular method
    ComputeSourcePtr getComputeSource(TupleSpec& schema, ComputePlan& plan) override {

        return std::make_shared<VectorTupleSetIterator>(

            // constructs a list of data objects to iterate through
            []() -> void* {

                // this implementation only serves six pages
                static int numPages = 0;
                if (numPages == 6)
                    return nullptr;

                // create a page, loading it with random data
                void* myPage = malloc(1024 * 1024);
                {
                    const UseTemporaryAllocationBlock tempBlock{myPage, 1024 * 1024};

                    // write a bunch of supervisors to it
                    Handle<Vector<Handle<int>>> data = makeObject<Vector<Handle<int>>>();
                    int i = 0;
                    try {
                        for (; true; i++) {
                            Handle<int> myInt = makeObject<int>(i);
                            data->push_back(myInt);
                        }
                    } catch (NotEnoughSpace& e) {
                        std::cout << "got to " << i << " when proucing data for SillyReadOfA.\n";
                        getRecord(data);
                    }
                }
                numPages++;
                return myPage;
            },

            // frees the list of data objects that have been iterated
            [](void* freeMe) -> void { free(freeMe); },

            // and this is the chunk size, or number of items to put into each tuple set
            24);
    }
};

class SillyReadOfB : public ScanSet<StringIntPair> {

    ENABLE_DEEP_COPY

    // eventually, this method should be moved into a class that works with the system to
    // iterate through pages that are pulled from disk/RAM by the system... a programmer
    // should not provide this particular method
    ComputeSourcePtr getComputeSource(TupleSpec& schema, ComputePlan& plan) override {

        return std::make_shared<VectorTupleSetIterator>(

            // constructs a list of data objects to iterate through
            []() -> void* {
                // this implementation only serves six pages
                static int numPages = 0;
                if (numPages == 6)
                    return nullptr;

                // create a page, loading it with random data
                void* myPage = malloc(1024 * 1024);
                {
                    const UseTemporaryAllocationBlock tempBlock{myPage, 1024 * 1024};

                    // write a bunch of supervisors to it
                    Handle<Vector<Handle<StringIntPair>>> data =
                        makeObject<Vector<Handle<StringIntPair>>>();

                    int i = 0;
                    try {
                        for (; true; i++) {
                            std::ostringstream oss;
                            oss << "My string is " << i;
                            oss.str();
                            Handle<StringIntPair> myPair = makeObject<StringIntPair>(oss.str(), i);
                            data->push_back(myPair);
                        }
                    } catch (NotEnoughSpace& e) {
                        std::cout << "got to " << i << " when proucing data for SillyReadOfB.\n";
                        getRecord(data);
                    }
                }
                numPages++;
                return myPage;
            },

            // frees the list of data objects that have been iterated
            [](void* freeMe) -> void { free(freeMe); },

            // and this is the chunk size, or number of items to put into each tuple set
            24);
    }
};

class SillyReadOfC : public ScanSet<String> {

    ENABLE_DEEP_COPY

    // eventually, this method should be moved into a class that works with the system to
    // iterate through pages that are pulled from disk/RAM by the system... a programmer
    // should not provide this particular method
    ComputeSourcePtr getComputeSource(TupleSpec& schema, ComputePlan& plan) override {

        return std::make_shared<VectorTupleSetIterator>(

            // constructs a list of data objects to iterate through
            []() -> void* {

                // this implementation only serves six pages
                static int numPages = 0;
                if (numPages == 6)
                    return nullptr;

                // create a page, loading it with random data
                void* myPage = malloc(1024 * 1024);
                {
                    const UseTemporaryAllocationBlock tempBlock{myPage, 1024 * 1024};

                    // write a bunch of supervisors to it
                    Handle<Vector<Handle<String>>> data = makeObject<Vector<Handle<String>>>();

                    int j = 0;
                    try {
                        for (int i = 0; true; i += 3) {
                            std::ostringstream oss;
                            oss << "My string is " << i;
                            oss.str();
                            Handle<String> myString = makeObject<String>(oss.str());
                            data->push_back(myString);
                            j++;
                        }
                    } catch (NotEnoughSpace& e) {
                        std::cout << "got to " << j << " when proucing data for SillyReadOfC.\n";
                        getRecord(data);
                    }
                }
                numPages++;
                return myPage;
            },

            // frees the list of data objects that have been iterated
            [](void* freeMe) -> void { free(freeMe); },

            // and this is the chunk size, or number of items to put into each tuple set
            24);
    }
};

class SillyReadOfD : public ScanSet<int> {

    ENABLE_DEEP_COPY

    // eventually, this method should be moved into a class that works with the system to
    // iterate through pages that are pulled from disk/RAM by the system... a programmer
    // should not provide this particular method
    ComputeSourcePtr getComputeSource(TupleSpec& schema, ComputePlan& plan) override {

        return std::make_shared<VectorTupleSetIterator>(

            // constructs a list of data objects to iterate through
            []() -> void* {

                // this implementation only serves six pages
                static int numPages = 0;
                if (numPages == 1)
                    return nullptr;

                // create a page, loading it with random data
                void* myPage = malloc(1024 * 1024);
                {
                    const UseTemporaryAllocationBlock tempBlock{myPage, 1024 * 1024};

                    // write a bunch of supervisors to it
                    Handle<Vector<Handle<int>>> data = makeObject<Vector<Handle<int>>>();

                    try {
                        for (int i = 0; true; i += 12) {
                            Handle<int> myInt = makeObject<int>(i);
                            data->push_back(myInt);
                        }
                    } catch (NotEnoughSpace& e) {
                        getRecord(data);
                    }
                }
                numPages++;
                return myPage;
            },

            // frees the list of data objects that have been iterated
            [](void* freeMe) -> void { free(freeMe); },

            // and this is the chunk size, or number of items to put into each tuple set
            24);
    }
};

int main() {

    // this is the object allocation block where all of this stuff will reside
    makeObjectAllocatorBlock(1024 * 1024, true);

    // here is the list of computations
    Vector<Handle<Computation>> myComputations;

    // create all of the computation objects
    Handle<Computation> readA = makeObject<SillyReadOfA>();
    Handle<Computation> readB = makeObject<SillyReadOfB>();
    Handle<Computation> readC = makeObject<SillyReadOfC>();
    Handle<Computation> readD = makeObject<SillyReadOfD>();
    Handle<Computation> myJoin = makeObject<SillyJoin>();
    Handle<Computation> myWriter = makeObject<SillyWrite>();

    // put them in the list of computations
    myComputations.push_back(readA);
    myComputations.push_back(readB);
    myComputations.push_back(readC);
    myComputations.push_back(myJoin);
    myComputations.push_back(myWriter);
    myComputations.push_back(readD);

    // now we create the TCAP string
    String myTCAPString =
        "/* scan the three inputs */ \n\
	        A (a) <= SCAN ('mySet', 'myData', 'ScanSet_0') \n\
	        B (aAndC) <= SCAN ('mySet', 'myData', 'ScanSet_1') \n\
	        C (c) <= SCAN ('mySet', 'myData', 'ScanSet_2') \n\
				\n\
		/* extract and hash a from A */ \n\
		AWithAExtracted (a, aExtracted) <= APPLY (A (a), A(a), 'JoinComp_3', 'self_0') \n\
		AHashed (a, hash) <= HASHLEFT (AWithAExtracted (aExtracted), A (a), 'JoinComp_3', '==_2') \n\
				\n\
		/* extract and hash a from B */ \n\
		BWithAExtracted (aAndC, a) <= APPLY (B (aAndC), B (aAndC), 'JoinComp_3', 'attAccess_1') \n\
		BHashedOnA (aAndC, hash) <= HASHRIGHT (BWithAExtracted (a), BWithAExtracted (aAndC), 'JoinComp_3', '==_2') \n\
				\n\
		/* now, join the two of them */ \n\
		AandBJoined (a, aAndC) <= JOIN (AHashed (hash), AHashed (a), BHashedOnA (hash), BHashedOnA (aAndC), 'JoinComp_3') \n\
				\n\
		/* and extract the two atts and check for equality */ \n\
		AandBJoinedWithAExtracted (a, aAndC, aExtracted) <= APPLY (AandBJoined (a), AandBJoined (a, aAndC), 'JoinComp_3', 'self_0') \n\
		AandBJoinedWithBothExtracted (a, aAndC, aExtracted, otherA) <= APPLY (AandBJoinedWithAExtracted (aAndC), \n\
			AandBJoinedWithAExtracted (a, aAndC, aExtracted), 'JoinComp_3', 'attAccess_1') \n\
		AandBJoinedWithBool (aAndC, a, bool) <= APPLY (AandBJoinedWithBothExtracted (aExtracted, otherA), AandBJoinedWithBothExtracted (aAndC, a), \n\
			'JoinComp_3', '==_2') \n\
		AandBJoinedFiltered (a, aAndC) <= FILTER (AandBJoinedWithBool (bool), AandBJoinedWithBool (a, aAndC), 'JoinComp_3') \n\
				\n\
		/* now get ready to join the strings */ \n\
		AandBJoinedFilteredWithC (a, aAndC, cExtracted) <= APPLY (AandBJoinedFiltered (aAndC), AandBJoinedFiltered (a, aAndC), 'JoinComp_3', 'attAccess_3') \n\
		BHashedOnC (a, aAndC, hash) <= HASHLEFT (AandBJoinedFilteredWithC (cExtracted), AandBJoinedFilteredWithC (a, aAndC), 'JoinComp_3', '==_5') \n\
		CwithCExtracted (c, cExtracted) <= APPLY (C (c), C (c), 'JoinComp_3', 'self_4') \n\
		CHashedOnC (c, hash) <= HASHRIGHT (CwithCExtracted (cExtracted), CwithCExtracted (c), 'JoinComp_3', '==_5') \n\
				\n\
		/* join the two of them */ \n\
		BandCJoined (a, aAndC, c) <= JOIN (BHashedOnC (hash), BHashedOnC (a, aAndC), CHashedOnC (hash), CHashedOnC (c), 'JoinComp_3') \n\
				\n\
		/* and extract the two atts and check for equality */ \n\
		BandCJoinedWithCExtracted (a, aAndC, c, cFromLeft) <= APPLY (BandCJoined (aAndC), BandCJoined (a, aAndC, c), 'JoinComp_3', 'attAccess_3') \n\
		BandCJoinedWithBoth (a, aAndC, c, cFromLeft, cFromRight) <= APPLY (BandCJoinedWithCExtracted (c), BandCJoinedWithCExtracted (a, aAndC, c, cFromLeft), \n\
			'JoinComp_3', 'self_4') \n\
		BandCJoinedWithBool (a, aAndC, c, bool) <= APPLY (BandCJoinedWithBoth (cFromLeft, cFromRight), BandCJoinedWithBoth (a, aAndC, c), \n\
			'JoinComp_3', '==_5') \n\
		last (a, aAndC, c) <= FILTER (BandCJoinedWithBool (bool), BandCJoinedWithBool (a, aAndC, c), 'JoinComp_3') \n\
				\n\
		/* now, scan the last table */ \n\
	        D (d) <= SCAN ('mySet', 'myData', 'ScanSet_5') \n\
				\n\
		/* hash the left and the right */ \n\
		DWithDExtracted (d, dExtracted) <= APPLY (D(d), D(d), 'JoinComp_3', 'self_7') \n\
		DHashed (d, hash) <= HASHRIGHT (DWithDExtracted (dExtracted), DWithDExtracted (d), 'JoinComp_3', '==_8') \n\
		LastWithAExtracted (a, c, aAndC, aExtracted) <= APPLY (last (a), last(a, c, aAndC), 'JoinComp_3', 'self_6') \n\
		LastHashed (a, c, aAndC, hash) <= HASHLEFT (LastWithAExtracted (aExtracted), LastWithAExtracted (a, c, aAndC), 'JoinComp_3', '==_8') \n\
				\n\
		/* join */ \n\
		AlmostDone (c, a, aAndC, d) <= JOIN (LastHashed (hash), LastHashed (c, a, aAndC), DHashed (hash), DHashed (d), 'JoinComp_3') \n\
				\n\
		/* and check for equality */ \n\
		AlmostDoneWithA (a, c, aAndC, d, aExtracted) <= APPLY (AlmostDone (a), AlmostDone (a, c, aAndC, d), 'JoinComp_3', 'self_6') \n\
		AlmostDoneWithBoth (a, c, aAndC, d, aExtracted, dExtracted) <= APPLY (AlmostDoneWithA (d), \n\
			AlmostDoneWithA (a, c, aAndC, d, aExtracted), 'JoinComp_3', 'self_7') \n\
		AlmostDoneWithEquals (a, aAndC, c, d, bool) <= APPLY (AlmostDoneWithBoth (aExtracted, dExtracted), \n\
			AlmostDoneWithBoth (a, aAndC, c, d), 'JoinComp_3', '==_8') \n\
		PrettyMuchDone (a, c, aAndC, d) <= FILTER (AlmostDoneWithEquals (bool), AlmostDoneWithEquals (a, c, aAndC, d), 'JoinComp_3') \n\
				\n\
		/* and here is the answer */ \n\
		almostFinal (result) <= APPLY (PrettyMuchDone (a, aAndC, c, d), PrettyMuchDone (), 'JoinComp_3', 'native_lambda_11') \n\
	        nothing () <= OUTPUT (almostFinal (result), 'outSet', 'myDB', 'SetWriter_4')";

    // and create a query object that contains all of this stuff
    Handle<ComputePlan> myPlan = makeObject<ComputePlan>(myTCAPString, myComputations);

    // now, let's pretend that myPlan has been sent over the network, and we want to execute it...
    // first we build
    // a pipeline into the first join
    void* whereHashTableForASits;
    void* whereHashTableForBSits;
    void* whereHashTableForLastSits;

    PipelinePtr myPipeline = myPlan->buildPipeline(
        std::string("A"),          /* this is the TupleSet the pipeline starts with */
        std::string("AHashed"),    /* this is the TupleSet the pipeline ends with */
        std::string("JoinComp_3"), /* and since multiple Computation objects can consume the */
                                   /* same tuple set, we apply the Computation as well */

        // this lambda supplies new temporary pages to the pipeline
        []() -> std::pair<void*, size_t> {
            void* myPage = malloc(64 * 1024 * 1024);
            return std::make_pair(myPage, 64 * 1024 * 1024);
        },

        // this lambda frees temporary pages that do not contain any important data
        [](void* page) { free(page); },

        // and this lambda remembers the page that *does* contain important data...
        // in this simple aggregation, that one page will contain the hash table with
        // all of the aggregated data.
        [&](void* page) {
            whereHashTableForASits = page;
            std::cout << "Remembering where hash for A is located.\n";
            std::cout << "It is at " << (size_t)whereHashTableForASits << ".\n";
        });

    // and now, simply run the pipeline and then destroy it!!!
    std::cout << "\nRUNNING PIPELINE\n";
    myPipeline->run();
    std::cout << "\nDONE RUNNING PIPELINE\n";
    myPipeline = nullptr;

    // now, let's pretend that myPlan has been sent over the network, and we want to execute it...
    // first we build
    // a pipeline into the first join
    std::map<std::string, ComputeInfoPtr> info;
    info[std::string("AandBJoined")] = std::make_shared<JoinArg>(*myPlan, whereHashTableForASits);
    myPipeline = myPlan->buildPipeline(
        std::string("B"),          /* this is the TupleSet the pipeline starts with */
        std::string("BHashedOnC"), /* this is the TupleSet the pipeline ends with */
        std::string("JoinComp_3"), /* and since multiple Computation objects can consume the */
                                   /* same tuple set, we apply the Computation as well */

        // this lambda supplies new temporary pages to the pipeline
        []() -> std::pair<void*, size_t> {
            void* myPage = malloc(64 * 1024 * 1024);
            return std::make_pair(myPage, 64 * 1024 * 1024);
        },

        // this lambda frees temporary pages that do not contain any important data
        [](void* page) { free(page); },

        // and this lambda remembers the page that *does* contain important data...
        // in this simple aggregation, that one page will contain the hash table with
        // all of the aggregated data.
        [&](void* page) {
            std::cout << "Getting the hash table for B\n";
            whereHashTableForBSits = page;
        },

        info);

    // and now, simply run the pipeline and then destroy it!!!
    std::cout << "\nRUNNING PIPELINE\n";
    myPipeline->run();
    std::cout << "\nDONE RUNNING PIPELINE\n";
    myPipeline = nullptr;

    // used to store info about the joins
    info[std::string("BandCJoined")] = std::make_shared<JoinArg>(*myPlan, whereHashTableForBSits);

    // now, let's pretend that myPlan has been sent over the network, and we want to execute it...
    // first we build
    // a pipeline into the first join
    myPipeline = myPlan->buildPipeline(
        std::string("C"),          /* this is the TupleSet the pipeline starts with */
        std::string("LastHashed"), /* this is the TupleSet the pipeline ends with */
        std::string("JoinComp_3"), /* and since multiple Computation objects can consume the */
                                   /* same tuple set, we apply the Computation as well */

        // this lambda supplies new temporary pages to the pipeline
        []() -> std::pair<void*, size_t> {
            void* myPage = malloc(128 * 1024 * 1024);
            return std::make_pair(myPage, 128 * 1024 * 1024);
        },

        // this lambda frees temporary pages that do not contain any important data
        [](void* page) { free(page); },

        // and this lambda remembers the page that *does* contain important data...
        // in this simple aggregation, that one page will contain the hash table with
        // all of the aggregated data.
        [&](void* page) {
            std::cout << "Getting the hash table for Last\n";
            whereHashTableForLastSits = page;
        },

        info);

    std::cout << "\nRUNNING PIPELINE\n";
    myPipeline->run();
    std::cout << "\nDONE RUNNING PIPELINE\n";
    myPipeline = nullptr;

    info[std::string("AlmostDone")] = std::make_shared<JoinArg>(*myPlan, whereHashTableForLastSits);

    myPipeline = myPlan->buildPipeline(
        std::string("D"),           /* this is the TupleSet the pipeline starts with */
        std::string("almostFinal"), /* this is the TupleSet the pipeline ends with */
        std::string("SetWriter_4"), /* and since multiple Computation objects can consume the */
                                    /* same tuple set, we apply the Computation as well */

        // this lambda supplies new temporary pages to the pipeline
        []() -> std::pair<void*, size_t> {
            void* myPage = malloc(1024 * 1024);
            return std::make_pair(myPage, 1024 * 1024);
        },

        // this lambda frees temporary pages that do not contain any important data
        [](void* page) { free(page); },

        // and this lambda remembers the page that *does* contain important data...
        // in this simple aggregation, that one page will contain the hash table with
        // all of the aggregated data.
        [](void* page) {
            std::cout << "\nAsked to save page at address " << (size_t)page << "!!!\n";
            Handle<Vector<Handle<String>>> myVec =
                ((Record<Vector<Handle<String>>>*)page)->getRootObject();
            std::cout << "Found that this has " << myVec->size() << " strings in it.\n";
            if (myVec->size() > 0)
                std::cout << "First one is '" << *((*myVec)[0]) << "'\n";
            free(page);
        },

        info);

    // and now, simply run the pipeline and then destroy it!!!
    std::cout << "\nRUNNING PIPELINE\n";
    myPipeline->run();
    std::cout << "\nDONE RUNNING PIPELINE\n";
    myPipeline = nullptr;

    // and be sure to delete the contents of the ComputePlan object... this always needs to be done
    // before the object is written to disk or sent accross the network, so that we don't end up
    // moving around a C++ smart pointer, which would be bad
    myPlan->nullifyPlanPointer();

    free(whereHashTableForASits);
    free(whereHashTableForBSits);
    free(whereHashTableForLastSits);
}
