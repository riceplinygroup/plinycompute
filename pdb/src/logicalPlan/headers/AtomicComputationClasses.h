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

#ifndef COMP_CLASSES_H
#define COMP_CLASSES_H

#include "TupleSpec.h"
#include "AtomicComputationList.h"

#include "KeyValueList.h"

// NOTE: these are NOT part of the pdb namespace because they need to be included in an "extern
// C"...
// I am not sure whether that is possible... perhaps we try moving them to the pdb namespace later.

// this is a computation that applies a lambda to a tuple set
struct ApplyLambda : public AtomicComputation {

private:
    std::string lambdaName;

public:
    ~ApplyLambda() {}

    ApplyLambda(TupleSpec& input,
                TupleSpec& output,
                TupleSpec& projection,
                std::string nodeName,
                std::string lambdaNameIn)
        : AtomicComputation(input, output, projection, nodeName), lambdaName(lambdaNameIn) {}

	// ss107: New Constructor:
	ApplyLambda (TupleSpec &input, 
				 TupleSpec &output, 
				 TupleSpec &projection, 
				 std :: string nodeName, 
				 std :: string lambdaNameIn, 
                 KeyValueList &useMe) :
		AtomicComputation (input, output, projection, nodeName, useMe.getKeyValuePairs()), lambdaName (lambdaNameIn) {}


    std::string getAtomicComputationType() override {
        return std::string("Apply");
    }

    AtomicComputationTypeID getAtomicComputationTypeID() override {
        return ApplyLambdaTypeID;
    }

    std::pair<std::string, std::string> findSource(std::string attName,
                                                   AtomicComputationList& allComps) override {

        // the output from the apply is:
        //
        // (set of projection atts) (new attribute created from apply)
        //
        // find where the attribute appears in the outputs
        int counter = findPosInOutputAtts(attName);

        // if the attribute we are asking for is at the end (where the result of the lambda
        // application goes)
        // then we asked for it
        if (counter == getOutput().getAtts().size() - 1) {
            return std::make_pair(getComputationName(), lambdaName);
        }

        // otherwise, find our parent
        return allComps.getProducingAtomicComputation(getProjection().getSetName())
            ->findSource((getProjection().getAtts())[counter], allComps);
    }

    // returns the name of the lambda we are supposed to apply
    std::string& getLambdaToApply() {
        return lambdaName;
    }

    friend std::ostream& operator<<(std::ostream& os, const AtomicComputationList& printMe);
};

// this is a computation that applies a hash to a particular attribute in a tuple set
struct HashLeft : public AtomicComputation {

private:
    std::string lambdaName;

public:
    ~HashLeft() {}

    HashLeft(TupleSpec& input,
             TupleSpec& output,
             TupleSpec& projection,
             std::string nodeName,
             std::string lambdaNameIn)
        : AtomicComputation(input, output, projection, nodeName), lambdaName(lambdaNameIn) {}

	// ss107: New Constructor:
	HashLeft (TupleSpec &input, 
			  TupleSpec &output, 
              TupleSpec &projection, 
              std :: string nodeName, 
			  std :: string lambdaNameIn, 
			  KeyValueList &useMe) :
		AtomicComputation (input, output, projection, nodeName, useMe.getKeyValuePairs()), lambdaName (lambdaNameIn) {}



    std::string getAtomicComputationType() override {
        return std::string("HashLeft");
    }

    AtomicComputationTypeID getAtomicComputationTypeID() override {
      return HashLeftTypeID;
    }

    // returns the name of the lambda we are supposed to apply
    std::string& getLambdaToApply() {
        return lambdaName;
    }

    std::pair<std::string, std::string> findSource(std::string attName,
                                                   AtomicComputationList& allComps) override {

        // The output from the hash should be
        //
        // (projection atts) (hash value)
        //

        // find where the attribute appears in the outputs
        int counter = findPosInOutputAtts(attName);

        // if the attribute we are asking for is at the end (where the result of the lambda
        // application goes)
        // then we asked for it
        if (counter == getOutput().getAtts().size() - 1) {
            std::cout << "Why are you trying to find the origin of a hash value??\n";
            exit(1);
        }

        // otherwise, find our parent
        return allComps.getProducingAtomicComputation(getProjection().getSetName())
            ->findSource((getProjection().getAtts())[counter], allComps);
    }

    friend std::ostream& operator<<(std::ostream& os, const AtomicComputationList& printMe);
};

// this is a computation that applies a lambda to a tuple set
struct HashRight : public AtomicComputation {

private:
    std::string lambdaName;

public:
    ~HashRight() {}

    HashRight(TupleSpec& input,
              TupleSpec& output,
              TupleSpec& projection,
              std::string nodeName,
              std::string lambdaNameIn)
        : AtomicComputation(input, output, projection, nodeName), lambdaName(lambdaNameIn) {}

	// ss107: New Constructor:
	HashRight (TupleSpec &input, 
			   TupleSpec &output, 
               TupleSpec &projection, 
               std :: string nodeName, 
               std :: string lambdaNameIn, 
               KeyValueList &useMe) :
		AtomicComputation (input, output, projection, nodeName, useMe.getKeyValuePairs()), lambdaName (lambdaNameIn) {}


    std::string getAtomicComputationType() override {
        return std::string("HashRight");
    }

    AtomicComputationTypeID getAtomicComputationTypeID() override {
      return HashRightTypeID;
    }

    // returns the name of the lambda we are supposed to apply
    std::string& getLambdaToApply() {
        return lambdaName;
    }

    std::pair<std::string, std::string> findSource(std::string attName,
                                                   AtomicComputationList& allComps) override {

        // The output from the hash should be
        //
        // (projection atts) (hash value)
        //

        // find where the attribute appears in the outputs
        int counter = findPosInOutputAtts(attName);

        // if the attribute we are asking for is at the end (where the result of the lambda
        // application goes)
        // then we asked for it
        if (counter == getOutput().getAtts().size() - 1) {
            std::cout << "Why are you trying to find the origin of a hash value??\n";
            exit(1);
        }

        // otherwise, find our parent
        return allComps.getProducingAtomicComputation(getProjection().getSetName())
            ->findSource((getProjection().getAtts())[counter], allComps);
    }

    friend std::ostream& operator<<(std::ostream& os, const AtomicComputationList& printMe);
};


// this is a computation that adds 1  to each tuple of a tuple set
struct HashOne : public AtomicComputation {


public:
    ~HashOne() {}

    HashOne(TupleSpec& input, TupleSpec& output, TupleSpec& projection, std::string nodeName)
        : AtomicComputation(input, output, projection, nodeName) {

        // std :: cout << "HashOne input tuple spec: " << input << ", output tuple spec: " << output
        // << ", projection tuple spec: " << projection << std :: endl;
    }

	// ss107: New Constructor:
	HashOne (TupleSpec &input, TupleSpec &output, TupleSpec &projection, std :: string nodeName, KeyValueList &useMe) :
	                AtomicComputation (input, output, projection, nodeName, useMe.getKeyValuePairs()) {}


    std::string getAtomicComputationType() override {
        return std::string("HashOne");
    }

    AtomicComputationTypeID getAtomicComputationTypeID() override {
      return HashOneTypeID;
    }

    std::pair<std::string, std::string> findSource(std::string attName,
                                                   AtomicComputationList& allComps) override {

        // The output from the hash should be
        //
        // (projection atts) (hash value)
        //

        // find where the attribute appears in the outputs
        int counter = findPosInOutputAtts(attName);

        // otherwise, find our parent
        return allComps.getProducingAtomicComputation(getProjection().getSetName())
            ->findSource((getProjection().getAtts())[counter], allComps);
    }

    friend std::ostream& operator<<(std::ostream& os, const AtomicComputationList& printMe);
};


// this is a computation that flatten each tuple of a tuple set
struct Flatten : public AtomicComputation {


public:
    ~Flatten() {}

    Flatten(TupleSpec& input, TupleSpec& output, TupleSpec& projection, std::string nodeName)
        : AtomicComputation(input, output, projection, nodeName) {
	// ss107: New Constructor:
    Flatten (TupleSpec &input, TupleSpec &output, TupleSpec &projection, std :: string nodeName, KeyValueList &useMe) :
	                AtomicComputation (input, output, projection, nodeName, useMe.getKeyValuePairs()) {}



        // std :: cout << "Flatten input tuple spec: " << input << ", output tuple spec: " << output
        // << ", projection tuple spec: " << projection << std :: endl;
    }

    std::string getAtomicComputationType() override {
        return std::string("Flatten");
    }

    AtomicComputationTypeID getAtomicComputationTypeID() override {
      return FlattenTypeID;
    }

    std::pair<std::string, std::string> findSource(std::string attName,
                                                   AtomicComputationList& allComps) override {

        // The output from the hash should be
        //
        // (projection atts) (hash value)
        //
        // std :: cout << "Flatten findSource for attName=" << attName << std :: endl;
        // find where the attribute appears in the outputs
        int counter = findPosInOutputAtts(attName);

        if (counter == getOutput().getAtts().size() - 1) {
            return std::make_pair(getComputationName(), std::string(""));
        }

        // otherwise, find our parent
        // std :: cout << "Flatten projection set name is " << getProjection().getSetName() << std
        // :: endl;

        // std :: cout << "Flatten getProjection is " << getProjection() << std :: endl;
        return allComps.getProducingAtomicComputation(getProjection().getSetName())
            ->findSource((getProjection().getAtts())[counter], allComps);
    }

    friend std::ostream& operator<<(std::ostream& os, const AtomicComputationList& printMe);
};


// this is a computation that performs a filer over a tuple set
struct ApplyFilter : public AtomicComputation {

public:
    ~ApplyFilter() {}

    ApplyFilter(TupleSpec& input, TupleSpec& output, TupleSpec& projection, std::string nodeName)
        : AtomicComputation(input, output, projection, nodeName) {
        // std :: cout << "Filter input tuple spec: " << input << ", output tuple spec: " << output
        // << ", projection tuple spec: " << projection << std :: endl;
    }

	// ss107: New Constructor:
	ApplyFilter (TupleSpec &input, TupleSpec &output, TupleSpec &projection, std :: string nodeName, KeyValueList &useMe) :
		AtomicComputation (input, output, projection, nodeName, useMe.getKeyValuePairs()) {}

    std::string getAtomicComputationType() override {
        return std::string("Filter");
    }

    AtomicComputationTypeID getAtomicComputationTypeID() override {
      return ApplyFilterTypeID;
    }

    std::pair<std::string, std::string> findSource(std::string attName,
                                                   AtomicComputationList& allComps) override {

        // the output from the filter should be identical to the set of projection attributes
        // find where the attribute appears in the outputs
        int counter = findPosInOutputAtts(attName);

        // otherwise, find our parent
        return allComps.getProducingAtomicComputation(getProjection().getSetName())
            ->findSource((getProjection().getAtts())[counter], allComps);
    }
};

// this is a computation that aggregates a tuple set
struct ApplyAgg : public AtomicComputation {

public:
    ~ApplyAgg() {}

    ApplyAgg(TupleSpec& input, TupleSpec& output, TupleSpec& projection, std::string nodeName)
        : AtomicComputation(input, output, projection, nodeName) {}

	// ss107: New Constructor:
	ApplyAgg (TupleSpec &input, TupleSpec &output, TupleSpec &projection, std :: string nodeName, KeyValueList &useMe) :
		AtomicComputation (input, output, projection, nodeName, useMe.getKeyValuePairs()) {}

    std::string getAtomicComputationType() override {
        return std::string("Aggregate");
    }

    AtomicComputationTypeID getAtomicComputationTypeID() override {
      return ApplyAggTypeID;
    }

    std::pair<std::string, std::string> findSource(std::string attName,
                                                   AtomicComputationList& allComps) override {

        // The output from the aggregate should be a single attribute
        // find where the attribute appears in the outputs
        int counter = findPosInOutputAtts(attName);

        // if the attribute we are asking for is at the end, it means it's produced by this
        // aggregate
        // then we asked for it
        if (counter == 0) {
            return std::make_pair(getComputationName(), std::string(""));
        }

        // if it is not at the end, if makes no sense
        std::cout << "How did we ever get here trying to find an attribute produced by an agg??\n";
        exit(1);
    }
};

// this is a computation that produces a tuple set by scanning a set stored in the database
struct ScanSet : public AtomicComputation {

    std::string dbName;
    std::string setName;

public:
    ~ScanSet() {}

    ScanSet(TupleSpec& output, std::string dbName, std::string setName, std::string nodeName)
        : AtomicComputation(TupleSpec(), output, TupleSpec(), nodeName),
          dbName(dbName),
          setName(setName) {}

	// ss107: New Constructor:
	ScanSet (TupleSpec &output, std :: string dbName, std :: string setName, std :: string nodeName, KeyValueList &useMe) :
		AtomicComputation (TupleSpec (), output, TupleSpec (), nodeName, useMe.getKeyValuePairs()), dbName (dbName), setName (setName) {}


    std::string getAtomicComputationType() override {
        return std::string("Scan");
    }

    AtomicComputationTypeID getAtomicComputationTypeID() override {
      return ScanSetAtomicTypeID;
    }

    std::string& getDBName() {
        return dbName;
    }

    std::string& getSetName() {
        return setName;
    }

    std::pair<std::string, std::string> findSource(std::string attName,
                                                   AtomicComputationList& allComps) override {

        // The output from the scan should be a single attribute
        // find where the attribute appears in the outputs
        int counter = findPosInOutputAtts(attName);

        // if the attribute we are asking for is at the end (where the result of the lambda
        // application goes)
        // then we asked for it
        if (counter == 0) {
            return std::make_pair(getComputationName(), std::string(""));
        }

        // if it is not at the end, if makes no sense
        std::cout
            << "How did we ever get here trying to find an attribute produced by a scan set??\n";
        exit(1);
    }
};

// this is a computation that writes out a tuple set
struct WriteSet : public AtomicComputation {

    std::string dbName;
    std::string setName;

public:
    ~WriteSet() {}

    WriteSet(TupleSpec& input,
             TupleSpec& output,
             TupleSpec& projection,
             std::string dbName,
             std::string setName,
             std::string nodeName)
        : AtomicComputation(input, output, projection, nodeName),
          dbName(dbName),
          setName(setName) {}

	// ss107: New Constructor:
	WriteSet (TupleSpec &input, TupleSpec &output, TupleSpec &projection, std :: string dbName, std :: string setName, std :: string nodeName, KeyValueList &useMe) :
		AtomicComputation (input, output, projection, nodeName, useMe.getKeyValuePairs()), dbName (dbName), setName (setName) {}

    std::string getAtomicComputationType() override {
        return std::string("WriteSet");
    }

    AtomicComputationTypeID getAtomicComputationTypeID() override {
      return WriteSetTypeID;
    }

    std::string& getDBName() {
        return dbName;
    }

    std::string& getSetName() {
        return setName;
    }

    std::pair<std::string, std::string> findSource(std::string attName,
                                                   AtomicComputationList& allComps) override {
        std::cout << "How did we ever get to a write set trying to find an attribute??\n";
        exit(1);
    }
};

struct ApplyJoin : public AtomicComputation {

    TupleSpec rightInput;
    TupleSpec rightProjection;
    // JiaNote: added below for physical planning
    // if traversed is set to true, we know that one input has been processed, and the other input
    // can go through the pipeline
    bool traversed = false;
    // JiaNote: added below for hash partitioned join
    bool toPartitionLHS = false;

public:
    ApplyJoin(TupleSpec& output,
              TupleSpec& lInput,
              TupleSpec& rInput,
              TupleSpec& lProjection,
              TupleSpec& rProjection,
              std::string nodeName)
        : AtomicComputation(lInput, output, lProjection, nodeName),
          rightInput(rInput),
          rightProjection(rProjection) {
        traversed = false;
        toPartitionLHS = false;
    }


	// ss107: New Constructor: Added Jia's correction too:
	ApplyJoin (TupleSpec &output, TupleSpec &lInput, TupleSpec &rInput, TupleSpec &lProjection, TupleSpec &rProjection, std :: string nodeName, KeyValueList &useMe) :
		AtomicComputation (lInput, output, lProjection, nodeName, useMe.getKeyValuePairs()), rightInput (rInput),
		rightProjection (rProjection) {
        traversed = false;
        toPartitionLHS = false;
	}



    TupleSpec& getRightProjection() {
        return rightProjection;
    }

    TupleSpec& getRightInput() {
        return rightInput;
    }

    std::string getAtomicComputationType() override {
        return std::string("JoinSets");
    }

    AtomicComputationTypeID getAtomicComputationTypeID() override {
      return ApplyJoinTypeID;
    }

    bool isTraversed() {
        return this->traversed;
    }

    void setTraversed(bool traversed) {
        this->traversed = traversed;
    }

    bool isPartitioningLHS() {
        return this->toPartitionLHS;
    }

    void setPartitioningLHS(bool toPartitionLHS) {
        this->toPartitionLHS = toPartitionLHS;
    }


    std::pair<std::string, std::string> findSource(std::string attName,
                                                   AtomicComputationList& allComps) override {

        // The output from the join should be
        //
        // (left projection atts) (right projection atts)
        //
        // so find where the attribute in question came from
        int counter = findPosInOutputAtts(attName);

        // if it came from the left, then we recurse and find it
        if (counter < getProjection().getAtts().size()) {
            return allComps.getProducingAtomicComputation(getProjection().getSetName())
                ->findSource((getProjection().getAtts())[counter], allComps);

            // otherwise, if it came from the right, recurse and find it
        } else if (counter < getProjection().getAtts().size() + rightProjection.getAtts().size()) {
            return allComps.getProducingAtomicComputation(rightProjection.getSetName())
                ->findSource(
                    (rightProjection.getAtts())[counter - getProjection().getAtts().size()],
                    allComps);

        } else {
            std::cout << "Why in the heck did we not find the producer when checking a join!!??\n";
            exit(1);
        }
    }
};

#endif
