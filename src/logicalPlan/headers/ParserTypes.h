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

#ifndef PARSER_TYPES_H
#define PARSER_TYPES_H

#include <iostream>
#include <memory>
#include <stdlib.h>
#include <string>
#include <utility>
#include <vector>
#include <map>

using std::string;

/************************************************/
/** HERE WE DEFINE ALL OF THE STRUCTS THAT ARE **/
/** PASSED AROUND BY THE PARSER                **/
/************************************************/

// this is a list of attributes
struct AttList {

private:

	std :: vector <std :: string> atts;

public:

        ~AttList () {}
	AttList () {}

	void appendAttribute (char *appendMe) {
		atts.push_back (std :: string (appendMe));
	}

	void appendAttribute (std :: string appendMe) {
		atts.push_back (appendMe);
	}
	
	std :: vector <std :: string> &getAtts () {
		return atts;
	}
	
	friend struct TupleSpec;

};

// and here is the specifier for a tuple
struct TupleSpec {

private:

	std :: string setName;
	std :: vector <std :: string> atts;

public:

	TupleSpec () {}

	~TupleSpec () {}
	TupleSpec (std :: string setNameIn, AttList &useMe) {
		setName = setNameIn;
		atts = useMe.atts;
	}

	std :: string &getSetName () {
		return setName;
	}

	std :: vector <std :: string> &getAtts () {
		return atts;
	}
	
	friend std :: ostream& operator<<(std :: ostream& os, const TupleSpec& printMe);
};

inline std :: ostream& operator<<(std :: ostream& os, const TupleSpec& printMe) {
	os << printMe.setName << " (";
	bool first = true;
	for (auto &a : printMe.atts) {
		if (!first) 
			os << ", ";
		first = false;
		os << a;
	}
	os << ")";
	return os;
}

struct Computation;
struct ComputationList;
typedef std :: shared_ptr <struct Computation> ComputationPtr;

// base class for a computation
struct Computation {

private:

	TupleSpec input;
	TupleSpec output;
	TupleSpec projection;
	ComputationPtr me;

public:

	// returns the type of this computation
	virtual std :: string getComputationName () = 0;

	virtual ~Computation () {}

	ComputationPtr getShared () {
		return me;
	}

	void destroyPtr () {
		me = nullptr;
	}

	Computation (TupleSpec &inputIn, TupleSpec &outputIn, TupleSpec &projectionIn) : 
		input (inputIn), output (outputIn), projection (projectionIn) {}
	
	void setShared (ComputationPtr meIn) {
		me = meIn;
	}

	// gets the tuple set specifier for the output of this computation
	TupleSpec &getOutput () {
		return output;
	}	

	// gets the name of the tuple set produced by this computation
	std :: string &getOutputName () {
		return output.getSetName ();
	}

	// gets the specifier for the input tuple set used by this computation
	TupleSpec &getInput () {
		return input;
	}	

	// gets the name of the tuple set operated on by this computation
	std :: string &getInputName () {
		return input.getSetName ();
	}

	// gets the specifier for the set of output attributes that will be produced by this computation
	TupleSpec &getProjection () {
		return projection;
	}

	friend std :: ostream& operator<<(std :: ostream& os, const ComputationList& printMe);
};

// this is a computation that applies a lambda
struct ApplyLambda : public Computation {

private:
	
	std :: string lambdaName;

public:

	~ApplyLambda () {}

	ApplyLambda (TupleSpec &input, TupleSpec &output, TupleSpec &projection, std :: string lambdaNameIn) :
		Computation (input, output, projection) {
		lambdaName = lambdaNameIn;
	}

	std :: string getComputationName () override {
		return std :: string ("Apply");
	}

	// returns the name of the lambda we are supposed to apply
	std :: string &getLambdaToApply () {
		return lambdaName;
	}

	friend std :: ostream& operator<<(std :: ostream& os, const ComputationList& printMe);
};

// and this is a computation that performs a filer
struct ApplyFilter : public Computation {

public:

	~ApplyFilter () {}

	ApplyFilter (TupleSpec &input, TupleSpec &output, TupleSpec &projection) : Computation (input, output, projection) {}

	std :: string getComputationName () override {
		return std :: string ("Filter");
	}

};

// and here is a list of computations 
struct ComputationList {

private:

	std :: map <std :: string, ComputationPtr> producers;
	std :: map <std :: string, std :: vector <ComputationPtr>> consumers;

public:

	// gets the computation that builds the tuple set with the specified name
	ComputationPtr getProducingComputation (std :: string outputName) {
		if (producers.count (outputName) == 0) {
			std :: cout << "This is really bad... can't find the guy producing this output\n";
		}
		return producers [outputName];
	}

	// gets the list of comptuations that consume the tuple set with the specified name
	std :: vector <ComputationPtr> &getConsumingComputations (std :: string inputName) {
		if (consumers.count (inputName) == 0) {
			std :: cout << "This is really bad... can't find the guy consuming this input\n";
		}
		return consumers [inputName];
	}

	void addComputation (ComputationPtr addMe) {
		producers[addMe->getOutputName ()] = addMe;
		if (consumers.count (addMe->getInputName ()) == 0) {
			std :: vector <ComputationPtr> rhs;
			consumers[addMe->getInputName ()] = rhs;
		}
		consumers[addMe->getInputName ()].push_back (addMe);
		addMe->destroyPtr ();
	}

	friend std :: ostream& operator<<(std :: ostream& os, const ComputationList& printMe);
};

inline std :: ostream& operator<<(std :: ostream& os, const ComputationList& printMe) {
	for (auto &a : printMe.producers) {
		os << a.second->output << " <= " << a.second->getComputationName () << 
			"(" << a.second->input << ", " << a.second->projection;
		if (a.second->getComputationName () == "Apply") {
			ApplyLambda *temp = (ApplyLambda *) a.second.get ();
			os << ", " << temp->lambdaName;
		}
		os << ")\n";
	}
	return os;
}


struct Input {

private:

	TupleSpec output;
	std :: string dbName;
	std :: string setName;

public:

	Input () {}

	// gets the tuple specifier for the set that is constructed from this input
	TupleSpec &getOutput () {
		return output;
	}	

	// gets the name of the set that is constructed from this input
	std :: string &getOutputName () {
		return output.getSetName ();
	}
	
	Input (TupleSpec &useMe, std :: string dbNameIn, std :: string setNameIn) : output (useMe) {
		dbName = dbNameIn;
		setName = setNameIn;
	}

    string getDbName()
    {
        return dbName;
    }

    string getSetName()
    {
        return setName;
    }


	friend std :: ostream& operator<<(std :: ostream& os, const Input& printMe);
};

inline std :: ostream& operator<<(std :: ostream& os, const Input& printMe) {
	os << printMe.output << " <= (" << printMe.dbName << ", " << printMe.setName << ")\n";
	return os;
}

struct InputList {

private:

	std :: map <std :: string, Input> inputs;

public:

	// gets the person who produces the specified Tuple Set
	Input &getProducer (std :: string outputTupleSetName) {
		if (inputs.count (outputTupleSetName) == 0) {
			std :: cout << "This is really bad... can't find the guy producing this output\n";
		}
		return inputs [outputTupleSetName];
	}

	void addInput (Input &addMe) {
		inputs [addMe.getOutputName ()] = addMe;
	}

    Input& getInput(string outputName)
    {
        return inputs[outputName];
    }

    unsigned long size()
    {
        return inputs.size();
    }

	friend std :: ostream& operator<<(std :: ostream& os, const InputList& printMe);
};

inline std :: ostream& operator<<(std :: ostream& os, const InputList& printMe) {
	for (auto &a : printMe.inputs) {
		os << a.second << "\n";
	}
	return os;
}

struct Output {

private:

	TupleSpec input;
	std :: string dbName;
	std :: string setName;

public:

	TupleSpec &getInput () {
		return input;
	}	

	// gets the name of the tuple set that is written as output
	std :: string &getInputName () {
		return input.getSetName ();
	}
	
	// gets the name of the database set written as output
	std :: string &getdbName () {
		return dbName;
	}

	// gets the name of the database written to by this output
	std :: string &getSetName () {
		return setName;
	}

	Output (TupleSpec &useMe, std :: string dbNameIn, std :: string setNameIn) : input (useMe) {
		input = useMe;
		dbName = dbNameIn;
		setName = setNameIn;
	}

	friend std :: ostream& operator<<(std :: ostream& os, const Output& printMe);
};

inline std :: ostream& operator<<(std :: ostream& os, const Output& printMe) {
	os << "(" << printMe.dbName << ", " << printMe.setName << ") <= " << printMe.input << "\n";
	return os;
}

struct OutputList {

private:

	std :: map <std :: string, std :: vector <Output>> consumers;

public:

	void addOutput (Output &addMe) {
		if (consumers.count (addMe.getInputName ()) == 0) {
			std :: vector <Output> rhs;
			consumers[addMe.getInputName ()] = rhs;
		}
		consumers[addMe.getInputName ()].push_back (addMe);
	}

	std :: vector <Output> &getConsumers (std :: string inputName) {
		if (consumers.count (inputName) == 0) {
			std :: cout << "This is really bad... can't find the guy consuming this input\n";
		}
		return consumers [inputName];
	}

	friend std :: ostream& operator<<(std :: ostream& os, const OutputList& printMe);
};

inline std :: ostream& operator<<(std :: ostream& os, const OutputList& printMe) {
	for (auto &a : printMe.consumers) {
		for (auto &b : a.second) {
			os << b << "\n";
		}
	}
	return os;
}

struct LogicalPlan {

private:

	OutputList outputs;
	InputList inputs;
	ComputationList computations;

public:

	LogicalPlan (OutputList &outputsIn, InputList &inputsIn, ComputationList &computationsIn) {
		outputs = outputsIn;
		inputs = inputsIn;
		computations = computationsIn;
	}	

	OutputList &getOutputs () {
		return outputs;
	}	

	InputList &getInputs () {
		return inputs;
	}	

	ComputationList &getComputations () {
		return computations;
	}	

	friend std :: ostream& operator<<(std :: ostream& os, const LogicalPlan& printMe);
};

inline std :: ostream& operator<<(std :: ostream& os, const LogicalPlan& printMe) {
	os << "Outputs:\n" << printMe.outputs << "Inputs:\n" << printMe.inputs << "Computations:\n" << printMe.computations;
	return os;
}

#endif
