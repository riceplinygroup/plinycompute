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

#ifndef LAMBDA_HELPER_H
#define LAMBDA_HELPER_H

#include <memory>
#include <vector>
#include <functional>
#include "Literal.h"
#include "Object.h"
#include "Handle.h"
#include "Ptr.h"
#include "TupleSpec.h"
#include "ComputeExecutor.h"

namespace pdb {

// The basic idea is that we have a "class Lambda <typename Out>" that is returned by the query object.

// Internally, the query object creates a "class LambdaTree <Out>" object.  The reason that the
// query internally constructs a "class LambdaTree <Out>" whereas the query returns a 
// "class Lambda <Out>" is that there may be a mismatch between the two type parameters---the 
// LambdaTree may return a "class LambdaTree <Ptr<Out>>" object for efficiency.  Thus, we allow 
// a "class Lambda <Out>" object to be constructed with either a  "class LambdaTree <Out>"
// or a  "class LambdaTree <Ptr<Out>>".  We don't want to allow implicit conversions between
//  "class LambdaTree <Out>" and "class LambdaTree <Ptr<Out>>", however, which is why we need
// the separate type.

// Each "class LambdaTree <Out>" object is basically just a wrapper for a shared_ptr to a
// "TypedLambdaObject <Out> object".  So that we can pass around pointers to these things (without
// return types), "TypedLambdaObject <Out>" derives from "GenericLambdaObject". 

// forward delcaration
template <typename Out>
class TypedLambdaObject;

// we wrap up a shared pointer (rather than using a simple shared pointer) so that we 
// can override operations on these guys (if we used raw shared pointers, we could not)
template <typename ReturnType>
class LambdaTree {

private:

	std :: shared_ptr <TypedLambdaObject <ReturnType>> me;

public:

	LambdaTree () {}

	auto &getPtr () {
		return me;
	}

	LambdaTree <ReturnType> *operator -> () const {
		return me.get ();
	}

	LambdaTree <ReturnType> &operator * () const {
		return *me;
	}
	
	template <class Type> 
	LambdaTree (std :: shared_ptr <Type> meIn) {
		me = meIn;	
	}

	LambdaTree (const LambdaTree <ReturnType> &toMe) : me (toMe.me) {}

	LambdaTree <ReturnType> &operator = (const LambdaTree <ReturnType> &toMe) {
		me = toMe.me;
		return *this;
	}	

	template <class Type>
	LambdaTree <ReturnType> &operator = (std :: shared_ptr <Type> toMe) {
		me = toMe;
		return *this;
	}	
};

class GenericLambdaObject;
typedef std :: shared_ptr <GenericLambdaObject> GenericLambdaObjectPtr;

// this is the base class from which all pdb :: Lambdas derive
class GenericLambdaObject {

public:

	// this gets an executor that appends the result of running this lambda to the end of each tuple
	virtual ComputeExecutorPtr getExecutor (TupleSpec &inputSchema, TupleSpec &attsToOperateOn, TupleSpec &attsToIncludeInOutput) = 0;
	
	// returns the name of this LambdaBase type, as a string
	virtual std :: string getTypeOfLambda () = 0;

        // one big technical problem is that when tuples are added to a hash table to be recovered
        // at a later time, we we break a pipeline.  The difficulty with this is that when we want
        // to probe a hash table to find a set of hash values, we can't use the input TupleSet as
        // a way to create the columns to store the result of the probe.  The hash table needs to
        // be able to create (from scratch) the columns that store the output.  This is a problem,
        // because the hash table has no information about the types of the objects that it contains.
	// The way around this is that we have a function attached to each GenericLambdaObject that allows
	// us to ask the GenericLambdaObject to try to add a column to a tuple set, of a specific type,
	// where the type name is specified as a string.  When the hash table needs to create an output
	// TupleSet, it can ask all of the GenericLambdaObjects associated with a query to create the
	// necessary columns, as a way to build up the output TupleSet.  This method is how the hash
	// table can ask for this.  It takes tree args: the type  of the column that the hash table wants
	// the tuple set to build, the tuple set to add the column to, and the position where the
	// column will be added.  If the GenericLambdaObject cannot build the column (it has no knowledge
	// of that type) a false is returned.  Otherwise, a true is returned.
	virtual bool addColumnToTupleSet (std :: string &typeToMatch, TupleSetPtr addToMe, int posToAddTo) = 0;

	// returns the number of children of this Lambda type
	virtual int getNumChildren () = 0;

	// gets a particular child of this Lambda
	virtual GenericLambdaObjectPtr getChild (int which) = 0;

        // gets TCAP string corresponding to this Lambda
        virtual std :: string toTCAPString (std :: string inputTupleSetName, std :: vector<std :: string> inputColumnNames, std :: vector<std :: string> inputColumnsToApply, int lambdaLabel, std :: string computationName, int computationLabel, std :: string& outputTupleSetName, std :: vector<std :: string> & outputColumns, std :: string& outputColumnName) {

                std :: string tcapString = "";
                std :: string lambdaType = getTypeOfLambda();
                outputTupleSetName = lambdaType.substr(0, 5)+"_"+std :: to_string(lambdaLabel)+"OutFor"+computationName+std :: to_string(computationLabel);

                outputColumnName = lambdaType.substr(0, 5)+"_"+std ::to_string(lambdaLabel)+"_"+std ::to_string(computationLabel);
                outputColumns.clear();
                for (int i = 0; i < inputColumnNames.size(); i++) {
                    outputColumns.push_back(inputColumnNames[i]);
                }
                outputColumns.push_back(outputColumnName);
                tcapString += outputTupleSetName + "(" + outputColumns[0];
                for (int i = 1; i < outputColumns.size(); i++) {
                    tcapString += ",";
                    tcapString += outputColumns[i];
                }
                tcapString += ") <= APPLY (";
                tcapString += inputTupleSetName + "(" + inputColumnsToApply[0];
                for (int i = 1; i < inputColumnsToApply.size(); i++) {
                    tcapString += ",";
                    tcapString += inputColumnsToApply[i];
                }
                tcapString += "), " + inputTupleSetName + "(" + inputColumnNames[0];
                for (int i = 1; i < inputColumnNames.size(); i++) {
                    tcapString += ",";
                    tcapString += inputColumnNames[i];
                }
                tcapString += "), '" + computationName + "_"  + std :: to_string(computationLabel) + "', '"+ getTypeOfLambda() + "_" + std :: to_string(lambdaLabel) +"')\n";

                return tcapString;


}

	// returns a string containing the type that is returned when this lambda is executed
	virtual std :: string getOutputType () = 0;

	virtual ~GenericLambdaObject () {}
};

// this is the lamda type... queries are written by supplying code that
// creates these objects
template <typename Out>
class TypedLambdaObject : public GenericLambdaObject {

	public:

		std :: string getOutputType () override {
			return getTypeName <Out> ();
		}

		virtual ~TypedLambdaObject () {}
};

}

#endif
