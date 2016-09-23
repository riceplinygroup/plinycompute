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

#ifndef QUERY_BASE_H
#define QUERY_BASE_H

#include "Object.h"
#include "PDBString.h"
#include "PDBVector.h"
#include "QueryAlgo.h"

namespace pdb {

// all queries are descended from this
class QueryBase : public Object {

public:

	QueryBase () {
		whichSet = "";
		whichDB = "";
	}

	void print () {
		std :: cout << whichDB << "." << whichSet << ": " << getQueryType () << "(" << getOutputType () << ")\n";
		for (int i = 0; i < getNumInputs (); i++) {
			if (getIthInput (i) == nullptr)
				std :: cout << "<nullptr>";
			else
				getIthInput (i)->print ();	
		}
	}

	// gets the output type of this query as a string
	virtual std :: string getOutputType () = 0;

        // gets the number of intputs to this query type
        virtual int getNumInputs () = 0;

        // gets the name of the i^th input type...
        virtual std :: string getIthInputType (int i) = 0;

	/**
     * Executes the given algorithm on the QueryRoot.
     * @param algo the algorithm to execute.
     */
	virtual void execute(QueryAlgo& algo) = 0;

	// gets the name of this particular query type ("selection", "join", etc.)
	virtual std :: string getQueryType () = 0;

	// getters/setters for the output database name and set name for this query
	void setDBName (std :: string toMe) {
		whichDB = toMe;
	}

	// used to mark this query as bad
	void setError () {
		isError = true;
	}

	void setSetName (std :: string toMe) {
		whichSet = toMe;
	}

	bool wasError () {
		return isError;
	}

	std :: string getDBName () {
		return whichDB;
	}

	std :: string getSetName () {
		return whichSet;
	}

        // gets a handle to the i^th input to this query, which is also a query
        Handle <QueryBase> &getIthInput (int i) const {
        	return (*inputs)[i];
        }
	bool hasInput()
	{
        return !inputs.isNullPtr();
	}

	// set the first slot, by default
	bool setInput (Handle <QueryBase> toMe) {
		return setInput (0, toMe);
	}

        // sets the i^th input to be the output of a specific query... returns
        // true if this is OK, false if it is not
        bool setInput (int whichSlot, Handle <QueryBase> toMe) {

                // set the array of inputs if it is a nullptr
                if (inputs == nullptr) {
                        inputs = makeObject <Vector <Handle <QueryBase>>> (getNumInputs ());
                        for (int i = 0; i < getNumInputs (); i++) {
                                inputs->push_back (nullptr);
                        }
                }

		// if we are adding this query to a valid slot
                if (whichSlot < getNumInputs ()) {

                        // make sure the output type of the guy we are accepting meets the input type
                        if (getIthInputType (whichSlot) != toMe->getOutputType ()) {
				std :: cout << "Cannot set output of query node with output of type " << toMe->getOutputType () << " to be the input";
				std :: cout << " of a query with input type " << getIthInputType (whichSlot) << ".\n";
				isError = true;
                                return false;
			}

                        (*inputs)[whichSlot] = toMe;

			// carry throgh an error
			if (toMe->isError)
				isError = true;

			// make sure that the database names match up
			if (getDBName () != "" && getDBName () != toMe->getDBName ()) {
				std :: cout << "This is bad; you seem to be combining inputs from different databases.\n";
				std :: cout << "DBs used are " << getDBName () << " and " << toMe->getDBName () << ".\n";	
				isError = true;
                                return false;
			} else {

				// if we have not yet gotten the DB name, then set it here
				setDBName (toMe->getDBName ());
			}

                        return true;
                }

                return false;
        }

private:

	// this is the name of the database/set combo where the answer of this query is stored
	String whichDB; 
	String whichSet; 
	
	// if there was an error
	bool isError = false;

	// all of the queries that are input into this one
	Handle <Vector <Handle <QueryBase>>> inputs;
};

}

#endif
