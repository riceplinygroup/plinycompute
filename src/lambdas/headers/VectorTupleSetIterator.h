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

#ifndef VECTOR_TUPLESET_ITER_H
#define VECTOR_TUPLESET_ITER_H

namespace pdb {

// this class iterates over an input pdb :: Vector, breaking it up into a series of TupleSet objects
class VectorTupleSetIterator : public ComputeSource {

private:

	// function to call to get another vector to process
	std :: function <void * ()> getAnotherVector;

	// function to call to free the vector
	std :: function <void (void *)> doneWithVector;

	// this is the vector to process
	Handle <Vector <Handle <Object>>> iterateOverMe;

	// the pointer to the current page holding the vector, and the last page that we previously processed
	Record <Vector <Handle <Object>>> *myRec, *lastRec;

	// how many objects to put into a chunk
	size_t chunkSize;

	// where we are in the chunk
	size_t pos;	

	// and the tuple set we return
	TupleSetPtr output;

public:

	// the first param is a callback function that the iterator will call in order to obtain the page holding the next vector to iterate
	// over.  The secomd param is a callback that the iterator will call when the specified page is done being processed and can be
	// freed.  The third param tells us how many objects to put into a tuple set
	VectorTupleSetIterator (std :: function <void * ()> getAnotherVector, 
		std :: function <void (void *)> doneWithVector, size_t chunkSize) : 
		getAnotherVector (getAnotherVector), doneWithVector (doneWithVector), chunkSize (chunkSize) {

		// create the tuple set that we'll return during iteration
		output = std :: make_shared <TupleSet> ();

		// extract the vector from the input page
		myRec = (Record <Vector <Handle <Object>>> *) getAnotherVector ();
		iterateOverMe = myRec->getRootObject ();

		// create the output vector and put it into the tuple set
		std :: vector <Handle <Object>> *inputColumn = new std :: vector <Handle <Object>>;
		output->addColumn (0, inputColumn, true); 

		// we are at position zero
		pos = 0;

		// and we have no data so far
		lastRec = nullptr;
	}

	// returns the next tuple set to process, or nullptr if there is not one to process
	TupleSetPtr getNextTupleSet () override {

		// if we made it here with lastRec being a valid pointer, then it means
		// that we have gone through an entire cycle, and so all of the data that
		// we will ever reference stored in lastRec has been fluhhed through the
		// pipeline; hence, we can kill it
		if (lastRec != nullptr) {
			doneWithVector (lastRec);
			lastRec = nullptr;
		}

		// see if there are no more items in the vector to iterate over
		if (pos == iterateOverMe->size ()) {

			// this means that we got to the end of the vector
			lastRec = myRec;

			// try to get another vector
			myRec = (Record <Vector <Handle <Object>>> *) getAnotherVector ();

			// if we could not, then we are outta here
			if (myRec == nullptr)
				return nullptr;

			// and reset everything
			iterateOverMe = myRec->getRootObject ();
			pos = 0;
		}

		// compute how many slots in the output vector we can fill
		int numSlotsToIterate = chunkSize;
		if (numSlotsToIterate + pos > iterateOverMe->size ()) {
			numSlotsToIterate = iterateOverMe->size () - pos;
		}

		// resize the output vector as appropriate
		std :: vector <Handle <Object>> &inputColumn = output->getColumn <Handle <Object>> (0);
		inputColumn.resize (numSlotsToIterate);

		// fill it up
		for (int i = 0; i < numSlotsToIterate; i++) {
			inputColumn[i] = (*iterateOverMe)[pos];	
			pos++;
		}

		// and return the output TupleSet
		return output;
	}	

	~VectorTupleSetIterator () {
		
		// if lastRec is not a nullptr, then it means that we have not yet freed it
		if (lastRec != nullptr)
			doneWithVector (lastRec);
		lastRec = nullptr;
	}
};

}

#endif
