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

#ifndef LDA_DOC_WORD_TOPIC_ASSIGNMENT_H
#define LDA_DOC_WORD_TOPIC_ASSIGNMENT_H

#include "Object.h"
#include "PDBVector.h"
#include "Handle.h"

// By Shangyu

using namespace pdb;
using namespace pdb;

class LDADocWordTopicAssignment : public Object {

private:

        int docID;
        int wordID;
	Vector<int> topicAssignment;

public:

	ENABLE_DEEP_COPY

        LDADocWordTopicAssignment () {}

	
	LDADocWordTopicAssignment (int fromDoc, int fromWord, Handle<Vector<int>>& fromAssignment) {
		this->docID = fromDoc;
		this->wordID = fromWord;
		this->topicAssignment = *fromAssignment;
	}
	

	/*	
	void setInt(int fromInt) {
		this->myInt = fromInt;
	}

	void setVector(Handle<Vector<double>>& fromVector) {
		this->myVector = fromVector;
	}
	*/

	int getDoc() {
		return this->docID;
	}

	int getWord() {
		return this->wordID;
	}


	
	Vector<int>& getTopicAssignment() {
		return this->topicAssignment;
	}
	

        ~LDADocWordTopicAssignment () {}

};


#endif
