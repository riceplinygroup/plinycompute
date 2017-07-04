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
#ifndef StarsIn_H
#define StarsIn_H

#include "Object.h"
#include "Handle.h"
#include "PDBString.h"

class StarsIn: public pdb::Object {

public:
	
	pdb::Handle<pdb::String> movieTitle;
	pdb::Handle<pdb::String> starName;
	int movieYear;
	
	ENABLE_DEEP_COPY

	// Default constructor and destructor:
	~StarsIn() {
	}
	StarsIn() {}
	

	// Constructor with arguments using std::string
	StarsIn(std:: string movieTitle, std:: string starName, int movieYear) {
		this->movieTitle = pdb::makeObject <pdb::String> (movieTitle);
		this->starName= pdb::makeObject <pdb::String>(starName);
		this->movieYear = movieYear;
	}
	
	
};

#endif