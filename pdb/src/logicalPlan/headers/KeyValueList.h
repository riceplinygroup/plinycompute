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

#ifndef KEY_VALUE_LIST_H
#define KEY_VALUE_LIST_H

#include <iostream>
#include <memory>
#include <stdlib.h>
#include <string>
#include <utility>
#include <vector>
#include <map>
#include <unordered_map>
#include <memory>

// this is a list of key value pairs... used to build up schema specifications
struct KeyValueList {

private:

	std::shared_ptr<std::map <std::string, std::string>> keyValuePairs;

public:

	KeyValueList () {
		keyValuePairs = std::make_shared<std::map <std::string, std::string>>();
	}

	~KeyValueList () = default;

	void appendkeyValuePair (std::string keyName, std::string valueName) {
		(*keyValuePairs)[keyName] = valueName;
	}

	std::shared_ptr<std::map <std::string, std::string>> &getKeyValuePairs () {
		return keyValuePairs;
	}
	
	//friend struct TupleSpec;

};

#endif

