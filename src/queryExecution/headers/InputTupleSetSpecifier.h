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
#ifndef INPUT_TUPLESET_SPECIFIER_HEADER
#define INPUT_TUPLESET_SPECIFIER_HEADER

// by Jia, Mar 2017

namespace pdb {

class InputTupleSetSpecifier {

public:
    // default constructor
    InputTupleSetSpecifier() {
        this->tupleSetName = "";
    }


    // constructor
    InputTupleSetSpecifier(std::string tupleSetName,
                           std::vector<std::string> columnNamesToKeep,
                           std::vector<std::string> columnNamesToApply) {

        this->tupleSetName = tupleSetName;

        for (int i = 0; i < columnNamesToKeep.size(); i++) {
            this->columnNamesToKeep.push_back(columnNamesToKeep[i]);
        }
        for (int i = 0; i < columnNamesToApply.size(); i++) {
            this->columnNamesToApply.push_back(columnNamesToApply[i]);
        }
    }

    // destructor
    ~InputTupleSetSpecifier() {}

    // return tuple set name
    std::string& getTupleSetName() {
        return this->tupleSetName;
    }

    // return column names to keep in the output

    std::vector<std::string>& getColumnNamesToKeep() {
        return this->columnNamesToKeep;
    }

    // return column names to apply a lambda

    std::vector<std::string>& getColumnNamesToApply() {
        return this->columnNamesToApply;
    }

    // print

    void print() {

        std::cout << "TupleSetName:" << tupleSetName << std::endl;
        std::cout << "Columns to keep in output:" << std::endl;
        for (int i = 0; i < columnNamesToKeep.size(); i++) {
            std::cout << columnNamesToKeep[i] << std::endl;
        }
        std::cout << "Columns to apply:" << std::endl;
        for (int i = 0; i < columnNamesToApply.size(); i++) {
            std::cout << columnNamesToApply[i] << std::endl;
        }
    }

    void clear() {
        this->columnNamesToKeep.clear();
        this->columnNamesToApply.clear();
    }

private:
    // name of the the tuple set
    std::string tupleSetName;

    // column names in the tuple set to keep in the output
    std::vector<std::string> columnNamesToKeep;

    // column names in the tuple set to apply
    std::vector<std::string> columnNamesToApply;
};
}

#endif
