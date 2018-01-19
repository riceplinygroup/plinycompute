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
#ifndef TUPLESET_SPECIFIER_HEADER
#define TUPLESET_SPECIFIER_HEADER


namespace pdb {

class TupleSetSpecifier {

public:
    // default constructor
    TupleSetSpecifier() {
        this->tupleSetName = "";
    }


    // constructor
    TupleSetSpecifier(std::string tupleSetName,
                      std::vector<std::string> columnNamesToKeep,
                      std::vector<std::string> columnNamesToApply) {

        this->tupleSetName = tupleSetName;
        this->columnNamesToKeep = columnNamesToKeep;
        this->columnNamesToApply = columnNamesToApply;
    }

    // return tuple set name
    std::string getTupleSetName() {
        return tupleSetName;
    }

    // return column names to keep in the output

    std::vector<std::string> getColumnNamesToKeep() {
        return columnNamesToKeep;
    }

    // return column names to apply a lambda

    std::vector<std::string> getColumnNamesToApply() {
        return columnNamesToApply;
    }

    // set tuple set name
    void setTupleSetName(std::string tupleSetName) {
        this->tupleSetName = tupleSetName;
    }

    // set column names to keep in the output
    void setColumnsToKeep(std::vector<std::string> columnsToKeep) {
        this->columnNamesToKeep = columnsToKeep;
    }

    // set column names to apply
    void setColumnsToApply(std::vector<std::string> columnsToApply) {
        this->columnsToApply = columnsToApply;
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
