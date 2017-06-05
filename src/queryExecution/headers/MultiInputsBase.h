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
#ifndef MULTI_INPUTS_BASE
#define MULTI_INPUTS_BASE

//by Jia, May 2017

namespace pdb {


//this class represents all inputs for a multi-input computation like Join
//this is used in query graph analysis
class MultiInputsBase {

private:

       //tuple set names for each input
       std :: vector < std :: string> tupleSetNamesForInputs;

       //input columns for each input
       std :: vector < std :: vector < std :: string >> inputColumnsForInputs;

       //input column to apply for each input
       std :: vector < std :: vector <std :: string>> inputColumnsToApplyForInputs;

       //lambda names to extract for each input and each predicate
       std :: vector <std :: map <std :: string, std :: vector <std :: string>>> lambdaNamesForInputs;


       //input names for this join operation
       std :: vector <std :: string> inputNames;

       //input names show up in join predicates
       std :: vector <std :: string> inputNamesInPredicates;

       //input names show up in join projection
       std :: vector <std :: string> inputNamesInProjection;


       int numPredicates;


       int numInputs;

public:

        void setNumInputs (int numInputs) {
            this->numInputs = numInputs;
        }

        int getNumInputs () {
            return this->numInputs;
        }

        void setNumPredicates (int numPredicates) {
            this->numPredicates = numPredicates;
        }

        int getNumPredicates () {
            return this->numPredicates;
        }

        // returns the latest tuple set name that contains the i-th input
        std :: string getTupleSetNameForIthInput (int i)  {
                if (i >= this->getNumInputs()) {
                    return "";
                }
                return tupleSetNamesForInputs[i];
        }

        // set the latest tuple set name that contains the i-th input
        void setTupleSetNameForIthInput (int i, std::string name)  {
                int numInputs = this->getNumInputs();
                if (tupleSetNamesForInputs.size() != numInputs) {
                    tupleSetNamesForInputs.resize(numInputs);
                }
                if (i < numInputs) {
                     tupleSetNamesForInputs[i] = name;
                }
        }

        // get latest input columns for the tupleset for the i-th input
        std :: vector < std :: string> getInputColumnsForIthInput (int i)  {
                if (i >= this->getNumInputs()) {
                    std :: vector<std :: string> ret;
                    return ret;
                }
                return inputColumnsForInputs[i];
        }

        // set latest input columns for the tupleset for the i-th input
        void setInputColumnsForIthInput (int i, std :: vector < std :: string> & columns)  {
                int numInputs = this->getNumInputs();
                if ( inputColumnsForInputs.size() != numInputs) {
                    inputColumnsForInputs.resize(numInputs);
                }
                if (i < numInputs) {
                    inputColumnsForInputs[i] = columns;
                }
        }

        // get latest input column to apply for the tupleset for the i-th input
        std :: vector <std :: string> getInputColumnsToApplyForIthInput (int i)  {
                if ( i >= this->getNumInputs()) {
                    std :: vector <std :: string> ret;
                    return ret;
                }
                return inputColumnsToApplyForInputs[i];

        }



        // set latest input column to apply for the tupleset for the i-th input
        void setInputColumnsToApplyForIthInput (int i, std :: vector < std :: string > & columnsToApply)  {
                int numInputs = this->getNumInputs();
                if (inputColumnsToApplyForInputs.size() != numInputs) {
                    inputColumnsToApplyForInputs.resize(numInputs);
                }
                if (i < numInputs) {
                    inputColumnsToApplyForInputs[i] = columnsToApply;
                }
        }

        // set latest input column to apply for the tupleset for the i-th input
        void setInputColumnsToApplyForIthInput (int i, std :: string columnToApply)  {
                int numInputs = this->getNumInputs();
                if (inputColumnsToApplyForInputs.size() != numInputs) {
                    inputColumnsToApplyForInputs.resize(numInputs);
                }
                if (i < numInputs) {
                    inputColumnsToApplyForInputs[i].clear();
                    inputColumnsToApplyForInputs[i].push_back(columnToApply);
                }
        }

        // get lambdas to extract for the i-th input, and j-th predicate
        std :: vector<std :: string> getLambdasForIthInputAndPredicate (int i, std :: string predicateLambda) {
                if ( i >= this->getNumInputs()) {
                    std :: vector <std :: string> ret;
                    return ret;
                }
                return lambdaNamesForInputs[i][predicateLambda];
        }

        //set lambdas for the i-th input, and j-th predicate
        void setLambdasForIthInputAndPredicate (int i, std :: string predicateLambda, std::string lambdaName) {
               int numInputs = this->getNumInputs();
               if (lambdaNamesForInputs.size() != numInputs) {
                   lambdaNamesForInputs.resize(numInputs);
               }
               if (i < numInputs) {
                   lambdaNamesForInputs[i][predicateLambda].push_back(lambdaName);
               }    
        }

        //get the name for the i-th input
        std :: string getNameForIthInput(int i) {
               if (i >= this->getNumInputs()) {
                   return "";
               }
               return inputNames[i];
        }


        //set the name for the i-th input
        void setNameForIthInput(int i, std :: string name) {
               int numInputs = this->getNumInputs();
               if (inputNames.size() != numInputs) {
                   inputNames.resize(numInputs);
               }
               if (i < numInputs) {
                   inputNames[i] = name;
               }
        }

        //get the input names in predicates
        std :: vector <std :: string> getInputsInPredicates() {
               return this->inputNamesInPredicates;
        }


        //add name to names in predicates
        void addInputNameToPredicates(std :: string name) {

               auto iter = std :: find(inputNamesInPredicates.begin(), inputNamesInPredicates.end(), name);
               if (iter == inputNamesInPredicates.end()) {
                    inputNamesInPredicates.push_back(name);
               }
        } 

        //get the input names in projection
        std :: vector <std :: string> getInputsInProjection () {
               return inputNamesInProjection;
        }


        //set the name for the i-th input
        void addInputNameToProjection(std :: string name) {

               auto iter = std :: find(inputNamesInProjection.begin(), inputNamesInProjection.end(), name);
               if (iter == inputNamesInProjection.end()) {
                   inputNamesInProjection.push_back(name);
               }

        }


        //get a vector of inputs that are not in join predicates
        std :: vector < std :: string > getNamesNotInPredicates() {
               std :: vector <std :: string> ret;
               for (int i = 0; i < inputNames.size(); i++) {
                       auto iter = std :: find (inputNamesInPredicates.begin(), inputNamesInPredicates.end(), inputNames[i]);
                       if (iter == inputNamesInPredicates.end()) {
                           ret.push_back(inputNames[i]);
                       }
               }
               return ret;
        }

        //get a vector of inputs that are not in projection
        std :: vector < std :: string > getNamesNotInProjection() {
               std :: vector <std :: string> ret;
               for (int i = 0; i < inputNames.size(); i++) {
                       auto iter = std :: find (inputNamesInProjection.begin(), inputNamesInProjection.end(), inputNames[i]);
                       if (iter == inputNamesInProjection.end()) {
                           ret.push_back(inputNames[i]);
                       }
               }
               return ret;
        }
        
        //check whether an input name is in projection
        bool isInputInProjection(std :: string name) {
             auto iter = std :: find(inputNamesInProjection.begin(), inputNamesInProjection.end(), name);
             if (iter != inputNamesInProjection.end()) {
                 return true;
             } else {
                 return false;
             }
        }
};

}

#endif
