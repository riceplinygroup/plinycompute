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
#ifndef QUERY_GRAPH_ANALYZER_HEADER
#define QUERY_GRAPH_ANALYZER_HEADER

//by Jia, Mar 2017

#include "Handle.h"
#include "Computation.h"
#include "InputTupleSetSpecifier.h"
#include "PDBVector.h"
#include <vector>

namespace pdb {


/*
 * This class encapsulates the analyzer to user query graph
 * The user query graph should not have loops
 */
class QueryGraphAnalyzer {

public:

    //constructor
    QueryGraphAnalyzer ( std :: vector <Handle<Computation>>& queryGraph );

    //constructor
    QueryGraphAnalyzer ( Handle<Vector<Handle<Computation>>> queryGraph );

    //to convert user query to a tcap string
    std :: string parseTCAPString();

    //to traverse the sub-tree and put each traversed computation to a vector
    void parseComputations ( std :: vector <Handle<Computation>> & computations, Handle<Computation> sink);

    //to convert user query to a pdb::Vector of computations
    //this method will invoke makeObject, but will not allocate allocation blocks
    //you must ensure current allocation block has sufficient memory before invoking this method
    void parseComputations ( std :: vector <Handle<Computation>> & computations);

    //to traverse from a graph sink recursively
    void traverse(std :: vector<std :: string> & tcapStrings, Handle<Computation> sink, std :: vector <InputTupleSetSpecifier>  inputTupleSets, int & computationLabel, std :: string & outputTupleSetName, std :: vector<std :: string> & outputColumnNames, std :: string & addedOutputColumnName);

    //to clear traversal marks on the subtree rooted at sink
    void clearGraphMarks (Handle<Computation> sink);

    //to clear all traversal marks
    void clearGraphMarks();

private:

    //user query graph
    std :: vector <Handle<Computation>> queryGraph;



};




}



#endif
