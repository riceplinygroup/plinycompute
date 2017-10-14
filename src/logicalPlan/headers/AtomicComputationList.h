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

#ifndef COMP_LIST_H
#define COMP_LIST_H

#include <iostream>
#include <memory>
#include <stdlib.h>
#include <string>
#include <utility>
#include <vector>
#include <map>

#include "AtomicComputation.h"

// this is an indexed list of computations
struct AtomicComputationList {

private:
    // a map from the name of a TupleSet to the AtomicComputation that produced it
    std::map<std::string, AtomicComputationPtr> producers;

    // a map from the name of a TupleSet to the AtomicComputation(s) that will consume it
    std::map<std::string, std::vector<AtomicComputationPtr>> consumers;

    // a list of all of the ScanSet objects
    std::vector<AtomicComputationPtr> scans;

public:
    // gets the computation that builds the tuple set with the specified name
    AtomicComputationPtr getProducingAtomicComputation(std::string outputName);

    // gets the list of comptuations that consume the tuple set with the specified name
    std::vector<AtomicComputationPtr>& getConsumingAtomicComputations(std::string inputName);

    // this effectively gets all of the leaves of the graph, since it returns all of the scans...
    // every
    // AtomicComputationPtr in the returned list will point to a ScanSet object
    std::vector<AtomicComputationPtr>& getAllScanSets();

    // add an atomic computation to the graph
    void addAtomicComputation(AtomicComputationPtr addMe);

    friend std::ostream& operator<<(std::ostream& os, const AtomicComputationList& printMe);
};

#endif
