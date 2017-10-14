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
#ifndef SUPPLIER_INFO_H
#define SUPPLIER_INFO_H

#include "Object.h"
#include "PDBVector.h"
#include "PDBString.h"
#include "Handle.h"
#include "PDBMap.h"

using namespace pdb;

class SupplierInfo : public pdb::Object {

public:
    Handle<Map<String, Vector<int>>> soldPartIDs = nullptr;

    String supplierName;

    String tempCustomer;

    int tempPart;

    ENABLE_DEEP_COPY

    // Default constructor:
    SupplierInfo() {}

    // Default destructor:
    ~SupplierInfo() {}

    // Constructor with arguments:
    SupplierInfo(String supplierName, String tempCustomer, int tempPart) {
        this->supplierName = supplierName;
        this->tempCustomer = tempCustomer;
        this->tempPart = tempPart;
        this->soldPartIDs = nullptr;
    }

    String& getKey() {
        return this->supplierName;
    }

    SupplierInfo& getValue() {
        return *this;
    }

    SupplierInfo& operator+(SupplierInfo& addMeIn) {
        if (soldPartIDs == nullptr) {
            soldPartIDs = makeObject<Map<String, Vector<int>>>();
        }
        if (addMeIn.soldPartIDs == nullptr) {
            Vector<int>& myVec = (*soldPartIDs)[addMeIn.tempCustomer];
            myVec.push_back(addMeIn.tempPart);
        } else {
            Map<String, Vector<int>>& myLhs = *soldPartIDs;
            Map<String, Vector<int>>& myRhs = *(addMeIn.soldPartIDs);
            auto iter = myRhs.begin();
            PDBMapIterator<String, Vector<int>> endIter = myRhs.end();
            while (iter != endIter) {
                String& myKey = (*iter).key;
                Vector<int>& myVec = myLhs[myKey];
                Vector<int>& otherVec = (*iter).value;
                int* myOtherData = otherVec.c_ptr();
                size_t mySize = myVec.size();
                size_t myOtherSize = otherVec.size();
                for (size_t i = mySize; i < mySize + myOtherSize; i++) {
                    try {
                        myVec.push_back(myOtherData[i - mySize]);


                    } catch (NotEnoughSpace& n) {
                        size_t curSize = myVec.size();
                        for (size_t j = mySize; j < curSize; j++) {
                            myVec.pop_back();
                        }
                        throw n;
                    }
                }
                ++iter;
            }
        }
        return *this;
    }

    int print() {
        std::cout << "SupplierName: " << supplierName << " [ ";
        if (soldPartIDs == nullptr) {
            return 0;
        }
        auto iter = soldPartIDs->begin();
        int count = 0;
        while (iter != soldPartIDs->end()) {
            pdb::String customerName = (*iter).key;
            pdb::Vector<int> partIDs = (*soldPartIDs)[customerName];
            std::cout << "Customer: " << customerName.c_str() << " (";
            for (int i = 0; i < partIDs.size(); ++i) {
                std::cout << " " << partIDs[i] << ",";
            }
            std::cout << ") ";
            ++iter;
            count++;
        }
        std::cout << "  ] " << std::endl;
        std::cout << " the count is " << count << std::endl;
        return count;
    }
};

#endif
