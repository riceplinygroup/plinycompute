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
#ifndef CustomerSupplierPartGroupBy_H
#define CustomerSupplierPartGroupBy_H

#include "ClusterAggregateComp.h"

#include "Lambda.h"
#include "LambdaCreationFunctions.h"

#include "PDBVector.h"
#include "PDBString.h"

#include "CustomerSupplierPart.h"
#include "CustomerSupplierPartFlat.h"
#include "SupplierData.h"



// template <class OutputClass, class InputClass, class KeyClass, class ValueClass>
class CustomerSupplierPartGroupBy : public ClusterAggregateComp <SupplierData, CustomerSupplierPartFlat, String, Handle<Map<String, Vector<int>>>> {

public:

        ENABLE_DEEP_COPY

		CustomerSupplierPartGroupBy () {}

        // the below constructor is NOT REQUIRED
        // user can also set output later by invoking the setOutput (std :: string dbName, std :: string setName)  method
        CustomerSupplierPartGroupBy (std :: string dbName, std :: string setName) {
                this->setOutput(dbName, setName);
        }

        // the key type must have == and size_t hash () defined
        Lambda <String> getKeyProjection (Handle <CustomerSupplierPartFlat> aggMe) override {
            return makeLambda (aggMe, [] (Handle <CustomerSupplierPartFlat> & aggMe) {
                      	String myKey = aggMe->getSupplierName();
                             		 return myKey;
                          });
        }

        // the value type must have + defined
        Lambda <Handle<Map<String, Vector<int>>>> getValueProjection (Handle <CustomerSupplierPartFlat> aggMe) override {
                return makeLambda (aggMe, [] (Handle <CustomerSupplierPartFlat> & aggMe) {

                    Handle<Map<String, Vector<int>>> ret = makeObject<Map<String, Vector<int>>> ();

                    Vector<int>  partKeys = aggMe->getPartKeys();

                    // inside the map key is the customerName
                    (*ret)[aggMe->getCustomerName()]=partKeys;

                    return ret;
                    });
        }
};


namespace pdb {

inline Handle<Map<String, Vector<int>>> &operator+ (Handle<Map<String, Vector<int>>> &lhs, Handle<Map<String, Vector<int>>> &rhs) {
       auto iter = rhs->begin();
       while (iter != rhs->end()) {
           String myKey = (*iter).key;
           if (lhs->count(myKey) == 0) {
               try {
                   (*lhs)[myKey] = (*iter).value;
               } catch ( NotEnoughSpace &n ) {
                   //std :: cout << "not enough space when inserting new pair" << std :: endl;
                   lhs->setUnused (myKey);
                   throw n;
               }
           } else {

                   size_t mySize = (*lhs)[myKey].size();
                   size_t otherSize = (*iter).value.size();
                   //std :: cout << "mySize is " << mySize << " and otherSize is " << otherSize << std :: endl;
                   for (size_t i = mySize; i < mySize + otherSize; i++) {
                       try {

                               (*lhs)[myKey].push_back((*iter).value[i-mySize]);

                       } catch (NotEnoughSpace &n) {

                               //std :: cout << i << ": not enough space when updating value for pushing back: " << (*lhs)[myKey].size() << std :: endl;
                               size_t curSize = (*lhs)[myKey].size();
                               for (size_t j = mySize; j < curSize; j++) {
                                    (*lhs)[myKey].pop_back();
                               }
                               //std :: cout << "size restored to " << (*lhs)[myKey].size() << std :: endl;
                               for (size_t j = 0; j < (*lhs)[myKey].size(); j++) {
                                    std :: cout << j << ": " << (*lhs)[myKey][j]<< ";";
                               }
                               throw n;

                       }

                   }
                   //std :: cout << "now my size is " << (*lhs)[myKey].size() << std :: endl;
           }
           ++iter;
       }
       return lhs;

}

}


#endif
