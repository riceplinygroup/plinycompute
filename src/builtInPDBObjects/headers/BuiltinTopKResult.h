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
#ifndef BUILTIN_TOPK_RESULT_H
#define BUILTIN_TOPK_RESULT_H

//PRELOAD %BuiltinTopKResult%

#ifndef K
    #define K 100
#endif

#include "Object.h"
#include "PDBVector.h"
#include <float.h>
#include "BuiltinTopKInput.h"

namespace pdb {

/* A class to wrap Top K (K-largest) values obtained so far */
/* This could be used to compute and represent partial result for each thread */
/* This could also be used to compute and represent the final aggregated result */

class BuiltinTopKResult : public Object {

public:

   ENABLE_DEEP_COPY

   BuiltinTopKResult() {}
   ~BuiltinTopKResult() {
       topK = nullptr;
   }

   void initialize() {

       topK = makeObject<Vector<Handle<BuiltinTopKInput>>>(K);
       minIndex = -1;
       minScore = DBL_MAX;

   }

   void reset() {

       int i;
       for (i=0; i<topK->size(); i++) {
          (*topK)[i]=nullptr;
       }
       minIndex = -1;
       minScore = DBL_MAX;
   }

   //TODO: to use comparator instead of ">="
   void updateTopK (double score, Handle<Object> element) {
       double curScore = score;
       size_t curSize = topK->size();
       //we do not have enough elements
       if (curSize < K) {
           Handle<BuiltinTopKInput> newTopKElement = makeObject<BuiltinTopKInput>(curScore, element);
           topK->push_back(newTopKElement);
           PDB_COUT << "add an element to topK with score="<< curScore << std::endl;
           if(curScore < minScore) {
                minIndex = curSize;
                minScore = curScore;
           }
           return;
       }
       //we have enought elements, if minScore is smaller than the cutoff value, we do nothing 
       if (curScore > minScore) {
           //Step 1. replace the minIndex using the new element
           PDB_COUT << "add an element to topK with score="<< curScore << std::endl;
           (*topK)[minIndex] = makeObject<BuiltinTopKInput>(score, element);
           //Step 2. update minIndex and minScore
           size_t i;
           //Step 2.1 re-initialize 
           minIndex = -1;
           minScore = DBL_MAX;
           double score;
           //Step 2.2 to find minIndex and minScore
           for (i = 0; i < topK->size(); i++) {
              if ((score = (*topK)[i]->getScore()) < minScore) {
                   minIndex = i;
                   minScore = score;
              } 
           }
       }
   }


   Handle<Vector<Handle<BuiltinTopKInput>>>& getTopK() {

       return topK;

   }

   

private:

   Handle<Vector<Handle<BuiltinTopKInput>>> topK;//using Handle to make deepCopy easy
   int minIndex;
   double minScore;

};

}

#endif
