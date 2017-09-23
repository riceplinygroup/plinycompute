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
#ifndef ALL_PARTS_H
#define ALL_PARTS_H

#include "Object.h"
#include "PDBVector.h"
#include "PDBString.h"
#include "Handle.h"
#include "Customer.h"

namespace pdb {

// this class is used to store information about the part list assocated with a give customer.
// The key of the customer is stored in custKey, and the list of unique parts purchased by that
// customer is store in the vector parts.
class AllParts : public Object {

        // the customer in question
        int custKey;

        // the list of all unique parts that this customer has purchased
        Handle <Vector <int>> parts;

public:

        ENABLE_DEEP_COPY

        // print out the information in the object
        void print () {
                std :: cout << "Customer " << custKey << " with parts ";
                for (int i = 0; i < parts->size (); i++) {
                        if (i != 0)
                                std :: cout << ", ";
                        std :: cout << (*parts)[i];
                }
        }

        // gets a list of parts as input as well as a customer, and returns the Jaccard similarity between the
        // input list and the set of parts that was purchased by this customer.  As a side effect, the cust key
        // as well as the list of all of the parts that were purchased by the customer is stored within the
        // object
        double fill (int *origList, int len, Customer &myCust) {

                // first, remember the customer key
                custKey = myCust.custKey;

                // new, we figure out how many parts there are in all
                unsigned totParts = 0;
                int orderSize = myCust.orders.size ();
                for (int i = 0; i < orderSize; i++) {
                        totParts += myCust.orders[i].lineItems.size ();
                }

                // this list will hold all of the parts
                int *allLines = new int[totParts];
                totParts = 0;

                // loop through each order
                for (int i = 0; i < orderSize; i++) {

                        // get all of the parts in this order and put them in our list
                        Vector <LineItem> &myLines = myCust.orders[i].lineItems;
                        int numLines = myLines.size ();
                        LineItem *lines = myLines.c_ptr ();
                        for (int j = 0; j < numLines; j++) {
                                allLines[totParts] = lines[j].part->partKey;
                                totParts++;
                        }
                }

                // now we sort them
                std :: sort (allLines, allLines + totParts);

                // and merge... this is how much we've merged
                int posInOrig = 0;
                int posInThis = 0;

                parts = makeObject <Vector <int>> ();
                Vector <int> &myList = *parts;
                while (true) {

                        // if we got to the end of either, break
                        if (posInThis == totParts || posInOrig == len)
                                break;

                        // first, loop to the last repeated value
                        while (posInThis + 1 < totParts && allLines[posInThis] == allLines[posInThis + 1])
                                posInThis++;

                        // next, see if the two are the same
                        if (allLines[posInThis] == origList[posInOrig]) {
                                myList.push_back (allLines[posInThis]);
                                posInThis++;
                                posInOrig++;

                        // otherwise, advance the smaller one
                        } else if (allLines[posInThis] < origList[posInOrig]) {
                                posInThis++;
                        } else {
                                posInOrig++;
                        }

                }

                // and get the number of unique items in the list of parts
                int numUnique = 0;
                posInThis = 0;
                while (true) {

                        if (posInThis == totParts)
                                break;

                        // loop to the last repeated value
                        while (posInThis + 1 < totParts && allLines[posInThis] == allLines[posInThis + 1])
                                posInThis++;

                        // saw another unique
                        numUnique++;

			// and move to the next one
			posInThis++;
                }

                // free the temp space used to store the sorted list!!
                delete [] allLines;

                // now return the computed similarity
                return myList.size () / (double) (numUnique + len - myList.size ());
        }

};

}
#endif
