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
#ifndef PDB_QUERYIRPROCESSOR_SELECTIONIRPAGEPROCESSOR_H
#define PDB_QUERYIRPROCESSOR_SELECTIONIRPAGEPROCESSOR_H

#include "Handle.h"
#include "SelectionIr.h"
#include "PDBVector.h"
#include "UseTemporaryAllocationBlock.h"

using std::function;

using pdb::Handle;
using pdb::makeObject;
using pdb::NotEnoughSpace;
using pdb::UseTemporaryAllocationBlock;
using pdb::UseTemporaryAllocationBlockPtr;
using pdb::Vector;


namespace pdb_detail
{

    template<typename Input, typename Output>
    class SelectionIrPageProcessor
    {

    public:

        SelectionIrPageProcessor(Handle <Vector <Handle <Input>>> inputRecords, void* outputPageLocation, size_t numBytesInOutputPage,
                                 Handle<RecordPredicateIr> condition)
            : _inputRecords(inputRecords)
        {
            _condition = condition->toLambda(_inputObject).getFunc();
            setOutputLocation(outputPageLocation, numBytesInOutputPage);

        }

        void setOutputLocation(void* outputPageLocation, size_t numBytesInPage)
        {
            // kill the old allocation block
            _outputAllocationBlock = nullptr;

            // create the new one
            _outputAllocationBlock = std :: make_shared <UseTemporaryAllocationBlock> (outputPageLocation, numBytesInPage);

            // and here's where we write the ouput to
            _outputRecordsAccum = Vector <Handle <Output>>(10);
        }


        bool process()
        {
           // Vector <Handle <Input>> &myInVec = *(inputVec);
          //  Vector <Handle <Output>> &myOutVec = *(outputVec);

//            // if we are finalized, see if there are some left over records
//            if (finalized) {
//                getRecord (outputVec);
//                return false;
//            }

            // we are not finalized, so process the page
            try
            {
                int vecSize = _inputRecords->size();
                for (; _nextUnprocessedInputIndex < vecSize; _nextUnprocessedInputIndex++)
                {
                    _inputObject = _inputRecords->operator[](_nextUnprocessedInputIndex);

                    if (_condition)
                        _outputRecordsAccum.push_back (_inputObject);

                }

                return true;

            }
            catch (NotEnoughSpace &n)
            {
              //  getRecord (outputVec);
                return false;
            }
        }

    private:

        // this is where we write the results
        UseTemporaryAllocationBlockPtr _outputAllocationBlock = nullptr;

        Handle <Input> _inputObject;

        function<bool()> _condition;

        Handle<Vector<Handle<Output>>> _inputRecords;

        Vector<Handle<Output>> _outputRecordsAccum;

        uint32_t _nextUnprocessedInputIndex = 0;

    };
}

#endif //PDB_QUERYIRPROCESSOR_SELECTIONIRPAGEPROCESSOR_H
