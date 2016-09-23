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
//
// Created by barnett on 9/21/16.
//

#ifndef PDB_QUERYIRPROCESSOR_SELECTIONIRPROCESSOR_H
#define PDB_QUERYIRPROCESSOR_SELECTIONIRPROCESSOR_H

#include "Employee.h"

using pdb::Handle;
using pdb::Vector;
using pdb::Employee;

namespace pdb_detail
{
    class SelectionIrProcessor
    {
    public:

        SelectionIrProcessor(Handle<SelectionIr> selection, size_t numBytesInPage, function<void*()> inputPageProvider,
                             function<void*()> outputPageProvider)
                : _condition(selection->getCondition()), _numBytesInPage(numBytesInPage),
                  _inputPageProvider(inputPageProvider), _outputPageProvider(outputPageProvider)
        {
        }

        SelectionIrProcessor(SelectionIr& selection, size_t numBytesInPage, function<void*()> inputPageProvider,
                             function<void*()> outputPageProvider)
                : _condition(selection.getCondition()), _numBytesInPage(numBytesInPage),
                  _inputPageProvider(inputPageProvider), _outputPageProvider(outputPageProvider)
        {
        }


        void process()
        {
            void* inputPageAddress = _inputPageProvider();

            loadNewOutputPage();

            Handle<Object> inputObject;
            function<bool()> condition = _condition->toLambda(inputObject).getFunc();

            while(inputPageAddress != nullptr)
            {
                Record <Vector <Handle <Object>>> *myRec = (Record <Vector <Handle <Object>>> *) inputPageAddress;
                Handle <Vector <Handle <Object>>> inputVec = myRec->getRootObject ();
                Vector <Handle <Object>> &myInVec = *(inputVec);

                int vecSize = myInVec.size ();
                for (uint32_t i = 0; i < vecSize; i++)
                {
                    inputObject = myInVec[i];

                    if (!condition())
                        continue;

                    bool appendToOutputSuccess = false;
                    while(!appendToOutputSuccess)
                    {
                        try
                        {
                            _outputVec->push_back(inputObject);
                            appendToOutputSuccess = true;
                        }
                        catch (NotEnoughSpace &n)
                        {

                            getRecord(_outputVec);
                            loadNewOutputPage();

                        }
                    }

                }

                inputPageAddress = _inputPageProvider();
            }
            getRecord (_outputVec);
        }

    private:

        void loadNewOutputPage()
        {
            void* outputPageAddress = _outputPageProvider();

            // kill the old allocation block
            _blockPtr = nullptr;

            // create the new one
            _blockPtr = std :: make_shared <UseTemporaryAllocationBlock> (outputPageAddress, _numBytesInPage);
            _outputVec = makeObject <Vector <Handle <Object>>> (10);
         //   _myOutVec = *(_outputVec);
        }


        UseTemporaryAllocationBlockPtr _blockPtr;

       // Vector <Handle <Output>> &_myOutVec;

        Handle <Vector <Handle <Object>>> _outputVec;

        function<void*()> _inputPageProvider;

        function<void*()> _outputPageProvider;

        Handle<RecordPredicateIr> _condition;

        size_t _numBytesInPage;

    };
}

#endif //PDB_QUERYIRPROCESSOR_SELECTIONIRPROCESSOR_H
