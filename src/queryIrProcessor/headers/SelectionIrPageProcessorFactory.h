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
////
//// Created by barnett on 9/20/16.
////
//
//#ifndef PDB_QUERYIRPROCESSOR_SELECTIONIRPAGEPROCESSORFACTORY_H
//#define PDB_QUERYIRPROCESSOR_SELECTIONIRPAGEPROCESSORFACTORY_H
//
//#include <memory>
//
//#include "Handle.h"
//#include "SelectionIrPageProcessor.h"
//#include "PDBVector.h"
//#include "Record.h"
//
// using std::make_shared;
// using std::shared_ptr;
//
//
// using pdb::Handle;
// using pdb::Record;
// using pdb::Vector;
//
// namespace pdb_detail
//{
//    class SelectionIrPageProcessorFactory
//    {
//
//    public:
//
//        SelectionIrPageProcessorFactory(shared_ptr<SelectionIr> selection) :
//        _condition(selection->getCondition())
//        {
//        }
//
//        template <typename Input, typename Output>
//        shared_ptr<SelectionIrPageProcessor<Input,Output>> makeProcessorForPage(void* inputPage,
//                                                                                void*
//                                                                                initialOutputPage,
//                                                                                size_t
//                                                                                numBytesInOutputPage)
//        {
//            Record <Vector <Handle <Input>>>* myRec = (Record <Vector <Handle
//            <Input>>>*)inputPage;
//            Handle<Vector <Handle <Input>>> inputRecords = myRec->getRootObject();
//
//            SelectionIrPageProcessor<Input,Output>* proc =
//                new SelectionIrPageProcessor<Input, Output>(inputRecords, initialOutputPage,
//                numBytesInOutputPage,
//                                                            _condition);
//
//            return shared_ptr<SelectionIrPageProcessor<Input,Output>>(proc);
//        }
//
//
//    private:
//
//        Handle<RecordPredicateIr> _condition;
//    };
//}
//
//#endif //PDB_QUERYIRPROCESSOR_SELECTIONIRPAGEPROCESSORFACTORY_H
