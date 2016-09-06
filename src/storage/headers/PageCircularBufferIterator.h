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
/* 
 * File:   PageCircularBufferIterator.h
 * Author: Jia
 *
 * Created on November 17, 2015, 10:16 AM
 */

#ifndef PAGECIRCULARBUFFERITERATOR_H
#define	PAGECIRCULARBUFFERITERATOR_H

#include "PageIterator.h"
#include "PageCircularBuffer.h"

#include <memory>
using namespace std;
class PageCircularBufferIterator;
typedef shared_ptr<PageCircularBufferIterator> PageCircularBufferIteratorPtr;

/**
 * This provides a wrapper of iterator to the page circular buffer.
 */
class PageCircularBufferIterator : public PageIteratorInterface {
public:
    PageCircularBufferIterator(unsigned int id, PageCircularBufferPtr buffer, pdb :: PDBLoggerPtr logger);
    ~PageCircularBufferIterator();

    /**
     * Return true only if the buffer is empty and closed.
     */
    bool hasNext() override;

    /**
     * Return the page at the head of the buffer.
     * It will block if the buffer is empty but not closed.
     * It will return the page from head of the buffer, if the buffer is not empty.
     * It will return nullptr, if the buffer is empty and is closed.
     */
    PDBPagePtr next() override;


    /**
     * Return the ID of this iterator.
     * The ID can be used to specify the scanning thread owning the iterator.
     */
    unsigned int getId();

private:
    PageCircularBufferPtr buffer;
    pdb :: PDBLoggerPtr logger;
    unsigned int id;
};

#endif	/* PAGECIRCULARBUFFERITERATOR_H */

