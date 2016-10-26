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
 * File:   PageIterator.h
 * Author: Jia
 *
 * Created on October 14, 2015, 10:18 AM
 */

#ifndef PAGEITERATOR_H
#define	PAGEITERATOR_H

#include "PDBPage.h"

#include <memory>
using namespace std;

class PageIteratorInterface;
typedef shared_ptr<PageIteratorInterface> PageIteratorPtr;

/**
 * There can be a lot of different containers to store pages.
 * Like file, input buffer, receive buffer, cache and etc.
 * This class wraps a unified interface for implementing various page iterators.
 */
class PageIteratorInterface {

public:
	/*
	 * To support polymorphism.
	 */
	virtual ~PageIteratorInterface() {};

	/**
	 * To return the next page. If there is no more page, return nullptr.
	 */
    virtual PDBPagePtr next() = 0;

    /**
     * If there is more page, return true, otherwise return false.
     */
    virtual bool hasNext() = 0;

   


};



#endif	/* PAGEITERATOR_H */

