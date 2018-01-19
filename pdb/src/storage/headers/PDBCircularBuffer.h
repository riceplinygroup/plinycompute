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
 * File:   PDBCircularBuffer.h
 * Author: Jia
 *
 * Created on November 17, 2015, 11:27 AM
 */

#ifndef PDBCIRCULARBUFFER_H
#define PDBCIRCULARBUFFER_H


#include "PDBLogger.h"

template <class T>
class PDBCircularBuffer {
public:
    PDBCircularBuffer(unsigned int bufferSize, pdb::PDBLoggerPtr logger);
    ~PDBCircularBuffer();

    int addToTail(T const&);
    T popFromHead();
    bool isFull();
    bool isEmpty();
    unsigned int getSize();

protected:
    int initArray();

private:
    T* array;
    pdb::PDBLoggerPtr logger;
    unsigned int maxArraySize;
    unsigned int arrayHead;
    unsigned int arrayTail;
};


#endif /* PDBCIRCULARBUFFER_H */
