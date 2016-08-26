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


/****************************************************
** COPYRIGHT 2016, Chris Jermaine, Rice University **
**                                                 **
** The MyDB Database System, COMP 530              **
** Note that this file contains SOLUTION CODE for  **
** A1.  You should not be looking at this file     **
** unless you have completed A1!                   **
****************************************************/

#ifndef PAGE_C
#define PAGE_C

#include "MyDB_BufferManager.h"
#include "MyDB_Page.h"
#include "MyDB_Table.h"

void *MyDB_Page :: getBytes (MyDB_PagePtr me) {
	parent.access (me);	
	return bytes;
}

void MyDB_Page :: wroteBytes () {
	isDirty = true;
}

MyDB_Page :: ~MyDB_Page () {}

MyDB_Page :: MyDB_Page (MyDB_TablePtr myTableIn, size_t iin, MyDB_BufferManager &parentIn) : 
	parent (parentIn), myTable (myTableIn), pos (iin) { 
	bytes = nullptr;
	isDirty = false;	
	refCount = 0;
	timeTick = -1;
}

void MyDB_Page :: flush (MyDB_PagePtr me) {
	parent.flush (me);
}

void MyDB_Page :: killpage (MyDB_PagePtr me) {
	parent.killPage (me);
}

MyDB_BufferManager &MyDB_Page :: getParent () {
	return parent;	
}

#endif

