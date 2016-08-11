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

#include <cstddef>
#include <iostream>
#include <vector>
#include <algorithm>
#include <iterator>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "Allocator.h"
#include "Handle.h"
#include "InterfaceFunctions.h"
#include "Object.h" 
#include "Record.h"  
#include "RefCountedObject.h" 
#include "PDBVector.h"
#include "PDBString.h"

Allocator allocator;

class Employee : public Object {

private:

	int height;
	int numYears;

public:

	~Employee () {}

	Employee  () {}

	Employee (int hightIn, int yrsIn) {
		height = hightIn;
		numYears = yrsIn;
	}

	void print () {
		std :: cout << "height: " << height << "; numYears " << numYears;
	}

	int getHeight () {
		return height;
	}
};

class Supervisor : public Object {

private:

	int salary;
	double weight;
	Handle <Employee> myEmployee;

public:

	Supervisor () {}

	Supervisor (int salIn, double wgtIn, Handle <Employee> &myEmp) {
		salary = salIn;
		weight = wgtIn;
		myEmployee = myEmp;
	}

	void print () {
		std :: cout << "sal: " << salary << "; weight: " << weight;
		std :: cout << "\nmyEmployee is: ";
		myEmployee->print ();
		std :: cout << "\n";
	}

	Handle <Employee> &getEmp () {
		return myEmployee;
	}
};

class WorkPlace : public Object {

private:

	Handle <Supervisor> sup;
	Handle <Employee> emp;
	
public:

	WorkPlace () {}

	void print () {
		std :: cout << "sup: ";
		sup->print ();
		std :: cout << "emp: ";
		emp->print ();
	}

	void assign (Handle <Supervisor> &supIn) {

		sup = supIn;
		emp = sup->getEmp ();
	}
};

int main () {

	// load up the allocator with RAM
	makeObjectAllocatorBlock (24, false);

	// get a handle to it
	std :: cout << "Getting a handle to the employee.\n";
	Handle <Employee> myEmp = makeObject <Employee> (100, 120);
	if (myEmp == nullptr) {
		std :: cout << "Correctly got not enough space to do allocation.\n";
	}

	// load up the allocator with RAM again
	makeObjectAllocatorBlock (24, true);

	// get a handle to it
	std :: cout << "Getting a handle to the employee.\n";
	try {
		Handle <Employee> myEmp = makeObject <Employee> (100, 120);
	} catch (NotEnoughSpace &e) {
		std :: cout << "Correctly got an exception.\n";
	}

	// load up the allocator with RAM a third time
	makeObjectAllocatorBlock (1024, false);

	myEmp = makeObject <Employee> (100, 120);
	Handle <Supervisor> left = makeObject <Supervisor> (431, 687.4, myEmp);
	Handle <Supervisor> right = makeObject <Supervisor> (134, 124.5, myEmp);

	std :: cout << "Getting a handle to the employee.\n";
	myEmp = makeObject <Employee> (100, 120);
	std :: cout << "Height is: " << myEmp->getHeight () << "\n";
	myEmp->print ();
	
	makeObjectAllocatorBlock (1024, false);
	
	std :: cout << "Getting another handle to the employee.\n";
	Handle <Employee> myEmp22 = makeObject <Employee> (1000, 1200);
	std :: cout << "Height is: " << myEmp22->getHeight () << "\n";
	myEmp22 = myEmp;

	makeObjectAllocatorBlock (8192, true);

	// make an object
	std :: cout << "\nMaking a supervisor and adding the emp to him.\n";
	Handle <Supervisor> mySup = makeObject <Supervisor> (134, 124.5, myEmp);
	mySup = makeObject <Supervisor> (134, 124.5, myEmp);
	mySup->print ();

	// and another
	std :: cout << "Making a workplace.\n";
	Handle <WorkPlace> myWorkplace = makeObject <WorkPlace> ();
	mySup->print ();

	std :: cout << "Doing the assign.\n";
	myWorkplace->assign (mySup);
	myWorkplace->print ();

	std :: cout << "\nHere is what I have.\n";
	mySup->print ();

	// now, use a user-supplied block
	std :: cout << "\nUsing user-supplied block.\n";
	{
		void *resultPage = malloc (1024);
		makeObjectAllocatorBlock (resultPage, 1024, true);
		Handle <Employee> myEmp = makeObject <Employee> (100, 120);
		Handle <Supervisor> boss = makeObject <Supervisor> (134, 124.5, myEmp);	
		makeObjectAllocatorBlock (8192, true);
		boss->print ();
		free (resultPage);
	}
	
	Handle <Object> foo = makeObject <Supervisor> (134, 124.5, myEmp);
	foo = mySup;

	std :: cout << "\nTesting the copy assignment.\n";
	Supervisor temp (134, 124.5, myEmp);
	mySup = getHandle <Supervisor> (temp);
	mySup->print ();

	Handle <Vector <int>> foo22 = makeObject <Vector <int>> (10);
	for (int i = 0; i < 100; i++) {
		foo22->push_back (i);
	}

	for (int i = 0; i < 100; i++) {
		std :: cout << (*foo22)[i] << " ";
	}
	std :: cout << "\n";

	Handle <Vector <Handle <Supervisor>>> foo23 = makeObject <Vector <Handle <Supervisor>>> (10);
	for (int i = 0; i < 10; i++) {
		if (i % 2) {
			foo23->push_back (left);
		} else {
			foo23->push_back (right);
		}
	}

	for (int i = 0; i < 10; i++) {
		(*foo23)[i]->print ();
	}

	makeObjectAllocatorBlock (1024 * 1204 * 128, false);
	for (int i = 0; i < 100; i++) {
		Handle <Object> foo = makeObject <Supervisor> (134, 124.5, myEmp);
	}

	std :: cout << "Making 100 objects, each on a different allocator.\n";
	for (int i = 0; i < 100; i++) {

		Handle <Object> foo = makeObject <Supervisor> (134, 124.5, myEmp);
		makeObjectAllocatorBlock (1024 * 1024, true);
	}

	// Create an object 
	myEmp = makeObject <Employee> (10000, 12000);
	Handle <Supervisor> foober = makeObject <Supervisor> (13444, 124.567, myEmp);
	
	// Use getRecord to get the raw bytes of the object 
	Record <Supervisor> *myBytes = getRecord <Supervisor> (foober);

	// Write the object to a file
	int filedesc = open ("testfile", O_CREAT | O_WRONLY | O_APPEND, S_IRUSR | S_IWUSR);
	write (filedesc, myBytes, myBytes->numBytes ());
	close (filedesc);

	// Then, at a later time, get the object back 
	filedesc = open ("testfile", O_RDONLY);
	Record <Supervisor> *myNewBytes = (Record <Supervisor> *) malloc (myBytes->numBytes ());
	read (filedesc, myNewBytes, myBytes->numBytes ());
	Handle <Supervisor> barbo = myNewBytes->getRootObject ();
	barbo->print ();	

	makeObjectAllocatorBlock (1024 * 128, true);
	int i;
	try {
		// put a lot of copies of it into a vector
		std :: cout << "Adding a lot of copies of restored spervisor to the vector.\n";
		Handle <Vector <Handle <Supervisor>>> supers = makeObject <Vector <Handle <Supervisor>>> (10);
		for (int i = 0; i < 1024 * 1024 * 128; i++) {
			supers->push_back (barbo);
		}

	} catch (NotEnoughSpace &e) {
		std :: cout << "Finally, after " << i << " inserts, ran out of RAM for the objects.\n";
	}

	// delete the memory we allocated
	free (myNewBytes);

	// ****************** Jia's code ********************//
	// set up the naive storage contexts
        #define PAGE_RAW_SIZE (65536 - 512)
        char * page = (char *) malloc (PAGE_RAW_SIZE * sizeof(char));
        char * resultPage = (char *) malloc (PAGE_RAW_SIZE * sizeof(char));

	// load up the allocator with RAM
	makeObjectAllocatorBlock (PAGE_RAW_SIZE, true);

	// create a vector of Supervisor objects
	Handle <Vector <Handle <Supervisor>>> supervisors = makeObject <Vector <Handle <Supervisor>>> (10);

	try {
		//to create as much data as possible
		while (1) {
			Handle <Employee> myEmp = makeObject <Employee> (100, 120);
			Handle <Supervisor> boss = makeObject <Supervisor> (134, 124.5, myEmp);
			supervisors->push_back(boss);
		}
	} catch (NotEnoughSpace &e) {

		//simulate sending data to a storage page
		std :: cout << "to send a page to storage...\n";
		Record<Vector<Handle<Supervisor>>> * recordBytes = getRecord(supervisors);
		memcpy(page, (char *)recordBytes, recordBytes->numBytes());
		std :: cout << "page is stored.\n";
	}


	std :: cout << "Are " << getBytesAvailableInCurrentAllocatorBlock () << " free bytes in current allocation block.\n";
	std :: cout << "Are " << getNumObjectsInCurrentAllocatorBlock () << " individual objects in the current block.\n";
	std :: cout << "Now I am going to render all of those unreachable via an assign to a nullptr.\n";
	supervisors = nullptr;
	std :: cout << "Are " << getNumObjectsInCurrentAllocatorBlock () << " individual objects in the current block.\n";
	std :: cout << "Are " << getNumObjectsInHomeAllocatorBlock (myEmp) << " individual objects in myEmp's block.\n";
	std :: cout << "Now I am going to assign myEmP to a nullptr.\n";
	myEmp = nullptr;
	std :: cout << "Are " << getNumObjectsInHomeAllocatorBlock (myEmp) << " individual objects in myEmp's block.\n";

	free (page);
	free (resultPage);

	makeObjectAllocatorBlock (PAGE_RAW_SIZE, true);
	Handle <String> myString = makeObject <String> ("this is my string.");
	std :: cout << *myString << "\n";
	*myString = "Oh my goodness, I love strings so much.";	
	std :: cout << *myString << "\n";
	*myString = "Oh my goodness, goggo.";	
	std :: cout << *myString << "\n";
}
