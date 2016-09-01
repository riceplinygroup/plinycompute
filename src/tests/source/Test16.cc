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

//Pipeline performance test by Jia
//Aug 30, 2016

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
#include <chrono>
#include <ctime>
#include "Allocator.h"
#include "Handle.h"
#include "InterfaceFunctions.h"
#include "Object.h" 
#include "Record.h"  
#include "RefCountedObject.h" 
#include "PDBVector.h"
#include "PDBString.h"
#include "MyEmployee.h"

#include <memory>
using namespace std;

class Page;
typedef shared_ptr<Page> PagePtr;

class TestDataGenerator;
typedef shared_ptr<TestDataGenerator> TestDataGeneratorPtr;

class Operator;
typedef shared_ptr<Operator> OperatorPtr;

class HalfAgeOperator;
typedef shared_ptr<HalfAgeOperator> HalfAgeOperatorPtr;

class DoubleSalaryOperator;
typedef shared_ptr<DoubleSalaryOperator> DoubleSalaryOperatorPtr;

class LinearPipeline;
typedef shared_ptr<LinearPipeline> LinearPipelinePtr;

using namespace pdb; 

Allocator allocator;

/**
 * A simple page class initialized as a vector of vectors
 */

class Page {

private:

    char * rawBytes;
    size_t size;
    Handle<Vector<Handle<Vector<Handle<Object>>>>> vectors;

public:

    void setNull () {
	vectors = nullptr;
    }

    ~Page () {
            free(rawBytes);
            rawBytes = nullptr;
    }

    Page (char * rawBytesIn, size_t sizeIn) {
            rawBytes = rawBytesIn;
            size = sizeIn;
            //to initialize the page into a vector of vectors
            //each subvector represents a batch
            makeObjectAllocatorBlock (rawBytes, size, true);
            vectors = makeObject <Vector<Handle<Vector<Handle<Object>>>>> (10);
    }

    Handle<Vector<Handle<Object>>> addVector() {
            Handle<Vector<Handle<Object>>> curVec = makeObject<Vector<Handle<Object>>> ();
            vectors->push_back (curVec);
            return curVec;
    }

    Handle<Vector<Handle<Vector<Handle<Object>>>>> getVectors() {
            return vectors;
    }

    char * getRawBytes () {
            return rawBytes;
    }
};




/**
 * The abstract operator class, it is used to construct the pipeline
 * It should be provided by the user, and invoked through shared library
 */

class Operator {

protected:

    PagePtr page;
    Handle<Vector<Handle<Object>>> outVec;

public:

    void setNull () {
	outVec = nullptr;
    }

    Operator (PagePtr pageIn) {
            page = pageIn;
    }

    void loadOutVec() {
            try {
                this->outVec = page->addVector();
            } catch (NotEnoughSpace &e) {
                 std :: cout << "Page is fully written, so we can not add a new vector!\n";
            }
    }

    Handle<Vector<Handle<Object>>> getOutVec() {
            return this->outVec;
    }

    virtual void run (Handle<Vector<Handle<Object>>> inVec) = 0;

};

/**
 * A simple operator that will multiply an employee's age by 0.5.
 */

class HalfAgeOperator : public Operator {

public:

    HalfAgeOperator (PagePtr pageIn): Operator (pageIn) {
    }

    virtual void run (Handle<Vector<Handle<Object>>> inVec) {

        try {
              int vecSize = inVec->size();
              for (int posInInput = 0; posInInput < vecSize; posInInput++) {
                      Handle<MyEmployee> inObject = unsafeCast<MyEmployee, Object>((*inVec)[posInInput]);
                      Handle<MyEmployee> outObject = makeObject<MyEmployee> (inObject->getAge()/2, inObject->getSalary());
                      this->outVec->push_back(outObject);
              }
        } catch (NotEnoughSpace &n) {
              std :: cout << "Page is fully written, so we can not half age!\n";
        }

    }

};


/**
 * A simple operator that will double an employee's salary by 2.
 */

class DoubleSalaryOperator : public Operator {

public:

    DoubleSalaryOperator (PagePtr pageIn): Operator (pageIn) {
    }

    virtual void run (Handle<Vector<Handle<Object>>> inVec) {

        try {
              int vecSize = inVec->size();
              for (int posInInput = 0; posInInput < vecSize; posInInput++) {
                      Handle<MyEmployee> inObject = unsafeCast<MyEmployee, Object>((*inVec)[posInInput]);
                      Handle<MyEmployee> outObject = makeObject<MyEmployee> (inObject->getAge(), 2*inObject->getSalary());
                      this->outVec->push_back(outObject);
              }
        } catch (NotEnoughSpace &n) {
              std :: cout << "Page is fully written, so we can not double salary!\n";
        }

    }

};



/**
 * A simple sequential pipeline implementation
 */

class LinearPipeline {

private:

    std::vector<OperatorPtr> * operators;

public:

    ~LinearPipeline() {
         delete operators;
    }

    LinearPipeline() {
         operators = new std::vector<OperatorPtr> () ;
    }

    // add operator to pipeline
    void addOperator(OperatorPtr op) {
         operators->push_back(op);
    }

    // run the pipeline on input page
    void run( PagePtr oneHugePageAsInput, int triggerOperatorId ) {
         Handle<Vector<Handle<Vector<Handle<Object>>>>>  myVectors = oneHugePageAsInput->getVectors();
         int i;
         for (i = triggerOperatorId; i < myVectors->size(); i++) {
                 
                  Handle<Vector<Handle<Object>>> inputVec = (*myVectors)[i];
                  for (int j = 0; j < operators->size(); j++) {
                         OperatorPtr op = (*operators)[j];
                         op->loadOutVec();
                         op->run (inputVec);
                         inputVec = op->getOutVec();
                  }

         }
         cout << "run " << i << " batches in total!\n";

    }

};


/**
 * A class to load data into a page
 */

class TestDataGenerator {

private:

    int batchSize;
    PagePtr page;

public:

    TestDataGenerator (int batchSizeIn, PagePtr pageIn) {
            batchSize = batchSizeIn;
            page = pageIn;
    }

    void run() {

            int count = 0;
            try {
                    while (1) {
                              Handle<Vector<Handle<Object>>> vector = page->addVector();
                              for (int i = 0; i < batchSize; i++) {
                                       
                                       Handle<MyEmployee> employee = makeObject<MyEmployee> (i%100, i*100);
                                       vector->push_back(employee);
                                       
                              }
                              count ++;
                    }            

             } catch (NotEnoughSpace &e) {
                 std :: cout << count+1 << " batches added!\n";
                 std :: cout << "Page is fully written, and let's test!\n";
             }
    }

};


int main (int argc, char * argv[]) {

       int batchSize = 100;
       if (argc != 2) {
           std :: cout << "error parameter, batch size is set to be default 100!\n";
       }
       else {
           batchSize = atoi(argv[1]);
           std :: cout << "batch size is set to be " << batchSize << std :: endl;
       }

       //load random myEmployees
       char * memoryPool = (char *) malloc (512*1024*1024*sizeof(char));
       PagePtr page = make_shared<Page>(memoryPool, 512*1024*1024);
       TestDataGeneratorPtr dataGenerator = make_shared<TestDataGenerator>(batchSize, page);
       dataGenerator->run();

       //create pipeline 
       LinearPipelinePtr pipeline = make_shared<LinearPipeline>();
       
       //create operator 1
       char * memoryPool1 = (char *) malloc (512*1024*1024*sizeof(char));
       PagePtr page1 = make_shared<Page>(memoryPool1, 512*1024*1024); //this will set the page to be current block
       HalfAgeOperatorPtr op1 = make_shared<HalfAgeOperator>(page1);
       pipeline->addOperator (op1);

       //create operator 2
       char * memoryPool2 = (char *) malloc (512*1024*1024*sizeof(char));
       PagePtr page2 = make_shared<Page>(memoryPool2, 512*1024*1024); // this will set the page to be current block
       DoubleSalaryOperatorPtr op2 = make_shared<DoubleSalaryOperator>(page2);
       pipeline->addOperator (op2);

       //create another memory pool to place intermediate data;
       char * tempMemoryPool = (char *) malloc (1024*1024*1024*sizeof(char));//this will set the page to be current block
       PagePtr tempPage = make_shared<Page>(tempMemoryPool, 1024*1024*1024);
 
       std :: cout << "running pipeline...\n";
       // for timing
       auto begin = std::chrono::high_resolution_clock::now();
       pipeline->run(page, 0);               
       auto end = std::chrono::high_resolution_clock::now();
       std::cout << "pipeline duration: " <<
                std::chrono::duration_cast<std::chrono::nanoseconds>(end-begin).count() << " ns." << std::endl;
       //TODO: how to allocate blocks for multiple operators???

       // we need to remove references to all of the Handles, which will deallocate all objects
       op1->setNull ();
       op2->setNull ();
       page1->setNull ();
       page2->setNull ();
       tempPage->setNull ();
       std :: cout << getNumObjectsInCurrentAllocatorBlock () << "\n";
}
