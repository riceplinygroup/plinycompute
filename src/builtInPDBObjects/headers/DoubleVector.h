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
#ifndef DOUBLE_VECTOR_H
#define DOUBLE_VECTOR_H

//by Jia, May 2017

#include "Object.h"
#include "Handle.h"
#include "PDBVector.h"

//PRELOAD %DoubleVector%


namespace pdb {

class DoubleVector: public Object {

public:
     Handle<Vector<double>> data;
     size_t size;

public:

     DoubleVector () {}

     DoubleVector ( size_t size ) {
         this->size = size;
         data = makeObject<Vector<double>> ( size, size );
     }

     ~DoubleVector () {
     }

     void setValues ( std :: vector <double> dataToMe) {
         if (dataToMe.size() >= size) {
             for (int i = 0; i < size; i++) {
                (*data)[i] = dataToMe[i];
                
             }
         } else {
             std :: cout << "my size is " << size << ", and input's size is " << dataToMe.size()<<std :: endl;
         }
         this->print();
     }

     size_t getSize () {
         return this->size;
     }

     Handle<Vector<double>> & getData () {
         return data;
     }

     double getDouble (int i) {
         if (i < this->size) {
            return (*data)[i];
         } else {
            return -1;
         }
     }

     void setDouble (int i, double val) {
         if (i < this->size) {
             (*data)[i] = val;
         } else {
            std :: cout << "Cannot assign the value " << val << "to the pos " << i << std :: endl;;
         }
     }


     void print() {
         for (int i = 0; i < this->getSize(); i++) {
             std :: cout << i << ": " << (*data)[i] << "; ";
         }
         std :: cout << std :: endl;
     }

     /* 
     void push_back (Handle<double> val) {
    	 data->push_back(*val);
     }
     */
     

     DoubleVector& operator + (DoubleVector &other) {
         //std :: cout << "me:" << this->getSize() << std :: endl;
         //this->print();
         //std :: cout << "other:" << other.getSize() << std :: endl;
         //other.print();

         if (this->getSize() <= other.getSize()) {
             for (int i = 0; i < this->getSize(); i++) {
                 (*data)[i] += other.getDouble(i);
             }
                        
         } else {
             for (int i = 0; i < other.getSize(); i++) {
                 (*data)[i] += other.getDouble(i);
             }

         } 
         return *this;
         
     }

     
     DoubleVector& operator / (int val) {
	 

	 Handle<DoubleVector> result = makeObject<DoubleVector>(this->getSize());
     	 for (int i = 0; i < this->getSize(); i++) {
		result->setDouble(i, (*data)[i] / val);
     	 }

         return *result;
         
     }



     /*
     DoubleVector operator + (DoubleVector other) {
	 
	 if (this->getSize() != other.getSize()) {
		std :: cout << "Can not add two DoubleVector with different sizes!" << std :: endl;
		return *this;
	 }

	 DoubleVector result = DoubleVector(this->getSize());
     	 for (int i = 0; i < this->getSize(); i++) {
		result.setDouble(i, (*data)[i] + other.getDouble(i));
     	 }

         return result;
         
     }
     */


     ENABLE_DEEP_COPY

     


};

}



#endif
