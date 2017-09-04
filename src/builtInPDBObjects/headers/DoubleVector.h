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
#include "Configuration.h"
#include <math.h>
//PRELOAD %DoubleVector%


#ifndef KMEANS_EPSILON
   #define KMEANS_EPSILON 2.22045e-16
#endif

namespace pdb {

class DoubleVector: public Object {

public:
     Handle<Vector<double>> data = nullptr;
     size_t size = 0;
     double norm = -1;
public:

     DoubleVector () { size = 0; }

     DoubleVector ( size_t size ) {
         this->size = size;
         data = makeObject<Vector<double>> ( size, size );
     }

     ~DoubleVector () {
     }

     void setValues ( std :: vector <double> dataToMe) {
         double *  rawData = data->c_ptr();
         if (dataToMe.size() >= size) {
             for (int i = 0; i < size; i++) {
                rawData[i] = dataToMe[i];
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

     double * getRawData () {
         if (data == nullptr) {
             return nullptr;
         }
         return data->c_ptr();
     }

     double getDouble (int i) {
         if (i < this->size) {
            return (*data)[i];
         } else {
            std :: cout << "Error in DoubleVector: Cannot get the value at the pos " << i << std :: endl;;
            exit(-1);
         }
     }

     void setDouble (int i, double val) {
         if (i < this->size) {
             (*data)[i] = val;
         } else {
            std :: cout << "Error in DoubleVector: Cannot assign the value " << val << "to the pos " << i << std :: endl;
            exit(-1);
         }
     }

     //following implementation of Spark MLLib
     //https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/mllib/linalg/Vectors.scala
     inline double getNorm2() {
         if (norm < 0 ) {
             norm = 0;
             double * rawData = data->c_ptr();
             size_t mySize = this->getSize();
             for ( int i = 0; i < mySize; i++) {
                 norm += rawData[i] * rawData[i];
             }
             norm = sqrt(norm);
         }
         return norm;
     }
     
     inline double dot (DoubleVector &other) {
          size_t mySize = this->size;
          double * rawData = data->c_ptr();
          double * otherRawData = other.getRawData();
          double dotSum = 0;
          for (size_t i = 0; i < mySize; i++) {
               dotSum += rawData[i] * otherRawData[i];
          }
          return dotSum;
     }

     //to get squared distance following SparkMLLib
    
     inline double getSquaredDistance (DoubleVector &other) {
          size_t mySize = this->getSize();
          size_t otherSize = other.getSize();
          double * rawData = data->c_ptr();
          double * otherRawData = other.getRawData();
          if (mySize != otherSize) {
              std :: cout << "Error in DoubleVector: dot size doesn't match" << std :: endl;
              exit(-1);
          }
          double distance = 0;
          size_t kv = 0;
          while (kv < mySize) {
               double score = rawData[kv] - otherRawData[kv];
               distance += score * score;
               kv ++;
          }
          return distance;

     }


     void print() {
         double * rawData = data->c_ptr();
         for (int i = 0; i < this->getSize(); i++) {
             std :: cout << i << ": " << rawData[i] << "; ";
         }
         std :: cout << std :: endl;
     }



     //this implementation is following Spark MLLib
     //https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/mllib/util/MLUtils.scala
     inline double getFastSquaredDistance (DoubleVector &other) {
         double precision = 0.00001;
         double myNorm = norm;
         double otherNorm = other.norm;
         double sumSquaredNorm = myNorm * myNorm + otherNorm * otherNorm;
         double normDiff = myNorm - otherNorm;
         double sqDist = 0.0;
         double precisionBound1 = 2.0 * KMEANS_EPSILON * sumSquaredNorm / (normDiff * normDiff + KMEANS_EPSILON);
         if (precisionBound1 < precision) {
             sqDist = sumSquaredNorm - 2.0 * dot (other);
         } else {
             sqDist = getSquaredDistance(other);
         }
         return sqDist;
     }


     inline DoubleVector& operator + (DoubleVector &other) {
         //std :: cout << "me:" << this->getSize() << std :: endl;
         //this->print();
         //std :: cout << "other:" << other.getSize() << std :: endl;
         //other.print();
         size_t mySize = this->getSize();
         size_t otherSize = other.getSize();
         if (mySize != otherSize) {
              std :: cout << "Error in DoubleVector: dot size doesn't match" << std :: endl;
              exit(-1);
         } 
         double * rawData = data->c_ptr();
         double * otherRawData = other.getRawData();
         for (int i = 0; i < mySize; i++) {
              rawData[i] += otherRawData[i];
         }
         return *this;
         
     }

     
     inline DoubleVector& operator / (int val) {
         if (val == 0) {
             std :: cout << "Error in DoubleVector: division by zero" << std :: endl;
             exit(-1);
         }	 
         size_t mySize = this->getSize();
         Handle<DoubleVector> result = makeObject<DoubleVector>(mySize);
         double * rawData = data->c_ptr();
         double * otherRawData = result->getRawData();
     	 for (int i = 0; i < mySize; i++) {
		otherRawData[i] = rawData[i] / val;
     	 }

         return *result;
         
     }

     //Shuffle the elements of an array into a random order, modifying the original array. Returns the original array.
     inline DoubleVector& randomizeInPlace () {
         double * rawData = data->c_ptr();
         size_t mySize = this->getSize();
         for (int i = mySize-1; i >= 0; i--) { 
             int j = rand()%mySize;
             double tmp = rawData[j];
             rawData[j] = rawData[i];
             rawData[i] = tmp;
         }
         return *this;
     }

    inline bool equals (Handle<DoubleVector>& other)  {
        size_t mySize = this->size;
        size_t otherSize = other->getSize();
        if (mySize != otherSize) {
             return false;
        }
        double * rawData =this->getRawData();
        double * otherRawData = other->getRawData();
        for (int i = 0; i < mySize; i++) {
             if (rawData[i] != otherRawData[i]) {
                 return false;
             }
        }
        return true;
    }



     ENABLE_DEEP_COPY

     


};

}



#endif
