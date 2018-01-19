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
#ifndef KMEANS_DOUBLE_VECTOR_H
#define KMEANS_DOUBLE_VECTOR_H


#include "Object.h"
#include "Handle.h"
#include "PDBVector.h"
#include "Configuration.h"
#include <math.h>
// PRELOAD %KMeansDoubleVector%


#ifndef KMEANS_EPSILON
#define KMEANS_EPSILON 2.22045e-16
#endif

#ifndef NUM_KMEANS_DIMENSIONS
#define NUM_KMEANS_DIMENSIONS 1000
#endif


/* This class implements a double vector based on a native array */
namespace pdb {

class KMeansDoubleVector : public Object {

public:
    double rawData[NUM_KMEANS_DIMENSIONS];
    double norm = -1;

public:
    KMeansDoubleVector() {}


    ~KMeansDoubleVector() {}

    void setValues(std::vector<double> dataToMe) {
        for (int i = 0; i < NUM_KMEANS_DIMENSIONS; i++) {
            rawData[i] = dataToMe[i];
        }
    }

    size_t getSize() {
        return NUM_KMEANS_DIMENSIONS;
    }


    double* getRawData() {
        return rawData;
    }

    double getDouble(int i) {
        return rawData[i];
    }

    void setDouble(int i, double val) {
        rawData[i] = val;
    }

    /* Compute the 2-norm */
    inline double getNorm2() {
        if (norm < 0) {
            norm = 0;
            for (int i = 0; i < NUM_KMEANS_DIMENSIONS; i++) {
                norm += rawData[i] * rawData[i];
            }
            norm = sqrt(norm);
        }
        return norm;
    }

    /* Dot product */
    inline double dot(KMeansDoubleVector& other) {
        double* otherRawData = other.getRawData();
        double dotSum = 0;
        for (size_t i = 0; i < NUM_KMEANS_DIMENSIONS; i++) {
            dotSum += rawData[i] * otherRawData[i];
        }
        return dotSum;
    }

    /* Compute the squared distance */
    inline double getSquaredDistance(KMeansDoubleVector& other) {
        double* otherRawData = other.getRawData();
        double distance = 0;
        size_t kv = 0;
        while (kv < NUM_KMEANS_DIMENSIONS) {
            double score = rawData[kv] - otherRawData[kv];
            distance += score * score;
            kv++;
        }
        return distance;
    }


    void print() {
        for (int i = 0; i < NUM_KMEANS_DIMENSIONS; i++) {
            std::cout << i << ": " << rawData[i] << "; ";
        }
        std::cout << std::endl;
    }


    /* 
     * Another way to compute the squared distance
     * Faster than the direct computing when both norms are given
     */
    inline double getFastSquaredDistance(KMeansDoubleVector& other) {
        double precision = 0.000001;
        double myNorm = norm;
        double otherNorm = other.norm;
        double sumSquaredNorm = myNorm * myNorm + otherNorm * otherNorm;
        double normDiff = myNorm - otherNorm;
        double sqDist = 0.0;
        double precisionBound1 =
            2.0 * KMEANS_EPSILON * sumSquaredNorm / (normDiff * normDiff + KMEANS_EPSILON);
        if (precisionBound1 < precision) {
            sqDist = sumSquaredNorm - 2.0 * dot(other);
        } else {
            sqDist = getSquaredDistance(other);
        }
        return sqDist;
    }

    /* Overload the + operator */
    inline KMeansDoubleVector& operator+(KMeansDoubleVector& other) {
        double* otherRawData = other.getRawData();
        for (int i = 0; i < NUM_KMEANS_DIMENSIONS; i++) {
            rawData[i] += otherRawData[i];
        }
        return *this;
    }

    /* Overload the / operator */
    inline KMeansDoubleVector& operator/(int val) {
        if (val == 0) {
            std::cout << "Error in KMeansDoubleVector: division by zero" << std::endl;
            exit(-1);
        }
        Handle<KMeansDoubleVector> result = makeObject<KMeansDoubleVector>();
        double* otherRawData = result->getRawData();
        for (int i = 0; i < NUM_KMEANS_DIMENSIONS; i++) {
            otherRawData[i] = rawData[i] / val;
        }

        return *result;
    }

    /* Shuffle the elements of an array to a random order */
    inline KMeansDoubleVector& randomizeInPlace() {
        for (int i = NUM_KMEANS_DIMENSIONS - 1; i >= 0; i--) {
            int j = rand() % NUM_KMEANS_DIMENSIONS;
            double tmp = rawData[j];
            rawData[j] = rawData[i];
            rawData[i] = tmp;
        }
        return *this;
    }

    /* Judge if two KMeansDoubleVector is equal */
    inline bool equals(Handle<KMeansDoubleVector>& other) {
        double* otherRawData = other->getRawData();
        for (int i = 0; i < NUM_KMEANS_DIMENSIONS; i++) {
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
