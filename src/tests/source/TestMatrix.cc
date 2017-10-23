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

#define NUM_ROW_DIM1 1000
#define NUM_COL_DIM1 1000
#define NUM_ROW_DIM2 1000
#define NUM_COL_DIM2 1000

#include <cstddef>
#include <iostream>
#include <vector>
#include <chrono>
#include <algorithm>
#include <iterator>
#include <cstring>
#include <unistd.h>
#include <iomanip>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <ctime>
#include <random>
#include <string>
#include <gsl/gsl_blas.h>
#include <eigen3/Eigen/Dense>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "Handle.h"
#include "PDBVector.h"
#include "InterfaceFunctions.h"

using namespace pdb;

//generate a random double
double drand() {

   return (double)rand()/RAND_MAX;

}

//generate matrix data
void generateData(double * matrix, int numRows, int numCols) {
    for (int i = 0; i < numRows; i++) {
        for (int j = 0; j < numCols; j++) {
            matrix[i*numRows+j] = drand();
        }
    } 
}

//run gsl multiplication on native array
void runNativeGSLMult(double * matrixLHS, double * matrixRHS, double * matrixProduct) {
    gsl_matrix_view lhs = gsl_matrix_view_array(matrixLHS, NUM_ROW_DIM1, NUM_COL_DIM1);
    gsl_matrix_view rhs = gsl_matrix_view_array(matrixRHS, NUM_ROW_DIM2, NUM_COL_DIM2);
    gsl_matrix_view product = gsl_matrix_view_array(matrixProduct, NUM_ROW_DIM1, NUM_COL_DIM2);
    gsl_blas_dgemm(CblasNoTrans, CblasNoTrans, 1.0, &lhs.matrix, &rhs.matrix, 0.0, &product.matrix);
    printf("first element is %g\n", matrixProduct[0]);
    printf("last element is %g\n", matrixProduct[NUM_ROW_DIM1*NUM_COL_DIM2-1]);
}

//run eigen multiplication on native array
void runNativeEigenMult(double * matrixLHS, double * matrixRHS, double * matrixProduct) {
    Eigen::Map<Eigen::Matrix<double, Eigen::Dynamic, Eigen::Dynamic, Eigen::RowMajor>> lhs (
       matrixLHS, NUM_ROW_DIM1, NUM_COL_DIM1);
    Eigen::Map<Eigen::Matrix<double, Eigen::Dynamic, Eigen::Dynamic, Eigen::RowMajor>> rhs (
       matrixRHS, NUM_ROW_DIM2, NUM_COL_DIM2);
    Eigen::Map<Eigen::Matrix<double, Eigen::Dynamic, Eigen::Dynamic, Eigen::RowMajor>> product (
       matrixProduct, NUM_ROW_DIM1, NUM_COL_DIM2);
    product = lhs * rhs;
    printf("first element is %g\n", matrixProduct[0]);
    printf("last element is %g\n", matrixProduct[NUM_ROW_DIM1*NUM_COL_DIM2-1]);
}

//run gsl multiplication on PDBVector
void runPDBGSLMult(Handle<pdb::Vector<double>> matrixLHS, Handle<pdb::Vector<double>> matrixRHS, Handle<pdb::Vector<double>> matrixProduct){
     runNativeGSLMult(matrixLHS->c_ptr(), matrixRHS->c_ptr(), matrixProduct->c_ptr());
}

//run eigen multiplication on PDBVector
void runPDBEigenMult(Handle<pdb::Vector<double>> matrixLHS, Handle<pdb::Vector<double>> matrixRHS, Handle<pdb::Vector<double>> matrixProduct) {
     runNativeEigenMult(matrixLHS->c_ptr(), matrixRHS->c_ptr(), matrixProduct->c_ptr());
}

//run gsl multiplication on std::vector
void runStdGSLMult(std::vector<double> * matrixLHS, std::vector<double> * matrixRHS, std::vector<double> * matrixProduct) {
     runNativeGSLMult(matrixLHS->data(), matrixRHS->data(), matrixProduct->data());
}

//run eigen multiplication on std::vector
void runStdEigenMult(std::vector<double> * matrixLHS, std::vector<double> * matrixRHS, std::vector<double> * matrixProduct) {
     runNativeEigenMult(matrixLHS->data(), matrixRHS->data(), matrixProduct->data());

}


int main(int argc, char* argv[]) {


    // this test compares following for C++ matrix operations
    // NativeGSLMult: apply gsl blas on Native C++ array
    // NativeEigenMult: apply eigen on Native C++ array
    // PDBGSLMult: apply gsl blas on PDBVector
    // PDBEigenMult: apply eigen on PDBVector
    // stdGSLMult: apply gsl blas on stdVector
    // stdEigenMult: apply eigen on stdVector

    //analyze parameters
    if (argc > 2) {
        std::cout << "Usage: #Mode (can be one of following, by default run All)\n";
        std::cout << "NativeGSLMult: apply gsl blas on Native C++ array\n";
        std::cout << "NativeEigenMult: apply eigen on Native C++ array\n";
        std::cout << "PDBGSLMult: apply gsl blas on PDBVector\n";
        std::cout << "PDBEigenMult: apply eigen on PDBVector\n";
        std::cout << "stdGSLMult: apply gsl blas on stdVector\n";
        std::cout << "stdEigenMult: apply eigen on stdVector\n";
        std::cout << "All: run all above tests\n";
        return (-1);
    }
    std::string mode;
    if (argc == 1) {

        mode = "All";

    } else {

        mode = argv[1];

    }
    //data structures
    //for NativeGSLMult
    double* nativeArrayLHS = nullptr;
    double* nativeArrayRHS = nullptr;
    double* nativeProduct = nullptr;
    Handle<pdb::Vector<double>> pdbVectorLHS = nullptr;
    Handle<pdb::Vector<double>> pdbVectorRHS = nullptr;
    Handle<pdb::Vector<double>> pdbVectorProduct = nullptr;
    std::vector<double> * stdVectorLHS = nullptr;
    std::vector<double> * stdVectorRHS = nullptr;
    std::vector<double> * stdVectorProduct = nullptr;

    //initialize random generator
    srand(time(NULL));    

    //generate data
    if((mode == "NativeGSLMult") || (mode == "NativeEigenMult") || (mode == "native")) {
        nativeArrayLHS = new double[NUM_ROW_DIM1*NUM_COL_DIM1]();
        generateData(nativeArrayLHS, NUM_ROW_DIM1, NUM_COL_DIM1);
        nativeArrayRHS = new double[NUM_ROW_DIM2*NUM_COL_DIM2]();
        generateData(nativeArrayRHS, NUM_ROW_DIM2, NUM_COL_DIM2);
        nativeProduct = new double[NUM_ROW_DIM1*NUM_COL_DIM2]();
    } else if ((mode == "PDBGSLMult") || (mode == "PDBEigenMult")) {
        makeObjectAllocatorBlock ((size_t)4*(size_t)1024*(size_t)1024*(size_t)1024, true);
        pdbVectorLHS = makeObject<pdb::Vector<double>>(NUM_ROW_DIM1 * NUM_COL_DIM1, NUM_ROW_DIM1 * NUM_COL_DIM1);
        generateData(pdbVectorLHS->c_ptr(), NUM_ROW_DIM1, NUM_COL_DIM1);
        pdbVectorRHS = makeObject<pdb::Vector<double>>(NUM_ROW_DIM2 * NUM_COL_DIM2, NUM_ROW_DIM2 * NUM_COL_DIM2);
        generateData(pdbVectorRHS->c_ptr(), NUM_ROW_DIM2, NUM_COL_DIM2);
        pdbVectorProduct = makeObject<pdb::Vector<double>>(NUM_ROW_DIM1 * NUM_COL_DIM2);
    } else if ((mode == "stdGSLMult") || (mode == "stdEigenMult")) {
        stdVectorLHS = new std::vector<double>(NUM_ROW_DIM1 * NUM_COL_DIM1);
        generateData(stdVectorLHS->data(), NUM_ROW_DIM1, NUM_COL_DIM1);
        stdVectorRHS = new std::vector<double>(NUM_ROW_DIM2 * NUM_COL_DIM2);
        generateData(stdVectorRHS->data(), NUM_ROW_DIM2, NUM_COL_DIM2);
        stdVectorProduct = new std::vector<double>(NUM_ROW_DIM1 * NUM_COL_DIM2);
    } else if (mode == "All") {
        makeObjectAllocatorBlock ((size_t)4*(size_t)1024*(size_t)1024*(size_t)1024, true);
        nativeArrayLHS = new double[NUM_ROW_DIM1*NUM_COL_DIM1]();
        generateData(nativeArrayLHS, NUM_ROW_DIM1, NUM_COL_DIM1);
        nativeArrayRHS = new double[NUM_ROW_DIM2*NUM_COL_DIM2]();
        generateData(nativeArrayRHS, NUM_ROW_DIM2, NUM_COL_DIM2);
        nativeProduct = new double[NUM_ROW_DIM1*NUM_COL_DIM2]();

        pdbVectorLHS = makeObject<pdb::Vector<double>>(NUM_ROW_DIM1 * NUM_COL_DIM1, NUM_ROW_DIM1 * NUM_COL_DIM1);
        generateData(pdbVectorLHS->c_ptr(), NUM_ROW_DIM1, NUM_COL_DIM1);
        pdbVectorRHS = makeObject<pdb::Vector<double>>(NUM_ROW_DIM2 * NUM_COL_DIM2, NUM_ROW_DIM2 * NUM_COL_DIM2);
        generateData(pdbVectorRHS->c_ptr(), NUM_ROW_DIM2, NUM_COL_DIM2);
        pdbVectorProduct = makeObject<pdb::Vector<double>>(NUM_ROW_DIM1 * NUM_COL_DIM2);

        stdVectorLHS = new std::vector<double>(NUM_ROW_DIM1 * NUM_COL_DIM1);
        generateData(stdVectorLHS->data(), NUM_ROW_DIM1, NUM_COL_DIM1);
        stdVectorRHS = new std::vector<double>(NUM_ROW_DIM2 * NUM_COL_DIM2);
        generateData(stdVectorRHS->data(), NUM_ROW_DIM2, NUM_COL_DIM2);
        stdVectorProduct = new std::vector<double>(NUM_ROW_DIM1 * NUM_COL_DIM2);
    }

    // for timing
    auto begin = std::chrono::high_resolution_clock::now();

    if (mode == "NativeGSLMult") {
        runNativeGSLMult(nativeArrayLHS, nativeArrayRHS, nativeProduct);
    } else if (mode == "NativeEigenMult") {
        runNativeEigenMult(nativeArrayLHS, nativeArrayRHS, nativeProduct);
    } else if (mode == "PDBGSLMult") {
        runPDBGSLMult(pdbVectorLHS, pdbVectorRHS, pdbVectorProduct);
    } else if (mode == "PDBEigenMult") {
        runPDBEigenMult(pdbVectorLHS, pdbVectorRHS, pdbVectorProduct);
    } else if (mode == "stdGSLMult") {
        runStdGSLMult(stdVectorLHS, stdVectorRHS, stdVectorProduct);
    } else if (mode == "stdEigenMult") {
        runStdEigenMult(stdVectorLHS, stdVectorRHS, stdVectorProduct);
    } else if (mode == "native") {
        runNativeGSLMult(nativeArrayLHS, nativeArrayRHS, nativeProduct);
        auto finished1 = std::chrono::high_resolution_clock::now();
        std::cout << "Duration for NativeGSLMult:"
              << std::chrono::duration_cast<std::chrono::nanoseconds>(finished1 - begin).count() << " ns."
              << std::endl;
        
        runNativeEigenMult(nativeArrayLHS, nativeArrayRHS, nativeProduct);
        auto finished2 = std::chrono::high_resolution_clock::now();
        std::cout << "Duration for NativeEigenMult:"
              << std::chrono::duration_cast<std::chrono::nanoseconds>(finished2 - finished1).count() << " ns."
              << std::endl;
    } else {
        runNativeGSLMult(nativeArrayLHS, nativeArrayRHS, nativeProduct);
        auto finished1 = std::chrono::high_resolution_clock::now();
        std::cout << "Duration for NativeGSLMult:"
              << std::chrono::duration_cast<std::chrono::nanoseconds>(finished1 - begin).count() << " ns."
              << std::endl;

        runNativeEigenMult(nativeArrayLHS, nativeArrayRHS, nativeProduct);
        auto finished2 = std::chrono::high_resolution_clock::now();
        std::cout << "Duration for NativeEigenMult:"
              << std::chrono::duration_cast<std::chrono::nanoseconds>(finished2 - finished1).count() << " ns."
              << std::endl;
        delete[] nativeArrayLHS;
        delete[] nativeArrayRHS;
        delete[] nativeProduct;
        auto begin3 = std::chrono::high_resolution_clock::now(); 
        runPDBGSLMult(pdbVectorLHS, pdbVectorRHS, pdbVectorProduct);
        auto finished3 = std::chrono::high_resolution_clock::now();
        std::cout << "Duration for PDBGSLMult:"
              << std::chrono::duration_cast<std::chrono::nanoseconds>(finished3 - begin3).count() << " ns."
              << std::endl;

        runPDBEigenMult(pdbVectorLHS, pdbVectorRHS, pdbVectorProduct);
        auto finished4 = std::chrono::high_resolution_clock::now();
        std::cout << "Duration for PDBEigenMult:"
              << std::chrono::duration_cast<std::chrono::nanoseconds>(finished4 - finished3).count() << " ns."
              << std::endl;
        makeObjectAllocatorBlock (1024, true);
        auto begin5 = std::chrono::high_resolution_clock::now();
        runStdGSLMult(stdVectorLHS, stdVectorRHS, stdVectorProduct);
        auto finished5 = std::chrono::high_resolution_clock::now();
        std::cout << "Duration for StdGSLMult:"
              << std::chrono::duration_cast<std::chrono::nanoseconds>(finished5 - begin5).count() << " ns."
              << std::endl;

        runStdEigenMult(stdVectorLHS, stdVectorRHS, stdVectorProduct);
        auto finished6 = std::chrono::high_resolution_clock::now();
        std::cout << "Duration for StdEigenMult:"
              << std::chrono::duration_cast<std::chrono::nanoseconds>(finished6 - finished5).count() << " ns."
              << std::endl;

    }



    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Duration for " << mode << ":" 
              << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << " ns."
              << std::endl;

}
