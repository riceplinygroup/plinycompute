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
#if !defined(MATRIX_BLOCK_H) && !defined(BUILTIN_MATRIX_BLOCK_H)
#define MATRIX_BLOCK_H

//#ifndef MAX_BLOCK_SIZE
//    #define MAX_BLOCK_SIZE 10000
//#endif

#ifndef EIGEN_CODE
    #define EIGEN_CODE 0
#endif

//by Binhang, May 2017

#include "Object.h"
#include "PDBVector.h"
#include "PDBString.h"
#include "Handle.h"
#include "ExportableObject.h"
#include <vector>

//LA libraries:
#include <eigen3/Eigen/Dense>


#include "MatrixMeta.h"
#include "MatrixData.h"
#include "LAMaxElementValueType.h"
#include "LAMinElementValueType.h"






class MatrixBlock : public ExportableObject {

private:
    MatrixMeta meta;
    MatrixData data;
   

public:
    
    const static int librayCode = EIGEN_CODE;

	ENABLE_DEEP_COPY

    ~MatrixBlock () { }
    MatrixBlock () {}

    MatrixBlock(int blockRowIndexIn, int blockColIndexIn, int rowNumsIn, int colNumsIn) {
        meta.blockRowIndex = blockRowIndexIn;
        meta.blockColIndex = blockColIndexIn;
        //meta.rowNums = rowNumsIn;
        //meta.colNums = colNumsIn;
        data.rowNums = rowNumsIn;
        data.colNums = colNumsIn;
        data.rawData = pdb::makeObject<pdb::Vector<double> >(rowNumsIn*colNumsIn, rowNumsIn*colNumsIn);
        std::cout << "MatrixBlock constructor RawData size:" << (data.rawData)->size() << std::endl;
    }

    void print () override {
        std :: cout << "Block: (" << meta.blockRowIndex <<","<< meta.blockColIndex << "), size: (" << data.rowNums <<","<< data.colNums<<"), length:" << data.rawData->size()<<" ";
        if(data.rawData->size()!=data.rowNums*data.colNums){
            std::cout<<"Matrix Error: size misMatch!" <<std::endl;
        }
        else{
            for(int i=0;i<data.rawData->size();i++){
                if(i%data.colNums==0){
                    std::cout << std::endl;
                }
                std::cout << (*(data.rawData))[i] <<" ";
            }
            std::cout << std::endl;
        }
    }

    std :: string toSchemaString ( std :: string format ) override {
        return "";
    }

    std :: string toValueString ( std :: string format ) override { 
        return "";
    }

    std :: vector < std :: string > getSupportedFormats () override {
        std :: vector < std :: string> ret;
        return ret;
    }

    int getBlockRowIndex () {
        return meta.blockRowIndex;
    }

    int getBlockColIndex () {
        return meta.blockColIndex;
    }

    int getRowNums() {
        return data.rowNums;
    }

    int getColNums() {
        return data.colNums;
    }
        
    pdb::Handle<pdb::Vector <double>>& getRawDataHandle(){
        return data.rawData;
    }

    MatrixMeta& getKey(){
        return meta;
    }

    MatrixData& getValue(){
        return data;
    }


    MatrixMeta& getMultiplyKey(){
        return meta;
    }

    MatrixData& getMultiplyValue(){
        data.setMatrixMultiplyFlag();
        return data;
    }

    //This is needed for row-wise computation
    MatrixMeta getRowKey(){
        MatrixMeta rowMeta;
        rowMeta.blockRowIndex = meta.blockRowIndex;
        rowMeta.blockColIndex = 0;
        return rowMeta;
    }

    //This is needed for col-wise computation
    MatrixMeta getColKey(){
        MatrixMeta colMeta;
        colMeta.blockRowIndex = 0;
        colMeta.blockColIndex = meta.blockColIndex;
        return colMeta;
    }

    //This is needed for row-wise computation
    MatrixData getRowMaxValue(){
        MatrixData maxRowData;
        maxRowData.setRowMaxFlag();
        maxRowData.rowNums = data.rowNums;
        maxRowData.colNums = 1;
        int bufferLength = maxRowData.rowNums*maxRowData.colNums;
        maxRowData.rawData = pdb::makeObject<pdb::Vector<double> >(bufferLength, bufferLength);

        Eigen::Map<Eigen::Matrix<double,Eigen::Dynamic,Eigen::Dynamic,Eigen::RowMajor> > currentMatrix((data.rawData)->c_ptr(),data.rowNums,data.colNums);

        Eigen::Map<Eigen::Matrix<double,Eigen::Dynamic,Eigen::Dynamic,Eigen::RowMajor> > rowMaxMatrix((maxRowData.rawData)->c_ptr(),maxRowData.rowNums, maxRowData.colNums);

        rowMaxMatrix = currentMatrix.rowwise().maxCoeff();

        std::cout <<"getRowMaxValue Matrix :"<< std::endl;
        this->print();
        std::cout <<"Row wise Max Matrix :"<< std::endl;
        maxRowData.print();
        return maxRowData;
    }

    //This is needed for row-wise computation
    MatrixData getRowMinValue(){
        MatrixData minRowData;
        minRowData.setRowMinFlag();
        minRowData.rowNums = data.rowNums;
        minRowData.colNums = 1;
        int bufferLength = minRowData.rowNums*minRowData.colNums;
        minRowData.rawData = pdb::makeObject<pdb::Vector<double> >(bufferLength, bufferLength);

        Eigen::Map<Eigen::Matrix<double,Eigen::Dynamic,Eigen::Dynamic,Eigen::RowMajor> > currentMatrix((data.rawData)->c_ptr(),data.rowNums,data.colNums);

        Eigen::Map<Eigen::Matrix<double,Eigen::Dynamic,Eigen::Dynamic,Eigen::RowMajor> > rowMinMatrix((minRowData.rawData)->c_ptr(),minRowData.rowNums, minRowData.colNums);

        rowMinMatrix = currentMatrix.rowwise().minCoeff();

        std::cout <<"getRowMinValue Matrix :"<< std::endl;
        this->print();
        std::cout <<"Row wise Min Matrix :"<< std::endl;
        minRowData.print();
        return minRowData;
    }

    //This is needed for col-wise computation
    MatrixData getColMaxValue(){
        MatrixData maxColData;
        maxColData.setColMaxFlag();
        maxColData.rowNums = 1;
        maxColData.colNums = data.colNums;
        int bufferLength = maxColData.rowNums*maxColData.colNums;
        maxColData.rawData = pdb::makeObject<pdb::Vector<double> >(bufferLength, bufferLength);

        Eigen::Map<Eigen::Matrix<double,Eigen::Dynamic,Eigen::Dynamic,Eigen::RowMajor> > currentMatrix((data.rawData)->c_ptr(),data.rowNums,data.colNums);

        Eigen::Map<Eigen::Matrix<double,Eigen::Dynamic,Eigen::Dynamic,Eigen::RowMajor> > colMaxMatrix((maxColData.rawData)->c_ptr(),maxColData.rowNums, maxColData.colNums);

        colMaxMatrix = currentMatrix.colwise().maxCoeff();

        std::cout <<"getColMaxValue Matrix :"<< std::endl;
        this->print();
        std::cout <<"Col wise Max Matrix :"<< std::endl;
        maxColData.print();
        return maxColData;
    }

    //This is needed for col-wise computation
    MatrixData getColMinValue(){
        MatrixData minColData;
        minColData.setColMinFlag();
        minColData.rowNums = 1;
        minColData.colNums = data.colNums;
        int bufferLength = minColData.rowNums*minColData.colNums;
        minColData.rawData = pdb::makeObject<pdb::Vector<double> >(bufferLength, bufferLength);

        Eigen::Map<Eigen::Matrix<double,Eigen::Dynamic,Eigen::Dynamic,Eigen::RowMajor> > currentMatrix((data.rawData)->c_ptr(),data.rowNums,data.colNums);

        Eigen::Map<Eigen::Matrix<double,Eigen::Dynamic,Eigen::Dynamic,Eigen::RowMajor> > colMinMatrix((minColData.rawData)->c_ptr(),minColData.rowNums, minColData.colNums);

        colMinMatrix = currentMatrix.colwise().minCoeff();

        std::cout <<"getColMinValue Matrix :"<< std::endl;
        this->print();
        std::cout <<"Col wise Min Matrix :"<< std::endl;
        minColData.print();
        return minColData;
    }

    //This is needed to find the max element
    LAMaxElementValueType getMaxElementValue(){
        LAMaxElementValueType result;
        for(int i=0;i<data.rawData->size();i++){
            if((*(data.rawData))[i] > result.getValue()){
                result.setValue((*(data.rawData))[i]);
                int globalRowIndex = meta.blockRowIndex*data.rowNums + i/data.colNums;
                result.setRowIndex(globalRowIndex);
                int globalColIndex = meta.blockColIndex*data.colNums + i%data.colNums;
                result.setColIndex(globalColIndex);
            }
        }
        std:: cout << "Max element in this block: "<< result.getValue() << " index:(" << result.getRowIndex() << "," << result.getColIndex() <<")."<< std::endl; 
        return result;
    }

    //This is needed to find the min element
    LAMinElementValueType getMinElementValue(){
        LAMinElementValueType result;
        for(int i=0;i<data.rawData->size();i++){
            if((*(data.rawData))[i] < result.getValue()){
                result.setValue((*(data.rawData))[i]);
                int globalRowIndex = meta.blockRowIndex*data.rowNums + i/data.colNums;
                result.setRowIndex(globalRowIndex);
                int globalColIndex = meta.blockColIndex*data.colNums + i%data.colNums;
                result.setColIndex(globalColIndex);
            }
        }
        std:: cout << "Min element in this block: "<< result.getValue() << " index:(" << result.getRowIndex() << "," << result.getColIndex() <<")."<< std::endl; 
        return result;
    }    
};


#endif
