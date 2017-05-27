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
#ifndef MATRIX_DATA_H
#define MATRIX_DATA_H

#include "Object.h"
#include "PDBVector.h"
#include "Handle.h"

class MatrixData :public pdb::Object{
public:

    ENABLE_DEEP_COPY

    ~MatrixData(){}
    MatrixData(){}


    pdb::Handle<pdb::Vector <double>> rawData;
    int rowNums = 0;
    int colNums = 0;

    /*
    MatrixData(int rowNumsIn, int colNumsIn){
        rowNums = rowNumsIn;
        colNums = colNumsIn;
        rawData = pdb::makeObject<pdb::Vector<double> >(rowNums*colNums, rowNums*colNums);
    }
    */
    void print(){
        std::cout<<"Row: "<<rowNums <<" Col: "<<colNums << " Buffer size:" << rawData->size() <<std::endl;
    }


    /*
    MatrixData operator + (MatrixData &other){
        std::cout<< "+ operator:" << std::endl;
        this->print();
        other.print();
        MatrixData result;
        if(rowNums != other.rowNums || colNums != other.colNums ){
            result.rowNums = 0;
            result.colNums = 0;
            result.rawData = pdb::makeObject<pdb::Vector<double> >();
            return result;
        }
        else{
            result.rowNums = rowNums;
            result.colNums = colNums;
            result.rawData = pdb::makeObject<pdb::Vector<double> >(rowNums*colNums, rowNums*colNums);
            for(int i=0;i<rowNums*colNums;i++){
                (*(result.rawData))[i]= (*rawData)[i] + (*(other.rawData))[i];
            }
            return result;
        }
    }*/

    MatrixData& operator + (MatrixData &other){
        std::cout<< "+ operator:" << std::endl;
        this->print();
        other.print();
        if(rowNums != other.rowNums || colNums != other.colNums ){
            this->rawData->clear();
        }
        else{
            for(int i=0;i<rowNums*colNums;i++){
                (*rawData)[i] += (*(other.rawData))[i];
            }
        }
        return *this;
    }
};

#endif