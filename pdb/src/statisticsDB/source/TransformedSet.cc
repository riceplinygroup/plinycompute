#ifndef TRANSFORMED_SET_CC
#define TRANSFORMED_SET_CC


#include "TransformedSet.h"

namespace pdb {

TransformedSet::TransformedSet (std::string databaseName,
                    std::string setName,
                    std::string transformationType,
                    std::string tcap,
                    int numPartitions) {

    this->databaseName = databaseName;
    this->setName = setName;
    this->transformationType = transformationType;
    this->tcap = tcap;
    this->numPartitions = numPartitions;

}

TransformedSet::~TransformedSet () {}


std::string TransformedSet::getDatabaseName() {

    return databaseName;

}


std::string TransformedSet::getSetName() {

    return setName;

}


std::string TransformedSet::getTransformationType() {

    return transformationType;

}

std::string TransformedSet::getTCAP() {

    return tcap;

}


int TransformedSet::getNumPartitions() {

    return numPartitions;

}


}


#endif
