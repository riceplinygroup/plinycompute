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
/*
 * CatalogStandardUserTypeMetadata.cc
 *
 */

#include "CatalogStandardUserTypeMetadata.h"

using namespace std;

CatalogStandardUserTypeMetadata::CatalogStandardUserTypeMetadata() {}

CatalogStandardUserTypeMetadata::CatalogStandardUserTypeMetadata(
    string itemIdIn, string objectIDIn, string objectTypeIn,
    string objectNameIn, string libraryBytesIn)
    : itemId(itemIdIn), objectID(objectIDIn), objectType(objectTypeIn),
      objectName(objectNameIn), libraryBytes(libraryBytesIn) {}

CatalogStandardUserTypeMetadata::CatalogStandardUserTypeMetadata(
    const CatalogStandardUserTypeMetadata &pdbCatalogEntryToCopy) {
  itemId = pdbCatalogEntryToCopy.itemId;
  objectID = pdbCatalogEntryToCopy.objectID;
  objectType = pdbCatalogEntryToCopy.objectType;
  objectName = pdbCatalogEntryToCopy.objectName;
  libraryBytes = pdbCatalogEntryToCopy.libraryBytes;
}

CatalogStandardUserTypeMetadata::~CatalogStandardUserTypeMetadata() {}

string CatalogStandardUserTypeMetadata::getItemKey() { return objectName; }

string CatalogStandardUserTypeMetadata::getItemId() { return itemId; }

string CatalogStandardUserTypeMetadata::getObjectID() { return objectID; }

string CatalogStandardUserTypeMetadata::getObjectType() { return objectType; }

string CatalogStandardUserTypeMetadata::getItemName() { return objectName; }

string CatalogStandardUserTypeMetadata::getLibraryBytes() {
  return libraryBytes;
}

void CatalogStandardUserTypeMetadata::setObjectId(string &objectIdIn) {
  objectID = objectIdIn;
}

void CatalogStandardUserTypeMetadata::setItemId(string &itemIdIn) {
  itemId = itemIdIn;
}

void CatalogStandardUserTypeMetadata::setItemKey(string &itemKeyIn) {
  objectName = itemKeyIn;
}

void CatalogStandardUserTypeMetadata::setItemName(string &itemIdIn) {
  objectName = itemIdIn;
}

void CatalogStandardUserTypeMetadata::setLibraryBytes(string &bytesIn) {
  libraryBytes = bytesIn;
}

string CatalogStandardUserTypeMetadata::printShort() {
  string output;
  output = "   Type ID: ";
  output.append(getObjectID().c_str())
      .append(" | Type Name: ")
      .append(getItemName().c_str());
  return output;
}
