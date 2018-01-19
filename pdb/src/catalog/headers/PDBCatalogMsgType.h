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
 * File:   PDBCatalogMsgType.h
 */

#ifndef PDBCATALOGMSGTYPE_H
#define PDBCATALOGMSGTYPE_H

// this lists all of the identifiers for the different types of Catalog Related
// Metadata

enum PDBCatalogMsgType {
  CatalogPDBNode, // 0

  CatalogPDBRegisteredObject, // 1

  CatalogPDBDatabase, // 2

  CatalogPDBUser, // 3

  CatalogGetPDBRegisteredObject, // 4

  GetSerializedCatalog, // 5

  SerializedCatalog, // 6

  GetCatalogMetadataAsString, // 7

  CatalogPDBSet, // 8

  CatalogPDBPermissions, // 9

  CatalogDataTypeId // 10

};

#endif /* PDBCATALOGMSGTYPE_H */
