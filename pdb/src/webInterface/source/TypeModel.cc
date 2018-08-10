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

#include <UseTemporaryAllocationBlock.h>
#include <TypeModel.h>

#include "TypeModel.h"

TypeModel::TypeModel(CatalogServer *catalogServer) : catalogServer(catalogServer) {}

mustache::data pdb::TypeModel::getTypes() {

  // all the stuff we create will be stored here
  const UseTemporaryAllocationBlock block(2 * 1024 * 1024);

  // the return value
  mustache::data ret = mustache::data();

  // grab the sets
  pdb::Handle<Vector<CatalogUserTypeMetadata>> listOfSets = makeObject<Vector<CatalogUserTypeMetadata>>();
  catalogServer->getTypes(listOfSets);

  // we put all the node data here
  mustache::data typeData = mustache::data::type::list;

  // go through each set
  for(int i = 0; i < listOfSets->size(); ++i) {

    // the data of this node
    mustache::data setData;

    // set the data
    setData.set("type-name", (std::string)(*listOfSets)[i].getItemName());
    setData.set("type-id", (std::string)(*listOfSets)[i].getItemId());
    setData.set("is-last", i == listOfSets->size() - 1);

    //TODO there is not timestamp for the set
    setData.set("registered", std::to_string(1533453548));

    typeData.push_back(setData);
  }

  // set the return result
  ret.set("types", typeData);
  ret.set("success", true);

  return ret;
}

mustache::data TypeModel::getType(string &typeID) {
  // all the stuff we create will be stored here
  const UseTemporaryAllocationBlock block(2 * 1024 * 1024);

  // the return value
  mustache::data ret = mustache::data();

  // grab the sets
  pdb::Handle<Vector<CatalogUserTypeMetadata>> theType = makeObject<Vector<CatalogUserTypeMetadata>>();
  catalogServer->getType(theType, typeID);

  if(theType->size() == 0) {
    ret.set("success", false);
    return ret;
  }

  // grab all the attributes
  int n = 0;
  mustache::data attributes = mustache::data::type::list;
  for(const auto &it :*(*theType)[0].getAttributes()){

    // the data of this attribute
    mustache::data attribute;

    // init the attributes
    attribute.set("type-name", it.type);
    attribute.set("attribute-name", it.name);
    attribute.set("size", std::to_string(it.size));
    attribute.set("offset", std::to_string(it.offset));
    attribute.set("is-static", it.isStatic ? "true" : "false");
    attribute.set("is-last", n == ((*theType)[0].getAttributes()->size() - 1));

    attributes.push_back(attribute);

    // increment
    n++;
  }

  // grab all the methods
  n = 0;
  mustache::data methods = mustache::data::type::list;
  for(const auto &it : *(*theType)[0].getMethods()) {

    // the data of this methods
    mustache::data method;

    // init the attributes
    method.set("method-name", it.name);
    method.set("method-symbol", it.symbol);
    method.set("return-type", it.returnType.name);
    method.set("return-type-size", std::to_string(it.returnType.size));
    method.set("is-last", n == ((*theType)[0].getMethods()->size() - 1));

    // init the parameters
    mustache::data parameters = mustache::data::type::list;

    // fill in the parameters
    int k = 0;
    for(const auto &jt : it.parameters) {

      // the data of this parameter
      mustache::data parameter;

      // set the
      parameter.set("name", jt.name);
      parameter.set("size", std::to_string(jt.size));
      parameter.set("is-last", k == (it.parameters.size() - 1));

      // add the parameter
      parameters.push_back(parameter);

      // increment
      k++;
    }

    // set the parameters
    method.set("parameters", parameters);

    // add the method
    methods.push_back(method);

    // increment
    n++;
  }

  // set the values
  ret.set("type-name", (std::string)(*theType)[0].getItemName());
  ret.set("type-id", (std::string)(*theType)[0].getObjectID());
  ret.set("attributes", attributes);
  ret.set("methods", methods);
  ret.set("success", true);

  return ret;
}
