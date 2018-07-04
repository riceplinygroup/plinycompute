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

#include "SymbolReader.h"
#include <err.h>
#include <fcntl.h>
#include <gelf.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <string>
#include <set>
#include <cxxabi.h>
#include <iostream>
#include <cassert>
#include <sstream>
#include <iterator>
#include <boost/regex.hpp>
#include <boost/algorithm/string/regex.hpp>
#include <PDBDebug.h>

namespace pdb {

SymbolReader::SymbolReader() {

  // true if it is loaded
  this->isLoaded = false;
}

SymbolReader::~SymbolReader() {

  // free the loaded resources
  if(isLoaded) {
    dwarfRes = dwarf_finish(dbg,&error);
    close(fd);
  }
}

bool SymbolReader::load(std::string fileName) {

  // set the file name
  this->fileName = fileName;

  // open the file
  fd = open(fileName.c_str(), O_RDONLY);
  if(fd < 0) {
    printf("Failure attempting to open %s\n", fileName.c_str());
    return false;
  }

  // init the dwarf processing
  dwarfRes = dwarf_init(fd, DW_DLC_READ, errhand, errarg, &dbg,&error);
  if(dwarfRes != DW_DLV_OK) {

    //close the file
    close(fd);

    // we failed to initialize the dwarf
    printf("Giving up, cannot do DWARF processing\n");
    return false;
  }

  // we managed to load this
  isLoaded = true;

  return true;
}

ClassInfo SymbolReader::getClassInformation(const std::type_info &typeInfo) {

  // grab the real name
  std::string realName;
  if(!realTypeName(typeInfo, realName)) {

    // the return
    ClassInfo ret;

    return ret;
  }

  // hierarchy
  std::vector<std::string> hierarchy;
  boost::algorithm::split_regex(hierarchy, realName, boost::regex("::"));

  return analyzeFile(hierarchy, [this](Dwarf_Debug dbg,
                                       Dwarf_Die in_die,
                                       int in_level,
                                       std::vector<std::string> &hierarchy,
                                       ClassInfo &ret) {
    this->searchForHierarchyInCu(dbg,
                                 in_die,
                                 in_level,
                                 hierarchy,
                                 ret); });
}

ClassInfo SymbolReader::getClassInformation(const std::string &typeSpec) {

  // hierarchy
  std::vector<std::string> hierarchy;
  boost::algorithm::split_regex(hierarchy, typeSpec, boost::regex("::"));

  return analyzeFile(hierarchy, [this](Dwarf_Debug dbg,
                                       Dwarf_Die in_die,
                                       int in_level,
                                       std::vector<std::string> &hierarchy,
                                       ClassInfo &ret) {
    this->searchForHierarchyInCu(dbg,
                                 in_die,
                                 in_level,
                                 hierarchy,
                                 ret); });
}


bool SymbolReader::realTypeName(const std::type_info& typeInfo, std::string &typeName) {

  // get the mangled name
  std::string mangledName = typeInfo.name();

  // demangle it
  int status = 0;
  const char *realName = abi::__cxa_demangle(mangledName.c_str(), nullptr, nullptr, &status);

  // did we succeed
  if(status != 0) {
    return false;
  }

  // set the real name as output
  typeName.assign(realName);

  // we succeeded
  return true;
}

ClassInfo SymbolReader::analyzeFile(std::vector<std::string> &hierarchy,
                                    std::function<void(Dwarf_Debug,
                                                       Dwarf_Die,
                                                       int,
                                                       std::vector<std::string> &,
                                                       ClassInfo &)> cuProcessor) {

  // the return stuff
  ClassInfo ret;

  Dwarf_Unsigned cu_header_length = 0;
  Dwarf_Half version_stamp = 0;
  Dwarf_Unsigned abbrev_offset = 0;
  Dwarf_Half address_size = 0;
  Dwarf_Unsigned next_cu_header = 0;
  Dwarf_Error error;
  int cu_number = 0;

  for (;; ++cu_number) {
    Dwarf_Die no_die = nullptr;
    Dwarf_Die cu_die = nullptr;
    int res = DW_DLV_ERROR;
    res = dwarf_next_cu_header(dbg,
                               &cu_header_length,
                               &version_stamp,
                               &abbrev_offset,
                               &address_size,
                               &next_cu_header,
                               &error);

    if (res == DW_DLV_ERROR) {
      printf("Error in dwarf_next_cu_header\n");
      exit(1);
    }

    if (res == DW_DLV_NO_ENTRY) {
      return ret;
    }

    // the CU will have a single sibling, a cu_die.
    res = dwarf_siblingof(dbg, no_die, &cu_die, &error);
    if (res == DW_DLV_ERROR) {
      printf("Error in dwarf_siblingof on CU die \n");
      return ret;
    }

    if (res == DW_DLV_NO_ENTRY) {

      // impossible case.
      printf("no entry! in dwarf_siblingof on CU die \n");
      return ret;
    }

    cuProcessor(dbg, cu_die, 0, hierarchy, ret);
    //getDieAndSiblings(dbg, cu_die, 0, hierarchy, ret);

    dwarf_dealloc(dbg, cu_die, DW_DLA_DIE);
  }
}

void SymbolReader::searchForHierarchyInCu(Dwarf_Debug dbg,
                                          Dwarf_Die in_die,
                                          int in_level,
                                          std::vector<std::string> &hierarchy,
                                          ClassInfo &ret) {

  int res = DW_DLV_ERROR;
  Dwarf_Die cur_die = in_die;
  Dwarf_Die child = nullptr;
  Dwarf_Error error;

  // if we exceeded the level
  if(in_level > hierarchy.size()) {
    return;
  }

  // grab the child
  res = dwarf_child(cur_die, &child, &error);

  // we are looking at this one
  cur_die = child;

  // traverse tree depth first
  while (res == DW_DLV_OK) {

    // is this the final class symbol
    if(isClassSymbol(dbg, cur_die, hierarchy.back()) && (in_level == hierarchy.size() - 1)) {
      extractClassInfo(cur_die, hierarchy, ret);
    }
      // is this a namespace or a class
    else if(isNamespaceOrClass(dbg, cur_die, hierarchy[in_level])) {
      searchForHierarchyInCu(dbg, cur_die, in_level + 1, hierarchy, ret);
    }

    res = dwarf_siblingof(dbg, cur_die, &cur_die, &error);
  };
}

bool SymbolReader::searchForTypeInCu(Dwarf_Debug dbg,
                                     Dwarf_Die in_die,
                                     int in_level,
                                     std::vector<std::string> &hierarchy,
                                     Dwarf_Die nameDie) {
  int res = DW_DLV_ERROR;
  Dwarf_Die cur_die = in_die;
  Dwarf_Die child = nullptr;
  Dwarf_Error error;

  // grab the child
  res = dwarf_child(cur_die, &child, &error);

  // we are looking at this one
  cur_die = child;

  // traverse tree depth first
  while (res == DW_DLV_OK) {

    // is this the final type symbol
    if(cur_die == nameDie) {
      return true;
    }

    if(searchForTypeInCu(dbg, cur_die, in_level + 1, hierarchy, nameDie)) {
      hierarchy.insert(hierarchy.begin(), getNamespaceOrClassName(dbg, cur_die));
      return true;
    }

    res = dwarf_siblingof(dbg, cur_die, &cur_die, &error);
  };

  return false;
}


std::string SymbolReader::getNamespaceOrClassName(Dwarf_Debug dbg, Dwarf_Die print_me) {

  // the return value
  std::string ret;

  // check if this entry is the symbol of the class
  char *name = nullptr;
  int gotName = !dwarf_diename(print_me, &name, &error);

  // grab the tag
  Dwarf_Half tag = 0;
  int gotTagName = !dwarf_tag(print_me, &tag, &error);

  // if it does have a name
  if(gotName && gotTagName && (tag == DW_TAG_class_type || tag == DW_TAG_namespace)) {
    ret.assign(name);
  }

  // deallocate the name
  dwarf_dealloc(dbg, name, DW_DLA_STRING);

  // return the value
  return ret;
}

bool SymbolReader::isClassSymbol(Dwarf_Debug dbg, Dwarf_Die print_me, std::string &realName) {

  // the return value
  bool ret = false;

  // check if this entry is the symbol of the class
  char *name = nullptr;
  int gotName = !dwarf_diename(print_me, &name, &error);

  // grab the tag
  Dwarf_Half tag = 0;
  int gotTagName = !dwarf_tag(print_me, &tag, &error);

  // if it does have a name
  if(gotName && gotTagName) {

    // is this our guy
    ret = std::string(name) == realName && tag == DW_TAG_class_type;
  }

  // deallocate the name
  dwarf_dealloc(dbg, name, DW_DLA_STRING);

  // return the value
  return ret;
}

bool SymbolReader::isAttributeSymbol(Dwarf_Die print_me) {

  // grab the tag
  Dwarf_Half tag = 0;
  int gotTagName = !dwarf_tag(print_me, &tag, &error);

  if(gotTagName) {
    return tag == DW_TAG_member;
  }

  return false;
}

bool SymbolReader::isMethodSymbol(Dwarf_Die print_me) {

  // grab the tag
  Dwarf_Half tag = 0;
  int gotTagName = !dwarf_tag(print_me, &tag, &error);

  if(gotTagName) {
    return tag == DW_TAG_subprogram;
  }

  return false;
}

bool SymbolReader::isNamespaceOrClass(Dwarf_Debug dbg, Dwarf_Die print_me, std::string &realName) {

  // the return value
  bool ret = false;

  // check if this entry is the symbol of the class
  char *name = nullptr;
  int gotName = !dwarf_diename(print_me, &name, &error);

  // grab the tag
  Dwarf_Half tag = 0;
  int gotTagName = !dwarf_tag(print_me, &tag, &error);

  // if it does have a name
  if(gotName && gotTagName) {

    // is this our guy
    ret = std::string(name) == realName && (tag == DW_TAG_class_type || tag == DW_TAG_namespace);
  }

  // deallocate the name
  dwarf_dealloc(dbg, name, DW_DLA_STRING);

  // return the value
  return ret;
}

/// TODO do better error handling
void SymbolReader::parseAttribute(Dwarf_Die cur_die, ClassInfo &ret) {

  char *name = nullptr;
  Dwarf_Attribute attr;
  Dwarf_Unsigned offset;
  Dwarf_Off typeOffset;
  Dwarf_Die typeDie;

  // grab the offset of the attribute
  int gotOffset = !dwarf_attr(cur_die, DW_AT_data_member_location, &attr, &error) &&
      !dwarf_formudata(attr, &offset, &error);

  // this has to hold
  assert(gotOffset);

  // check if this entry is the symbol of the class
  int gotName = !dwarf_attr(cur_die, DW_AT_name, &attr, &error) &&
      !dwarf_diename(cur_die, &name, &error);

  // this has to hold
  assert(gotName);

  // grab the offset to the type from the attribute
  int gotTypeOffset = !dwarf_attr(cur_die, DW_AT_type, &attr, &error) && !dwarf_global_formref(attr, &typeOffset, &error);

  // this has to hold
  assert(gotTypeOffset);

  // grab the type die
  int gotTypeDie = !dwarf_offdie_b(dbg, typeOffset, 1, &typeDie, &error);

  // this has to hold
  assert(gotTypeDie);

  std::string tmp;
  auto type = getType(typeDie, nullptr, tmp, 0, false);

  // set the extracted information
  AttributeInfo atInfo{};
  atInfo.name.assign(name);
  atInfo.size = type.size;
  atInfo.type.assign(type.name);
  atInfo.offset = offset;

  // add the attribute
  ret.attributes->push_back(atInfo);

  // free the memory
  dwarf_dealloc(dbg, name, DW_DLA_STRING);
}

AttributeType SymbolReader::getType(Dwarf_Die curDie, Dwarf_Die nameDie, std::string &previousName, unsigned int isPointer, bool isReference) {

  char *typeName = nullptr;
  Dwarf_Attribute attr;
  Dwarf_Unsigned size;
  Dwarf_Off typeOffset;
  Dwarf_Die typeDie;
  Dwarf_Half tag = 0;

  // grab the tag
  int gotTagName = !dwarf_tag(curDie, &tag, &error);

  // check if we have a tag if not return an empty result
  if(!gotTagName) {
    return AttributeType("", 0);
  }

  switch (tag) {

    case DW_TAG_const_type:
    case DW_TAG_typedef: {

      // the const type and type defs references the actual type therefore we must follow the reference
      int gotTypeOffset = !dwarf_attr(curDie, DW_AT_type, &attr, &error) &&
          !dwarf_global_formref(attr, &typeOffset, &error) &&
          !dwarf_offdie_b(dbg, typeOffset, 1, &typeDie, &error);

      // the base type has a name and size so we grab that
      int gotTypeName = !dwarf_attr(typeDie, DW_AT_name, &attr, &error) &&
          !dwarf_diename(typeDie, &typeName, &error);

      // check if there is something wrong
      if(!gotTypeOffset) {
        return AttributeType("", 0);
      }

      // convert the thing to a std string and free the memory
      if(gotTypeName) {
        nameDie = curDie;
        previousName.assign(typeName);
        dwarf_dealloc(dbg, typeName, DW_DLA_STRING);
      }

      // follow the type
      return getType(typeDie, nameDie, previousName, isPointer, isReference);
    }
    case DW_TAG_base_type:
    case DW_TAG_structure_type:
    case DW_TAG_class_type:
    case DW_TAG_union_type: {

      // the base type has a name and size so we grab that
      int gotTypeName = !dwarf_attr(curDie, DW_AT_name, &attr, &error) &&
          !dwarf_diename(curDie, &typeName, &error);

      // grab the size of the attribute
      int gotByteSize = !dwarf_attr(curDie, DW_AT_byte_size, &attr, &error) &&
          !dwarf_formudata(attr, &size, &error);


      // check if this thing has a size if it does not it might be a forward declaration
      if(!gotByteSize) {
        size = 0;
      }

      // do we have a pointer suffix or not
      std::string suffix = isPointer ? std::string(isPointer, '*') : "";
      suffix += isReference ? "&" : "";
      size = isPointer ? sizeof(int*) : size;

      // get the full name
      std::string fullName;

      // if we don't have the type name but have the size return the thing with the previous name
      if(!gotTypeName) {

        bool getFullName = getFullTypeName(nameDie, fullName);

        // return the name with the namespace
        return AttributeType(getFullName ? fullName + suffix : previousName + suffix, size);
      }

      // get the namespace of the type we got the name
      bool getFullName = getFullTypeName(curDie, fullName);

      // create the return value
      auto ret = AttributeType(getFullName ? fullName + suffix : std::string(typeName) + suffix, size);

      // free the memory
      dwarf_dealloc(dbg, typeName, DW_DLA_STRING);

      return ret;
    }
    case DW_TAG_pointer_type: {

      // try to find the root type of the pointer
      int gotTypeOffset = !dwarf_attr(curDie, DW_AT_type, &attr, &error) &&
          !dwarf_global_formref(attr, &typeOffset, &error) &&
          !dwarf_offdie_b(dbg, typeOffset, 1, &typeDie, &error);

      // if we can not go to any other object but this is a pointer we assume this is a void*
      if(!gotTypeOffset) {
        std::string pointerSuffix = isPointer ? std::string(isPointer, '*') : "";
        std::string referenceSuffix = isReference ? "&" : "";
        return AttributeType("void*" + pointerSuffix + referenceSuffix, sizeof(void*));
      }

      // follow the type
      return getType(typeDie, nameDie, previousName, ++isPointer, isReference);
    }
    case DW_TAG_reference_type: {

      // try to find the root type of the reference
      int gotTypeOffset = !dwarf_attr(curDie, DW_AT_type, &attr, &error) &&
          !dwarf_global_formref(attr, &typeOffset, &error) &&
          !dwarf_offdie_b(dbg, typeOffset, 1, &typeDie, &error);

      // something went wrong there
      if(!gotTypeOffset) {
        return AttributeType("", 0);
      }

      // follow the type
      return getType(typeDie, nameDie, previousName, ++isPointer, true);
    }
    default: {

      // this should not happen
      return AttributeType("", 0);
    }
  }
}

void SymbolReader::parseMethod(Dwarf_Die curDie, ClassInfo &info) {

  // the method info
  MethodInfo ret{};

  char *name = nullptr;
  char *symbol = nullptr;
  Dwarf_Attribute attr;
  Dwarf_Off typeOffset;
  Dwarf_Die typeDie;
  Dwarf_Die child = nullptr;
  Dwarf_Die sibling = nullptr;
  std::vector<AttributeType> parameters;

  // check if this entry is the symbol of the class
  int gotName = !dwarf_attr(curDie, DW_AT_name, &attr, &error) &&
      !dwarf_diename(curDie, &name, &error);

  // check if this entry is the symbol of the class
  int gotSymbol = !dwarf_attr(curDie, DW_AT_linkage_name, &attr, &error) &&
      !dwarf_die_text(curDie, DW_AT_linkage_name, &symbol, &error);

  // check if it has a return type
  int hasReturn = !dwarf_attr(curDie, DW_AT_type, &attr, &error) &&
      !dwarf_global_formref(attr, &typeOffset, &error) &&
      !dwarf_offdie_b(dbg, typeOffset, 1, &typeDie, &error);

  // this has not return value so set it to void
  if(!hasReturn) {
    ret.returnType.name.assign("void");
    ret.returnType.size = 0;
  }
  else {
    std::string emp;
    auto tmp = getType(typeDie, nullptr, emp, 0, false);

    // copy the type
    ret.returnType.name.assign(tmp.name);
    ret.returnType.size = sizeof(tmp.size);
  }

  // grab the parameters (they are children of the method and have the tag DW_TAG_formal_parameter)
  auto res = dwarf_child(curDie, &child, &error);

  // go through each sibling to get the next one
  sibling = child;
  std::pair<std::string, size_t> tmp;
  while (res == DW_DLV_OK) {

    // grab the tag
    Dwarf_Half tag = 0;
    int gotTagName = !dwarf_tag(sibling, &tag, &error);

    // check if this is a DW_TAG_formal_parameter if it is not skip it (although this might indicate an error)
    if(!gotTagName || tag != DW_TAG_formal_parameter) {
      continue;
    }

    // grab the type
    int hasType = !dwarf_attr(sibling, DW_AT_type, &attr, &error) &&
        !dwarf_global_formref(attr, &typeOffset, &error) &&
        !dwarf_offdie_b(dbg, typeOffset, 1, &typeDie, &error);

    // check if this has a type attribute if id does not skip (although this might indicate an error)
    if(!hasType) {
      continue;
    }

    // grab the type of the parameters
    std::string emp;
    auto type = getType(typeDie, nullptr, emp, 0, false);

    // store the parameter
    parameters.emplace_back(type);

    res = dwarf_siblingof(dbg, sibling, &sibling, &error);
  }

  // get the name of the method
  if(gotName) {
    ret.name.assign(name);
  }

  // do we have a symbol
  if(gotSymbol) {
    ret.symbol.assign(symbol);
  }

  // copy parameter list
  ret.parameters = parameters;

  // free the memory
  dwarf_dealloc(dbg, name, DW_DLA_STRING);
  dwarf_dealloc(dbg, symbol, DW_DLA_STRING);

  // store the parsed method
  info.methods->emplace_back(ret);
}

void SymbolReader::extractClassInfo(Dwarf_Die in_die, std::vector<std::string> &hierarchy, ClassInfo &ret) {

  Dwarf_Die child = nullptr;
  Dwarf_Die sib_die = nullptr;
  Dwarf_Die cur_die = in_die;

  // concatenate the hierarchy
  ret.className.clear();
  for(auto &it : hierarchy) {
    ret.className += it + "::";
  }

  // remove the last two colons
  ret.className.pop_back();
  ret.className.pop_back();

  // grab the child
  auto res = dwarf_child(cur_die, &child, &error);

  // traverse tree depth first
  if (res == DW_DLV_OK) {

    sib_die = child;
    while (res == DW_DLV_OK) {
      cur_die = sib_die;

      if(isAttributeSymbol(cur_die)){
        parseAttribute(cur_die, ret);
      }
      else if(isMethodSymbol(cur_die)) {
        parseMethod(cur_die, ret);
      }

      res = dwarf_siblingof(dbg, cur_die, &sib_die, &error);
    };
  }
}

bool SymbolReader::getFullTypeName(Dwarf_Die typeDie, std::string &outTypeName) {

  Dwarf_Signed numberOfTypes;
  Dwarf_Type *types;
  Dwarf_Error err;
  Dwarf_Off dieOffset;
  Dwarf_Off curOffset;
  int result;
  char *typeName;

  // grab the offset of the die
  result = dwarf_dieoffset(typeDie, &dieOffset, &err);
  if (result != DW_DLV_OK) {

    PDB_COUT << "Could not determine the die offset. \n";

    return false;
  }

  // grab the types
  result = dwarf_get_pubtypes(dbg, &types, &numberOfTypes, &err);
  if (result != DW_DLV_OK) {

    // log the error
    PDB_COUT << "Could not find the public types. Problem in the debug data \n";

    // return an empty string
    outTypeName.assign("");

    return false;
  }

  // iterate over the returned array of descriptors.
  for (int i = 0; i < numberOfTypes; i++) {

    // grab the offset
    result = dwarf_pubtype_type_die_offset(types[i], &curOffset, &err);

    // if it is fine and it matches the offset
    if (result == DW_DLV_OK && dieOffset == curOffset) {

      // grab the full name
      result = dwarf_pubtypename(types[i], &typeName, &err);

      // if we got it
      if(result == DW_DLV_OK) {
        outTypeName.assign(typeName);
        return true;
      }

      // log the error
      PDB_COUT << "Could not find the full type. Problem in the debug data \n";

      // return an empty string
      outTypeName.assign("");
    }
  }

  // deallocate the returned array.
  dwarf_types_dealloc(dbg, types, numberOfTypes);

  // log the error
  PDB_COUT << "Could not find the the type, type is private.";

  // return the namespace
  return false;
}

}
