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

#ifndef TESTINGCLASSSTUFF_SYMBOLREADER_H
#define TESTINGCLASSSTUFF_SYMBOLREADER_H

#include <libelf.h>
#include <gelf.h>
#include <string>
#include <libdwarf/dwarf.h>
#include <libdwarf/libdwarf.h>
#include <vector>
#include <memory>

namespace pdb {

struct attributeType {

  attributeType() = default;

  /**
   * Constructor with parameters
   * @param name - the name of the type
   * @param size - the size of the type in bytes
   */
  attributeType(const std::string &name, size_t size) : name(name), size(size) {}

  /**
   * The name of the type
   */
  std::string name;

  /**
   * The size of the type
   */
  size_t size = 0;

};

struct attributeInfo {

  /**
   * The name of the attribute
   */
  std::string name;

  /**
   * The size of the attribute
   */
  size_t size;

  /**
   * The offset of the attribute
   */
  size_t offset;

  /**
   * The type of the attribute
   */
  std::string type;
};

struct methodInfo {

  /**
   * The name of the method
   */
  std::string name;

  /**
   * The symbol of the method
   */
  std::string symbol;

  /**
   * The return type of the method
   */
  attributeType returnType;


  /**
   * The parameters of the method
   */
  std::vector<attributeType> parameters;

};

struct classInfo {

  /**
   * Define the default constructor
   */
  classInfo() {
    attributes = std::make_shared<std::vector<attributeInfo>>();
    methods = std::make_shared<std::vector<methodInfo>>();
  }

  /**
   * The class name with the full namespace specification
   */
  std::string className;

  /**
   * The attributes of the class
   */
  std::shared_ptr<std::vector<attributeInfo>> attributes;

  /**
   * The methods of the class
   */
  std::shared_ptr<std::vector<methodInfo>> methods;

};

class SymbolReader {

 public:

  /**
   * Initialize the SymbolReader
   */
  explicit SymbolReader();

  /**
   * Destroy the SymbolReader
   */
  virtual ~SymbolReader();

  /**
   * Loads the file returns false if it fails or true if it can be loaded
   * @return true if it succeed false otherwise
   */
  bool load(std::string &fileName);

  /**
   *
   * @param typeInfo
   * @return
   */
  classInfo getClassInformation(const std::type_info &typeInfo);

  /**
   *
   * @param typeSpec
   * @return
   */
  classInfo getClassInformation(const std::string &typeSpec);

 private:

  /**
   * Is this loaded
   */
  bool isLoaded;

  /**
   * The name of file we are opening
   */
  std::string fileName;

  /**
   * The file table index of the file we need to load
   */
  int fd = -1;

  /**
   * The dwarf resource
   */
  int dwarfRes = DW_DLV_ERROR;

  Dwarf_Debug dbg = nullptr;
  Dwarf_Error error;
  Dwarf_Handler errhand = nullptr;
  Dwarf_Ptr errarg = nullptr;

  bool realTypeName(const std::type_info& typeInfo, std::string &typeName);

  classInfo analyzeFile(std::vector<std::string> &realName);

  void getDieAndSiblings(Dwarf_Debug dbg, Dwarf_Die in_die, int in_level, std::vector<std::string> &hierarchy, classInfo &ret);

  bool isClassSymbol(Dwarf_Debug dbg, Dwarf_Die print_me, std::string &realName);

  bool isNamespaceOrClass(Dwarf_Debug dbg, Dwarf_Die print_me, std::string &realName);

  bool isAttributeSymbol(Dwarf_Die print_me);

  bool isMethodSymbol(Dwarf_Die print_me);

  void parseAttribute(Dwarf_Die cur_die, classInfo &ret);

  attributeType getType(Dwarf_Die cur_die, std::string &previousName, unsigned int isPointer);

  void extractClassInfo(Dwarf_Debug dbg, Dwarf_Die in_die, std::vector<std::string> &hierarchy, classInfo &ret);

  void parseMethod(Dwarf_Die curDie, classInfo &info);
};

}



#endif //TESTINGCLASSSTUFF_SYMBOLREADER_H
