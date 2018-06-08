#!/usr/bin/python
#  Copyright 2018 Rice University                                           
#                                                                           
#  Licensed under the Apache License, Version 2.0 (the "License");          
#  you may not use this file except in compliance with the License.         
#  You may obtain a copy of the License at                                  
#                                                                           
#      http://www.apache.org/licenses/LICENSE-2.0                           
#                                                                           
#  Unless required by applicable law or agreed to in writing, software      
#  distributed under the License is distributed on an "AS IS" BASIS,        
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
#  See the License for the specific language governing permissions and      
#  limitations under the License.                                           
#  ======================================================================== 
import os
import re
from sys import platform as _platform
import glob
from os import path

# script that installs dependencies required by PlinyCompute 

#
#Name	Homepage	Ubutnu Packages
#Snappy	https://github.com/google/snappy	libsnappy1v5, libsnappy-dev
#GSL	https://www.gnu.org/software/gsl/	libgsl-dev
#Eigen                                          libeigen3-dev
#Boost	http://www.boost.org/	libboost-dev, libboost-program-options-dev, libboost-filesystem-dev, libboost-system-dev
#Bison	https://www.gnu.org/software/bison/	bison
#Flex	https://github.com/westes/flex	flex
#

if _platform == 'darwin':
    print ("It is MacOS")
    os.system("brew install ossp-uuid")
    os.system("brew install gsl")
    os.system("brew install snappy")
    os.system("brew install bison")
    os.system("brew install flex")
    os.system("brew install eigen")
elif _platform == 'linux' or _platform == "linux2":
    print ("It is Linux, try apt-get install") 		
    os.system("sudo apt-get -y install uuid-dev")
    os.system("sudo apt-get -y install libeigen3-dev")
    os.system("sudo apt-get -y install libgsl-dev")
    os.system("sudo apt-get -y install libsnappy1v5 libsnappy-dev")
    os.system("sudo apt-get -y install libboost-dev libboost-program-options-dev libboost-filesystem-dev libboost-system-dev")
    os.system("sudo apt-get -y install bison flex")
else:
    print ("Platform " + _platform + " is not supported.")


