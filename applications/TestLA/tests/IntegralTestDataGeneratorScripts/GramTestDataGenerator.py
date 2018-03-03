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
# The idea is stolen from Shangyu's ICDE paper, created by Binhang June 2017
import random
import sys
import os


path = "./applications/TestLA/tests/Benchmark/"
if not os.path.exists(path):
	os.makedirs(path)


data_num = int(sys.argv[1])
dim = int(sys.argv[2])
blockRowSize = int(sys.argv[3])
blockColSize = int(sys.argv[4])
assert data_num% blockRowSize==0 and dim%blockColSize ==0
blockRowNum = data_num/blockRowSize
blockColNum = dim/blockColSize


fileName = path+"Gram_M_"+str(blockRowSize)+"_"+str(blockColSize)+"_"+str(blockRowNum)+"_"+str(blockColNum)+".data"
blocks = open(fileName,"w")
code = open(path+"Task01_Gram_"+str(data_num)+"_"+str(dim)+"_"+str(blockRowSize)+"_"+str(blockColSize)+".pdml", "w")


print "data_num: " + str(data_num) + "	dim: " + str(dim) + "  block row size: " +str(blockRowSize) + "  block col size: " + str(blockColSize) + "  block row number: "+ str(blockRowNum) +"  block col number: "+str(blockColNum)


data = []
for i in xrange(data_num):
    row = []    
    for j in xrange(dim):
        row.append(random.random())
    data.append(row)
print "Generate Data is done!"


for i in xrange(data_num/blockRowSize):
	for j in xrange(dim/blockColSize):
		blocks.write(str(i))
		blocks.write(" ")
		blocks.write(str(j))
		blocks.write(" ")
		for ii in xrange(blockRowSize):
			for jj in xrange(blockColSize):
				blocks.write(str(data[i*blockRowSize+ii][j*blockColSize+jj]))
				blocks.write(" ")
		blocks.write("\n")


code.write("X = load("+str(blockRowSize)+","+str(blockColSize)+","+str(blockRowNum)+","+str(blockColNum)+',\"'+fileName+'\")\n')
code.write("M = X '* X\n")

blocks.close()
code.close()
print "Write to PDB is done!"