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
# The idea is stolen from Shangyu's ICDE paper, created by Binhang, June 2017
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


fileNameX = path+"L2_X_"+str(data_num)+"_"+str(dim)+"_"+str(blockRowSize)+"_"+str(blockColSize)+".data"
blocks_X = open(fileNameX, "w")
fileNamey = path+"L2_y_"+str(data_num)+"_"+str(blockRowSize)+".data"
blocks_y = open(fileNamey, "w")
code = open(path+"Task02_L2_"+str(data_num)+"_"+str(dim)+"_"+str(blockRowSize)+"_"+str(blockColSize)+".pdml", "w")

print "data_num: " + str(data_num) + "  dim: " + str(dim) + "  block row size: " +str(blockRowSize) + "  block col size: " + str(blockColSize) + "  block row number: "+ str(blockRowNum) +"  block col number: "+str(blockColNum)


b=[]
for i in range(dim):
    b.append(random.random())

X = []
y = []
for i in xrange(data_num):
    x_i = []
    y_i = 0.0
    for j in xrange(dim):
        a = random.random()
        x_i.append(a)
        y_i += b[j]*a
    y_i += random.gauss(0,1.0) 
    X.append(x_i)
    y.append(y_i)
print "Generate Data is done!"


for i in xrange(data_num/blockRowSize):
    for j in xrange(dim/blockColSize):
        blocks_X.write(str(i))
        blocks_X.write(" ")
        blocks_X.write(str(j))
        blocks_X.write(" ")
        for ii in xrange(blockRowSize):
            for jj in xrange(blockColSize):
                blocks_X.write(str(X[i*blockRowSize+ii][j*blockColSize+jj]))
                blocks_X.write(" ")
        blocks_X.write("\n")
    blocks_y.write(str(i))
    blocks_y.write(" ")
    blocks_y.write("0")
    blocks_y.write(" ")
    for ii in xrange(blockRowSize):
        blocks_y.write(str(y[i*blockRowSize+ii]))
    blocks_y.write("\n")
blocks_X.close()
blocks_y.close()

code.write("X = load("+str(blockRowSize)+","+str(blockColSize)+","+str(blockRowNum)+","+str(blockColNum)+',\"'+fileNameX+'\")\n')
code.write("y = load("+str(blockRowSize)+", 1, "+str(blockRowNum,)+ ', 1, \"'+fileNamey+'\")\n')
code.write("beta = (X '* X)^-1 %*% (X '* y)")
code.close()
print "Write to PDB is done!"