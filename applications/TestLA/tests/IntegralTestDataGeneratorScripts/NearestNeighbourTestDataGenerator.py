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
# This a new experiments to replace ICDE paper's outlier dectect task, created by Binhang, June 2017
import random
import numpy as np
import sys
import os


path = "./applications/TestLA/tests/Benchmark/"
if not os.path.exists(path):
    os.makedirs(path)


data_num = int(sys.argv[1])
dim = int(sys.argv[2])
blockRowSize = int(sys.argv[3])
blockColSize = int(sys.argv[4])
blockRowNum = data_num/blockRowSize
blockColNum = dim/blockColSize 
assert data_num% blockRowSize==0 and dim%blockColSize ==0

print "data_num: " + str(data_num) + "  dim: " + str(dim) + "  block row size: " +str(blockRowSize) + "  block col size: " + str(blockColSize) + "  block row number: "+ str(blockRowNum) +"  block col number: "+str(blockColNum)
# Generate the data
t=[]
for i in range(dim):
    t.append(random.random())

X = []
for i in xrange(data_num):
    x_i = []
    for j in xrange(dim):
        x_i.append(random.random()) 
    X.append(x_i)

# M = y^Ty, m should be positive definite
y = np.zeros((dim, dim))
M = np.zeros((dim, dim))
while (1):
    for i in range(0, dim):
        for j in range(0, dim):
            y[i, j] = random.random()
    y_matrix = np.matrix(y)
    M = np.dot(y_matrix.getT(), y_matrix)
    if np.all(np.linalg.eigvals(M) > 0):
        break
print "Generate Data is done!"



fileNameX = path+"NN_X_"+str(data_num)+"_"+str(dim)+"_"+str(blockRowSize)+"_"+str(blockColSize)+".data"
blocks_X = open(fileNameX, "w")
fileNamet = path+"NN_t_"+str(dim)+"_"+str(blockColSize)+".data"
blocks_t = open(fileNamet, "w")
fileNameM = path+"NN_M_"+str(dim)+"_"+str(blockColSize)+".data"
blocks_M = open(fileNameM, "w")
codePDB = open(path+"Task03_NN_"+str(data_num)+"_"+str(dim)+"_"+str(blockRowSize)+"_"+str(blockColSize)+".pdml","w")

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
blocks_X.close()

for j in xrange(dim/blockColSize):
    blocks_t.write("0")
    blocks_t.write(" ")
    blocks_t.write(str(j))
    blocks_t.write(" ")
    for jj in xrange(blockColSize):
        blocks_t.write(str(t[j*blockColSize+jj]))
    blocks_t.write("\n")
blocks_t.close()

for i in xrange(dim/blockColSize):
    for j in xrange(dim/blockColSize):
        blocks_M.write(str(i))
        blocks_M.write(" ")
        blocks_M.write(str(j))
        blocks_M.write(" ")
        for ii in xrange(blockColSize):
            for jj in xrange(blockColSize):
                blocks_M.write(str(M[i*blockColSize+ii, j*blockColSize+jj]))
                blocks_M.write(" ")
        blocks_M.write("\n")
blocks_M.close()
# Automatically generate code for pliny DSL
codePDB.write("X = load("+str(blockRowSize)+","+str(blockColSize)+","+str(blockRowNum)+","+str(blockColNum)+',\"'+fileNameX+'\")\n')
codePDB.write("t = load(1, "+str(blockColSize)+", 1, "+str(blockColNum,)+ ', \"'+fileNamet+'\")\n')
codePDB.write("M = load("+str(blockColSize)+","+str(blockColSize)+","+str(blockColNum)+","+str(blockColNum)+',\"'+fileNameM+'\")\n')
codePDB.write("D = X - duplicateRow(t,"+str(blockRowSize)+","+str(blockRowNum)+")\n")
codePDB.write("i = min(rowSum(D %*% M * D))")
codePDB.close()
print "Write to PDB is done!"