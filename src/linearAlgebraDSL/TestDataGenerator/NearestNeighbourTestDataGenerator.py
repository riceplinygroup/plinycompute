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
import time
import random
import numpy as np
import sys


data_num = int(sys.argv[1])
dim = int(sys.argv[2])
blockRowSize = int(sys.argv[3])
blockColSize = int(sys.argv[4])
blockRowNum = data_num/blockRowSize
blockColNum = dim/blockColSize 
assert data_num% blockRowSize==0 and dim%blockColSize ==0

fileNameX_csv = "./src/linearAlgebraDSL/TestDataGenerator/NN_X_"+str(data_num)+"_"+str(dim)+".csv"
lines_X = open(fileNameX_csv, "w")
lines_mtd_X = open(fileNameX_csv+".mtd", "w")
fileNamet_csv = "./src/linearAlgebraDSL/TestDataGenerator/NN_t_"+str(data_num)+".csv"
lines_t = open(fileNamet_csv, "w")
lines_mtd_t = open(fileNamet_csv+".mtd", "w")
fileNameM_csv = "./src/linearAlgebraDSL/TestDataGenerator/NN_M_"+str(dim)+".csv"
lines_M = open(fileNameM_csv, "w")
lines_mtd_M = open(fileNameM_csv+".mtd", "w")

fileNameX = "./src/linearAlgebraDSL/TestDataGenerator/NN_X_"+str(data_num)+"_"+str(dim)+"_"+str(blockRowSize)+"_"+str(blockColSize)+".data"
blocks_X = open(fileNameX, "w")
fileNamet = "./src/linearAlgebraDSL/TestDataGenerator/NN_t_"+str(dim)+"_"+str(blockColSize)+".data"
blocks_t = open(fileNamet, "w")
fileNameM = "./src/linearAlgebraDSL/TestDataGenerator/NN_M_"+str(dim)+"_"+str(blockColSize)+".data"
blocks_M = open(fileNameM, "w")

fileNameX_txt = "./src/linearAlgebraDSL/TestDataGenerator/NN_X_"+str(data_num)+"_"+str(dim)+".txt"
spark_X = open(fileNameX_txt,"w")
fileNamet_txt = "./src/linearAlgebraDSL/TestDataGenerator/NN_t_"+str(data_num)+".txt"
spark_t = open(fileNamet_txt,"w")
fileNameM_txt = "./src/linearAlgebraDSL/TestDataGenerator/NN_M_"+str(dim)+".txt"
spark_M = open(fileNameM_txt,"w")

code = open("./src/linearAlgebraDSL/DSLSamples/Task03_NN_"+str(int(time.time()))+".pdml","w")
codeSystemML = open("./src/linearAlgebraDSL/DSLSamples/Task03_NN_SystemML_"+str(int(time.time()))+".dml","w")
scriptSpark = open("./src/linearAlgebraDSL/DSLSamples/Task03_NN_Spark_"+str(int(time.time()))+".sh","w")


print "data_num: " + str(data_num) + "  dim: " + str(dim) + "  block row size: " +str(blockRowSize) + "  block col size: " + str(blockColSize) + "  block row number: "+ str(blockRowNum) +"  block col number: "+str(blockColNum)

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



for i in xrange(data_num):
    for j in xrange(dim-1):
        lines_X.write(str(X[i][j]))
        lines_X.write(str(", "))
    lines_X.write(str(str(X[i][dim-1])))
    lines_X.write("\n")
lines_X.close()

for j in xrange(dim-1):
    lines_t.write(str(t[j]))
    lines_t.write(", ")
lines_t.write(str(t[dim-1]))
lines_t.write("\n")
lines_t.close()

for i in xrange(dim):
    for j in xrange(dim-1):
        lines_M.write(str(M[i,j]))
        lines_M.write(", ")
    lines_M.write(str(M[i,dim-1]))
    lines_M.write("\n")
lines_M.close()    

lines_mtd_X.write("{\"rows\": " + str(data_num) +", \"cols\": " + str(dim) + ", \"format\": \"csv\"}")
lines_mtd_t.write("{\"rows\": " + str(1) +", \"cols\": " + str(dim) + ", \"format\": \"csv\"}")
lines_mtd_M.write("{\"rows\": " + str(dim) +", \"cols\": " + str(dim) + ", \"format\": \"csv\"}")
lines_mtd_X.close()
lines_mtd_t.close()
lines_mtd_M.close()

for i in xrange(data_num):
    spark_X.write(str(i))
    for j in xrange(dim):
        spark_X.write(",")
        spark_X.write(str(X[i][j]))
    spark_X.write("\n")
spark_X.close()

for j in xrange(dim-1):
    spark_t.write(str(t[j]))
    spark_t.write(",")
spark_t.write(str(t[dim-1]))
spark_t.write("\n")
spark_t.close()

for i in xrange(dim):
    spark_M.write(str(i))
    for j in xrange(dim):
        spark_M.write(",")
        spark_M.write(str(M[i,j]))
    spark_M.write("\n")
spark_M.close()



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
code.write("X = load("+str(blockRowSize)+","+str(blockColSize)+","+str(blockRowNum)+","+str(blockColNum)+',\"'+fileNameX+'\")\n')
code.write("t = load(1, "+str(blockColSize)+", 1, "+str(blockColNum,)+ ', \"'+fileNamet+'\")\n')
code.write("M = load("+str(blockColSize)+","+str(blockColSize)+","+str(blockColNum)+","+str(blockColNum)+',\"'+fileNameM+'\")\n')
code.write("D = X - duplicateRow(t,"+str(blockRowSize)+","+str(blockRowNum)+")\n")
code.write("i = min(rowSum(D %*% M * D))")
code.close()

# Automatically generate code for SystemML DSL
codeSystemML.write('X = read(\"'+fileNameX_csv+'\");\n')
codeSystemML.write('t = read(\"'+fileNamet_csv+'\");\n')
codeSystemML.write('M = read(\"'+fileNameM_csv+'\");\n')
codeSystemML.write("duplicateRow = function (matrix[double] row, int nums) return (matrix[double] rows) {\n")
codeSystemML.write("\trows = row;\n")
codeSystemML.write("\tfor (i in 2:nums) {\n")
codeSystemML.write("\t\trows = rbind(rows,row);\n")
codeSystemML.write("\t}\n")
codeSystemML.write("}\n")
codeSystemML.write("Dup = duplicateRow(t, nrow(X));\n")
codeSystemML.write("Diff = X - Dup;\n")
codeSystemML.write("i = rowIndexMin(t(rowSums(Diff %*% M * Diff)));\n")
codeSystemML.write('print(\"Nearest item index:\" + toString(i, decimal=0));\n')
codeSystemML.close()

# Automatically generate script to run spark
scriptSpark.write('YOUR_SPARK_HOME/bin/spark-submit ')
scriptSpark.write('--class \"NearestNeighbourSearch\" ')
scriptSpark.write('--master local[4] ')
scriptSpark.write('target/scala-2.11/NearestNeighbourSearch_2.11-1.0.jar ')
scriptSpark.write(str(data_num))
scriptSpark.write(" ")
scriptSpark.write(str(dim))
scriptSpark.write(" ")
scriptSpark.write(fileNameX_txt)
scriptSpark.write(" ")
scriptSpark.write(fileNamet_txt)
scriptSpark.write(" ")
scriptSpark.write(fileNameM_txt)
scriptSpark.close()

