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


sparkFlag = False
systemMLFlag = False
PDBFlag = True
SciDBFlag = False

data_num = int(sys.argv[1])
dim = int(sys.argv[2])
blockRowSize = int(sys.argv[3])
blockColSize = int(sys.argv[4])
blockRowNum = data_num/blockRowSize
blockColNum = dim/blockColSize 
assert data_num% blockRowSize==0 and dim%blockColSize ==0
hostname = "ip-172-30-4-168"


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


# Write the input of SystemML
if systemMLFlag:
    local_path_SystemML = "./SystemML/data/"
    fileNameX_SystemML_csv = "NN_X_"+str(data_num)+"_"+str(dim)+".csv"
    systemML_X = open(local_path_SystemML + fileNameX_SystemML_csv, "w")
    systemML_mtd_X = open(local_path_SystemML + fileNameX_SystemML_csv+".mtd", "w")
    fileNamet_SystemML_csv = "NN_t_"+str(dim)+".csv"
    systemML_t = open(local_path_SystemML + fileNamet_SystemML_csv, "w")
    systemML_mtd_t = open(local_path_SystemML + fileNamet_SystemML_csv+".mtd", "w")
    fileNameM_SystemML_csv = "NN_M_"+str(dim)+".csv"
    systemML_M = open(local_path_SystemML + fileNameM_SystemML_csv, "w")
    systemML_mtd_M = open(local_path_SystemML + fileNameM_SystemML_csv+".mtd", "w")  
    codeSystemML = open("./SystemML/Task03_NN_SystemML_"+str(dim)+".dml","w")  

    for i in xrange(data_num):
        for j in xrange(dim-1):
            systemML_X.write(str(X[i][j]))
            systemML_X.write(str(", "))
        systemML_X.write(str(str(X[i][dim-1])))
        systemML_X.write("\n")
    systemML_X.close()

    for j in xrange(dim-1):
        systemML_t.write(str(t[j]))
        systemML_t.write(", ")
    systemML_t.write(str(t[dim-1]))
    systemML_t.write("\n")
    systemML_t.close()

    for i in xrange(dim):
        for j in xrange(dim-1):
            systemML_M.write(str(M[i,j]))
            systemML_M.write(", ")
        systemML_M.write(str(M[i,dim-1]))
        systemML_M.write("\n")
    systemML_M.close()    

    systemML_mtd_X.write("{\"rows\": " + str(data_num) +", \"cols\": " + str(dim) + ", \"format\": \"csv\"}")
    systemML_mtd_t.write("{\"rows\": " + str(1) +", \"cols\": " + str(dim) + ", \"format\": \"csv\"}")
    systemML_mtd_M.write("{\"rows\": " + str(dim) +", \"cols\": " + str(dim) + ", \"format\": \"csv\"}")
    systemML_mtd_X.close()
    systemML_mtd_t.close()
    systemML_mtd_M.close()

    systemML_master_path = "file:/home/ubuntu/Nearest_Neighbour_Search/SystemML/data/"

    # Automatically generate code for SystemML DSL
    #codeSystemML.write('X = read(\"'+'hdfs://'+hostname+':9000/SystemML/data/'+fileNameX_SystemML_csv+'\");\n')
    #codeSystemML.write('t = read(\"'+'hdfs://'+hostname+':9000/SystemML/data/'+fileNamet_SystemML_csv+'\");\n')
    #codeSystemML.write('M = read(\"'+'hdfs://'+hostname+':9000/SystemML/data/'+fileNameM_SystemML_csv+'\");\n')
    codeSystemML.write('X = read(\"'+systemML_master_path+fileNameX_SystemML_csv+'\");\n')
    codeSystemML.write('t = read(\"'+systemML_master_path+fileNamet_SystemML_csv+'\");\n')
    codeSystemML.write('M = read(\"'+systemML_master_path+fileNameM_SystemML_csv+'\");\n')
    #codeSystemML.write("duplicateRow = function (matrix[double] row, int nums) return (matrix[double] rows) {\n")
    #codeSystemML.write("\trows = row;\n")
    #codeSystemML.write("\tfor (i in 2:nums) {\n")
    #codeSystemML.write("\t\trows = rbind(rows,row);\n")
    #codeSystemML.write("\t}\n")
    #codeSystemML.write("}\n")
    codeSystemML.write("duplicateRow = function (matrix[double] row, int nums) return (matrix[double] rows) {\n")
    codeSystemML.write("\trows = matrix(0,rows=nums,cols=ncol(row));\n")
    codeSystemML.write("\tparfor (i in 1:nums) {\n")
    codeSystemML.write("\t\trows[i,] = row;\n")
    codeSystemML.write("\t}\n")
    codeSystemML.write("}\n")
    codeSystemML.write("Dup = duplicateRow(t, nrow(X));\n")
    codeSystemML.write("Diff = X - Dup;\n")
    codeSystemML.write("i = rowIndexMin(t(rowSums(Diff %*% M * Diff)));\n")
    codeSystemML.write('print(\"Nearest item index:\" + toString(i, decimal=0));\n')
    codeSystemML.close()
    print "Write to SystemML is done!"


# Write the input of PDB
if PDBFlag:
    fileNameX = "./PDB/data/NN_X_"+str(data_num)+"_"+str(dim)+"_"+str(blockRowSize)+"_"+str(blockColSize)+".data"
    blocks_X = open(fileNameX, "w")
    fileNamet = "./PDB/data/NN_t_"+str(dim)+"_"+str(blockColSize)+".data"
    blocks_t = open(fileNamet, "w")
    fileNameM = "./PDB/data/NN_M_"+str(dim)+"_"+str(blockColSize)+".data"
    blocks_M = open(fileNameM, "w")
    codePDB = open("./PDB/Task03_NN_"+str(dim)+".pdml","w")

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


# Write the input of Spark
if sparkFlag:
    local_path_Spark = "./Spark/data/"
    fileNameX_Spark_txt = "NN_Spark_X_"+str(data_num)+"_"+str(dim)+".txt"
    spark_X = open(local_path_Spark + fileNameX_Spark_txt,"w")
    fileNamet_Spark_txt = "NN_Spark_t_"+str(dim)+".txt"
    spark_t = open(local_path_Spark + fileNamet_Spark_txt,"w")
    fileNameM_Spark_txt = "NN_Spark_M_"+str(dim)+".txt"
    spark_M = open(local_path_Spark + fileNameM_Spark_txt,"w")
    scriptSpark = open("./Spark/Task03_NN_Spark_Submit_"+str(dim)+".sh","w")

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
    # Automatically generate script to run spark
    scriptSpark.write('$SPARK_HOME/bin/spark-submit ')
    scriptSpark.write('--class \"NearestNeighbourSearch\" ')
    scriptSpark.write('--master spark://'+hostname+':7077 ')
    scriptSpark.write('--deploy-mode cluster ')
    scriptSpark.write('hdfs://'+hostname+':9000/nnsearch-project_2.11-1.0.jar ')
    scriptSpark.write(str(data_num))
    scriptSpark.write(" ")
    scriptSpark.write(str(dim))
    scriptSpark.write(" ")
    scriptSpark.write('hdfs://'+hostname+':9000/SparkML/data/'+fileNameX_Spark_txt)
    scriptSpark.write(" ")
    scriptSpark.write('hdfs://'+hostname+':9000/SparkML/data/'+fileNamet_Spark_txt)
    scriptSpark.write(" ")
    scriptSpark.write('hdfs://'+hostname+':9000/SparkML/data/'+fileNameM_Spark_txt)
    scriptSpark.close()
    print "Write to Spark is done!"


# Write the input of SciDB
if SciDBFlag:
    local_path_SciDB = "./SciDB/data/"
    fileNameX_SciDB_txt = "NN_SciDB_X_"+str(data_num)+"_"+str(dim)+".txt"
    SciDB_X = open(local_path_SciDB + fileNameX_SciDB_txt,"w")
    fileNamet_SciDB_txt = "NN_SciDB_t_"+str(dim)+".txt"
    SciDB_t = open(local_path_SciDB + fileNamet_SciDB_txt,"w")
    fileNameM_SciDB_txt = "NN_SciDB_M_"+str(dim)+".txt"
    SciDB_M = open(local_path_SciDB + fileNameM_SciDB_txt,"w")

    SciDB_X.write("[\n")
    for i in xrange(data_num):
        SciDB_X.write("[")
        for j in xrange(dim-1):
            SciDB_X.write("("+str(X[i][j])+"),")
        if i == data_num-1:
            SciDB_X.write("("+str(X[i][dim-1])+")]\n")
        else:
            SciDB_X.write("("+str(X[i][dim-1])+")],\n")
    SciDB_X.write("]\n")
    SciDB_X.close()

    SciDB_t.write("[\n[")
    for j in xrange(dim-1):
        SciDB_t.write("("+str(t[j])+"),")
    SciDB_t.write("("+str(t[dim-1])+")]")
    SciDB_t.write("\n]")
    SciDB_t.close()

    SciDB_M.write("[\n")
    for i in xrange(dim):
        SciDB_M.write("[")
        for j in xrange(dim-1):
            SciDB_M.write("("+str(M[i,j])+"),")
        if i == dim-1:
            SciDB_M.write("("+str(M[i,dim-1])+")]\n")
        else:
            SciDB_M.write("("+str(M[i,dim-1])+")],\n")
    SciDB_M.write("]\n")
    SciDB_M.close()
    print "Write to SciDB is done!"