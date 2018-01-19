Here is the instruction to run my linear algebra experiments. The instruction is for a single machine experiment, however, to apply it on a cluster should be straightforward. Make sure you are under the ObjectQueryModel root path.


1. To run Gram task: 
First run: 
************************************************************************************
python ./src/linearAlgebraDSL/TestDataGenerator/GramTestDataGenerator.py N1 N2 N3 N4 
************************************************************************************
To generate the simulated data and code for my linear algebra DSL, where N1 is the matrix row size, N2 is the matrix col size, N3 is the block row size, N4 is the block col size. The generated data will be located in ./src/linearAlgebraDSL/TestDataGenerator/, while the generated data will be located in /src/linearAlgebraDSL/DSLSamples/Task01_Gram_$N1_$N2_$N3_$N4.pdml. 

Then run: 
*************************************************************************************************************
python ./scripts/debugTests.py ./bin/testLA21_Instance ./src/linearAlgebraDSL/DSLSamples/Task01_Gram_XXX.pdml 
*************************************************************************************************************


2. To run L2 linear regression task: 
First run: 
**********************************************************************************
python ./src/linearAlgebraDSL/TestDataGenerator/L2TestDataGenerator.py N1 N2 N3 N4 
**********************************************************************************
To generate the simulated data and code for my linear algebra DSL, where N1 is the matrix row size for X, N2 is the matrix col size for X, N3 is the block row size for X, N4 is the block col size for X. The generated data will be located in ./src/linearAlgebraDSL/TestDataGenerator/, while the generated data will be located in /src/linearAlgebraDSL/DSLSamples/Task02_L2_$N1_$N2_$N3_$N4.pdml, where XXX is the timestamp to prevent overwriting the same file. 

Then run: 
***********************************************************************************************************
python ./scripts/debugTests.py ./bin/testLA21_Instance ./src/linearAlgebraDSL/DSLSamples/Task02_L2_XXX.pdml
***********************************************************************************************************


3. To run Nearest Neighbor task:  
First run: 
************************************************************************************************
python ./src/linearAlgebraDSL/TestDataGenerator/NearestNeighbourTestDataGenerator.py N1 N2 N3 N4 
************************************************************************************************
To generate the simulated data and code for my linear algebra DSL, where N1 is the matrix row size for X, N2 is the matrix col size for X, N3 is the block row size for X, N4 is the block col size for X. The generated data will be located in ./src/linearAlgebraDSL/TestDataGenerator/, while the generated data will be located in /src/linearAlgebraDSL/DSLSamples/Task03_NN_$N1_$N2_$N3_$N4.pdml, where XXX is the timestamp to prevent overwriting the same file. 

Then run: 
***********************************************************************************************************
python ./scripts/debugTests.py ./bin/testLA21_Instance ./src/linearAlgebraDSL/DSLSamples/Task03_NN_XXX.pdml
***********************************************************************************************************



Created by Binhang Yuan, July 2017.