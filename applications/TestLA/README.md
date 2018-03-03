# lilLinAlg

## Overview

This application is a linear algebra domain specific language built on top of PC. 

The sharedLibraries directory includes all the shared libraries that are used in this DSL, for example, how we store matrix blocks, and how we implement different operators in our DSL.

The linearAlgebraDSL directory includes the implementation of the DSL interpreter, for this part, we leverage some convenience from flex and bison to build the abstract syntax tree of our DSL; then we construct the computation DAG (query plan for PC) and throw this DAG to PC, PC will do the computation.   

The tests directory includes all the tests. The unit tests are named as TestLA_unitXX.cc, they are actually testing the shared libraries in C++. The TestLA_Parser.cc tests whether the interpreter can generate correct abstract syntax tree of the DSL. Finally, TestLA_Instance.cc is the integral thing that are used to handl the dsl code, and it is the entry point for our benchmarks.     


## Run the Benchmark in the Paper.
Here is the instruction to run my linear algebra experiments. The instruction is for a single machine experiment, however, to apply it on a cluster should be straightforward. Make sure you are under the ObjectQueryModel root path.


#### To run Gram task: 
First run: 
************************************************************************************
python ./applications/TestLA/tests/IntegralTestDataGeneratorScripts/GramTestDataGenerator.py N1 N2 N3 N4 
************************************************************************************
To generate the simulated data and code for my linear algebra DSL, where N1 is the matrix row size, N2 is the matrix col size, N3 is the block row size, N4 is the block col size. The generated data will be located in ./src/linearAlgebraDSL/TestDataGenerator/, while the generated data will be located in /src/linearAlgebraDSL/DSLSamples/Task01_Gram_$N1_$N2_$N3_$N4.pdml. 

Then run: 
*************************************************************************************************************
python ./scripts/debugTests.py ./bin/testLA_Instance ./applications/TestLA/tests/Benchmark/Task01_Gram_XXX.pdml 
*************************************************************************************************************


#### To run L2 linear regression task: 
First run: 
**********************************************************************************
python ./applications/TestLA/tests/IntegralTestDataGeneratorScripts/L2TestDataGenerator.py N1 N2 N3 N4 
**********************************************************************************
To generate the simulated data and code for my linear algebra DSL, where N1 is the matrix row size for X, N2 is the matrix col size for X, N3 is the block row size for X, N4 is the block col size for X. The generated data will be located in ./src/linearAlgebraDSL/TestDataGenerator/, while the generated data will be located in /src/linearAlgebraDSL/DSLSamples/Task02_L2_$N1_$N2_$N3_$N4.pdml, where XXX is the timestamp to prevent overwriting the same file. 

Then run: 
***********************************************************************************************************
python ./scripts/debugTests.py ./bin/testLA_Instance ./applications/TestLA/tests/Benchmark/Task02_L2_XXX.pdml
***********************************************************************************************************


#### To run Nearest Neighbor task:  
First run: 
************************************************************************************************
python ./applications/TestLA/tests/IntegralTestDataGeneratorScripts/NearestNeighbourTestDataGenerator.py N1 N2 N3 N4 
************************************************************************************************
To generate the simulated data and code for my linear algebra DSL, where N1 is the matrix row size for X, N2 is the matrix col size for X, N3 is the block row size for X, N4 is the block col size for X. The generated data will be located in ./src/linearAlgebraDSL/TestDataGenerator/, while the generated data will be located in /src/linearAlgebraDSL/DSLSamples/Task03_NN_$N1_$N2_$N3_$N4.pdml, where XXX is the timestamp to prevent overwriting the same file. 

Then run: 
***********************************************************************************************************
python ./scripts/debugTests.py ./bin/testLA_Instance ./applications/TestLA/tests/Benchmark/Task03_NN_XXX.pdml
***********************************************************************************************************
