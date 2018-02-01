###
### TestLA01_Transpose
###

# create the target
add_pdb_application(TestLA01_Transpose)

# add a build dependency to build-tests target
add_dependencies(build-tests TestLA01_Transpose)

# add build dependencies to shared libraries it uses
add_dependencies(TestLA01_Transpose LATransposeSelection)
add_dependencies(TestLA01_Transpose LAScanMatrixBlockSet)
add_dependencies(TestLA01_Transpose LAWriteMatrixBlockSet)
add_dependencies(TestLA01_Transpose MatrixBlock)

###
### TestLA02_Add
###

# create the target
add_pdb_application(TestLA02_Add)

# add a build dependency to build-tests target
add_dependencies(build-tests TestLA02_Add)

# add build dependencies to shared libraries it uses
add_dependencies(TestLA02_Add LAAddJoin)
add_dependencies(TestLA02_Add LAScanMatrixBlockSet)
add_dependencies(TestLA02_Add LAWriteMatrixBlockSet)
add_dependencies(TestLA02_Add MatrixBlock)

###
### TestLA03_Substract
###

# create the target
add_pdb_application(TestLA03_Substract)

# add a build dependency to build-tests target
add_dependencies(build-tests TestLA03_Substract)

# add build dependencies to shared libraries it uses
add_dependencies(TestLA03_Substract LAAddJoin)
add_dependencies(TestLA03_Substract LAScanMatrixBlockSet)
add_dependencies(TestLA03_Substract LAWriteMatrixBlockSet)
add_dependencies(TestLA03_Substract MatrixBlock)

###
### TestLA04_Multiply
###

# create the target
add_pdb_application(TestLA04_Multiply)

# add a build dependency to build-tests target
add_dependencies(build-tests TestLA04_Multiply)

# add build dependencies to shared libraries it uses
add_dependencies(TestLA04_Multiply MatrixMeta)
add_dependencies(TestLA04_Multiply MatrixData)
add_dependencies(TestLA04_Multiply MatrixBlock)
add_dependencies(TestLA04_Multiply LAMultiply1Join)
add_dependencies(TestLA04_Multiply LAMultiply2Aggregate)
add_dependencies(TestLA04_Multiply LAScanMatrixBlockSet)
add_dependencies(TestLA04_Multiply LAWriteMatrixBlockSet)

###
### TestLA05_MaxElement
###

# create the target
add_pdb_application(TestLA05_MaxElement)

# add a build dependency to build-tests target
add_dependencies(build-tests TestLA05_MaxElement)

# add build dependencies to shared libraries it uses
add_dependencies(TestLA05_MaxElement MatrixBlock)
add_dependencies(TestLA05_MaxElement LAMaxElementValueType)
add_dependencies(TestLA05_MaxElement LAMaxElementOutputType)
add_dependencies(TestLA05_MaxElement LAMaxElementAggregate)
add_dependencies(TestLA05_MaxElement LAScanMatrixBlockSet)
add_dependencies(TestLA05_MaxElement LAWriteMaxElementSet)

###
### TestLA06_MinElement
###

# create the target
add_pdb_application(TestLA06_MinElement)

# add a build dependency to build-tests target
add_dependencies(build-tests TestLA06_MinElement)

# add build dependencies to shared libraries it uses
add_dependencies(TestLA06_MinElement MatrixBlock)
add_dependencies(TestLA06_MinElement LAMinElementValueType)
add_dependencies(TestLA06_MinElement LAMinElementOutputType)
add_dependencies(TestLA06_MinElement LAMinElementAggregate)
add_dependencies(TestLA06_MinElement LAScanMatrixBlockSet)
add_dependencies(TestLA06_MinElement LAWriteMinElementSet)

###
### TestLA07_TransposeMultiply
###

# create the target
add_pdb_application(TestLA07_TransposeMultiply)

# add a build dependency to build-tests target
add_dependencies(build-tests TestLA07_TransposeMultiply)

# add build dependencies to shared libraries it uses
add_dependencies(TestLA07_TransposeMultiply MatrixMeta)
add_dependencies(TestLA07_TransposeMultiply MatrixData)
add_dependencies(TestLA07_TransposeMultiply MatrixBlock)
add_dependencies(TestLA07_TransposeMultiply LATransposeMultiply1Join)
add_dependencies(TestLA07_TransposeMultiply LAMultiply2Aggregate)
add_dependencies(TestLA07_TransposeMultiply LAScanMatrixBlockSet)
add_dependencies(TestLA07_TransposeMultiply LAWriteMatrixBlockSet)

###
### TestLA07_TransposeMultiply_Gram
###

# create the target
add_pdb_application(TestLA07_TransposeMultiply_Gram)

# add a build dependency to build-tests target
add_dependencies(build-tests TestLA07_TransposeMultiply_Gram)

# add build dependencies to shared libraries it uses
add_dependencies(TestLA07_TransposeMultiply_Gram MatrixMeta)
add_dependencies(TestLA07_TransposeMultiply_Gram MatrixData)
add_dependencies(TestLA07_TransposeMultiply_Gram MatrixBlock)
add_dependencies(TestLA07_TransposeMultiply_Gram LATransposeMultiply1Join)
add_dependencies(TestLA07_TransposeMultiply_Gram LAMultiply2Aggregate)
add_dependencies(TestLA07_TransposeMultiply_Gram LAScanMatrixBlockSet)
add_dependencies(TestLA07_TransposeMultiply_Gram LAWriteMatrixBlockSet)

###
### TestLA08_RowMax
###

# create the target
add_pdb_application(TestLA08_RowMax)

# add a build dependency to build-tests target
add_dependencies(build-tests TestLA08_RowMax)

# add build dependencies to shared libraries it uses
add_dependencies(TestLA08_RowMax MatrixMeta)
add_dependencies(TestLA08_RowMax MatrixData)
add_dependencies(TestLA08_RowMax MatrixBlock)
add_dependencies(TestLA08_RowMax LARowMaxAggregate)
add_dependencies(TestLA08_RowMax LAScanMatrixBlockSet)
add_dependencies(TestLA08_RowMax LAWriteMatrixBlockSet)

###
### TestLA09_RowMin
###

# create the target
add_pdb_application(TestLA09_RowMin)

# add a build dependency to build-tests target
add_dependencies(build-tests TestLA09_RowMin)

# add build dependencies to shared libraries it uses
add_dependencies(TestLA09_RowMin MatrixMeta)
add_dependencies(TestLA09_RowMin MatrixData)
add_dependencies(TestLA09_RowMin MatrixBlock)
add_dependencies(TestLA09_RowMin LARowMinAggregate)
add_dependencies(TestLA09_RowMin LAScanMatrixBlockSet)
add_dependencies(TestLA09_RowMin LAWriteMatrixBlockSet)

###
### TestLA10_ColMax
###

# create the target
add_pdb_application(TestLA10_ColMax)

# add a build dependency to build-tests target
add_dependencies(build-tests TestLA10_ColMax)

# add build dependencies to shared libraries it uses
add_dependencies(TestLA10_ColMax MatrixMeta)
add_dependencies(TestLA10_ColMax MatrixData)
add_dependencies(TestLA10_ColMax MatrixBlock)
add_dependencies(TestLA10_ColMax LAColMaxAggregate)
add_dependencies(TestLA10_ColMax LAScanMatrixBlockSet)
add_dependencies(TestLA10_ColMax LAWriteMatrixBlockSet)

###
### TestLA11_ColMin
###

# create the target
add_pdb_application(TestLA11_ColMin)

# add a build dependency to build-tests target
add_dependencies(build-tests TestLA11_ColMin)

# add build dependencies to shared libraries it uses
add_dependencies(TestLA11_ColMin MatrixMeta)
add_dependencies(TestLA11_ColMin MatrixData)
add_dependencies(TestLA11_ColMin MatrixBlock)
add_dependencies(TestLA11_ColMin LAColMinAggregate)
add_dependencies(TestLA11_ColMin LAScanMatrixBlockSet)
add_dependencies(TestLA11_ColMin LAWriteMatrixBlockSet)

###
### TestLA12_DuplicateRow
###

# create the target
add_pdb_application(TestLA12_DuplicateRow)

# add a build dependency to build-tests target
add_dependencies(build-tests TestLA12_DuplicateRow)

# add build dependencies to shared libraries it uses
add_dependencies(TestLA12_DuplicateRow MatrixMeta)
add_dependencies(TestLA12_DuplicateRow MatrixData)
add_dependencies(TestLA12_DuplicateRow MatrixBlock)
add_dependencies(TestLA12_DuplicateRow LAColMinAggregate)
add_dependencies(TestLA12_DuplicateRow LAScanMatrixBlockSet)
add_dependencies(TestLA12_DuplicateRow LAWriteMatrixBlockSet)

###
### TestLA13_DuplicateCol
###

# create the target
add_pdb_application(TestLA13_DuplicateCol)

# add a build dependency to build-tests target
add_dependencies(build-tests TestLA13_DuplicateCol)

# add build dependencies to shared libraries it uses
add_dependencies(TestLA13_DuplicateCol MatrixMeta)
add_dependencies(TestLA13_DuplicateCol MatrixData)
add_dependencies(TestLA13_DuplicateCol MatrixBlock)
add_dependencies(TestLA13_DuplicateCol LADuplicateColMultiSelection)
add_dependencies(TestLA13_DuplicateCol LAScanMatrixBlockSet)
add_dependencies(TestLA13_DuplicateCol LAWriteMatrixBlockSet)

###
### TestLA14_Inverse
###

# create the target
add_pdb_application(TestLA14_Inverse)

# add a build dependency to build-tests target
add_dependencies(build-tests TestLA14_Inverse)

# add build dependencies to shared libraries it uses
add_dependencies(TestLA14_Inverse MatrixMeta)
add_dependencies(TestLA14_Inverse MatrixData)
add_dependencies(TestLA14_Inverse MatrixBlock)
add_dependencies(TestLA14_Inverse LASingleMatrix)
add_dependencies(TestLA14_Inverse LAInverse1Aggregate)
add_dependencies(TestLA14_Inverse LAInverse2Selection)
add_dependencies(TestLA14_Inverse LAInverse3MultiSelection)
add_dependencies(TestLA14_Inverse LAScanMatrixBlockSet)
add_dependencies(TestLA14_Inverse LAWriteMatrixBlockSet)

###
### TestLA15_ScaleMultiply
###

# create the target
add_pdb_application(TestLA15_ScaleMultiply)

# add a build dependency to build-tests target
add_dependencies(build-tests TestLA15_ScaleMultiply)

# add build dependencies to shared libraries it uses
add_dependencies(TestLA15_ScaleMultiply MatrixBlock)
add_dependencies(TestLA15_ScaleMultiply LAScaleMultiplyJoin)
add_dependencies(TestLA15_ScaleMultiply LAScanMatrixBlockSet)
add_dependencies(TestLA15_ScaleMultiply LAWriteMatrixBlockSet)

###
### TestLA16_RowSum
###

# create the target
add_pdb_application(TestLA16_RowSum)

# add a build dependency to build-tests target
add_dependencies(build-tests TestLA16_RowSum)

# add build dependencies to shared libraries it uses
add_dependencies(TestLA16_RowSum MatrixMeta)
add_dependencies(TestLA16_RowSum MatrixData)
add_dependencies(TestLA16_RowSum MatrixBlock)
add_dependencies(TestLA16_RowSum LARowSumAggregate)
add_dependencies(TestLA16_RowSum LAScanMatrixBlockSet)
add_dependencies(TestLA16_RowSum LAWriteMatrixBlockSet)

###
### TestLA17_ColSum
###

# create the target
add_pdb_application(TestLA17_ColSum)

# add a build dependency to build-tests target
add_dependencies(build-tests TestLA17_ColSum)

# add build dependencies to shared libraries it uses
add_dependencies(TestLA17_ColSum MatrixMeta)
add_dependencies(TestLA17_ColSum MatrixData)
add_dependencies(TestLA17_ColSum MatrixBlock)
add_dependencies(TestLA17_ColSum LAColSumAggregate)
add_dependencies(TestLA17_ColSum LAScanMatrixBlockSet)
add_dependencies(TestLA17_ColSum LAWriteMatrixBlockSet)