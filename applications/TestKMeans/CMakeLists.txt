###
### TestKMeans
###

# create the target
add_pdb_application(TestKMeans)

# add a build dependency to build-tests target
add_dependencies(build-tests TestKMeans)

# add build dependencies to shared libraries it uses
add_dependencies(TestKMeans KMeansAggregateOutputType)
add_dependencies(TestKMeans KMeansCentroid)
add_dependencies(TestKMeans WriteSumResultSet)
add_dependencies(TestKMeans KMeansAggregate)
add_dependencies(TestKMeans KMeansDataCountAggregate)
add_dependencies(TestKMeans ScanKMeansDoubleVectorSet)
add_dependencies(TestKMeans ScanDoubleArraySet)
add_dependencies(TestKMeans WriteKMeansSet)
add_dependencies(TestKMeans KMeansDataCountAggregate)
add_dependencies(TestKMeans KMeansSampleSelection)
add_dependencies(TestKMeans KMeansNormVectorMap)
add_dependencies(TestKMeans WriteKMeansDoubleVectorSet)