import breeze.stats.distributions._
import breeze.linalg._

val uniformDist = Uniform(0, 10)

val gaussianDist = Gaussian(5, 1)

val poissonDist = Poisson(5)

println (uniformDist.sample())

//Returns a sample vector of size that is passed in as parameter
println (uniformDist.sample(2))

val uniformWithoutSize = DenseVector.rand(10)
println ("uniformWithoutSize \n" + uniformWithoutSize)

val uniformVectInRange = DenseVector.rand(10, uniformDist)

val gaussianVector = DenseVector.rand(10, gaussianDist)
println ("gaussianVector \n" + gaussianVector)


val poissonVector = DenseVector.rand(10, poissonDist)
println ("poissonVector \n"+poissonVector)



