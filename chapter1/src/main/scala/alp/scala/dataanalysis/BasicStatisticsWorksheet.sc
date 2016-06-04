import breeze.linalg._
import breeze.numerics._

val evenNosTill20Double = DenseVector.rangeD(0, 20, 2)
val meanVarianceResult = breeze.stats.meanAndVariance(evenNosTill20Double)
val stddevResult = breeze.stats.stddev(evenNosTill20Double)

// MAX
val intMaxOfVectorVals = max(evenNosTill20Double)
// SUM
val intSumOfVectorVals = sum(evenNosTill20Double)
// Square root of all elements
val sqrtOfVectorVals = sqrt(evenNosTill20Double)
// Log of all elements
val log2VectorVals = log(evenNosTill20Double)

