import breeze.linalg._
import breeze.stats._
import breeze.numerics._

val simpleMatrix2 = DenseMatrix((4.0, 7.0), (3.0, -5.0))

meanAndVariance(simpleMatrix2)

val stdResult = stddev(simpleMatrix2)

val simpleMatrix = DenseMatrix((1, 2, 3), (11, 12, 13), (21, 22, 23))

val intMaxOfMatrixVals = max(simpleMatrix)

val intSumOfMatrixVals = sum(simpleMatrix)

val sqrtOfMatrixVals = sqrt(simpleMatrix)

val logOfMatrixVals = log(simpleMatrix)

val denseEig = eig(simpleMatrix2)

