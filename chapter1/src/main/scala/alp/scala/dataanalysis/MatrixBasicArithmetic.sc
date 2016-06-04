import breeze.linalg._

val simpleMatrix = DenseMatrix((1, 2, 3), (11, 12, 13), (21, 22, 23))

val identityMatrix = DenseMatrix.eye[Int](3)

val additionMatrix = identityMatrix + simpleMatrix

val simpleTimesIdentity = simpleMatrix * identityMatrix

val elementWiseMulti = identityMatrix :* simpleMatrix

val vertConcatMatrix = DenseMatrix.vertcat(identityMatrix, simpleMatrix)

val horzConcatMatrix = DenseMatrix.horzcat(identityMatrix, simpleMatrix)

val simpleMatrixAsDouble = convert(simpleMatrix, Double)

val simpleMatrix2 = DenseMatrix((4.0, 7.0), (3.0, -5.0))

val firstVector = simpleMatrix2(::, 0)

val secondVector = simpleMatrix2(::, 1)

val firstVectorByCols = simpleMatrix2(0 to 1, 0)

val firstRowStatingCols = simpleMatrix2(0, 0 to 1)

val firstRowAllCols = simpleMatrix2(0, ::)

val secondRow = simpleMatrix2(1, ::)

val firstRowFirstCol = simpleMatrix2(0, 0)

val transpose = simpleMatrix2.t

val inverse = inv(simpleMatrix2)

