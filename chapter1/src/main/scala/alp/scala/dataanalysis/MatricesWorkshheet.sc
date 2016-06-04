import breeze.linalg._

val simpleMatrix = DenseMatrix((1, 2, 3),(11, 12, 13),(21, 22, 23))

val sparseMatrix = CSCMatrix((1, 0, 0),(11, 0, 0),(0, 0, 23))

val denseZeros = DenseMatrix.zeros[Double](5, 4)

val compressedSparseMatrix = CSCMatrix.zeros[Double](5, 4)

val denseTabulate = DenseMatrix.tabulate[Double](5, 4)(
  (firstIdx,secondIdx)=>firstIdx*secondIdx)

val identityMatrix = DenseMatrix.eye[Int](3)

val randomMatrix = DenseMatrix.rand(4, 4)

val vectFromArray = new DenseMatrix(2, 2, Array(2, 3, 4, 5))
