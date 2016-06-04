import breeze.linalg._

val dense = DenseVector(1, 2, 3, 4, 5)
println(dense)

val sparse = SparseVector(0.0, 1.0, 0.0, 2.0, 0.0)
println(sparse)

val denseZeros = DenseVector.zeros[Double](5)

val sparseZeros = SparseVector.zeros[Double](5)

val denseTabulate = DenseVector.tabulate[Double](5)(index=>index*index)

val spaceVector = breeze.linalg.linspace(2, 10, 5)

val allNosTill10 = DenseVector.range(0, 10)

val evenNosTill10 = DenseVector.range(0, 20, 2)

val rangeD = DenseVector.rangeD(0.5, 20, 2.5)

val denseJust2s = DenseVector.fill(10, 2)

val fourThroughSevenIndexVector = allNosTill10.slice(4, 7)

val twoThroughNineSkip2IndexVector = allNosTill10.slice(2, 9, 2)

val vectFromArray = DenseVector(collection.immutable.Vector(1, 2, 3, 4))

val inPlaceValueAddition = evenNosTill10 + 2

//Scalar subtraction
val inPlaceValueSubtraction = evenNosTill10 - 2

//Scalar multiplication
val inPlaceValueMultiplication = evenNosTill10 * 2

//Scalar division
val inPlaceValueDivision = evenNosTill10 / 2


val justFive2s=DenseVector.fill(5, 2)

val zeroThrough4=DenseVector.range(0, 5, 1)

val dotVector=zeroThrough4.dot(justFive2s)

val additionVector = evenNosTill10 + denseJust2s

val fiveLength=DenseVector(1,2,3,4,5)

val tenLength=DenseVector.fill(10, 20)

fiveLength+tenLength

// tenLength + fiveLength -> returns an exception because 1st is bigger



