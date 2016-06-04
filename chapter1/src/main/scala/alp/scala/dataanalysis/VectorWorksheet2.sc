import breeze.linalg._

// sum 2 vectors (element-by-element)
val evenNosTill20=DenseVector.range(0, 20, 2)
val denseJust2s=DenseVector.fill(10, 2)
val additionVector=evenNosTill20 + denseJust2s

//Concatenate 2 vectores and convert vectores types
val justFive2s=DenseVector.fill(5, 2)
val zeroThrough4=DenseVector.range(0, 5, 1)
// vertically concat
val concatVector=DenseVector.vertcat(zeroThrough4, justFive2s)
// horizontally concat
val concatVector1=DenseVector.horzcat(zeroThrough4, justFive2s)
// Convert int into double
val evenNosTill20Double=breeze.linalg.convert(evenNosTill20, Double)

