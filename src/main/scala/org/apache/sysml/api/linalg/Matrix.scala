/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysml.api.linalg

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.sysml.api.linalg.api.:::
import org.apache.sysml.api.mlcontext.{MatrixMetadata, _}
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject
import org.apache.sysml.runtime.controlprogram.context.SparkExecutionContext

import scala.util.Random

/**
  * Matrix class for SystemML
  *
  * Represents the matrix that will be translated to SystemML's matrix type.
  *
  * @param nrow Number of rows of the matrix.
  * @param ncol Number of columns of the matrix.
  * @param matob The matrix object that is backing the matrix in SystemML. Is only available after execution.
  * @param sec The SparkExecutionContext that is used to access the Bufferpool and fetch matrix values. Only available
  *            after execution in SystemML.
  * @param localImpl Local implementation of the matrix. Only available when the Matrix is initialized with an array.
  */
class Matrix protected(val nrow: Int,
                       val ncol: Int,
                       private val matob: Option[MatrixObject] = None,
                       private val sec: Option[SparkExecutionContext] = None,
                       private val localImpl: Option[Array[Double]] = None) {
  //////////////////////////////////////////
  // Constructors
  //////////////////////////////////////////

  //////////////////////////////////////////
  // Accessors
  //////////////////////////////////////////

  /**
    * Get the value in cell with index (row, col).
    *
    * @param row Row index.
    * @param col Column index.
    * @return Value in cell (row, cell).
    */
  def apply(row: Int, col: Int): Double = ???

  /**
    * Get all values in the indexed row.
    *
    * @param row Row index.
    * @param col Operator for indexing all columns.
    * @return Matrix of dimension (1, cols) containing values of the indexed row.
    */
  def apply(row: Int, col: :::.type): Matrix = ???

  /**
    * Get all values in the indexed column.
    *
    * @param row Operator for indexing all rows.
    * @param col Column index.
    * @return Matrix of dimension (rows, 1) containing values of the indexed column.
    */
  def apply(row: :::.type, col: Int): Matrix = ???

  /**
    * Get all values in indexed rows. Selects a subset of rows to return.
    *
    * @param rows Range of rows to return.
    * @param cols Operator for indexing all columns.
    * @return Selected rows of the matrix.
    */
  def apply(rows: Range, cols: :::.type): Matrix = ???

  /**
    * Get all values in indexed columns. Selects a subset of columns to return.
    *
    * @param rows Operator for indexing all rows.
    * @param cols Range of columns to return.
    * @return Selected columns of the matrix.
    */
  def apply(rows: :::.type, cols: Range): Matrix = ???

  /**
    * Get an arbitrary submatrix.
    *
    * @param rows Rows to include in the submatrix.
    * @param cols Columns to include in the submatrix.
    * @return Submatrix of size (#selected rows, #selected columns).
    */
  def apply(rows: Range, cols: Range): Matrix = ???

  /**
    * Get a subset of a column.
    *
    * @param rows Subset of rows to include.
    * @param col Column index.
    * @return Matrix of dimension (#selected rows, 1) containing values of the indexed column.
    */
  def apply(rows: Range, col: Int): Matrix = ???

  /**
    * Get a subset of a row.
    *
    * @param row Subset of columns to include.
    * @param cols Row index.
    * @return Matrix of dimension (1, #selected columns) containing values of the indexed rows.
    */
  def apply(row: Int, cols: Range): Matrix = ???

  //////////////////////////////////////////
  // Left Indexing assignments
  //////////////////////////////////////////

  /**
    * Update the value in one matrix cell.
    *
    * @param row Row index.
    * @param col Column index.
    * @param value Value to update the cell with.
    * @return Updated matrix.
    */
  def update(row: Int, col: Int, value: Double): Matrix = ???

  /**
    * Update one row in the matrix.
    *
    * @param row The row to update.
    * @param col Operator to index all columns.
    * @param vec Matrix of dimension (1, #matrixcolumns).
    * @return Matrix with updated row.
    */
  def update(row: Int, col: :::.type, vec: Matrix): Matrix = ???

  /**
    * Update one column in the matrix.
    *
    * @param row Operator to index all rows.
    * @param col The column to update.
    * @param vec Matrix of dimension (#matrixrows, 1).
    * @return Matrix with updated column.
    */
  def update(row: :::.type, col: Int, vec: Matrix): Matrix = ???

  /**
    * Update a range of rows in the matrix.
    *
    * @param rows Range of rows to update.
    * @param cols Operator to select all columns.
    * @param mat Matrix of dimension (#rowsindexed, #matrixcolumns).
    * @return Matrix with updated rows.
    */
  def update(rows: Range, cols: :::.type, mat: Matrix): Matrix = ???

  /**
    * Update a range of columns in the matrix.
    *
    * @param rows Operator to select all rows.
    * @param cols Range of columns to update.
    * @param mat Matrix of dimensions (#matrixcolumns, #columnsindexed).
    * @return Matrix with updated columns.
    */
  def update(rows: :::.type, cols: Range, mat: Matrix): Matrix = ???

  /**
    * Update a submatrix of the matrix.
    *
    * @param rows Range of rows to update.
    * @param cols Range of columns to update.
    * @param mat Matrix of dimensions (#rowsindexed, #colsindexed).
    * @return Matrix with updated submatrix.
    */
  def update(rows: Range, cols: Range, mat: Matrix): Matrix = ???

  /**
    * Update part of a row in the matrix.
    *
    * @param row The row to update.
    * @param cols Range of columns to update in row.
    * @param mat Matrix of dimensions (1, #colsindexed)
    * @return Matrix with updated subset of the indexed row.
    */
  def update(row: Int, cols: Range, mat: Matrix): Matrix = ???

  /**
    * Update part of a column in the matrix.
    *
    * @param rows Range of rows to update.
    * @param col The column to update.
    * @param mat Matrix of dimensions (#rowsindexed, 1)
    * @return Matrix with updated subset of the indexed column.
    */
  def update(rows: Range, col: Int, mat: Matrix): Matrix = ???

  //////////////////////////////////////////
  // M o scalar
  //////////////////////////////////////////

  /**
    * Cell-wise matrix-scalar addition.
    *
    * @param that Scalar to add to each cell of the matrix.
    * @return Matrix with updated cell-values X + s.
    */
  def +(that: Double): Matrix = ???

  /**
    * Cell-wise matrix-scalar subtraction.
    *
    * @param that Scalar to subtract from each cell of the matrix.
    * @return Matrix with updated cell-values X - s.
    */
  def -(that: Double): Matrix = ???

  /**
    * Cell-wise matrix-scalar multiplication.
    *
    * @param that Scalar to multiply each cell of the matrix with.
    * @return Matrix with updated cell-values X * s.
    */
  def *(that: Double): Matrix = ???

  /**
    * Cell-wise matrix-scalar division.
    *
    * @param that Scalar to divide each cell of the matrix by.
    * @return Matrix with updated cell-values X / s.
    */
  def /(that: Double): Matrix = ???

  //////////////////////////////////////////
  // columnwise M o vector (broadcast operators)
  //////////////////////////////////////////

//  private def broadcastRows(mat: Matrix, vec: Vector, op: (Double, Double) => Double) = ???
//
//  private def broadcastCols(mat: Matrix, vec: Vector, op: (Double, Double) => Double) = ???
//
//  private def broadcast(mat: Matrix, vec: Vector)(op: (Double, Double) => Double) = ???
//
//  def +(that: Vector): Matrix = broadcast(this, that)(_ + _)
//
//  def -(that: Vector): Matrix = broadcast(this, that)(_ - _)
//
//  def *(that: Vector): Matrix = broadcast(this, that)(_ * _)
//
//  def /(that: Vector): Matrix = broadcast(this, that)(_ / _)

  //////////////////////////////////////////
  // cellwise M o M
  //////////////////////////////////////////

  /**
    * Cell-wise matrix-matrix addition.
    *
    * @param that Matrix of matching dimensions.
    *             Each value in cell (i, j) of that matrix is added to the value in cell (i, j) in this matrix.
    * @return Matrix with updated cell-values X + Y.
    */
  def +(that: Matrix): Matrix = ???

  /**
    * Cell-wise matrix-matrix subtraction.
    *
    * @param that Matrix of matching dimensions.
    *             Each value in cell (i, j) of that matrix is subtracted from the value in cell (i, j) in this matrix.
    * @return Matrix with updated cell-values X - Y.
    */
  def -(that: Matrix): Matrix = ???

  /**
    * Cell-wise matrix-matrix multiplication.
    *
    * @param that Matrix of matching dimensions.
    *             Each value in cell (i, j) of that matrix is added to the value in cell (i, j) in this matrix.
    * @return Matrix with updated cell-values X * Y.
    */
  def *(that: Matrix): Matrix = ???

  /**
    * Cell-wise matrix-matrix division.
    *
    * @param that Matrix of matching dimensions.
    *             Each value in cell (i, j) of this matrix is divided by the value in dell (i, j) in that matrix.
    * @return Matrix with updated cell-values X / Y.
    */
  def /(that: Matrix): Matrix = ???

  //////////////////////////////////////////
  // M x M -> M
  //////////////////////////////////////////

  /**
    * Matrix multiplication. If we assume that the `this` matrix has dimensions (m, n) then the argument must be of
    * dimensionality (n, k) and multiplication will produce a matrix of dimensions (m, k).
    *
    * @param that Matrix of dimension (n, k) to multiply this matrix with.
    *             Number of rows must match the number of columns of this matrix.
    * @return Matrix product A %*% B of dimensionality (m, k) if A.nrow = m, A.ncol = n, B.nrow = n, B.ncol = k.
    */
  def %*%(that: Matrix): Matrix = ???

  //////////////////////////////////////////
  // M operation
  //////////////////////////////////////////

  /**
    * Transpose the matrix. If the original matrix was of dimensionality (m, n) the transposed result will be of
    * dimensionality (n, m).
    *
    * @return Transpose of the matrix.
    */
  def t: Matrix = ???

  /**
    * Exponentiation of the matrix values.
    *
    * @param n The exponent to apply to each value in the amtrix.
    * @return Matrix where each value x in the matrix is multiplied n times by itself.
    */
  def ^(n: Int): Matrix = ???

  //////////////////////////////////////////
  // Convenience Transformations (Only to be used outside the macro)
  //////////////////////////////////////////

  /**
    * Transform the [[Matrix]] to a [[BinaryBlockMatrix]] which is SystemML's internal matrix representation for
    * Spark.
    *
    * @return [[BinaryBlockMatrix]] containing the values of the matrix in a Spark RDD.
    */
  protected[sysml] def toBinaryBlockMatrix(): BinaryBlockMatrix = (matob, sec) match {
    case (Some(mo), Some(ctx)) => MLContextConversionUtil.matrixObjectToBinaryBlockMatrix(mo, ctx)
    case _ => throw new RuntimeException("Matrix has not been evaluated in SystemML - can not create BinaryBlockMatrix")
  }

  /**
    * Transform the [[Matrix]] to a [[MatrixObject]] which is SystemML's internal generic matrix representation.
    *
    * @return [[MatrixObject]] containing the references to local or distributed matrix values.
    */
  protected[sysml] def toMatrixObject(): MatrixObject = matob match {
    case Some(mo) => mo
    case _ => throw new RuntimeException("Matrix has not been evaluated in SystemML - can not create MatrixObject")
  }

  /**
    * Get the [[MatrixMetadata]] for this [[Matrix]].
    *
    * @return [[MatrixMetadata]] belonging to this [[Matrix]].
    */
  protected[sysml] def getMatrixMetadata(): MatrixMetadata = {
    matob match {
      case Some(mo) => new MatrixMetadata(mo.getMatrixCharacteristics)
      case None     => throw new RuntimeException("Matrix has not been evaluated in SystemML - can not create MatrixMetaData")
    }
  }

  /**
    * Convert this [[Matrix]] to a Spark [[DataFrame]].
    * Each column in the matrix will become one column in the dataframe.
    *
    * @return DataFrame containing the matrix values.
    */
  def toDF(): Dataset[Row] = (matob, sec) match {
    case (Some(mo), Some(ctx)) =>
      MLContextConversionUtil.matrixObjectToDataFrame(mo, ctx, false)
    case _ =>
      throw new RuntimeException("Matrix has not been evaluated in SystemML - can not create DataFrame.")
      // TODO this should return Option[DataFrame] or create a DataFrame from the raw values.
//    {
//      val mlctx = implicitly[MLContext]
//      val spark = SparkSession.builder().getOrCreate()
//
//      val block: Array[Array[Double]] = to2D(matob.acquireRead().getDenseBlock)
//      val rows: Array[Row] = block.map(row => Row.fromSeq(row))
//      val rdd: RDD[Row] = mlctx.getSparkContext.parallelize(rows)
//      val schema = StructType((0 until this.ncol).map { i => StructField("C" + i, DoubleType, true) })
//      spark.createDataFrame(rdd, schema)
//    }
  }

  /**
    * Return the values of the matrix as a 2D double array.
    * NOTE: If the matrix only exists as a Spark datastructure, it will be fetched to the driver!
    *
    * @return Matrix values as 2D Array.
    */
  // FIXME this should not be here but instead we should allow to create MatrixObjects from Double Arrays
  def getValues(): Array[Array[Double]] = matob match {
    case Some(mo) => MLContextConversionUtil.matrixObjectTo2DDoubleArray(mo)
    case None     => localImpl match {
      case Some(impl) => to2D(impl)
      case None => throw new RuntimeException("Matrix has no underlying values.")
    }
  }

  /**
    * Convert a 1D row-major order array to a 2D array.
    *
    * @param values The Array to be converted.
    * @return The same values reorganized into a 2D Array of rows.
    */
  private def to2D(values: Array[Double]): Array[Array[Double]] = {
    val out = Array.fill(this.nrow, this.ncol)(0.0)
    for (i <- 0 until nrow; j <- 0 until ncol) {
      out(i)(j) = values(i* ncol + j)
    }
    out
  }

  override def equals(that: Any): Boolean = that match {
    case m: Matrix => {
      val zipped = this.getValues.zip(m.getValues)
      val sameElems = zipped.map(x => x._1.sameElements(x._2)).fold(true)(_ && _)
      sameElems && this.nrow == m.nrow && this.ncol == m.ncol
    }
    case _ => false
  }

  override def hashCode(): Int = this.getValues.hashCode() + this.nrow + this.ncol

  override def toString: String = {
    val m = 3
    val n = 10
    s"""
       |Printing first $m rows, $n cols:
       |values:
       |${getValues.map(_.take(n).mkString(", ")).take(m).mkString("\n")}
       |nrow:   $nrow
       |ncol:   $ncol
       |hasMO:  ${matob != null}
     """.stripMargin
  }
}

/**
  * Companion object. Contains factory methods to generate [[Matrix]] instances.
  */
object Matrix {

  /**
    * This should be the primary way of constructing a [[Matrix]] from an [[Array]] of values.
    * The [[Matrix]] is constructed row-major order, i.e. the [[Array]] (1, 2, 1, 2) with dimensions (2,2) will
    * generate the [[Matrix]]
    *   1 2
    *   1 2
    * @param impl The values that will be assignmed to the cells of the matrix in row-major order
    * @param rows Number of rows that the matrix should have.
    * @param cols Number of columns that the matrix should have. Note that rows * cols must be equal to impl.length.
    * @return a [[Matrix]] with values as cell entries and dimensionality (rows, cols)
    */
  def apply(impl: Array[Double], rows: Int, cols: Int): Matrix = {
    new Matrix(rows, cols, None, None, Some(impl))
  }

  /**
    * Generate a [[Matrix]] from a dataframe.
    * When used in the `systemml` macro, this will pass the [[DataFrame]] as an input to the MLContext. Transformation
    * will happen only when the [[SystemMLAlgorithm]] is executed.
    *
    * NOTE: SystemML can not transform arbitrary DataFrames to matrices. For supported matrix-formats please check
    * the doumentation. If SystemML can not support the DataFrame, it will transform it into its internal [[Frame]]
    * datatype. In that case it might not be compatible with other matrix-operations.
    *
    * @param df Input [[DataFrame]] of a supported format.
    * @return Matrix instance where each row contains the values of the rows in the DataFrame.
    */
  def fromDataFrame(df: DataFrame): Matrix = ???

  /**
    * Creates a Matrix by specifying a fill-function over matrix indices.
    *
    * @param rows Number of rows the resulting matrix will have.
    * @param cols Number of columns the resulting matrix will have.
    * @param gen Generation function. For each index pair, specify what value should be generated.
    * @return Matrix of dimensionality (rows, cols) and values generated by the generator function.
    */
  private[sysml] def fill(rows: Int, cols: Int)(gen: (Int, Int) => Double): Matrix = {
    require(rows * cols < Int.MaxValue)
    val array = new Array[Double](rows * cols)
    for (i <- 0 until rows; j <- 0 until cols) {
      array((i * cols) + j) = gen(i, j)
    }
    Matrix(array, rows, cols)
  }

  /**
    * Generate a matrix where all values are 0.0.
    *
    * @param rows Number of rows the resulting matrix will have.
    * @param cols Number of columns the resulting matrix will have.
    * @return Matrix of dimensionality (rows, cols) and all values are 1.0.
    */
  def zeros(rows: Int, cols: Int): Matrix = fill(rows, cols)((i, j) => 0.0)

  /**
    * Generate a matrix where all values are 1.0.
    *
    * @param rows Number of rows the resulting matrix will have.
    * @param cols Number of columns the resulting matrix will have.
    * @return Matrix of dimensionality (rows, cols) and all values are 1.0.
    */
  def ones(rows: Int, cols: Int): Matrix = fill(rows, cols)((i, j) => 1.0)

  /**
    * Generate a matrix where all values are initialized randomly.
    * By default, values will be initialized uniformly over the interval [-0.5, 0.5].
    *
    * @param rows Number of rows the resulting matrix will have.
    * @param cols Number of columns the resulting matrix will have.
    * @return Matrix of dimensionality (rows, cols) where each value is initialized randomly according to a distribution.
    */
  // TODO: support more parameters (min, max, distribution, sparsity, seed)
  def rand(rows: Int, cols: Int): Matrix = fill(rows, cols)((i, j) => Random.nextDouble())

  /**
    * Generate a square matrix where the diagonal contains the provided value and the remaining cells are 0.0.
    * *
    * @param value The value to place on the diagonal.
    * @param length Number of rows and columns the resulting matrix will have.
    * @return Square matrix where all values are 0.0 except for the diagonal where the value is equal to the
    *         provided value.
    */
  def diag(value: Double, length: Int): Matrix = fill(length, length)((i, j) => if (i == j) value else 0.0)

  /**
    * Reshapes the [[Matrix]] into a new format. cols * rows must equal the original number of elements.
    *
    * A matrix A of the form  1 3  will be reshaped into  1
    *                         2 4                         2
    *                                                     3
    *                                                     4
    * by using Matrix.reshape(A, 4, 1)
    *
    * @param mat The matrix to be reshaped of dimensionality (m, n).
    * @param rows Number k of rows of the new matrix.
    * @param cols Number l of columns of the new matrix.
    * @return New matrix with the new dimensions and rearranged values of dimensionality (k, l).
    */
  def reshape(mat: Matrix, rows: Int, cols: Int): Matrix = ???

}

