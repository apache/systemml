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

import org.apache.sysml.compiler.macros.RewriteMacros
import scala.language.experimental.macros

/**
  * Package object containing all builtin-functions for the SystemML API.
  */
package object api {
  import Format._

  /**
    * The entry point for the systemML macro
    */
  final def systemml[T](e: T): SystemMLAlgorithm[T] = macro RewriteMacros.impl[T]

  // whole row/column selector
  object :::

  ////////////////////////////////////////////////
  // BUILTIN FUNCTIONS
  ///////////////////////////////////////////////

  /**
    * Read a matrix from a file. Supported formats currently include csv and binary.
    *
    * @param path Path to the file on disk or hdfs.
    * @param format File format (CSV, BINARY).
    * @return Matrix that contains values read from the file.
    */
  def read(path: String, format: FileFormat): Matrix = ???

  /**
    * Write matrix to disc in specified format. Supported formats currently include csv and binary.
    *
    * @param mat The matrix that should be written to disk or hdfs.
    * @param path Path to write the matrix on disk or hdfs. Use file:// or hdfs:// to specify protocol.
    * @param format File format to write matrix data.
    */
  def write(mat: Matrix, path: String, format: Format.FileFormat): Unit = ???

  /**
    * Column-wise matrix concatenation. Concatenates the second matrix as additional columns to the first matrix.
    * If the first matrix is of dimensions m x n, the second matrix must be of dimensions m x o, i.e. have the same
    * number of rows as the first. The resulting matrix will be of dimensions m x (n + o).
    *
    * @param mat1 First (left) matrix m x n.
    * @param mat2 Second (right) matrix m x o.
    * @return Concatenated [left, right] matrix m x (n + o).
    */
  def cbind(mat1: Matrix, mat2: Matrix): Matrix = ???

  /**
    * Finds the smallest value in a matrix.
    *
    * @param mat Input matrix.
    * @return Smallest value in input matrix.
    */
  def min(mat: Matrix): Double = ???

  /**
    * Finds the largest value in a matrix.
    *
    * @param mat Input matrix.
    * @return Largest value in input matrix.
    */
  def max(mat: Matrix): Double = ???

  /**
    * Compares the values in corresponding matrix cells and returns a matrix containing the smaller values in this
    * comparison. Example: min([1, 2, 3], [2, 1, 0]) = [1, 1, 0]
    *
    * @param matA Matrix.
    * @param matB Matrix.
    * @return Matrix containing the smaller values from cellwise comparison of matA and matB.
    */
  def min(matA: Matrix, matB: Matrix): Matrix = ???

  /**
    * Compares the values in corresponding matrix cells and returns a matrix containing the larger values in this
    * comparison. Example: max([1, 2, 3], [2, 1, 0]) = [2, 2, 3]
    *
    * @param matA Matrix.
    * @param matB Matrix.
    * @return Matrix containing the larger values from cellwise comparison of matA and matB.
    */
  def max(matA: Matrix, matB: Matrix): Matrix = ???

  /**
    * Compares the values in the matrix to the given double value and returns a matrix containing the smaller value of
    * this comparison. Example: min([1, 4, 2], 3) = [1, 3, 2]
    *
    * @param mat Matrix.
    * @param b Double.
    * @return Matrix containing the smaller values from cellwise comparison of matA and b.
    */
  def min(mat: Matrix, b: Double): Matrix = ???

  /**
    * Compares the values in the matrix to the given double value and returns a matrix containing the larger value of
    * this comparison. Example: max([1, 4, 2], 3) = [3, 4, 3]
    * @param mat Matrix.
    * @param b Double.
    * @return Matrix containing the larger values from cellwise comparison of matA and b.
    */
  def max(mat: Matrix, b: Double): Matrix = ???

  /**
    * Compares doubles a and b and returns the smaller value.
    *
    * @param a Double.
    * @param b Double.
    * @return a, if a < b, b else.
    */
  def min(a: Double, b: Double): Double = ???

  /**
    * Compares doubles a and b and returns the larger value.
    *
    * @param a Double.
    * @param b Double.
    * @return a, if a > b, b else.
    */
  def max(a: Double, b: Double): Double = ???

  /**
    * Computes the product of all cells in the matrix.
    * Example: prod([1.0, 2.0, 3.0]) = 6.0
    *
    * @param mat Input Matrix.
    * @return Product of all cells of the input matrix.
    */
  def prod(mat: Matrix): Double = ???

  /**
    * Row-wise matrix concatenation. Concatenates the second matrix as additional rows to the first matrix.
    * Example: rbind([1, 2, 3], [4, 5, 6]) = [1, 2, 3]
    *                                        [4, 5, 6]
    *
    * @param top Matrix of which the rows will be on the top.
    * @param bottom Matrix of which the rows will be appended to the bottom of the top matrix.
    * @return Matrix which consists of the rows of the bottom matrix appended to the top matrix.
    */
  def rbind(top: Matrix, bottom: Matrix): Matrix = ???

  /**
    * Removes all empty rows or columns from the input matrix target X according to the specified margin.
    * Also, allows to apply a filter F before removing the empty rows/cols.
    *
    */
  def removeEmpty(target: Matrix, margin: Margin.MarginSelector, select: Boolean) = ???
  // TODO add test

  /**
    * Replace values that equal the pattern with the respective replacement value.
    * Example: pattern NaN can be replaced by another Double value.
    *
    * @param target The input matrix.
    * @param pattern Cell values that will be replaced.
    * @param replacement Replacement value for matching pattern values.
    * @return The input matrix with cell values equal to the pattern value replaced by the replacement value.
    */
  def replace(target: Matrix, pattern: Double, replacement: Double): Matrix = ???

  /**
    * Reverses the columns in a matrix.
    * Example: reverse([1, 2, 3]    [4, 5, 6]
    *                  [4, 5, 6]) = [1, 2, 3]
    *
    * @param target Input matrix.
    * @return Input matrix with reversed rows.
    */
  def rev(target: Matrix): Matrix = ???

  /**
    * Computes the sum of all elements in the matrix.
    *
    * @param mat Input matrix.
    * @return Sum of all elements in the matrix.
    */
  def sum(mat: Matrix): Double = ???

  /**
    * Parallel minimum. Computes cell-wise minimum between the cells of matrix A and B.
    *
    * @param matA Left input matrix.
    * @param matB Right input matrix.
    * @return Matrix containing the smaller values of the cell comparisons.
    */
  def pmin(matA: Matrix, matB: Matrix): Matrix = ???

  /**
    * Parallel minimum. Computes cell-wise minimum between the cells of matrix A and a double value.
    *
    * @param matA Input matrix.
    * @param value Input value.
    * @return Cell-wise minimum between cell entries of the input matrix and the provided value.
    */
  def pmin(matA: Matrix, value: Double): Matrix = ???

  /**
    * Parallel maximum. Computes cell-wise maximum between cells of matrix A and B.
    *
    * @param matA Left input matrix.
    * @param matB Right input matrix.
    * @return Matrix containing the larger values of the cell comparisons.
    */
  def pmax(matA: Matrix, matB: Matrix): Matrix = ???

  /**
    * Parallel maximum. Computes cell-wise maximum between the cells of matrix A and a double value.
    *
    * @param matA Input matrix.
    * @param value Input value.
    * @return Cell-wise maximum between cell entries of the input matrix and the provided value.
    */
  def pmax(matA: Matrix, value: Double): Matrix = ???

  /**
    * Find the column index of the smallest value in each row.
    * NOTE: Since DML is 1-based, the returned indices will be 1-based!
    *
    * @param mat Input Matrix.
    * @return Column vector containing the column-indices of the minimum value of each row.
    */
  def rowIndexMin(mat: Matrix): Matrix = ???

  /**
    * Find the column index of the largest value in each row.
    * NOTE: Since DML is 1-based, the returned indices will be 1-based!
    *
    * @param mat Input Matrix.
    * @return Column vector containing the column-indices of the minimum value of each row.
    */
  def rowIndexMax(mat: Matrix): Matrix = ???

  /**
    * Compute the mean of all values in the matrix.
    *
    * @param mat Input matrix.
    * @return Mean value over all cells of the input matrix.
    */
  def mean(mat: Matrix): Double = ???

  /**
    * Compute the sample variance over all cells in the matrix.
    *
    * @param mat Input matrix.
    * @return Variance of all values in the matrix.
    */
  def variance(mat: Matrix): Double = ???

  /**
    * Compute the sample standard deviation over all cells in the matrix.
    *
    * @param mat Input matrix.
    * @return Standard deviance of all values in the matrix.
    */
  def sd(mat: Matrix): Double = ???

  /**
    * Compute the k-th central moment of the values of a column vector.
    *
    * @param mat Input matrix, must be n x 1 (column vector).
    * @param k Moment to compute, k element of [2, 3, 4].
    * @return K-th central moment for values of the input matrix.
    */
  def moment(mat: Matrix, k: Int): Double = ???

  /**
    * Compute the k-th central moment of the values in a column-vector weighted by a weight matrix.
    *
    * @param mat Input matrix, must be n x 1 (column vector).
    * @param weights Weight matrix of the same dimension as the input matrix (n x 1).
    * @param k Moment to compute, k element of [2, 3, 4].
    * @return K-th central moment for values of the input matrix weighted by the weight matrix.
    */
  def moment(mat: Matrix, weights: Matrix, k: Int): Double = ???

  /**
    * Compute the sum over all values (rows) in each column.
    *
    * @param mat Input matrix.
    * @return Output matrix of size (1 x n) containing the sums over all values in each column.
    */
  def colSums(mat: Matrix): Matrix = ???

  /**
    * Compute the mean over all values (rows) in each column.
    *
    * @param mat Input matrix.
    * @return Output matrix of size (1 x n) containing the mean over all values in each column.
    */
  def colMeans(mat: Matrix): Matrix = ???

  /**
    * Compute the sample variance over all values (rows) in each column.
    *
    * @param mat Input matrix.
    * @return Output matrix of size (1 x n) containing the variance over all values in each column.
    */
  def colVars(mat: Matrix): Matrix = ???

  /**
    * Compute the sample standard deviation over all values (rows) in each column.
    *
    * @param mat Input matrix.
    * @return Output matrix of size (1 x n) containing the sample standard deviation over all values in each column.
    */
  def colSds(mat: Matrix): Matrix = ???

  /**
    * Compute the largest value over all values (rows) in each column.
    *
    * @param mat Input matrix.
    * @return Output matrix of size (1 x n) containing the largest value of all values in each column.
    */
  def colMaxs(mat: Matrix): Matrix = ???

  /**
    * Compute the smallest value over all values (rows) in each column.
    *
    * @param mat Input matrix.
    * @return Output matrix of size (1 x n) containing the smallest value of all values in each column.
    */
  def colMins(mat: Matrix): Matrix = ???

  /**
    * Compute the covariance between two column vectors of the same dimension.
    *
    * @param matA Left input vector of size n x 1.
    * @param matB Right input vector of size n x 1.
    * @return Covariance of both vectors.
    */
  def cov(matA: Matrix, matB: Matrix): Double = ???

  /**
    * Compute the covariance between two column vectors of the same dimension with extra weighting.
    *
    * @param matA Left input vector of size n x 1.
    * @param matB Right input vector of size n x 1.
    * @param weights Weight vector of size n x 1.
    * @return Weighted covariance of both vectors.
    */
  def cov(matA: Matrix, matB: Matrix, weights: Matrix): Double = ???

  /**
    * Compute the contingency table of two vectors A and B. The resulting table consists of max(A) rows and max(B)
    * columns. For the output F it holds that F[i,j] = |{ k | A[k] = i and B[k] = j, 1 ≤ k ≤ n }|
    *
    * @param matA Left input vector.
    * @param matB Right input vector.
    * @return Contingency table F.
    */
  def table(matA: Matrix, matB: Matrix): Matrix = ???

  /**
    * Compute the contingency table of two vectors A and B. The resulting table consists of max(A) rows and max(B)
    * columns. The weights are incorporated in the following way: F[i,j] = ∑kC[k], where A[k] = i and B[k] = j (1 ≤ k ≤ n)
    *
    * @param matA Left input vector.
    * @param matB Right input vector.
    * @param weights Weight vector of same dimensions as input.
    * @return Contingency table F.
    */
  def table(matA: Matrix, matB: Matrix, weights: Matrix): Matrix = ???

  // TODO cdf

  // TODO icdf

  // TODO aggregate

  /**
    * Computes the mean of all x in X such that x > quantile(X, 0.25) and x < quartile(X, 0.75).
    *
    * @param vec Input vector of dimensionality n x 1.
    * @return Interquartile mean.
    */
  def interQuartileMean(vec: Matrix): Double = ???

  /**
    * Computes the weighted mean of all x in X such that x > quantile(X, 0.25) and x < quartile(X, 0.75).
    *
    * @param vec Input vector of dimensionality n x 1.
    * @param weights Weight vector of dinemsionality n x 1. Note that the weights must be whole Doubles (no fractions)
    * @return Weighted interquartile mean.
    */
  def interQuartileMean(vec: Matrix, weights: Matrix): Double = ???

  /**
    * Computes the p-th quantile for input Vector X.
    *
    * @param mat Input vector of dimension n x 1.
    * @param p The requested p-quantile.
    * @return Value that represents the p-th quantile in X.
    */
  def quantile(mat: Matrix, p: Double): Double = ???

  /**
    * Computes a vector of p-quantiles for input Vector X.
    *
    * @param mat Input vector of dimension n x 1.
    * @param p The requested p-quantiles in a vector of dimension n x 1.
    * @return Values that represent the requested p-quantiles in X.
    */
  def quantile(mat: Matrix, p: Matrix): Matrix = ???

  /**
    * Computes the p-th quantile for input Vector X weighted by a weight vector of dimension n x 1.
    *
    * @param mat Input vector of dimension n x 1.
    * @param weights Weight vector of dimension n x 1.
    * @param p The requested p-quantile.
    * @return Value that represents the p-th quantile in X.
    */
  def quantile(mat: Matrix, weights: Matrix, p: Double): Double = ???

  /**
    * Computes a vector of p-quantiles for input Vector X weighted by a weight vector of dimension n x 1.
    *
    * @param mat Input vector of dimension n x 1.
    * @param weights Weight vector of dimension n x 1.
    * @param p The requested p-quantiles in a vector of dimension n x 1.
    * @return Values that represent the requested p-quantiles in X.
    */
  def quantile(mat: Matrix, weights: Matrix, p: Matrix): Matrix = ???

  /**
    * Compute the sum over all values (colums) in each row.
    *
    * @param mat Input matrix.
    * @return Output matrix of size (n x 1) containing the sums over all values in each row.
    */
  def rowSums(mat: Matrix): Matrix = ???

  /**
    * Compute the sample mean over all values (colums) in each row.
    *
    * @param mat Input matrix.
    * @return Output matrix of size (n x 1) containing the sample mean over all values in each row.
    */
  def rowMeans(mat: Matrix): Matrix = ???

  /**
    * Compute the sample variance over all values (colums) in each row.
    *
    * @param mat Input matrix.
    * @return Output matrix of size (n x 1) containing the sample variance over all values in each row.
    */
  def rowVars(mat: Matrix): Matrix = ???

  /**
    * Compute the sample standard deviation over all values (colums) in each row.
    *
    * @param mat Input matrix.
    * @return Output matrix of size (n x 1) containing the sample standard deviation over all values in each row.
    */
  def rowSds(mat: Matrix): Matrix = ???

  /**
    * Compute the largest value over all values (colums) in each row.
    *
    * @param mat Input matrix.
    * @return Output matrix of size (n x 1) containing the largest value of all values in each row.
    */
  def rowMaxs(mat: Matrix): Matrix = ???

  /**
    * Compute the smallest value over all values (colums) in each row.
    *
    * @param mat Input matrix.
    * @return Output matrix of size (n x 1) containing the smallest value of all values in each row.
    */
  def rowMins(mat: Matrix): Matrix = ???

  /**
    * Compute the cumulative sum over columns of a matrix.
    *
    * Example:       ([1 2])   [1  2]
    *          cumsum([3 4]) = [4  6]
    *                ([5 6])   [9 12]
    *
    * @param mat Input matrix.
    * @return Matrix of same dimension as input matrix containing the cumulative sum over the columns of the input.
    */
  def cumsum(mat: Matrix): Matrix = ???

  /**
    * Compute the cumulative product over columns of a matrix.
    *
    * Example:        ([1 2])   [ 1  2]
    *          cumprod([3 4]) = [ 3  8]
    *                 ([5 6])   [15 48]
    *
    * @param mat Input matrix.
    * @return Matrix of same dimension as input matrix containing the cumulative product over the columns of the input.
    */
  def cumprod(mat: Matrix): Matrix = ???

  /**
    * Compute the cumulative minimum over columns of a matrix.
    *
    * Example:        ([1 2])   [1 2]
    *           cummin([3 4]) = [1 2]
    *                 ([5 6])   [1 2]
    *
    * @param mat Input matrix.
    * @return Matrix of same dimension as input matrix containing the cumulative minimum over the columns of the input.
    */
  def cummin(mat: Matrix): Matrix = ???

  /**
    * Compute the cumulative maximum over columns of a matrix.
    *
    * Example:        ([1 2])   [1 2]
    *           cummax([3 4]) = [3 4]
    *                 ([5 6])   [4 6]
    *
    * @param mat Input matrix.
    * @return Matrix of same dimension as input matrix containing the cumulative maximum over the columns of the input.
    */
  def cummax(mat: Matrix): Matrix = ???

  /**
    * Compute the natural logarithm with base e.
    *
    * @param x Input value.
    * @return log(x, e)
    */
  def log(x: Double): Double = ???

  /**
    * compute the natural logarithm with base e for every cell in the matrix.
    *
    * @param mat Input matrix.
    * @return Matrix where each cell contains the natural logarithm of each corresponding cell in the input matrix.
    */
  def log(mat: Matrix): Matrix = ???

  /**
    * Compute logarithm with respect to a given base.
    *
    * @param x Input value.
    * @param base Base for computing the logarithm.
    * @return Logarithm with respect to provided base.
    */
  def log(x: Double, base: Double): Double = ???

  /**
    * Compute logarithm with respect to a given base.
    *
    * @param mat Input matrix.
    * @param base Base for computing the logarithm.
    * @return Matrix containing the logarithm for each cell value with respect to provided base.
    */
  def log(mat: Matrix, base: Double): Matrix = ???

  /**
    * Compute absolute value of provided input.
    *
    * @param x Input value.
    * @return Absolute value.
    */
  def abs(x: Double): Double = ???

  /**
    * Compute absolute value of provided input.
    *
    * @param mat Input value.
    * @return Matrix of absolute values for each cell.
    */
  def abs(mat: Matrix): Matrix = ???

  /**
    * Compute the exponential of x, pow(e, x).
    *
    * @param x
    * @return
    */
  def exp(x: Double): Double = ???

  /**
    * Compute the exponential values of all cells x in a matrix X, pow(e, x).
    *
    * @param mat
    * @return
    */
  def exp(mat: Matrix): Matrix = ???

  /**
    * Compute the square-root of the input value.
    *
    * @param x Input value.
    * @return Square-root of input value.
    */
  def sqrt(x: Double): Double = ???

  /**
    * Compute the square-root for each value in the matrix.
    *
    * @param mat Input matrix.
    * @return Matrix of square-roots for each value.
    */
  def sqrt(mat: Matrix): Matrix = ???

  /**
    * Round the double value to the next integer. If (x >= 0.5) 1.0 else 0.0
    *
    * @param x Input value.
    * @return Input value rounded to next integer number.
    */
  def round(x: Double): Double = ???

  /**
    * Round each value in the matrix to the next integer value. If (x >= 0.5) 1.0 else 0.0
    *
    * @param mat Input matrix.
    * @return Matrix containing rounded values of the input matrix.
    */
  def round(mat: Matrix): Matrix = ???

  /**
    * Round input value to the next lower integer value.
    * Examples: floor(0.9) = 0.0; floor(0.1) = 0.0
    *
    * @param x Input value.
    * @return Input value rounded to the next lower integer value.
    */
  def floor(x: Double): Double = ???

  /**
    * Round each value in the input matrix to the next lower integer value.
    *
    * @param mat Input matrix.
    * @return Matrix where each value is the next lower integer value of the input matrix.
    */
  def floor(mat: Matrix): Matrix = ???

  /**
    * Round input value to the next largest integer value.
    * Examples: ceil(0.1) = 1.0; ceil(0.9) = 1.0
    *
    * @param x Input value.
    * @return Input value rounded to the next larger integer value.
    */
  def ceil(x: Double): Double = ???

  /**
    * Round each value in the input matrix to the next larger integer value.
    *
    * @param mat Input matrix.
    * @return Matrix where each value is the next larger integer value of the input matrix.
    */
  def ceil(mat: Matrix): Matrix = ???

  /**
    * Compute sin(x).
    *
    * @param x Input value.
    * @return Sin(x).
    */
  def sin(x: Double): Double = ???

  /**
    * Compute sin(X) for input matrix X.
    *
    * @param mat Input matrix.
    * @return Sin(x).
    */
  def sin(mat: Matrix): Matrix = ???

  /**
    * Compute cos(x).
    *
    * @param x Input value.
    * @return Cos(x).
    */
  def cos(x: Double): Double = ???

  /**
    * Compute cos(X) for input matrix X.
    *
    * @param mat Input matrix.
    * @return Cos(X).
    */
  def cos(mat: Matrix): Double = ???

  /**
    * Compute tan(x).
    *
    * @param x Input value.
    * @return Tan(x).
    */
  def tan(x: Double): Double = ???

  /**
    * Compute tan(X) for input matrix X.
    *
    * @param mat Input matrix.
    * @return Tan(X).
    */
  def tan(mat: Matrix): Matrix = ???

  /**
    * Compute asin(x).
    *
    * @param x Input value.
    * @return Asin(x).
    */
  def asin(x: Double): Double = ???

  /**
    * Compute asin(X) for input matrix X.
    *
    * @param mat Input matrix.
    * @return Asin(X).
    */
  def asin(mat: Matrix): Matrix = ???

  /**
    * Compute acos(x).
    *
    * @param x Input value.
    * @return Acos(x).
    */
  def acos(x: Double): Double = ???

  /**
    * Compute acos(X) for input matrix X.
    *
    * @param mat Input matrix.
    * @return Acos(X).
    */
  def acos(mat: Matrix): Matrix = ???

  /**
    * Compute atan(x).
    *
    * @param x Input value.
    * @return Atan(x).
    */
  def atan(x: Double): Double = ???

  /**
    * Compute atan(X) for input matrix X.
    *
    * @param mat Input matrix.
    * @return Atan(X).
    */
  def atan(mat: Matrix): Matrix = ???

  /**
    * Compute the sign of the input value.
    *
    * @param x Input value.
    * @return 1.0 if x > 0, 0.0 if x == 0, -1.0 else
    */
  def sign(x: Double): Double = ???

  /**
    * Compute the sign for every value in the matrix.
    *
    * @param mat Input matrix.
    * @return Matrix of signs for each value in the input matrix. Signs are computed in the following way:
    *         1.0 if x > 0, 0.0 if x == 0, -1.0 else
    */
  def sign(mat: Matrix): Matrix = ???

  /**
    * Computethe Cholesky decomposition of the symmetric input matrix.
    *
    * @param mat Symmetric input matrix.
    * @return Cholesky factorization of input matrix.
    */
  def cholesky(mat: Matrix): Matrix = ???

  /**
    * Create a vector from the diagonal of a square input matrix.
    *
    * @param mat Square input matrix.
    * @return Vector containing the diagonal elements of the suqare input matrix.
    */
  def diag(mat: Matrix): Matrix = ???

  /***
    * Computes the least squares solution for system of linear equations A %*% x = b
    * It finds x such that ||A%*%x – b|| is minimized. The solution vector x is computed using a QR decomposition of A.
    *
    * Note: only for data that fits in the driver memory, not distributed!
    *
    * @param A Coefficient matrix.
    * @param b Right hand side of the equation.
    * @return Solution for the unknowns of the equation.
    */
  def solve(A: Matrix, b: Matrix): Matrix = ???

  /**
    * Computes the trace of a matrix. The trace of a matrix is defined as the sum of the elements on the diagonal.
    *
    * @param mat Square input matrix.
    * @return Sum of the elements on the diagonal.
    */
  def trace(mat: Matrix): Double = ???

  /**
    * Parallel predicate of all cell values.
    * Applies the predicate p to all cells x and replaces the cell with either 0.0 (false) or 1.0 (true).
    *
    * @param mat Input matrix where each cell value is used as first argument for the predicate.
    * @param x The value to apply as second argument to the predicate.
    * @param op The predicate operator to use.
    * @return Matrix containing 1.0 (true) or 0.0 (false) depending on the evaluation of the predicate.
    */
  @deprecated
  def ppred(mat: Matrix, x: Double, op: String): Matrix = ???

  ///////////////////////////////////
  // Implicit Matrix and Matrix Ops
  ///////////////////////////////////

  /** This allows operations with Matrixs and Matrices as left arguments such as Double * Matrix */
  implicit class MatrixOps(private val n: Double) extends AnyVal {
    def +(v: Matrix): Matrix = v + n

    def -(v: Matrix): Matrix = v - n

    def *(v: Matrix): Matrix = v * n

    def /(v: Matrix): Matrix = v / n
  }

  object Format {
    sealed trait FileFormat
    case object CSV extends FileFormat
    case object BINARY extends FileFormat
  }

  object Margin {
    sealed trait MarginSelector
    case object Rows extends MarginSelector
    case object Cols extends MarginSelector
  }
}
