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

import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.sysml.api.BaseAPISpec
import org.apache.sysml.api.linalg._
import org.apache.sysml.api.linalg.api._
import org.apache.sysml.api.mlcontext.MLContext
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class APISpec extends BaseAPISpec {
  val spark = SparkSession.builder().appName("ApiSpec").master("local[*]").getOrCreate()
  val sc    = spark.sparkContext

  implicit var mlctx: MLContext = _

  "Constructors" - {

    "Matrix" - {
      "ones, zeros, rand, diag" in {
        mlctx = new MLContext(sc)

        val algorithm = systemml {
          val A = Matrix.ones(2, 2)
          val B = Matrix.zeros(2, 2)
          val C = Matrix.rand(2, 2)
          val D = Matrix.diag(1.0, 2)

          (A, B, C, D)
        }

        algorithm.inputs shouldBe empty
        algorithm.outputs shouldEqual Array("A", "B", "C", "D")

        val result = algorithm.run(mlctx, true)

        result._1 shouldEqual Matrix(Array(1.0, 1.0, 1.0, 1.0), 2, 2)
        result._2 shouldEqual Matrix(Array(0.0, 0.0, 0.0, 0.0), 2, 2)

        result._4 shouldEqual Matrix(Array(1.0, 0.0, 0.0, 1.0), 2, 2)
      }

      "fromDataFrame" in {
        mlctx = new MLContext(sc)

        object dfTest extends Serializable {

          val numRows = 10
          val numCols = 7

          val data = sc.parallelize(0 to numRows - 1).map { _ => Row.fromSeq(Array.fill(numCols)(Random.nextDouble)) }
          val schema = StructType((0 to numCols - 1).map { i => StructField("C" + i, DoubleType, true) })
          val df = spark.createDataFrame(data, schema)

          val algorithm = systemml {
            val A = Matrix.fromDataFrame(df)
            val B = Matrix.fromDataFrame(df)

            (A, B)
          }
        }

        dfTest.algorithm.inputs.length shouldBe 1

        val dfName = dfTest.algorithm.inputs.headOption match {
          case Some((name, _)) => name
          case None => ""
        }

        dfName shouldEqual "df"

        dfTest.algorithm.outputs shouldEqual Array("A", "B")

        val result = dfTest.algorithm.run(mlctx, true)

        // TODO check result
      }

      "apply" in {
        mlctx = new MLContext(sc)

        val algorithm = systemml {
          val A = Matrix(Array(1.0, 2.0, 3.0, 4.0), 2, 2)
          val B = Matrix(Array(1.0, 2.0, 3.0, 4.0), 2, 2)

          (A, B)
        }

        algorithm.inputs shouldBe empty
        algorithm.outputs shouldEqual Array("A", "B")

        val result = algorithm.run(mlctx, true)

        result shouldEqual(Matrix(Array(1.0, 2.0, 3.0, 4.0), 2, 2),
                           Matrix(Array(1.0, 2.0, 3.0, 4.0), 2, 2))
      }

      "reshape" in {
        mlctx = new MLContext(sc)

        val algorithm = systemml {
          val A = Matrix(Array(1.0, 2.0, 3.0, 4.0), 2, 2)
          val B = Matrix.reshape(A, 4, 1)
          val C = Matrix.reshape(B, 2, 2)

          (A, B, C)
        }

        algorithm.inputs shouldBe empty
        algorithm.outputs shouldEqual Array("A", "B", "C")

        val result = algorithm.run(mlctx, true)

        result shouldEqual(Matrix(Array(1.0, 2.0, 3.0, 4.0), 2, 2),
                           Matrix(Array(1.0, 2.0, 3.0, 4.0), 4, 1),
                           Matrix(Array(1.0, 2.0, 3.0, 4.0), 2, 2))
      }

      "indexing" in {
        mlctx = new MLContext(sc)

        val algorithm = systemml {
          val A = Matrix.ones(3, 3)
          val B = Matrix.ones(3, 3)
          val C = Matrix.zeros(3, 3)
          val D = Matrix.zeros(3, 3)

          val a = A(0, 0) // (idx, idx)
          val b = B(0, :::)  // (idx, :::)
          val c = C(:::, 0) // (:::, idx)
          val d = D(:::, 0 to 1) // (:::, range)
          val e = A(0 until 2, :::) // (range, :::)
          val f = B(0, 0 until 2) // (idx, range)
          val g = C(0 to 1, 0) // (range, idx)
          val h = D(0 until 2, 0 to 1) // (range, range)

          (a, b, c, d, e, f, g, h)
        }

        algorithm.inputs shouldBe empty
        algorithm.outputs shouldEqual Array("a", "b", "c", "d", "e", "f", "g", "h")

        val result = algorithm.run(mlctx, true)

        result shouldEqual(1.0,
                           Matrix(Array(1.0, 1.0, 1.0), 1, 3),
                           Matrix(Array(0.0, 0.0, 0.0), 3, 1),
                           Matrix(Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0), 3, 2),
                           Matrix(Array(1.0, 1.0, 1.0, 1.0, 1.0, 1.0), 2, 3),
                           Matrix(Array(1.0, 1.0), 1, 2),
                           Matrix(Array(0.0, 0.0), 2, 1),
                           Matrix(Array(0.0, 0.0, 0.0, 0.0), 2, 2))
      }

      "updating" in {
        mlctx = new MLContext(sc)

        val algorithm = systemml {
          val A = Matrix.zeros(2, 3)
          val B = Matrix.zeros(2, 3)
          val C = Matrix.zeros(2, 3)
          val D = Matrix.zeros(2, 3)
          val E = Matrix.zeros(2, 3)
          val F = Matrix.zeros(2, 3)
          val G = Matrix.zeros(2, 3)
          val H = Matrix.zeros(2, 3)

          A(0, 0) = 1.0 // (idx, idx) = Double
          B(0, :::) = Vector.ones(3).t // (idx, :::) = Matrix
          C(:::, 0) = Vector.ones(2) // (:::, idx) = Matrix
          D(0, 0 to 1) = Vector.ones(2).t // (idx, range) = Matrix
          E(0 until 2, 1) = Vector.ones(2) // (range, idx) = Matrix
          F(0 until 2, 0 to 1) = Matrix.ones(2, 2) // (range, range) = Matrix
          G(0 to 1, :::) = Matrix.ones(2, 3) // (range, :::) = Matrix
          H(:::, 1 until 3) = Matrix.ones(2, 2) // (:::, range) = Matrix

          (A, B, C, D, E, F, G, H)
        }

        algorithm.inputs shouldBe empty
        algorithm.outputs shouldEqual Array("A", "B", "C", "D", "E", "F", "G", "H")

        val result = algorithm.run(mlctx, true)

        result._1 shouldEqual Matrix(Array(1.0, 0.0, 0.0,
                                           0.0, 0.0, 0.0), 2, 3)
        result._2 shouldEqual Matrix(Array(1.0, 1.0, 1.0,
                                           0.0, 0.0, 0.0), 2, 3)
        result._3 shouldEqual Matrix(Array(1.0, 0.0, 0.0,
                                           1.0, 0.0, 0.0), 2, 3)
        result._4 shouldEqual Matrix(Array(1.0, 1.0, 0.0,
                                           0.0, 0.0, 0.0), 2, 3)
        result._5 shouldEqual Matrix(Array(0.0, 1.0, 0.0,
                                           0.0, 1.0, 0.0), 2, 3)
        result._6 shouldEqual Matrix(Array(1.0, 1.0, 0.0,
                                           1.0, 1.0, 0.0), 2, 3)
        result._7 shouldEqual Matrix(Array(1.0, 1.0, 1.0,
                                           1.0, 1.0, 1.0), 2, 3)
        result._8 shouldEqual Matrix(Array(0.0, 1.0, 1.0,
                                           0.0, 1.0, 1.0), 2, 3)
      }
    }

    "Vector" - {
      "apply" in {
        mlctx = new MLContext(sc)

        val algorithm = systemml {
          val v = Vector.apply(Array(1.0, 2.0, 3.0, 4.0))
          val w = Vector.apply(Array(1.0, 2.0, 3.0, 4.0))

          (v, w)
        }

        algorithm.inputs shouldBe empty
        algorithm.outputs shouldEqual Array("v", "w")

        val result = algorithm.run(mlctx, true)

        result shouldEqual(Matrix(Array(1.0, 2.0, 3.0, 4.0), 4, 1),
                           Matrix(Array(1.0, 2.0, 3.0, 4.0), 4, 1))
      }

      "rand, ones, zeros" in {
        mlctx = new MLContext(sc)

        val algorithm = systemml {
          val v = Vector.rand(4)
          val w = Vector.ones(4)
          val x = Vector.zeros(4)

          (v, w, x)
        }

        algorithm.inputs shouldBe empty
        algorithm.outputs shouldEqual Array("v", "w", "x")

        val result = algorithm.run(mlctx, true)

        result._2 shouldEqual Matrix(Array(1.0, 1.0, 1.0, 1.0), 4, 1)
        result._3 shouldEqual Matrix(Array(0.0, 0.0, 0.0, 0.0), 4, 1)
      }
    }
  }

  "Unary Operations" - {

    "Matrix" - {

      ".t, .ncol, .nrow" in {
        mlctx = new MLContext(sc)

        val algorithm = systemml {
          val A = Matrix(Array(1.0, 2.0, 3.0, 4.0), 2, 2)

          val B = A.t
          val C = A.nrow
          val D = A.ncol

          (B, C, D)
        }

        algorithm.inputs shouldBe empty
        algorithm.outputs shouldEqual Array("B", "C", "D")

        val result = algorithm.run(mlctx, true)

        result shouldEqual(Matrix(Array(1.0, 3.0, 2.0, 4.0), 2, 2), 2, 2)
      }
    }
  }

  "Binary Operations" - {
    "Scalar - Scalar" - {

      "+, -, *, /" - {

        "Double - Double" in {
          mlctx = new MLContext(sc)

          val algorithm = systemml {
            val a = 5.0
            val b = 2.0

            val c = a + b
            val d = a - b
            val e = a * b
            val f = a / b

            (c, d, e, f)
          }

          algorithm.inputs shouldBe empty
          algorithm.outputs shouldEqual Array("c", "d", "e", "f")

          val result = algorithm.run(mlctx, true)

          result shouldEqual(7.0, 3.0, 10.0, 2.5)
        }

        "Int - Int" in {
          mlctx = new MLContext(sc)

          val algorithm = systemml {
            val a = 5
            val b = 2

            val c = a + b
            val d = a - b
            val e = a * b
            val f = a / b

            (c, d, e, f)
          }

          algorithm.inputs shouldBe empty
          algorithm.outputs shouldEqual Array("c", "d", "e", "f")

          val result = algorithm.run(mlctx, true)

          result shouldEqual(7, 3, 10, 2.5)
        }

        "Double - Int" in {
          mlctx = new MLContext(sc)

          val algorithm = systemml {
            val a = 5.0
            val b = 2

            val c = a + b
            val d = a - b
            val e = a * b
            val f = a / b

            (c, d, e, f)
          }

          algorithm.inputs shouldBe empty
          algorithm.outputs shouldEqual Array("c", "d", "e", "f")

          val result = algorithm.run(mlctx, true)

          result shouldEqual(7.0, 3.0, 10.0, 2.5)
        }

        "Int - Double" in {
          mlctx = new MLContext(sc)

          val algorithm = systemml {
            val a = 5
            val b = 2.0

            val c = a + b
            val d = a - b
            val e = a * b
            val f = a / b

            (c, d, e, f)
          }

          algorithm.inputs shouldBe empty
          algorithm.outputs shouldEqual Array("c", "d", "e", "f")

          val result = algorithm.run(mlctx, true)

          result shouldEqual(7.0, 3.0, 10.0, 2.5)
        }
      }
    }

    "Matrix - Scalar" - {
      "+, -, *, /" - {
        "Matrix - Double" in {
          mlctx = new MLContext(sc)

          val algorithm = systemml {
            val A = Matrix.ones(2, 2)
            val b = 5.0

            val C = A + b
            val D = A - b
            val E = A * b
            val F = A / b

            (C, D, E, F)
          }

          algorithm.inputs shouldBe empty
          algorithm.outputs shouldEqual Array("C", "D", "E", "F")

          val result = algorithm.run(mlctx, true)

          result shouldEqual(Matrix(Array(6.0, 6.0, 6.0, 6.0), 2, 2),
            Matrix(Array(-4.0, -4.0, -4.0, -4.0), 2, 2),
            Matrix(Array(5.0, 5.0, 5.0, 5.0), 2, 2),
            Matrix(Array(0.2, 0.2, 0.2, 0.2), 2, 2))
        }

        "Matrix - Int" in {
          mlctx = new MLContext(sc)

          val algorithm = systemml {
            val A = Matrix.ones(2, 2)
            val b = 5

            val C = A + b
            val D = A - b
            val E = A * b
            val F = A / b

            (C, D, E, F)
          }

          algorithm.inputs shouldBe empty
          algorithm.outputs shouldEqual Array("C", "D", "E", "F")

          val result = algorithm.run(mlctx, true)

          result shouldEqual(Matrix(Array(6.0, 6.0, 6.0, 6.0), 2, 2),
            Matrix(Array(-4.0, -4.0, -4.0, -4.0), 2, 2),
            Matrix(Array(5.0, 5.0, 5.0, 5.0), 2, 2),
            Matrix(Array(0.2, 0.2, 0.2, 0.2), 2, 2))
        }

        "Double - Matrix" in {
          mlctx = new MLContext(sc)

          val algorithm = systemml {
            val A = 5.0
            val b = Matrix.ones(2, 2)

            val C = A + b
            val D = A - b
            val E = A * b
            val F = A / b

            (C, D, E, F)
          }

          algorithm.inputs shouldBe empty
          algorithm.outputs shouldEqual Array("C", "D", "E", "F")

          val result = algorithm.run(mlctx, true)

          result shouldEqual(Matrix(Array(6.0, 6.0, 6.0, 6.0), 2, 2),
            Matrix(Array(4.0, 4.0, 4.0, 4.0), 2, 2),
            Matrix(Array(5.0, 5.0, 5.0, 5.0), 2, 2),
            Matrix(Array(5.0, 5.0, 5.0, 5.0), 2, 2))
        }

        "Int - Matrix" in {
          mlctx = new MLContext(sc)

          val algorithm = systemml {
            val A = 5
            val b = Matrix.ones(2, 2)

            val C = A + b
            val D = A - b
            val E = A * b
            val F = A / b

            (C, D, E, F)
          }

          algorithm.inputs shouldBe empty
          algorithm.outputs shouldEqual Array("C", "D", "E", "F")

          val result = algorithm.run(mlctx, true)

          result shouldEqual(Matrix(Array(6.0, 6.0, 6.0, 6.0), 2, 2),
            Matrix(Array(4.0, 4.0, 4.0, 4.0), 2, 2),
            Matrix(Array(5.0, 5.0, 5.0, 5.0), 2, 2),
            Matrix(Array(5.0, 5.0, 5.0, 5.0), 2, 2))
        }
      }
    }

    "Matrix - Matrix" - {
      "+, -, *, /, %*%" in {
        mlctx = new MLContext(sc)

        val algorithm = systemml {
          val A = Matrix.ones(2, 2)
          val B = Matrix.ones(2, 2)

          val C = A + B
          val D = A - B
          val E = A * B
          val F = A / B
          val G = A %*% B

          (C, D, E, F, G)
        }

        algorithm.inputs shouldBe empty
        algorithm.outputs shouldEqual Array("C", "D", "E", "F", "G")

        val result = algorithm.run(mlctx, true)

        result shouldEqual(Matrix(Array(2.0, 2.0, 2.0, 2.0), 2, 2),
          Matrix(Array(0.0, 0.0, 0.0, 0.0), 2, 2),
          Matrix(Array(1.0, 1.0, 1.0, 1.0), 2, 2),
          Matrix(Array(1.0, 1.0, 1.0, 1.0), 2, 2),
          Matrix(Array(2.0, 2.0, 2.0, 2.0), 2, 2))
      }
    }
  }

  "Builtin functions" - {

    "cbind" in {
      mlctx = new MLContext(sc)

      val algorithm = systemml {
        val A = Matrix.zeros(3, 3)
        val B = Matrix.ones(3, 2)
        val v = Vector.ones(3)

        val C = cbind(A, B)
        val D = cbind(B, A)
        val E = cbind(A, v)

        (C, D, E)
      }

      algorithm.inputs shouldBe empty
      algorithm.outputs shouldEqual Array("C", "D", "E")

      val result = algorithm.run(mlctx, true)

      result._1 shouldEqual Matrix(Array(0.0, 0.0, 0.0, 1.0, 1.0,
                                         0.0, 0.0, 0.0, 1.0, 1.0,
                                         0.0, 0.0, 0.0, 1.0, 1.0), 3, 5)

      result._2 shouldEqual Matrix(Array(1.0, 1.0, 0.0, 0.0, 0.0,
                                         1.0, 1.0, 0.0, 0.0, 0.0,
                                         1.0, 1.0, 0.0, 0.0, 0.0), 3, 5)

      result._3 shouldEqual Matrix(Array(0.0, 0.0, 0.0, 1.0,
                                         0.0, 0.0, 0.0, 1.0,
                                         0.0, 0.0, 0.0, 1.0), 3, 4)
    }

    "min(x), max(x)" in {
      mlctx = new MLContext(sc)

      val algorithm = systemml {
        val A = Matrix(Array(1.0, -3.0, -4.0,
                             0.0, 0.0, 0.0,
                             1.0, -3.0, -4.0), 3, 3)

        val B = Matrix(Array(9.999999, 10e6, -9.999999, -10e5), 2, 2)

        val C = Matrix.zeros(10, 10)

        val a = min(A)
        val b = max(A)
        val c = min(B)
        val d = max(B)
        val e = min(C)
        val f = max(C)

        (a, b, c, d, e, f)
      }

      algorithm.inputs shouldBe empty
      algorithm.outputs shouldEqual Array("a", "b", "c", "d", "e", "f")

      val result = algorithm.run(mlctx, true)

      result shouldEqual (-4.0, 1.0, -10e5, 10e6, 0.0, 0.0)
    }

    "min(x, y), max(x, y)" in {
      mlctx = new MLContext(sc)

      val algorithm = systemml {
        val A = Matrix(Array(1.0,      -3.0, -4.0,        0.0,   0.0, 0.0, 1.0, -3.0, -4.0), 3, 3)

        val B = Matrix(Array(9.999999, 10e6, -9.999999, -10e5, 0.0, 1.0, 5.0, -3.0, 10e-5), 3, 3)

        val D = 5.0
        val E = 10e-5

        // matrix - matrix
        val a = min(A, A)
        val b = max(A, A)
        val c = min(A, B)
        val d = max(A, B)
        val e = min(B, A)
        val f = max(B, A)

        // matrix - double
        val g = min(A, D)
        val h = max(A, D)
        val i = min(B, E)
        val j = max(B, E)

        // double - double
        val k = min(D, E)
        val l = max(D, E)
        val m = min(E, D)
        val n = max(E, D)

        (a, b, c, d, e, f, g, h, i, j, k, l, m, n)
      }

      algorithm.inputs shouldBe empty
      algorithm.outputs shouldEqual Array("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n")

      val result = algorithm.run(mlctx, true)

      result shouldEqual (
        Matrix(Array(1.0, -3.0, -4.0, 0.0, 0.0, 0.0, 1.0, -3.0, -4.0), 3, 3),
        Matrix(Array(1.0, -3.0, -4.0, 0.0, 0.0, 0.0, 1.0, -3.0, -4.0), 3, 3),
        Matrix(Array(1.0, -3.0, -9.999999, -10e5, 0.0, 0.0, 1.0, -3.0, -4.0), 3, 3),
        Matrix(Array(9.999999, 10e6, -4.0, 0.0, 0.0, 1.0, 5.0, -3.0, 10e-5), 3, 3),
        Matrix(Array(1.0, -3.0, -9.999999, -10e5, 0.0, 0.0, 1.0, -3.0, -4.0), 3, 3),
        Matrix(Array(9.999999, 10e6, -4.0, 0.0, 0.0, 1.0, 5.0, -3.0, 10e-5), 3, 3),
        Matrix(Array(1.0, -3.0, -4.0, 0.0, 0.0, 0.0, 1.0, -3.0, -4.0), 3, 3),
        Matrix(Array(5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0), 3, 3),
        Matrix(Array(10e-5, 10e-5, -9.999999, -10e5, 0.0, 10e-5, 10e-5, -3.0, 10e-5), 3, 3),
        Matrix(Array(9.999999, 10e6, 10e-5, 10e-5, 10e-5, 1.0, 5.0, 10e-5, 10e-5), 3, 3),
        10e-5,
        5.0,
        10e-5,
        5.0
      )
    }

    "prod" in {
      mlctx = new MLContext(sc)

      val algorithm = systemml {
        val A = Matrix(Array(1.0, 1.0, 1.0, 1.0), 2, 2)
        val B = Vector.zeros(3)
        val C = Matrix(Array(-3.0, 1.0, -3.0, -2.0), 2, 2)

        val a = prod(A)
        val b = prod(B)
        val c = prod(C)

        (a, b, c)
      }

      algorithm.inputs shouldBe empty
      algorithm.outputs shouldEqual Array("a", "b", "c")

      val result = algorithm.run(mlctx, true)

      result shouldEqual(1.0, 0.0, -18.0)
    }

    "rbind" in {
      mlctx = new MLContext(sc)

      val algorithm = systemml {
        val A = Matrix.zeros(2, 3)
        val B = Matrix.ones(2, 3)
        val v = Vector.ones(3).t

        val D = rbind(A, B)
        val E = rbind(B, A)
        val F = rbind(A, v)
        val G = rbind(v, v)

        (D, E, F, G)
      }

      algorithm.inputs shouldBe empty
      algorithm.outputs shouldEqual Array("D", "E", "F", "G")

      val result = algorithm.run(mlctx, true)

      result shouldEqual(
        Matrix(Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0), 4, 3),
        Matrix(Array(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0), 4, 3),
        Matrix(Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0), 3, 3),
        Matrix(Array(1.0, 1.0, 1.0, 1.0, 1.0, 1.0), 2, 3)
      )
    }

    "removeEmpty" ignore {

    }

    "replace" in {
      mlctx = new MLContext(sc)

      val algorithm = systemml {
        val A = Matrix(Array(1.0, Double.NaN, 1.0, Double.NaN, Double.NaN, 1.0), 2, 3)
        val B = Matrix(Array(0.0, -2.0, Double.MaxValue, 0.0, Double.MinPositiveValue, 0.0), 2, 3)
        val C = Matrix(Array(0.0, Double.PositiveInfinity, Double.NegativeInfinity, 0.0, 0.01, 0.0), 2, 3)

        val D = replace(A, Double.NaN, -1.0)
        val E = replace(B, Double.MaxValue, Double.MinPositiveValue)
        val F = replace(B, Double.MinPositiveValue, Double.MaxValue)
        val G = replace(C, Double.PositiveInfinity, 10e6)
        val H = replace(C, Double.NegativeInfinity, -10e6)

        (D, E, F, G, H)
      }

      algorithm.inputs shouldBe empty
      algorithm.outputs shouldEqual Array("D", "E", "F", "G", "H")

      val result = algorithm.run(mlctx, true)

      result shouldEqual(
        Matrix(Array(1.0, -1.0, 1.0, -1.0, -1.0, 1.0), 2, 3),
        Matrix(Array(0.0, -2.0, Double.MinPositiveValue, 0.0, Double.MinPositiveValue), 2, 3),
        Matrix(Array(0.0, -2.0, Double.MaxValue, 0.0, Double.MaxValue, 0.0), 2, 3),
        Matrix(Array(0.0, 10e6, Double.NegativeInfinity, 0.0, 0.01, 0.0), 2, 3),
        Matrix(Array(0.0, Double.PositiveInfinity, -10e6, 0.0, 0.01, 0.0), 2, 3)
      )
    }

    "rev" in {
      mlctx = new MLContext(sc)

      val algorithm = systemml {
        val A = Vector(Array(1.0, 2.0, 3.0, 4.0))
        val B = Matrix(Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0), 2, 3)
        val C = Matrix(Array(1.0, 1.0, 1.0, 0.0), 2, 2)

        val D = rev(A)
        val E = rev(B)
        val F = rev(C)

        (D, E, F)
      }

      algorithm.inputs shouldBe empty
      algorithm.outputs shouldEqual Array("D", "E", "F")

      val result = algorithm.run(mlctx, true)

      result shouldEqual(
        Matrix(Array(4.0, 3.0, 2.0, 1.0), 4, 1),
        Matrix(Array(4.0, 5.0, 6.0, 1.0, 2.0, 3.0), 2, 3),
        Matrix(Array(1.0, 0.0, 1.0, 1.0), 2, 2)
      )
    }

    "sum" in {
      mlctx = new MLContext(sc)

      val algorithm = systemml {
        val A = Matrix.ones(3, 3)
        val B = Matrix.zeros(3, 3)
        val C = Matrix(Array(1.0, -1.0, 2.0, -2.0), 2, 2)

        val a = sum(A)
        val b = sum(B)
        val c = sum(C)

        (a, b, c)
      }

      algorithm.inputs shouldBe empty
      algorithm.outputs shouldEqual Array("a", "b", "c")

      val result = algorithm.run(mlctx, true)

      result shouldEqual(9.0, 0.0, 0.0)
    }

    "pmin, pmax" in {
      mlctx = new MLContext(sc)

      val algorithm = systemml {
        val A = Matrix.ones(3, 3)
        val B = Matrix(Array(1.0, -1.0, 5.0, -10e5, 10e5, 0.0, Double.MaxValue, Double.PositiveInfinity, Double.NegativeInfinity), 3, 3)
        val s = -1.0

        val D = pmin(A, B)
        val E = pmax(A, B)
        val F = pmin(B, s)
        val G = pmax(B, s)

        (D, E, F, G)
      }

      algorithm.inputs shouldBe empty
      algorithm.outputs shouldEqual Array("D", "E", "F", "G")

      val result = algorithm.run(mlctx, true)

      result._1 shouldEqual Matrix(Array(1.0, -1.0, 1.0, -10e5, 1.0, 0.0, 1.0, 1.0, Double.NegativeInfinity), 3, 3)
      result._2 shouldEqual Matrix(Array(1.0, 1.0, 5.0, 1.0, 10e5, 1.0, Double.MaxValue, Double.PositiveInfinity, 1.0), 3, 3)
      result._3 shouldEqual Matrix(Array(-1.0, -1.0, -1.0, -10e5, -1.0, -1.0, -1.0, -1.0, Double.NegativeInfinity), 3, 3)
      result._4 shouldEqual Matrix(Array(1.0, -1.0, 5.0, -1.0, 10e5, 0.0, Double.MaxValue, Double.PositiveInfinity, -1.0), 3, 3)
    }

    "rowIndexMin, rowIndexMax" in {
      mlctx = new MLContext(sc)

      val algorithm = systemml {
        val A = Matrix(Array(0.0, -1.0, 1.0, Double.NegativeInfinity, Double.PositiveInfinity, 0.0), 3, 2)
        val B = Matrix(Array(-1.0, 0.0, 1.0, 0.0), 2, 2)

        val C = rowIndexMin(A)
        val D = rowIndexMax(A)
        val E = rowIndexMin(B)
        val F = rowIndexMax(B)

        (C, D, E, F)
      }

      algorithm.inputs shouldBe empty
      algorithm.outputs shouldEqual Array("C", "D", "E", "F")

      val result = algorithm.run(mlctx, true)

      result._1 shouldEqual Matrix(Array(2.0, 2.0, 2.0), 3, 1)
      result._2 shouldEqual Matrix(Array(1.0, 1.0, 1.0), 3, 1)
      result._3 shouldEqual Matrix(Array(1.0, 2.0), 2, 1)
      result._4 shouldEqual Matrix(Array(2.0, 1.0), 2, 1)
    }

    "mean" in {
      mlctx = new MLContext(sc)

      val algorithm = systemml {
        val A = Matrix.zeros(3, 3)
        val B = Matrix.ones(3, 3)
        val C = Matrix.diag(1.0, 3)
        val D = Matrix(Array(1.0, 2.0, 3.0, 4.0), 2, 2)

        val a = mean(A)
        val b = mean(B)
        val c = mean(C)
        val d = mean(D)

        (a, b, c, d)
      }

      algorithm.inputs shouldBe empty
      algorithm.outputs shouldEqual Array("a", "b", "c", "d")

      val result = algorithm.run(mlctx, true)

      result shouldEqual(0.0, 1.0, 3.0/9.0, 2.5)
    }

    "var, sd" in {
      mlctx = new MLContext(sc)

      val algorithm = systemml {
        val A = Matrix.ones(3, 3)
        val B = Matrix.diag(6.0, 3)
        val C = Matrix(Array(1.0, 2.0, 3.0, 4.0), 2, 2)

        val a = variance(A)
        val b = sd(A)
        val c = variance(B)
        val d = sd(B)
        val e = variance(C)
        val f = sd(C)

        (a, b, c, d, e, f)
      }

      algorithm.inputs shouldBe empty
      algorithm.outputs shouldEqual Array("a", "b", "c", "d", "e", "f")

      val result = algorithm.run(mlctx, true)

      result shouldEqual(0.0, 0.0, 9.0, 3.0, 5.0/3.0, Math.sqrt(5.0/3.0))
    }

    "moment" in {
      mlctx = new MLContext(sc)

      val algorithm = systemml {
        val A = Vector(Array(1.0, 2.0, 3.0, 4.0))
        val B = Vector.ones(4)

        val a = moment(A, 2)
        val b = moment(A, 3)
        val c = moment(A, 4)

        val d = moment(A, B, 2)
        val e = moment(A, B, 3)
        val f = moment(A, B, 4)

        (a, b, c, d, e, f)
      }

      algorithm.inputs shouldBe empty
      algorithm.outputs shouldEqual Array("a", "b", "c", "d", "e", "f")

      val result = algorithm.run(mlctx, true)

      result shouldEqual(1.25, 0.0, 2.5625, 1.25, 0.0, 2.5625)
    }

    "colSums, colMeans, colVars, colSds, colMaxs, colMins" in {
      mlctx = new MLContext(sc)

      val algorithm = systemml {
        val A = Matrix(Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0), 3, 3)

        val a = colSums(A)
        val b = colMeans(A)
        val c = colVars(A)
        val d = colSds(A)
        val e = colMins(A)
        val f = colMaxs(A)

        (a, b, c, d, e, f)
      }

      algorithm.inputs shouldBe empty
      algorithm.outputs shouldEqual Array("a", "b", "c", "d", "e", "f")

      val result = algorithm.run(mlctx, true)

      result._1 shouldEqual Matrix(Array(12.0, 15.0, 18.0), 1, 3)
      result._2 shouldEqual Matrix(Array(4.0, 5.0, 6.0), 1, 3)
      result._3 shouldEqual Matrix(Array(9.0, 9.0, 9.0), 1, 3)
      result._4 shouldEqual Matrix(Array(3.0, 3.0, 3.0), 1, 3)
      result._5 shouldEqual Matrix(Array(1.0, 2.0, 3.0), 1, 3)
      result._6 shouldEqual Matrix(Array(7.0, 8.0, 9.0), 1, 3)
    }

    "cov" in {
      mlctx = new MLContext(sc)

      val algorithm = systemml {
        val A = Vector(Array(1.0, 5.5, 7.8, 4.2, -2.7, -5.4, 8.9))
        val B = Vector(Array(0.1, 1.5, 0.8, -4.2, 2.7, -9.4, -1.9))
        val W = Vector.ones(7)

        val C = cov(A, B)
        val D = cov(A, B, W)

        (C, D)
      }

      algorithm.inputs shouldBe empty
      algorithm.outputs shouldEqual Array("C", "D")

      val result = algorithm.run(mlctx, true)

      result._1 shouldEqual 8.697380952380952
      result._2 shouldEqual 8.697380952380952
    }

    "table" in {
      mlctx = new MLContext(sc)

      val algorithm = systemml {
        val A = Vector(Array(1.0, 2.0, 3.0))
        val B = Vector(Array(1.0, 2.0, 3.0))
        val W = Vector(Array(2.0, 3.0, 4.0))

        val D = table(A, B)
        val E = table(A, B, W)

        (D, E)
      }

      algorithm.inputs shouldBe empty
      algorithm.outputs shouldEqual Array("D", "E")

      val result = algorithm.run(mlctx, true)

      result._1 shouldEqual Matrix(Array(1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0), 3, 3)
      result._2 shouldEqual Matrix(Array(2.0, 0.0, 0.0, 0.0, 3.0, 0.0, 0.0, 0.0, 4.0), 3, 3)
    }

    "cdf" ignore {
    }

    "icdf" ignore {
    }

    "aggregate" ignore {
    }

    "interQuartileMean" in {
      mlctx = new MLContext(sc)

      val algorithm = systemml {
        val A = Vector(Array(5.0, 8.0, 4.0, 38.0, 8.0, 6.0, 9.0, 7.0, 7.0, 3.0, 1.0, 6.0))
        val W = Vector(Array(2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0))

        val a = interQuartileMean(A)
        val b = interQuartileMean(A, W)

        (a, b)
      }

      algorithm.inputs shouldBe empty
      algorithm.outputs shouldEqual Array("a", "b")

      val result = algorithm.run(mlctx, true)

      result shouldEqual(6.5, 6.5)
    }

    "quantile" in {
      mlctx = new MLContext(sc)

      val algorithm = systemml {
        val A = Vector(Array(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0))
        val W = Vector(Array(2.0, 2.0, 2.0, 2.0, 2.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0))
        val P = Vector(Array(0.25, 0.5, 0.75))

        val a = quantile(A, 0.25)
        val b = quantile(A, 0.5)
        val c = quantile(A, 0.75)
        val d = quantile(A, W, 0.25)
        val e = quantile(A, W, 0.5)
        val f = quantile(A, W, 0.75)

        val G = quantile(A, P)
        val H = quantile(A, W, P)

        (a, b, c, d, e, f, G, H)
      }

      algorithm.inputs shouldBe empty
      algorithm.outputs shouldEqual Array("a", "b", "c", "d", "e", "f", "G", "H")

      val result = algorithm.run(mlctx, true)

      result shouldEqual(
        0.2, 0.5, 0.8, 0.1, 0.3, 0.6,
        Matrix(Array(0.2, 0.5, 0.8), 3, 1),
        Matrix(Array(0.1, 0.3, 0.6), 3, 1))
    }

    "rowSums, rowMeans, rowVars, rowSds, rowMaxs, rowMins" in {
      mlctx = new MLContext(sc)

      val algorithm = systemml {
        val A = Matrix(Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0), 3, 3).t

        val a = rowSums(A)
        val b = rowMeans(A)
        val c = rowVars(A)
        val d = rowSds(A)
        val e = rowMins(A)
        val f = rowMaxs(A)

        (a, b, c, d, e, f)
      }

      algorithm.inputs shouldBe empty
      algorithm.outputs shouldEqual Array("a", "b", "c", "d", "e", "f")

      val result = algorithm.run(mlctx, true)

      result._1 shouldEqual Matrix(Array(12.0, 15.0, 18.0), 3, 1)
      result._2 shouldEqual Matrix(Array(4.0, 5.0, 6.0), 3, 1)
      result._3 shouldEqual Matrix(Array(9.0, 9.0, 9.0), 3, 1)
      result._4 shouldEqual Matrix(Array(3.0, 3.0, 3.0), 3, 1)
      result._5 shouldEqual Matrix(Array(1.0, 2.0, 3.0), 3, 1)
      result._6 shouldEqual Matrix(Array(7.0, 8.0, 9.0), 3, 1)
    }

    "cumsum, cumprod, cummin, cummax" in {
      mlctx = new MLContext(sc)

      val algorithm = systemml {
        val A = Matrix(Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0), 3, 2)

        val B = cumsum(A)
        val C = cumprod(A)
        val D = cummin(A)
        val E = cummax(A)

        (B, C, D, E)
      }

      algorithm.inputs shouldBe empty
      algorithm.outputs shouldEqual Array("B", "C", "D", "E")

      val result = algorithm.run(mlctx, true)

      result._1 shouldEqual Matrix(Array(1.0, 2.0, 4.0, 6.0, 9.0, 12.0), 3, 2)
      result._2 shouldEqual Matrix(Array(1.0, 2.0, 3.0, 8.0, 15.0, 48.0), 3, 2)
      result._3 shouldEqual Matrix(Array(1.0, 2.0, 1.0, 2.0, 1.0, 2.0), 3, 2)
      result._4 shouldEqual Matrix(Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0), 3, 2)
    }

    "sample" ignore {

    }

    "outer" ignore {

    }

    "exp, log, abs, sqrt, round, floor, ceil" in {
      mlctx = new MLContext(sc)

      val algorithm = systemml {
        val a = 0.5
        val b = -0.5
        val A = Matrix(Array(0.3, 0.5, 0.7, 1.0), 2, 2)
        val B = Matrix(Array(-0.3, -0.5, -0.7, -1.0), 2, 2)

        val c = exp(a)
        val C = exp(A)
        val d = log(a)
        val D = log(A)
        val e = log(a, 10.0)
        val E = log(A, 10.0)
        val f = abs(b)
        val F = abs(B)
        val g = sqrt(a)
        val G = sqrt(A)
        val h = round(a)
        val H = round(A)
        val i = floor(a)
        val I = floor(A)
        val j = ceil(a)
        val J = ceil(A)

        (c, d, e, f, g, h, i, j, C, D, E, F, G, H, I, J)
      }

      algorithm.inputs shouldBe empty
      algorithm.outputs shouldEqual Array("c", "d", "e", "f", "g", "h", "i", "j", "C", "D", "E", "F", "G", "H", "I", "J")

      val result = algorithm.run(mlctx, true)

      // TODO add correctness test
    }

    "sin, cos, tan, asin, acos, atan" in {
      mlctx = new MLContext(sc)

      val algorithm = systemml {
        val a = 0.5
        val A = Matrix(Array(0.3, 0.5, 0.7, 1.0), 2, 2)

        val c = sin(a)
        val C = sin(A)
        val d = cos(a)
        val D = cos(A)
        val e = tan(a)
        val E = tan(A)
        val f = asin(a)
        val F = asin(A)
        val g = acos(a)
        val G = acos(A)
        val h = atan(a)
        val H = atan(A)

        (c, d, e, f, g, h, C, D, E, F, G, H)
      }

      algorithm.inputs shouldBe empty
      algorithm.outputs shouldEqual Array("c", "d", "e", "f", "g", "h", "C", "D", "E", "F", "G", "H")

      val result = algorithm.run(mlctx, true)

      // TODO add correctness test
    }

    "sign" in {
      mlctx = new MLContext(sc)

      val algorithm = systemml {
        val a = -1.0
        val b = 1.0
        val A = Matrix(Array(-10e5, 10.0, -0.1, 0.0), 2, 2)

       // val c = sign(a)
       // val d = sign(b)
        val B = sign(A)

        //(c, d, B)
        B
      }

      algorithm.inputs shouldBe empty
      algorithm.outputs shouldEqual Array("B")

      val result = algorithm.run(mlctx, true)

      result shouldEqual Matrix(Array(-1.0, 1.0, -1.0, 0.0), 2, 2)
    }

    "cholesky" in {
      mlctx = new MLContext(sc)

      val algorithm = systemml {
        val A = Matrix(Array(4.0, 12.0, -16.0, 12.0, 37.0, -43.0, -16.0, -43.0, 98.0), 3, 3)

        val B = cholesky(A)

        B
      }

      algorithm.inputs shouldBe empty
      algorithm.outputs shouldEqual Array("B")

      val result = algorithm.run(mlctx, true)

      result shouldEqual Matrix(Array(2.0, 0.0, 0.0, 6.0, 1.0, 0.0, -8.0, 5.0, 3.0), 3, 3)
    }

    "diag" in {
      mlctx = new MLContext(sc)

      val algorithm = systemml {
        val A = Matrix.diag(5.0, 4)

        val B = diag(A)

        B
      }

      algorithm.inputs shouldBe empty
      algorithm.outputs shouldEqual Array("B")

      val result = algorithm.run(mlctx, true)

      result shouldEqual Matrix(Array(5.0, 5.0, 5.0, 5.0), 4, 1)
    }

    "eigen" ignore {

    }

    "lu" ignore {

    }

    "qr" ignore {

    }

    "solve" in {
      mlctx = new MLContext(sc)

      val algorithm = systemml {
        val A = Matrix(Array(1.0, 1.0, 1.0, 0.0, 2.0, 5.0, 2.0, 5.0, -1.0), 3, 3)
        val b = Vector(Array(6.0, -4.0, 27.0))

        val X = solve(A, b)

        X
      }

      algorithm.inputs shouldBe empty
      algorithm.outputs shouldEqual Array("X")

      val result = algorithm.run(mlctx, true)

      result shouldEqual Matrix(Array(4.0, 3.0, -2.0), 3, 1)
    }

    "transpose" in {
      mlctx = new MLContext(sc)

      val algorithm = systemml {
        val A = Matrix(Array(1.0, 2.0, 3.0, 4.0), 2, 2)
        val B = A.t

        B
      }

      algorithm.inputs shouldBe empty
      algorithm.outputs shouldEqual Array("B")

      val result = algorithm.run(mlctx, true)

      result shouldEqual Matrix(Array(1.0, 3.0, 2.0, 4.0), 2, 2)
    }

    "trace" in {
      mlctx = new MLContext(sc)

      val algorithm = systemml {
        val A = Matrix(Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0), 3, 3)

        val a = trace(A)

        a
      }

      algorithm.inputs shouldBe empty
      algorithm.outputs shouldEqual Array("a")

      val result = algorithm.run(mlctx, true)

      result shouldEqual 15.0
    }
  }
}
