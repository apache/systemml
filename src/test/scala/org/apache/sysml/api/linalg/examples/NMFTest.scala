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

package org.apache.sysml.api.linalg.examples

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.sysml.api.linalg._
import org.apache.sysml.api.linalg.api._
import org.apache.sysml.api.mlcontext.MLContext
import org.apache.spark.sql.functions.udf
import org.junit.runner.RunWith
import org.scalatest.FreeSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NMFTest extends FreeSpec {

  val spark = SparkSession.builder().master("local[*]").appName("NMF").getOrCreate()
  val sc: SparkContext = spark.sparkContext

  "NMF" in {

    object NMF {
      // read data
      val df = spark.read.format("com.databricks.spark.csv").option("comment", "#").option("header", "true").load(getClass.getResource("/cs_abstracts").getPath)

      // combine titles and abstracts
      def combine = udf((x: String, y: String) => (x, y) match {
        case (x: String, y: String) => x ++ y
        case (x: String, null) => x
        case (null, y: String) => y
      })

      val dfTransformed = df.withColumn("combined", combine.apply(df("title"), df("abstract")))

      // tokenize
      val tokenizer = new Tokenizer().setInputCol("combined").setOutputCol("words")
      val wordsData = tokenizer.transform(dfTransformed)

      // hashing transformer to get term frequency
      val hashingTF = new HashingTF()
        .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(999)

      val featurizedData = hashingTF.transform(wordsData)

      // compute inverse document frequency
      val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
      val idfModel = idf.fit(featurizedData)
      val rescaledData = idfModel.transform(featurizedData)

      // combine titles and abstracts
      def project = udf((x: SparseVector) => x match {
        case y: SparseVector => y.toDense
      })

      val tfidf = rescaledData.withColumn("dense_features", project.apply(rescaledData("features"))).select("dense_features")

      val mlctx: MLContext = new MLContext(sc)

      def run = {

        val nmf = systemml {
          val V = Matrix.fromDataFrame(tfidf)
          // tfidf feature matrix coming from somewhere
          val k = 40
          val m, n = V.nrow
          // dimensions of tfidf
          val maxIters = 200

          var W = Matrix.rand(m, k)
          var H = Matrix.rand(k, n)

          for (i <- 0 to maxIters) {
            //main loop
            H = H * (W.t %*% V) / (W.t %*% (W %*% H))
            W = W * (V %*% H.t) / (W %*% (H %*% H.t))
          }

          (W, H) // return values
        }

        val (w, h) = nmf.run(mlctx, true)
        println("W: " + w)
      }
    }

    NMF.run
  }
}
