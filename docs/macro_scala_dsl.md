# SystemML Scala DSL
Developer Documentation

## Overview

The SystemML Scala DSL enables data scientists to write algorithms for SystemML
using Scala syntax with a special `Matrix` type. This matrix comes with the promise that every operation on it will be executed using the SystemML compiler and runtime. The DSL is based on a quotation approach but will be further developed to enable eager or lazily evaluated linear algebra expressions. Due to the constraints of SystemML and using its language (DML) as a target language, we do not support the full set of Scala expressions in the current DSL. After reading through this documentation you should know what is and what isn't supported.

To get you motivated, here is a first example of an algorithm written in DML and executed using the `MLContext` compared to the same algorithm written in our Scala DSL:

<head>
<title>DML vs. Scala DSL Syntax</title>
</head>
<body>
<table border="1" width="100%">
<caption>The same algorithm implemented in the external DSL (DML) and the embedded Scala DSL</caption><tr>
<th>DML</th>
<th>Scala</th>
</tr>
<tr>
<td>
<pre lang="r">
V = read("/path/to/file.csv", format="csv")
k = 40
m = nrow(V)
n = nrow(V)
maxIters = 200

W = rand(rows=m, cols=k)
H = rand(rows=k, cols=n)

for (i in 0 + 1:maxIters + 1) {
  H = ((H * (t(W) %*% V)) / (t(W) %*% (W %*% H)))
  W = ((W * (V %*% t(H))) / (W %*% (H %*% t(H))))
}
</pre>
</td>
<td>
<pre lang="scala">
val V = read("/path/to/file.csv", CSV)
val k = 40
val m, n =  V.nrow
val maxIters = 200

var W = Matrix.rand(m, k)
var H = Matrix.rand(k, n)

for (i <- 0 to maxIters) { //main loop
    H = H * (W.t %*% V) / (W.t %*% (W %*% H))
    W = W * (V %*% H.t) / (W %*% (H %*% H.t))
}
</pre>
</td>
</tr>
</table>
</body>

## Getting Started

The current API to write user code for SystemML includes a `Matrix` and `Vector` type, where vector is just syntactic sugar for a column matrix with a single column in SystemML.
Additionally, we aim to provide all builtin functions that SystemML currently supports. If you encounter problems or missing features, please add it to the list [here](ADD LINK TO  JIRA)

To write algorithms, you can either setup SystemML in the Spark shell for write algorithm in your favorite IDE and use SystemML with `spark-submit`.

### Algorithm structure

Algorithms in the Scala DSL are implemented as arguments to the macro `systemml` which works just like a Scala function and takes a Scala expression as input. The simplest setup would be in a Scala `App` object as in:

```scala
package org.apache.sysml.api.linalg.examples

import org.apache.sysml.api.linalg._
import org.apache.sysml.api.linalg.api._

object MyAlgorithm extends App {

val algorithm = systemml {
    // code written here will be executed in SystemML
  }
}
```

`App` objects in Scala don't need a `main`-method and can be directly executed from within the IDE. The function `systemml` takes our Scala code and converts it into DML code. It then wraps everything inside an instance of the class `SystemMLAlgorithm[T]` and returns it. The type parameter `T` will be the type of the returned values inside the `systemml` block. To make it more concrete, let's consider a simple example:

```scala
package org.apache.sysml.api.linalg.examples

import org.apache.sysml.api.linalg._
import org.apache.sysml.api.linalg.api._

object MyAlgorithm extends App {

val algorithm = systemml {
  val A: Matrix = Matrix.rand(5, 3)
  val B: Matrix = Matrix.rand(3, 7)
  val C: Matrix = A %*% B
  C
  }
}
```

This code performs a simple matrix multiplication of two random matrices and returns the result in the variable `C`. The result of `systemml` is then `SystemMLAlgorithm[Matrix]`. Up until now, no code has actually been executed. To run the algorithm with SystemML on Spark we have to provide the `MLContext` object as an implicit and invoke the `run()` method on the algorithm instance:

```scala
package org.apache.sysml.api.linalg.examples

import org.apache.spark.sql.SparkSession

import org.apache.sysml.api.mlcontext.MLContext
import org.apache.sysml.api.linalg._
import org.apache.sysml.api.linalg.api._

object MyAlgorithm extends App {
  val spark = SparkSession.builder().master("local[*]").appName("MyAlgorithm").getOrCreate()
  val sc    = spark.sparkContext

  val mlctx = new MLContext(sc)

  val algorithm = systemml {
    val A: Matrix = Matrix.rand(5, 3)
    val B: Matrix = Matrix.rand(3, 7)
    val C: Matrix = A %*% B
    C
  }

  val result: Matrix = algorithm.run(mlctx)
}
```

This will execute the algorithm using SystemML and Spark. Executing the `run()`-method then returns the result of type `Matrix`. If you run the code in a spark shell, you will not have to create the `SparkSession` and `SparkContext` yourself but can use the pre-initialized ones. To get the generated DML code, you can give an additional `printDML` parameter to `run(mlctx, printDML=true)`.

Internally, the `systemml` function converts the Scala code into DML and you will get to see the generated code once you execute `run()`. For our above example, the expanded/transformed version would look like this:

```Scala
val algorithm = new SystemMLAlgorithm[Matrix]  {

  def run(ml: MLContext, printDML: Boolean = false): Matrix = {
    val dmlString = s"""| A = rand(5, 3)
                        | B = rand(3, 7)
                        | C = A %*% B
                    """

    val script = dml(dmlString).in(Seq()).out(Seq("C"))
    val res = ml.execute(script)
    val out = res.getTuple[Matrix]("C")
    out._1
  }

  val result: Matrix = algorithm.run()
}
```
### Error handling

Before execution, we print the generated DML code including line-numbers and optionally the plan of generated instructions. Error-messages returned from SystemML refer to the line-numbers in generated DML while errors thrown during translation can have different causes and therefore different appearances. We're working on improved error handling in both cases. If you get a message that says `Error during macro expansion...` then it's probably not your fault and something inside the translation phase went wrong. In this case, please open a new Jira issue with your code and the error message.

If the error comes from SystemML, it could be that there is an error in the generated DML code or that there is a semantic error in your code. In both cases feel free to report the error or ask on our [Mailing List]() for help.

### Restrictions

Due to the limitations of DML we can not translate every possible Scala code into DML. This is especially true for features like Exception handling, pattern-matching and user-defined functions (UDFS). We are working on expanding the set of features with UDF having a high priority to enable library-building. Another limitation is the usage of buildin-functions from Scala-base and Scala collections.

We recomment that inside the `systemml` block you only use the types provided in the `org.apache.sysml.api.linalg` package with Scala types `Int` and `Double`. We also allow the use of `println` from `scala.Predef`.
For SystemML built-in functions you should be able to find everything you need that is available in DML. For a full list, chekc the scaladoc in the `org.apache.sysml.api.linalg.api` package object.

Scala features that are not support include:
- Pattern matching
- User defined functions and lambdas (except `for-loops`)
- Scala collections
- types except for `Int` and `Double`

##A larger example

As an example that involves control flow (the for-loop) we show how the NMF algorithm from our [examples package]() can be implemented in the Scala DSL. You can find more examples in there!
The code includes preprocessing with spark of a dataset that consists of title, author, and abstract of papers in areas of computer science. The algorithm tries to identify the topics that the papers cover by transforming them into an adequate features space using Spark tokenizers and feature-transformers. When the data is in the correct format, we convert it to a dataframe and pass it into the `systemml` function block where it is transformed into a matrix and the Non-negative matrix factorization (NMF) algorithm factors out the topics:

```Scala
object NMF extends App {

  val spark = SparkSession.builder().master("local[*]").appName("NMF").getOrCreate()
  val sc: SparkContext = spark.sparkContext

  // read data
  val df = spark.read.format("com.databricks.spark.csv").option("header", "true").load(getClass.getResource("/cs_abstracts").getPath)

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

  val nmf = systemml {
    val V = Matrix.fromDataFrame(tfidf) // tfidf feature matrix coming from somewhere
    val k = 40
    val m, n = V.nrow // dimensions of tfidf
    val maxIters = 200

    var W = Matrix.rand(m, k)
    var H = Matrix.rand(k, n)

    for (i <- 0 to maxIters) { //main loop
      H = H * (W.t %*% V) / (W.t %*% (W %*% H))
      W = W * (V %*% H.t) / (W %*% (H %*% H.t))
    }

    (W, H) // return values
  }

  val (w, h) = nmf.run(mlctx, false)
  println("W: " + w)
}
```
We write the whole algorithm inside the `systemml` block and specify our return value at the end of the block as it is usually done in Scala. The `systemml` macro returns an instance of the class `SystemMLAlgorithm` which includes additional boilerplate code for execution in SystemML. To actually execute the generated code we call:

```Scala
val (w, h) = nmf.run(mlctx)
```

This will run the generated code on SystemML and return the requested values. Internally, the `SystemMLAlgorithm.run()` method uses SystemML's `MLContext`. The `systemml` macro can automatically discover that we want `(W, H)` as our return value and makes sure that these are set in the `MLContext`. Similarly it can discover that we have passed the dataframe `df` into the macro and sets it as an input to the `MLContext`.

### Input and Output handling

In the case of NMF we did not load data from any external data-source. In most scenarios, our data will come from some outside source. We provide several ways of passing this data to SystemML. The easiest way is to read data from a file using the builtin `read(...)` primitive that directly maps to SystemML's builtin `read`-primitive.

Additionally, data can be passed from a Spark `DataFrame` that has been created before. The following example shows the usage of passing a dataframe to SystemML:

```Scala
val numRows = 10000
val numCols = 1000
val data = sc.parallelize(0 to numRows-1).map { _ => Row.fromSeq(Seq.fill(numCols)(Random.nextDouble)) }
val schema = StructType((0 to numCols-1).map { i => StructField("C" + i, DoubleType, true) } )
val df = spark.createDataFrame(data, schema)

val alg = systemml {
      val matrix: Matrix = Matrix.fromDataFrame(df)

      val minOut = min(matrix)
      val maxOut = max(matrix)
      val meanOut = mean(matrix)

      (minOut, maxOut, meanOut)
    }

val  (minOut: Double, maxOut: Double, meanOut: Double) = alg.run(mlctx)

println(s"The minimum is $minOut, maximum: $maxOut, mean: $meanOut")
```

Similar to the automatic setting of output values, we can automatically find out that the argument to the matrix constructor is a Spark `Dataframe` and set the corresponding input parameter in the `MLContext`.

### Debugging and error messaged / FAQ

As an overwiew, we present the main differences between DML and our Scala DSL in the following table. Since the error- and debug facilities of SystemML and our Scala DSL are still in its infancy, the best way of debugging right now is to look at the generated DML code and plan of instructions that are printed before executing a DSL written algorithm.

Main differences between the Scala DSL and DML. Notice that these are only important for debugging purposes. To write an algorihm in the Scala DSL you should always use the Scala way just like you would for your other Scala programs, e.g. think 0-indexing and write `&&` for `AND`. The conversion is done in the compiler and these differences are just for relating generated to written code.

|Feature          | |    DML       |   Scala DSL    |
| :---            | |   :---:      |     :---:      |
| Array-indexing  | | 1-based      | 0-based        |
| Logical `AND`   | |   `&`        | `&&`           |
| Logical `OR`    | |     \|       |   \|\|         |

### Implementation

The translation of Scala code into DML is realized by using the [Emma compilation framework]() which uses Scala macros to allow for modifications of Scala code. The input code is represented as abstract syntax tree (AST) and different transformations facilitate the analysis and transformation of this AST. Finally, we map the nodes of the Scala AST (such as value-definitions and function applications) to the corresponding constructs in DML. Using macros allows for a holistic view of the program and potentially even for efficient optimizations. One complication is that Scala is based on expressions while SystemML DML is focused on statements. This requires some transformation and analysis of the Scala AST to transform expressions into statements. For example the above snippet that shows the generated DML code for the matrix multiplication lacks the "C" as compared to the Scala code. We remove this variable in the DML code because the SystemML parser does not allow for single expression statements. At the same time, we have to remember the name and type of "C" to later retain it from the `MLContext` after execution and to avoid typecheck errors due to wront return values.

One of the strengths of this approach is that while we write our programs we have the full guarantee of type-safety and support from our IDE. Additionally, the AST includes control-flow structures such as `for`-loops and conditional `if-then-else` expressions that can be translated natively into DML. This allows for efficient execution and full optimization potential from SystemML's internal optimizer.
