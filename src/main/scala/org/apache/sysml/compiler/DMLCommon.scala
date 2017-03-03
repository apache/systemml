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

package org.apache.sysml.compiler

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.sysml.api.linalg.api.:::
import org.apache.sysml.api.linalg.{Matrix, Vector}
import org.apache.sysml.api.mlcontext.MLContext
import org.emmalanguage.ast.AST
import org.emmalanguage.compiler.Common

trait DMLCommon extends AST {

  class Environment(val inputs: Map[String, u.TermSymbol],
                    val bindingRefs: Map[String, u.TermSymbol],
                    val offset: Int) {

  }

  // --------------------------------------------------------------------------
  // API
  // --------------------------------------------------------------------------
  import universe._

  protected[sysml] object DMLAPI {

    protected def op(name: String): u.MethodSymbol =
      methodIn(module, name)

    protected def methodIn(target: u.Symbol, name: String): u.MethodSymbol =
      target.info.member(api.TermName(name)).asMethod


    val predefModuleSymbol      = api.Sym[scala.Predef.type].asModule
    val apiModuleSymbol         = api.Sym[org.apache.sysml.api.linalg.api.`package`.type].asModule
    val matrixSymbol            = api.Sym[org.apache.sysml.api.linalg.Matrix].asClass
    val vectorModuleSymbol      = api.Sym[org.apache.sysml.api.linalg.Vector.type ].asModule
    val matrixOpsSymbol         = api.Sym[org.apache.sysml.api.linalg.api.MatrixOps].asClass
    val doubleSymbol            = api.Sym[scala.Double].asClass
    val intSymbol               = api.Sym[scala.Int].asClass
    val matrixModuleSymbol      = matrixSymbol.companion.asModule
    def module                  = apiModuleSymbol

    private def matrixOp(name: String) = methodIn(matrixSymbol, name)
    private def matrixOp(name: String, paramType: u.Type) = methodIn(matrixSymbol, name, paramType)

    private def doubleOp(name: String) = methodIn(doubleSymbol, name)
    private def doubleOp(name: String, paramType: u.Type) = methodIn(doubleSymbol, name, paramType)

    private def intOp(name: String) = methodIn(intSymbol, name)
    private def intOp(name: String, paramType: u.Type) = methodIn(intSymbol, name, paramType)

    private def getMethodAlternativesFor(target: u.Symbol, name: String): List[u.Symbol] = target.info.member(api.TermName(name)).alternatives

    /**
      * Find the method alternative for overloaded methods that matches the parameter signature
      *
      * @param target the target module in which the method is defined
      * @param name name of the method
      * @param paramLists types in the parameter lists
      * @return Symbol for the matching method definition
      */
    def methodIn(target: u.Symbol, name: String, paramLists: Seq[Seq[u.Type]]): u.MethodSymbol = {
      val alternatives = getMethodAlternativesFor(target, name)
      val inParamLists = paramLists.flatten

      val matching = for (alt <- alternatives) yield {
        val altParamLists = alt.typeSignature.paramLists.flatten.map(_.typeSignature.finalResultType)
        val matches = inParamLists.zip(altParamLists).forall { case (p1: u.Type, p2: u.Type) => p1 =:= p2 }

        if (matches) Some(alt) else None
      }

      matching.flatten match {
        case Nil => abort(s"No method alternative found for method $name in module $target.")
        case (x: u.MethodSymbol) :: Nil => x
        case _ => abort(s"Multiple method alternatives found for method $name in module $target.")
      }
    }

    def methodIn(target: u.Symbol, name: String, paramTpe: u.Type): u.MethodSymbol = methodIn(target, name, Seq(Seq(paramTpe)))

//    val applySeqDouble = {
//      val apply = matrixModuleSymbol.typeSignature.member(u.TermName("apply"))
//      val syms    = apply.alternatives.map(_.asMethod)
//      val sym     = syms.find { m =>
//        m.paramLists match {
//          case (arg :: xs) :: Nil
//            if arg.asTerm.typeSignature.erasure =:= u.typeOf[Seq[Any]] => true
//          case _ => false
//        }
//      } getOrElse abort(s"No generic apply method found: $syms")
//
//      sym
//    }
//
//
//    val applyArrayDouble = {
//      val apply = matrixModuleSymbol.typeSignature.member(u.TermName("apply"))
//      val syms    = apply.alternatives.map(_.asMethod)
//      val sym     = syms.find { m =>
//        m.paramLists match {
//          case (arg :: xs) :: Nil
//            if arg.asTerm.typeSignature.erasure =:= u.typeOf[Array[Double]] => true
//          case _ => false
//        }
//      } getOrElse abort(s"No generic apply method found: $syms")
//
//      sym
//    }

    def methodInMod(target: u.Symbol, name: String, paramTypes: List[u.Type]) = {
      val method = target.typeSignature.member(u.TermName(name))

      val syms    = method.alternatives.map(_.asMethod)
      val sym     = syms.find { m =>
        m.paramLists match {
          case args :: Nil // only one parameter list
            if args.length == paramTypes.length &&
              args.map(_.asTerm.typeSignature).zip(paramTypes).forall { case (actual, should) => actual =:= should } => true
          case _ => false
        }
      } getOrElse abort(s"No generic apply method found for target: $target, methods: $syms, parameter types: $paramTypes")

      sym

    }

    // type constructors
    val MLContext   = api.Type[MLContext].typeConstructor
    val Matrix      = api.Type[Matrix].typeConstructor
    val DataFrame   = api.Type[DataFrame].typeConstructor
    val Double      = api.Type[Double].typeConstructor
    val Int         = api.Type[Int].typeConstructor
    val SeqDouble   = api.Type[Seq[Double]].typeConstructor

    /**
      * This is a list of all supported operations both on primitives and our Matrix. There are things we can do and things
      * we can't do on primitives in DML.
      */

    // Sources
    val zeros               = methodIn(matrixModuleSymbol, "zeros")
    val zerosV              = methodIn(vectorModuleSymbol, "zeros")
    val ones                = methodIn(matrixModuleSymbol, "ones")
    val onesV               = methodIn(vectorModuleSymbol, "ones")
    val rand                = methodIn(matrixModuleSymbol, "rand")
    val randV               = methodIn(vectorModuleSymbol, "rand")
    val diag                = methodIn(matrixModuleSymbol, "diag")
    val fromDataFrame       = methodIn(matrixModuleSymbol, "fromDataFrame")
    val reshape             = methodIn(matrixModuleSymbol, "reshape")
    val applyArray1D        = methodInMod(matrixModuleSymbol, "apply", List(u.typeOf[Array[Double]], u.typeOf[Int], u.typeOf[Int]))
    val applyArrayV         = methodIn(vectorModuleSymbol, "apply")

    // matrix operators
    val nrow            = matrixOp("nrow")
    val ncol            = matrixOp("ncol")
    val pow             = matrixOp("^", Int)
    val transpose       = matrixOp("t")

    val matmult         = matrixOp("%*%")
    val timesDouble     = matrixOp("*", Double)
    val divDouble       = matrixOp("/", Double)
    val plusDouble      = matrixOp("+", Double)
    val minusDouble     = matrixOp("-", Double)
    val timesMatrix     = matrixOp("*", Matrix)
    val divMatrix       = matrixOp("/", Matrix)
    val plusMatrix      = matrixOp("+", Matrix)
    val minusMatrix     = matrixOp("-", Matrix)

    val indexII         = methodInMod(matrixSymbol, "apply", List(u.typeOf[Int], u.typeOf[Int]))
    val indexIR         = methodInMod(matrixSymbol, "apply", List(u.typeOf[Int], u.typeOf[Range]))
    val indexRI         = methodInMod(matrixSymbol, "apply", List(u.typeOf[Range], u.typeOf[Int]))
    val indexRR         = methodInMod(matrixSymbol, "apply", List(u.typeOf[Range], u.typeOf[Range]))
    val indexIA         = methodInMod(matrixSymbol, "apply", List(u.typeOf[Int], u.typeOf[:::.type]))
    val indexAI         = methodInMod(matrixSymbol, "apply", List(u.typeOf[:::.type], u.typeOf[Int]))
    val indexRA         = methodInMod(matrixSymbol, "apply", List(u.typeOf[Range], u.typeOf[:::.type]))
    val indexAR         = methodInMod(matrixSymbol, "apply", List(u.typeOf[:::.type], u.typeOf[Range]))

    val updateII        = methodInMod(matrixSymbol, "update", List(u.typeOf[Int], u.typeOf[Int], u.typeOf[Double]))
    val updateIR        = methodInMod(matrixSymbol, "update", List(u.typeOf[Int], u.typeOf[Range], u.typeOf[Matrix]))
    val updateRI        = methodInMod(matrixSymbol, "update", List(u.typeOf[Range], u.typeOf[Int], u.typeOf[Matrix]))
    val updateRR        = methodInMod(matrixSymbol, "update", List(u.typeOf[Range], u.typeOf[Range], u.typeOf[Matrix]))
    val updateIA        = methodInMod(matrixSymbol, "update", List(u.typeOf[Int], u.typeOf[:::.type ], u.typeOf[Matrix]))
    val updateAI        = methodInMod(matrixSymbol, "update", List(u.typeOf[:::.type ], u.typeOf[Int], u.typeOf[Matrix]))
    val updateRA        = methodInMod(matrixSymbol, "update", List(u.typeOf[Range], u.typeOf[:::.type ], u.typeOf[Matrix]))
    val updateAR        = methodInMod(matrixSymbol, "update", List(u.typeOf[:::.type], u.typeOf[Range], u.typeOf[Matrix]))

    // Double/Double  operators
    val plusDD    = doubleOp("+", Double)
    val minusDD   = doubleOp("-", Double)
    val timesDD   = doubleOp("*", Double)
    val divDD     = doubleOp("/", Double)
    val geqDD     = doubleOp(">=", Double)
    val leqDD     = doubleOp("<=", Double)
    val lessDD    = doubleOp("<", Double)
    val greaterDD = doubleOp(">", Double)
    val modDD     = doubleOp("%", Double)

    // Double/Int  operators
    val plusDI    = methodIn(doubleSymbol, "+", Int)
    val minusDI   = methodIn(doubleSymbol, "-", Int)
    val timesDI   = methodIn(doubleSymbol, "*", Int)
    val divDI     = methodIn(doubleSymbol, "/", Int)
    val geqDI     = methodIn(doubleSymbol, ">=", Int)
    val leqDI     = methodIn(doubleSymbol, "<=", Int)
    val lessDI    = methodIn(doubleSymbol, "<", Int)
    val greaterDI = methodIn(doubleSymbol, ">", Int)
    val modDI     = methodIn(doubleSymbol, "%", Int)

    // Double/Matrix
    val plusDM    = methodIn(matrixOpsSymbol, "+", Matrix)
    val minusDM   = methodIn(matrixOpsSymbol, "-", Matrix)
    val timesDM   = methodIn(matrixOpsSymbol, "*", Matrix)
    val divDM     = methodIn(matrixOpsSymbol, "/", Matrix)
//    val geqDM     = doubleOp(">=", Matrix)
//    val leqDM     = doubleOp("<=", Matrix)
//    val lessDM    = doubleOp("<", Matrix)
//    val greaterDM = doubleOp(">", Matrix)


    // Int/Int operators
    val plusII    = intOp("+", Int)
    val timesII   = intOp("*", Int)
    val divII     = intOp("/", Int)
    val minusII   = intOp("-", Int)
    val geqII     = intOp(">=", Int)
    val leqII     = intOp("<=", Int)
    val lessII    = intOp("<", Int)
    val greaterII = intOp(">", Int)
    val modII     = intOp("%", Int)

    // Int/Double operators
    val plusID    = intOp("+", Double)
    val timesID   = intOp("*", Double)
    val divID     = intOp("/", Double)
    val minusID   = intOp("-", Double)
    val geqID     = intOp(">=", Double)
    val leqID     = intOp("<=", Double)
    val lessID    = intOp("<", Double)
    val greaterID = intOp(">", Double)
    val modID     = intOp("%", Double)

    // builtin functions
    val sum         = methodIn(apiModuleSymbol, "sum")
    val read        = methodIn(apiModuleSymbol, "read")
    val ppred       = methodIn(apiModuleSymbol, "ppred")
    val cbind       = methodIn(apiModuleSymbol, "cbind")
    val minm        = methodInMod(apiModuleSymbol, "min", List(u.typeOf[Matrix]))
    val minmm       = methodInMod(apiModuleSymbol, "min", List(u.typeOf[Matrix], u.typeOf[Matrix]))
    val minmd       = methodInMod(apiModuleSymbol, "min", List(u.typeOf[Matrix], Double))
    val mindd       = methodInMod(apiModuleSymbol, "min", List(Double, Double))
    val maxm        = methodInMod(apiModuleSymbol, "max", List(u.typeOf[Matrix]))
    val maxmm       = methodInMod(apiModuleSymbol, "max", List(u.typeOf[Matrix], u.typeOf[Matrix]))
    val maxmd       = methodInMod(apiModuleSymbol, "max", List(u.typeOf[Matrix], Double))
    val maxdd       = methodInMod(apiModuleSymbol, "max", List(Double, Double))
    val prod        = methodIn(apiModuleSymbol, "prod")
    val rbind       = methodIn(apiModuleSymbol, "rbind")
    val removeEmpty = methodIn(apiModuleSymbol, "removeEmpty")
    val replace     = methodIn(apiModuleSymbol, "replace")
    val reverse     = methodIn(apiModuleSymbol, "rev")
    val pminmd      = methodInMod(apiModuleSymbol, "pmin", List(u.typeOf[Matrix], Double))
    val pminmm      = methodInMod(apiModuleSymbol, "pmin", List(u.typeOf[Matrix], u.typeOf[Matrix]))
    val pmaxmd      = methodInMod(apiModuleSymbol, "pmax", List(u.typeOf[Matrix], Double))
    val pmaxmm      = methodInMod(apiModuleSymbol, "pmax", List(u.typeOf[Matrix], u.typeOf[Matrix]))
    val rowIndexMin = methodIn(apiModuleSymbol, "rowIndexMin")
    val rowIndexMax = methodIn(apiModuleSymbol, "rowIndexMax")
    val mean        = methodIn(apiModuleSymbol, "mean")
    val variance    = methodIn(apiModuleSymbol, "variance")
    val sd          = methodIn(apiModuleSymbol, "sd")
    val moment      = methodInMod(apiModuleSymbol, "moment", List(u.typeOf[Matrix], Int))
    val momentw     = methodInMod(apiModuleSymbol, "moment", List(u.typeOf[Matrix], u.typeOf[Matrix], Int))
    val colSums     = methodIn(apiModuleSymbol, "colSums")
    val colMeans    = methodIn(apiModuleSymbol, "colMeans")
    val colVars     = methodIn(apiModuleSymbol, "colVars")
    val colSds      = methodIn(apiModuleSymbol, "colSds")
    val colMaxs     = methodIn(apiModuleSymbol, "colMaxs")
    val colMins     = methodIn(apiModuleSymbol, "colMins")
    val cov         = methodInMod(apiModuleSymbol, "cov", List(u.typeOf[Matrix], u.typeOf[Matrix]))
    val covw        = methodInMod(apiModuleSymbol, "cov", List(u.typeOf[Matrix], u.typeOf[Matrix], u.typeOf[Matrix]))
    val table       = methodInMod(apiModuleSymbol, "table", List(u.typeOf[Matrix], u.typeOf[Matrix]))
    val tablew      = methodInMod(apiModuleSymbol, "table", List(u.typeOf[Matrix], u.typeOf[Matrix], u.typeOf[Matrix]))
    // cdf
    // icdf
    // aggregate
    val interqm     = methodInMod(apiModuleSymbol, "interQuartileMean", List(u.typeOf[Matrix]))
    val interqmw    = methodInMod(apiModuleSymbol, "interQuartileMean", List(u.typeOf[Matrix], u.typeOf[Matrix]))
    val quantile    = methodInMod(apiModuleSymbol, "quantile", List(u.typeOf[Matrix], Double))
    val quantilem   = methodInMod(apiModuleSymbol, "quantile", List(u.typeOf[Matrix], u.typeOf[Matrix]))
    val quantilew   = methodInMod(apiModuleSymbol, "quantile", List(u.typeOf[Matrix], u.typeOf[Matrix], Double))
    val quantilewm  = methodInMod(apiModuleSymbol, "quantile", List(u.typeOf[Matrix], u.typeOf[Matrix], u.typeOf[Matrix]))
    val rowSums     = methodIn(apiModuleSymbol, "rowSums")
    val rowMeans    = methodIn(apiModuleSymbol, "rowMeans")
    val rowVars     = methodIn(apiModuleSymbol, "rowVars")
    val rowSds      = methodIn(apiModuleSymbol, "rowSds")
    val rowMaxs     = methodIn(apiModuleSymbol, "rowMaxs")
    val rowMins     = methodIn(apiModuleSymbol, "rowMins")
    val cumsum      = methodIn(apiModuleSymbol, "cumsum")
    val cumprod     = methodIn(apiModuleSymbol, "cumprod")
    val cummin      = methodIn(apiModuleSymbol, "cummin")
    val cummax      = methodIn(apiModuleSymbol, "cummax")
    val lognd       = methodInMod(apiModuleSymbol, "log", List(Double))
    val lognm       = methodInMod(apiModuleSymbol, "log", List(u.typeOf[Matrix]))
    val logbd       = methodInMod(apiModuleSymbol, "log", List(Double, Double))
    val logbm       = methodInMod(apiModuleSymbol, "log", List(u.typeOf[Matrix], Double))
    val absd        = methodInMod(apiModuleSymbol, "abs", List(Double))
    val absdm       = methodInMod(apiModuleSymbol, "abs", List(u.typeOf[Matrix]))
    val expd        = methodInMod(apiModuleSymbol, "exp", List(Double))
    val expm        = methodInMod(apiModuleSymbol, "exp", List(u.typeOf[Matrix]))
    val sqrtd       = methodInMod(apiModuleSymbol, "sqrt", List(Double))
    val sqrtm       = methodInMod(apiModuleSymbol, "sqrt", List(u.typeOf[Matrix]))
    val roundd      = methodInMod(apiModuleSymbol, "round", List(Double))
    val roundm      = methodInMod(apiModuleSymbol, "round", List(u.typeOf[Matrix]))
    val floord      = methodInMod(apiModuleSymbol, "floor", List(Double))
    val floorm      = methodInMod(apiModuleSymbol, "floor", List(u.typeOf[Matrix]))
    val ceild       = methodInMod(apiModuleSymbol, "ceil", List(Double))
    val ceilm       = methodInMod(apiModuleSymbol, "ceil", List(u.typeOf[Matrix]))
    val sind        = methodInMod(apiModuleSymbol, "sin", List(Double))
    val sinm        = methodInMod(apiModuleSymbol, "sin", List(u.typeOf[Matrix]))
    val cosd        = methodInMod(apiModuleSymbol, "cos", List(Double))
    val cosm        = methodInMod(apiModuleSymbol, "cos", List(u.typeOf[Matrix]))
    val tand        = methodInMod(apiModuleSymbol, "tan", List(Double))
    val tanm        = methodInMod(apiModuleSymbol, "tan", List(u.typeOf[Matrix]))
    val asind       = methodInMod(apiModuleSymbol, "asin", List(Double))
    val asinm       = methodInMod(apiModuleSymbol, "asin", List(u.typeOf[Matrix]))
    val acosd       = methodInMod(apiModuleSymbol, "acos", List(Double))
    val acosm       = methodInMod(apiModuleSymbol, "acos", List(u.typeOf[Matrix]))
    val atand       = methodInMod(apiModuleSymbol, "atan", List(Double))
    val atanm       = methodInMod(apiModuleSymbol, "atan", List(u.typeOf[Matrix]))
//    val signd       = methodInMod(apiModuleSymbol, "sign", List(Double))
    val signm       = methodInMod(apiModuleSymbol, "sign", List(u.typeOf[Matrix]))
    val cholesky    = methodIn(apiModuleSymbol, "cholesky")
    val diagm       = methodIn(apiModuleSymbol, "diag")
    val solve       = methodIn(apiModuleSymbol, "solve")
    val trace       = methodIn(apiModuleSymbol, "trace")

    val sourceOps   = Set(zeros, zerosV, ones, onesV, rand, randV, diag, fromDataFrame, applyArray1D, applyArrayV, reshape)

    val builtinOps  = Set(cbind, minm, minmm, minmd, mindd, maxm, maxmm, maxmd, maxdd, prod, rbind, removeEmpty,
                          replace, reverse, sum, pminmd, pminmm, pmaxmd, pmaxmm, rowIndexMin, rowIndexMax, mean,
                          variance, sd, moment, momentw, read, ppred, colSums, colMeans, colVars, colSds, colMaxs,
                          colMins, cov, covw, table, tablew, interqm, interqmw, quantile, quantilem, quantilew,
                          quantilewm, rowSums, rowMeans, rowVars, rowSds, rowMaxs, rowMins, cumsum, cumprod, cummax,
                          cummin, absd, absdm, lognd, lognm, logbd, logbm, expd, expm, sqrtd, sqrtm, roundd, roundm,
                          floord, floorm, ceild, ceilm, sind, sinm, cosd, cosm, tand, tanm, asind, asinm, acosd, acosm,
                          atand, atanm, signm, cholesky, diagm, solve, trace)

    val matOps      = Set(pow, nrow, ncol, transpose, matmult,
                          timesDouble, timesMatrix, divDouble, divMatrix, plusDouble, plusMatrix, minusDouble, minusMatrix,
                          indexII, indexIR, indexRI, indexIA, indexAI, indexRA, indexAR, indexRR,
                          updateII, updateIR, updateRI, updateIA, updateAI, updateRA, updateAR, updateRR)

    val doubleOps   = Set(plusDD, minusDD, timesDD, divDD, geqDD, leqDD, lessDD, greaterDD,
                          plusDI, minusDI, timesDI, divDI, geqDI, leqDI, lessDI, greaterDI,
                          plusDM, minusDM, timesDM, modDD, divDM)

    val intOps      = Set(plusII, minusII, timesII, divII, geqII, leqII, lessII, greaterII, modII,
                          plusID, minusID, timesID, divID, geqID, leqID, lessID, greaterID, modID)

    val ops: Set[u.MethodSymbol] = sourceOps | builtinOps | matOps | doubleOps | intOps

    val modules = Set(apiModuleSymbol, matrixModuleSymbol, predefModuleSymbol)

    // Set of valid inputs to the macro
    val inputs = Set(Matrix, DataFrame, MLContext)
    val primitives = Set(Double, Int)
  }
}
