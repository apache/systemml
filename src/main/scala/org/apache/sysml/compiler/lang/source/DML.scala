/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sysml.compiler.lang.source

import com.sun.tools.javac.code.TypeTag
import org.apache.sysml.api.linalg.Matrix
import org.apache.sysml.compiler.DMLCommon
import org.emmalanguage.compiler.lang.source.Source

trait DML extends DMLCommon with DMLSourceValidate {
  this: Source =>

  object DML {

    lazy val toDML = (tree: u.Tree) => (env: Environment) =>  DMLTransform.generateDML(unQualifyStatics(tree), env)

    lazy val valid = DMLSourceValidate.valid
    lazy val validate = (tree: u.Tree) => valid(tree).isGood

    private[source] object DMLTransform {

      type D = Environment => String
      // semantic domain (env => string representation)
      val indent = 0

      val generateDML: (u.Tree, Environment) => String = (tree: u.Tree, startingEnv: Environment) => {

        val matrixFuncs = Set("t", "nrow", "ncol")

        val printSym = (sym: u.Symbol) =>
          sym.name.decodedName.toString.stripPrefix("unary_")

        val isApply = (sym: u.MethodSymbol) =>
          sym.name == u.TermName("apply")

        val isUpdate = (sym: u.MethodSymbol) =>
          sym.name == u.TermName("update")

        val printMethod = (pre: String, sym: u.MethodSymbol, suf: String) =>
          if (isApply(sym)) ""
          else pre + printSym(sym) + suf

        val printConstructor = (sym: u.MethodSymbol, argss: Seq[Seq[D]], env: Environment, isVector: Boolean) => {
          val args = argss flatMap (args => args map (arg => arg(env)))

          sym.name match {
            case u.TermName("rand")  => if (isVector) s"rand(rows=${args(0)}, cols=1)" else s"""rand(rows=${args(0)}, cols=${args(1)})"""
            case u.TermName("zeros") => if (isVector) s"matrix(0.0, rows=${args(0)}, cols=1)" else s"matrix(0.0, rows=${args(0)}, cols=${args(1)})"
            case u.TermName("ones")  => if (isVector) s"matrix(1.0, rows=${args(0)}, cols=1)" else s"matrix(1.0, rows=${args(0)}, cols=${args(1)})"
            case u.TermName("diag")  => s"diag(matrix(${args(0)}, rows=${args(1)}, cols=1))"
            case u.TermName("apply") => if (isVector) {
              val rows = args(0).split(" ").length
              s"matrix(${args(0)}, rows=$rows, cols=1)"
            } else s"matrix(${args(0)}, rows=${args(1)}, cols=${args(2)})"
            case u.TermName("reshape") => s"matrix(${args(0)}, rows=${args(1)}, cols=${args(2)})"
            /* Here we just remove the call to Matrix.fromDataFrame(ref) with ref. We will take care of setting the input
               to the MLContext so that the actual dataframe reference will be passed with the name "ref"
             */
            case u.TermName("fromDataFrame") => args(0)
          }
        }

        val printBuiltin = (target: D, sym: u.MethodSymbol, argss: Seq[Seq[D]], env: Environment) => {
          val args = argss flatMap (args => args map (arg => arg(env)))

          sym.name match {
            case u.TermName("read") => {
              s"""read(${args(0)}, format="${args(1).toLowerCase()}")"""
            }

            case u.TermName("write") => {
              val format = args(2) match {
                case "CSV" => """format="csv""""
                case _ => throw new RuntimeException(s"Unsopported output format: ${args(2)}")
              }
              s"write(${args(0)}, ${args(1)}, $format)"
            }

            case u.TermName("variance") => s"var(${args.mkString(", ")})"

            case u.TermName(fname) if fname == "MatrixOps" => args.head.toString

            case u.TermName(fname) => s"$fname(${args.mkString(", ")})"

            case _ =>
              abort(s"Unsopported builtin call: $sym", sym.pos)
          }
        }

        val isUnary = (sym: u.MethodSymbol) =>
          sym.name.decodedName.toString.startsWith("unary_")

        /**
          * Convert Scala types to SystemML types
          * @param arg the type argument
          * @return DML type as string
          */
        def convertTypes(arg: u.Type): List[String] = arg match {
          case tpe if tpe =:= u.typeOf[Double] => List("double")
          case tpe if tpe =:= u.typeOf[Matrix] => List("matrix[double]")
          case tpe if tpe <:< u.typeOf[Product] => {
            val params = tpe.finalResultType.typeArgs
            params.flatMap(convertTypes(_))
          }
          case tpe => abort(s"Unsupported return type $tpe. Supported types are: Matrix, Double")
        }

        // format a block to have 2 whitespace indentaion
        val indent = (str: String) => {
          val lines = str.split("\n")

          if (lines.length > 1)
            lines.map(x => s"  $x").mkString("\n")
          else
            s"  $str"
        }

        /**
          *constructs a for loop by deconstructing foreach together with the range and the lambda */
        val forLoop = (target: D, targs: Seq[u.Type], args: Seq[D], env: Environment) => {
          val range = target(env)
          val lambda = args.map(x => x(env)).head

          val parts = lambda.split(" => ")
          val idx = parts(0).drop(1).dropRight(1) // remove braces
          // format the body with 2 spaces of indentation
          val body = indent(parts(1))

          s"""
             |for ($idx in $range) {
             |$body
             |}
            """.stripMargin.trim
        }

        val escape = (str: String) => str
          .replace("\b", "\\b")
          .replace("\n", "\\n")
          .replace("\t", "\\t")
          .replace("\r", "\\r")
          .replace("\f", "\\f")
          .replace("\"", "\\\"")
          .replace("\\", "\\\\")

        val alg = new Source.Algebra[D] {

          def empty: D = env => ""

          // Atomics
          def lit(value: Any): D = env => value match {
            //@formatter:off
            case value: Boolean => if (value) "TRUE" else "FALSE"
            case value: Char => s""""${value}""""
            case value: String => s""""${escape(value)}""""
            case null => "null"
            case value: Unit => ""
            case _ => value.toString
            //@formatter:on
          }

          def this_(sym: u.Symbol): D = env => {
            s"_this: ${sym.name.decodedName.toString}"
          }

          def bindingDef(lhs: u.TermSymbol, rhs: D): D = valDef(lhs, rhs)

          def ref(target: u.TermSymbol): D = ???

          def loop(cond: D, body: D): D = whileLoop(cond, body)

          def moduleAcc(target: D, member: u.ModuleSymbol): D = ???

          override def bindingRef(sym: u.TermSymbol): D = env => {
            printSym(sym)
          }

          override def moduleRef(target: u.ModuleSymbol): D = env =>
            printSym(target)

          // Definitions
          override def valDef(lhs: u.TermSymbol, rhs: D): D = env =>
            s"${printSym(lhs)} = ${rhs(env)}"

          override def parDef(lhs: u.TermSymbol, rhs: D): D = env => {
            val l = lhs.name.decodedName
            val tpe = lhs.typeSignature
            val r = rhs(env)

            if (r.isEmpty) {
              s"$l"
            } else {
              s"$l = $r"
            }
          }

          // Other

          /** type ascriptions such as `var x: Int` */
          def typeAscr(target: D, tpe: u.Type): D = env => "" // TODO check for types allowed in SystemML

          def defCall(target: Option[D], method: u.MethodSymbol, targs: Seq[u.Type], argss: Seq[Seq[D]]): D = env => {
            val s = target
            val args = argss flatMap (args => args map (arg => arg(env)))

            (target, argss) match {

              /* matches tuples */
              case (Some(tgt), _) if isApply(method) && api.Sym.tuples.contains(method.owner.companion) =>
                ""

              case (Some(tgt), _) if method == api.Sym.foreach || method.overrides.contains(api.Sym.foreach) =>
                forLoop(tgt, targs, argss.flatten, env)

              /* matches unary methods without arguments, e.g. A.t */
              case (Some(tgt), Nil) if isUnary(method) =>
                s"${printSym(method)}${tgt(env)}"

              /* matches apply methods with one argument */
              case (Some(tgt), ((arg :: Nil) :: Nil)) if isApply(method) => {
                val module = tgt(env)

                if (module == "Vector") { // Vector.apply(Array(...))
                  printConstructor(method, argss, env, true)
                }

                else {
                  s"${tgt(env)}${printMethod(" ", method, " ")}${arg(env)}"
                }
              }

              /* matches methods with one argument (%*%, read) */
              case (Some(tgt), (arg :: Nil) :: Nil) => {

                val module = tgt(env)

                // methods in the package object (builtins with one argument (read)
                if (module == "package") {
                  printBuiltin(tgt, method, argss, env)
                }

                // matrix constructors with one argument (fromDataFrame)
                else if (module == "Matrix") {
                  printConstructor(method, argss, env, false)
                }

                else if (module == "Vector") {
                  printConstructor(method, argss, env, true)
                }

                // methods from scala.predef with one argument (println(...) etc.)
                else if (module == "Predef") {
                  val name: u.TermName = method.name.decodedName.toTermName

                  name match {
                    case u.TermName(tn) if tn == "println" || tn == "print" => s"print(${arg(env)})"
                    case u.TermName("intWrapper") => s"${arg(env)}"
                    case _ => abort(s"scala.predef.$name is not supported in DML", method.pos)
                  }
                }

                // binary operators
                else {
                  method.name.decodedName.toTermName match {
                    case u.TermName("to")    => s"${tgt(env)} + 1:${arg(env)} + 1"
                    case u.TermName("until") => s"${tgt(env)} + 1:${arg(env)}"
                    case u.TermName("%") => s"($module %% ${args(0)})" // modulo in dml is %%
                    case u.TermName("&&") => s"($module & ${args(0)})" // && in dml is &
                    case u.TermName("||") => s"($module | ${args(0)})" // || in dml is |
                    case _ => s"($module ${method.name.decodedName} ${args(0)})"
                  }
                }
              }

              // matches apply methods with multiple arguments
              case (Some(tgt), (x :: xs) :: Nil) if isApply(method) => {
                val module = tgt(env)
                val argString = args.mkString(" ")

                if (module == "Array") {
                  // sequence/array constructors
                  s""""$argString""""
                }

                else if (module == "Matrix") {
                  printConstructor(method, argss, env, false)
                }

                else if (module == "Vector") {
                  printConstructor(method, argss, env, true)
                }

                else if (module == "package") {
                  // builtins
                  "builtin"
                }

                  // apply as indexing - convert Scala 0-based to DML 1-based
                else {
                  val rows = args(0) // rows
                  val cols = args(1) // columns

                  (rows, cols) match {
                    case (":::", c) if c.contains(":")  => s"$module[,$c]"
                    case (":::", c)                     => s"$module[,$c + 1]"
                    case (r, ":::") if r.contains(":")  => s"$module[$r,]"
                    case (r, ":::")                     => s"$module[$r + 1,]"
                    case (r, c) if  r.contains(":") &&
                                    c.contains(":")     => s"$module[$r,$c]"
                    case (r, c) if r.contains(":")      => s"$module[$r, $c + 1]"
                    case (r, c) if c.contains(":")      => s"$module[$r + 1, $c]"
                    case (r, c)                         => s"as.scalar($module[$r + 1,$c + 1])"
                  }
                }
              }

              // matches apply methods with multiple arguments
              case (Some(tgt), (x :: xs) :: Nil) if isUpdate(method) => {
                val module = tgt(env)

                // update on matrix objects (left indexing): A[r, c] = v === A.update(r, c, v)
                val rows = args(0) // rows
                val cols = args(1) // columns
                val value = args(2) // value to update with

                (rows, cols) match {
                  case (":::", c) if c.contains(":")  => s"$module[,$c] = $value"
                  case (":::", c)                     => s"$module[,$c + 1] = $value"
                  case (r, ":::") if r.contains(":")  => s"$module[$r,] = $value"
                  case (r, ":::")                     => s"$module[$r + 1,] = $value"
                  case (r, c) if  r.contains(":") &&
                    c.contains(":")     => s"$module[$r,$c] = $value"
                  case (r, c) if r.contains(":")      => s"$module[$r, $c + 1] = $value"
                  case (r, c) if c.contains(":")      => s"$module[$r + 1, $c] = $value"
                  case (r, c)                         => s"$module[$r + 1,$c + 1] = $value"
                }
              }

              // matches methods with multiple arguments (e.g. zeros(3, 3), write)
              case (Some(tgt), (x :: xs) :: Nil) => {

                val module = tgt(env)
                val argString = args.mkString(" ")

                if (module == "Matrix") {
                  printConstructor(method, argss, env, false)
                }

                else if (module == "Vector") {
                  printConstructor(method, argss, env, true)
                }

                else if (module == "package") {
                  // builtin
                  printBuiltin(tgt, method, argss, env)
                }

                else {
                  "case (Some(tgt), (x :: xs) :: Nil)"
                }
              }

              // matches functions without arguments (.t (transpose))
              case (Some(tgt), Nil) => {

                method.name.decodedName.toTermName match {
                  case u.TermName(tn) if matrixFuncs.contains(tn) => s"$tn(${tgt(env)})"
                  case u.TermName(tn) if tn == "toDouble" => tgt(env) // this is a scala implicit conversion from Int to Double
                  case _ => method.name.decodedName.toString
                }
              }

              case (Some(tgt), _) => abort(s"Matching error, please report the following: case case (Some(tgt), _): target ${tgt(env)}, method: $method")

              // matches functions that are not defined in a module or class (udfs)
              case (None, _) => {
                val name = method.name.decodedName
                val argString = args.mkString(", ")
                s"$name($argString)"
              }

              case _ =>
                abort(s"Unsupported function call! Calling the method $method is not supported!")
            }
          }

          def inst(target: u.Type, targs: Seq[u.Type], argss: Seq[Seq[D]]): D = ???

          def lambda(sym: u.TermSymbol, params: Seq[D], body: D): D = env => {
            val p = params.map(p => p(env))
            val b = body(env)

            s"""(${p.mkString(", ")}) => $b"""
          }

          def branch(cond: D, thn: D, els: D): D = env => {
            val thenBlock = indent(thn(env))
            val elseBlock = indent(els(env))

            if (elseBlock != "  ") {
              s"""
                 |if (${cond(env)}) {
                 |$thenBlock
                 |} else {
                 |$elseBlock
                 |}
            """.stripMargin.trim
            } else {
              s"""
                 |if (${cond(env)}) {
                 |$thenBlock
                 |}
            """.stripMargin.trim
            }
          }

          def block(stats: Seq[D], expr: D): D = env => {
            val statsString = stats.flatMap{x =>
              val res = x(env)
              if (env.bindingRefs.keySet.contains(res)) {
                None // if the statement is a single varref/valref, remove it
              } else {
                Some(res)
              }
            }.filter(x => x.trim.length > 0).mkString("\n")

            val exprString  = expr(env)

            // if the expression is a valref or varref, we leave it out because SystemML doesn't allow single expressions as statements
            val returnExpr = if (env.bindingRefs.keySet.contains(exprString)) {
              ""
            } else {
              exprString
            }

            s"""
               |$statsString
               |$returnExpr
            """.stripMargin.trim
          }

          def whileLoop(cond: D, body: D): D = env => {
            val formatted = indent(body(env))

            s"""
               |while(${cond(env)}) {
               |$formatted
               |}
             """.stripMargin.trim
          }

          def varMut(lhs: u.TermSymbol, rhs: D): D = env => {
            s"${lhs.name.decodedName} = ${rhs(env)}"
          }
        }
        Source.fold(alg)(tree)(startingEnv)
      }
    }
  }
}
