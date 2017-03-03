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

package org.apache.sysml.compiler.macros

import org.apache.sysml.api.linalg.api._
import org.apache.sysml.compiler.lang.source.DML
import org.emmalanguage.compiler.MacroCompiler
import cats.std.all._
import shapeless._

import scala.language.experimental.macros
import scala.reflect.macros.blackbox


class RewriteMacros(val c: blackbox.Context) extends MacroCompiler with DML {
  import DML._
  import u._

  override val rootPkg = "org.apache.sysml.api"

  ////////////////////////////////////////////////////////////////////////////////
  // PIPELINE
  ////////////////////////////////////////////////////////////////////////////////


  override lazy val preProcess: Seq[u.Tree => u.Tree] = Seq(
    fixSymbolTypes,
    stubTypeTrees,
    unQualifyStatics,
    normalizeStatements
  )

  /** Standard pipeline suffix. Brings a tree into a form acceptable for `scalac` after being transformed. */
  override lazy val postProcess: Seq[u.Tree => u.Tree] = Seq(
    api.Owner.atEncl,
    qualifyStatics,
    restoreTypeTrees
  )

  def dmlPipeline(typeCheck: Boolean = false, withPre: Boolean = true, withPost: Boolean = true)
                 (transformations: (u.Tree => u.Tree)*): u.Tree => u.Tree = {

    val bld = Seq.newBuilder[u.Tree => u.Tree]
    if (typeCheck) bld += { this.typeCheck(_) }
    if (withPre)   bld ++= preProcess
    bld ++= transformations
    if (withPost)  bld ++= postProcess
    val steps = bld.result()

    if (!printAllTrees) scala.Function.chain(steps)
    else scala.Function.chain(List(print) ++ steps.flatMap(List(_, print)))
  }

  /** Ordering symbols by their name. */
  implicit private val byName: Ordering[u.TermSymbol] =
  Ordering.by(_.name.toString)

  // liftable for input parameters
  implicit val lift = u.Liftable[(String, u.TermSymbol)] { p =>
    q"(${p._1}, ${p._2})"
  }

  private def isValidInput(input: u.MethodSymbol): Boolean = {
    DMLAPI.inputs.exists(input.returnType.finalResultType <:< _) || isPrimitive(input)
  }

  private def isBuiltin(input: u.MethodSymbol): Boolean = {
    DMLAPI.ops.contains(input)
  }

  private def isApply(input: u.MethodSymbol): Boolean = {
    input.name == u.TermName("apply")
    input.name == u.TermName("toDouble")
  }

  private def isPrimitive(input: u.MethodSymbol): Boolean = {
    DMLAPI.primitives.exists(_ =:= input.returnType.finalResultType)
  }

  private def isValidModule(input: u.ModuleSymbol): Boolean = {
    DMLAPI.modules.contains(input)
  }

  ////////////////////////////////////////////////////////////////////////////////
  // MACRO IMPLEMENTATION
  ////////////////////////////////////////////////////////////////////////////////

  /**
    * The macro entry point to transform the tree and generate the DML Algorithm object
    * @param e the expression inside the parallelize macro
    * @tparam T type of the expression
    * @return an [[org.apache.sysml.api.linalg.SystemMLAlgorithm]] of type T that can execute the DML script and return the result of type T
    */
  def impl[T: c.WeakTypeTag](e: u.Expr[T]) = {

    /*
    General validation of source language.
    Make sure that only things are used that we can support in Scala. This does not include more domain specific
    semantic validation which is done later.
    */
    validate(e.tree)

    /*
    Here we collect inputs (i.e. the instances in the closure that can be passed to MLContext) and also perform some
    more domain specific semantic validation, making sure that we only reference things in the outside scope that can
    safely be translated to DML.
     */
    val Attr.all(_, _, _, bindingRefs :: valdefs :: modules :: inputs :: defcalls :: HNil) = {
      api.TopDown
        .synthesize(Attr.collect[Set, u.TermSymbol] { // collect all def calls
          case api.DefCall(Some(target), method, targs, args) if !(isBuiltin(method) || isApply(method)) => method
        })
        .synthesize(Attr.collect[Set, u.TermSymbol] { // collect valid inputs to MLContext (def calls since they come from some other module)
          case api.DefCall(Some(target), method, targs, args) if isValidInput(method) && !(isBuiltin(method) || isApply(method)) => method
        })
        .synthesize(Attr.collect[Set, u.TermSymbol] { // collect valid module accesses
          case api.DefCall(Some(api.ModuleAcc(tree, sym)), method, targs, args) => sym
        })
        .synthesize(Attr.collect[Set, u.TermSymbol] { // collect all valdefs
          case api.ValDef(lhs, rhs) => lhs
        })
        .synthesize(Attr.collect[Set, u.TermSymbol] { // collect all bindingrefs
          case api.ValRef(sym) => sym
          case api.VarRef(sym) => sym
          case api.BindingRef(sym) => sym
          case api.DefCall(Some(target), method, targs, args) => method
        })
        .traverseAny(e.tree)
    }

    // take the closure and remove all "legal" accesses
    val closure = defcalls diff valdefs diff inputs

    // construct maps for all inputs to the MLContext and binding references
    val inputMap = inputs.map(x => x.name.decodedName.toString -> x).toMap
    val bindingRefMap = bindingRefs.map(x => x.name.decodedName.toString -> x).toMap

    /** extract the return type that has to be retained from mlcontext */
    val (outType: u.Type, outNames: List[u.Tree]) = e.tree match {
      case u.Block(_, expr) => expr match {
        case l: u.Literal if !(expr.tpe =:= u.typeOf[Unit]) => (l.tpe, List(l.value))
        case a: u.Apply if a.symbol.name == u.TermName("apply") => (a.tpe, a.args)
        case _ if expr.tpe =:= u.typeOf[Unit] =>
          (u.typeOf[Unit], List())
        case _ =>
          (expr.tpe, List(expr))
      }
      case _ =>
        (e.tree.tpe, e.tree)
    }

    // generate the actual DML code
    val dmlString = toDML(dmlPipeline(typeCheck = false)()(e.tree))(new Environment(inputMap, bindingRefMap, 0))

    // prepend line numbers
    val formatted = dmlString.split("\n").zipWithIndex.map(tup => f"${tup._2 + 1}%4d|${tup._1}").mkString("\n")

    // assemble the input and output parameters to MLContext
    val inParams  = inputs.map(in => (in.name.decodedName.toString, in))
    val outParams = outNames.map(_.symbol.name.toString)

    // assemble the type of the return expression we want from MLContext
    val outTypes  = outType.typeArgs match {
      case Nil => List(outType)
      case ls => ls
    }

    // if the return type is Unit, we don't want to call getTuple
    val result = if (outParams.isEmpty) q"()" else q"res.getTuple[..${outTypes}](..${outParams})"

    // this is a workaround for the fact that MLContext only returns tuples
    val out = if (outTypes.length == 1 && outParams.nonEmpty) q"out._1" else q"out"

    // Construct algorithm object
    val alg = q"""{
      new _root_.org.apache.sysml.api.linalg.SystemMLAlgorithm[${u.weakTypeOf[T]}]  {

      import _root_.org.apache.sysml.api.mlcontext.ScriptFactory._
      import _root_.scala.reflect._

      val inputs = Seq(..${inParams})
      val outputs = Seq(..${outParams})

      def run(ml: _root_.org.apache.sysml.api.mlcontext.MLContext, printDML: Boolean = false): ${u.weakTypeOf[T]} = {
        if (printDML) {
          println("=" * 80)
          println((" " * 26) + "RUNNING GENERATED DML SCRIPT")
          println("=" * 80)
          println(${formatted})
          println("=" * 80)
          println("Input parameters:" +  List(..${inParams}).mkString(", "))
        }

        val script = dml($dmlString).in(inputs).out(..${outParams})
        val res = ml.execute(script)
        val out = $result
        $out
      }
    }}"""

    val res = dmlPipeline(typeCheck = true)()(alg)
    c.Expr[T]((removeShadowedThis andThen unTypeCheck)(res))
  }
}
