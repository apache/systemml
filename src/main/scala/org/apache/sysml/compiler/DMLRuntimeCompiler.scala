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

package org.apache.sysml.compiler

import org.apache.sysml.compiler.lang.source.DML
import org.emmalanguage.compiler.RuntimeCompiler

class DMLRuntimeCompiler extends RuntimeCompiler with DML {


  override lazy val preProcess: Seq[u.Tree => u.Tree] = Seq(
    Source.removeImplicits(API.implicitTypes),
    fixSymbolTypes,
    //stubTypeTrees,
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
    //@formatter:off
    if (typeCheck) bld += { this.typeCheck(_) }
    if (withPre)   bld ++= preProcess
    bld ++= transformations
    if (withPost)  bld ++= postProcess
    //@formatter:on
    val steps = bld.result()

    if (!printAllTrees) Function.chain(steps)
    else Function.chain(List(print) ++ steps.flatMap(List(_, print)))
  }

  def toDML: u.Tree => String = (tree: u.Tree) => DML.toDML(tree)(new Environment(Map.empty[String, u.TermSymbol], Map.empty[String, u.TermSymbol], 0))
  lazy val valid = DMLSourceValidate.valid
}
