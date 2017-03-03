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

package org.apache.sysml.dmlCompiler.lang.source

import org.apache.sysml.compiler.DMLRuntimeCompiler
import org.emmalanguage.compiler.BaseCompilerSpec
import org.emmalanguage.util
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** A spec for SystemML Algorithms. */
@RunWith(classOf[JUnitRunner])
class LanguageSpec extends BaseCompilerSpec {

  val dmlCompiler: DMLRuntimeCompiler = new DMLRuntimeCompiler()
  import dmlCompiler._
  import Source.{Lang => src}
  import Validation._

  // ---------------------------------------------------------------------------
  // Helper methods
  // ---------------------------------------------------------------------------

  /** Example pre-processing pipeline. */
  lazy val pipeline = { dmlCompiler.typeCheck(_: u.Tree) }
    .andThen(dmlCompiler.fixSymbolTypes)
    .andThen(dmlCompiler.unQualifyStatics)
    .andThen(dmlCompiler.normalizeStatements)

  /** Extracts examples from a reified expression. */
  def extractFrom[A](expr: u.Expr[A]) =
    pipeline(expr.tree) match {
      case u.Apply(_, args) => args
      case u.Block(stats, _) => stats
    }

  /** Tree [[Validator]] matcher. */
  def satisfy(validator: Validator) =
    be (good) compose { (tree: u.Tree) =>
      time(validateAs(validator, tree), "validate")
    }

  // ---------------------------------------------------------------------------
  // Atomics
  // ---------------------------------------------------------------------------

  "Literals" - {
    // modeled by
    // - `Lit` objects in `Source.Lang`
    // - `Literal(Constant(value))` nodes in Scala ASTs

    val examples = extractFrom(u.reify(42, 4.2, "42", '!'))
    examples should not be empty

    "are valid language constructs" in {
      all (examples) should satisfy (valid.Lit)
    }
  }

  "References" - {
    // modeled by
    // - `Ref` objects in `Source.Lang`
    // - `Ident(sym)` nodes where `sym` is a (free) `TermSymbol` in Scala ASTs

    val examples = extractFrom(u.reify {
      val u = 42
      val v = 4.2
      var w = "42"
      object Module
      w += '!'
      (u, v, w, Module, scala.collection.Seq, util.Monoids)
      ()
    }).last.children.tail

    examples should not be empty

    "are valid language constructs" in {
      all (examples) should satisfy (valid.Ref)
    }
  }

  "This references" - {
    // modeled by
    // - `This` objects in `Source.Lang`
    // - `This(qual)` nodes in Scala ASTs

    val examples = extractFrom(u.reify {
      class Unqualified { println(this.toString) }
      class Qualified { println(LanguageSpec.this.x) }
      object Module { println(this.hashCode) }
      Module: AnyRef
    }).map(_.collect {
      case ths: u.This => ths
    }.head)

    examples should not be empty

    "are valid language constructs" in {
      all (examples) should satisfy (valid.This)
    }
  }

}
