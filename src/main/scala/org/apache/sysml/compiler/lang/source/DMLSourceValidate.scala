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

package org.apache.sysml.compiler.lang.source

import org.apache.sysml.compiler.DMLCommon
import org.emmalanguage.compiler.lang.source.Source

/**
  * Validation for the source language. This makes sure that only the supported Scala language features are used.
  * It does not do more detailed validation such as checking for valid module references and function calls.
  *
  * Things we don't currently support in the macro language are
  *   - pattern matching
  *   - do-while loops
  *   - function definitions and lambdas
  *   - parameter definitions
  */
private[sysml] trait DMLSourceValidate extends DMLCommon {
  self: Source =>

  import Validation._
  import UniverseImplicits._
  import Source.{Lang => src}

  /** Validation for the [[Source]] language. */
  object DMLSourceValidate {

    /** Fluid [[Validator]] builder. */
    implicit private class Check(tree: u.Tree) {

      /** Provide [[Validator]] to test. */
      case class is(expected: Validator) {

        /** Provide error message in case of validation failure. */
        def otherwise(violation: => String): Verdict =
          validateAs(expected, tree, violation)
      }

    }

    /** Validators for the [[Source]] language. */
    object valid {

      /** Validates that a Scala AST belongs to the supported [[Source]] language. */
      def apply(tree: u.Tree): Verdict =
        tree is valid.Term otherwise "Not a term"

      // ---------------------------------------------------------------------------
      // Atomics
      // ---------------------------------------------------------------------------

      lazy val Lit: Validator = {
        case src.Lit(_) => pass
      }

      lazy val Ref: Validator =
        oneOf(BindingRef, ModuleRef)

      lazy val This: Validator = {
        case src.This(_) => pass
      }

      lazy val Atomic: Validator =
        oneOf(Lit, This, Ref)

      // ---------------------------------------------------------------------------
      // Parameters
      // ---------------------------------------------------------------------------

      lazy val ParRef: Validator = {
        case src.ParRef(_) => pass
      }

      // ---------------------------------------------------------------------------
      // Values
      // ---------------------------------------------------------------------------

      lazy val ValRef: Validator = {
        case src.ValRef(_) => pass
      }

      lazy val ValDef: Validator = {
        case src.ValDef(_, rhs) =>
          rhs is Term otherwise s"Invalid ${src.ValDef} RHS"
      }

      // ---------------------------------------------------------------------------
      // Variables
      // ---------------------------------------------------------------------------

      lazy val VarRef: Validator = {
        case src.VarRef(_) => pass
      }

      lazy val VarDef: Validator = {
        case src.VarDef(_, rhs) =>
          rhs is Term otherwise s"Invalid ${src.VarDef} RHS"
      }

      lazy val VarMut: Validator = {
        case src.VarMut(_, rhs) =>
          rhs is Term otherwise s"Invalid ${src.VarMut} RHS"
      }

      // ---------------------------------------------------------------------------
      // Bindings
      // ---------------------------------------------------------------------------

      lazy val BindingRef: Validator =
        oneOf(ValRef, VarRef, ParRef)

      // no parameter definitions (since no lambdas)
      lazy val BindingDef: Validator =
        oneOf(ValDef, VarDef)

      // ---------------------------------------------------------------------------
      // Modules
      // ---------------------------------------------------------------------------

      lazy val ModuleRef: Validator = {
        case src.ModuleRef(_) => pass
      }

      lazy val ModuleAcc: Validator = {
        case src.ModuleAcc(target, _) =>
          target is Term otherwise s"Invalid ${src.ModuleAcc} target"
      }

      // ---------------------------------------------------------------------------
      // Methods
      // ---------------------------------------------------------------------------

      lazy val DefCall: Validator = {
        case src.DefCall(None, _, _, argss) =>
          all (argss.flatten) are Term otherwise s"Invalid ${src.DefCall} argument"
        case src.DefCall(Some(target), _, _, argss) => {
          target is Term otherwise s"Invalid ${src.DefCall} target"
        } and {
          all (argss.flatten) are Term otherwise s"Invalid ${src.DefCall} argument"
        }
      }

      // ---------------------------------------------------------------------------
      // Loops
      // ---------------------------------------------------------------------------

      lazy val While: Validator = {
        case src.While(cond, body) => {
          cond is Term otherwise s"Invalid ${src.While} condition"
        } and {
          body is Stat otherwise s"Invalid ${src.While} body"
        }
      }

      // no do-while loops
      lazy val Loop: Validator =
        oneOf(While)

      // ---------------------------------------------------------------------------
      // Terms
      // ---------------------------------------------------------------------------

      lazy val Block: Validator = {
        case src.Block(stats, expr) => {
          all (stats) are Stat otherwise s"Invalid ${src.Block} statement"
        } and {
          expr is Term otherwise s"Invalid last ${src.Block} expression"
        }
      }

      lazy val Branch: Validator = {
        case src.Branch(cond, thn, els) => {
          cond is Term otherwise s"Invalid ${src.Branch} condition"
        } and {
          all (thn, els) are Term otherwise s"Invalid ${src.Branch} expression"
        }
      }

      lazy val Inst: Validator = {
        case src.Inst(_, _, argss) =>
          all (argss.flatten) are Term otherwise s"Invalid ${src.Inst} argument"
      }

      lazy val TypeAscr: Validator = {
        case src.TypeAscr(expr, _) =>
          expr is Term otherwise s"Invalid ${src.TypeAscr} expression"
      }

      // We don't allow pattern matches, function definitions and lambdas
      lazy val Term: Validator =
        oneOf(Atomic, ModuleAcc, Inst, DefCall, Block, Branch, TypeAscr)

      // ---------------------------------------------------------------------------
      // Statements
      // ---------------------------------------------------------------------------

      lazy val For: Validator = {
        case src.DefCall(Some(xs), method, _, Seq(src.Lambda(_, Seq(_), body)))
          if method == api.Sym.foreach || method.overrides.contains(api.Sym.foreach) => {
          xs is Term otherwise "Invalid For loop generator"
        } and {
          body is Term otherwise "Invalid For loop body"
        }
      }

      lazy val Stat: Validator =
        oneOf(ValDef, VarDef, VarMut, Loop, For, Term)

    }

  }
}
