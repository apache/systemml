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

#include "jni_bridge.h"
#include "launcher.h"

// JNI Methods to get/release arrays
#define GET_ARRAY(env, input)                                                  \
  ((void *)env->GetPrimitiveArrayCritical(input, nullptr))

#define RELEASE_ARRAY(env, java, cpp)                                                  \
  (env->ReleasePrimitiveArrayCritical(java, cpp, 0))

JNIEXPORT jlong JNICALL
Java_org_apache_sysds_hops_codegen_SpoofCompiler_initialize_1cuda_1context(
    JNIEnv *env, jobject jobj, jint device_id) {
  return SpoofCudaContext::initialize_cuda(device_id);
}

JNIEXPORT void JNICALL
Java_org_apache_sysds_hops_codegen_SpoofCompiler_destroy_1cuda_1context(
    JNIEnv *env, jobject jobj, jlong ctx, jint device_id) {
  SpoofCudaContext::destroy_cuda(reinterpret_cast<SpoofCudaContext *>(ctx),
                                 device_id);
}

JNIEXPORT jboolean JNICALL
Java_org_apache_sysds_hops_codegen_SpoofCompiler_compile_1cuda_1kernel(
    JNIEnv *env, jobject jobj, jlong ctx, jstring name, jstring src) {
  SpoofCudaContext *ctx_ = reinterpret_cast<SpoofCudaContext *>(ctx);
  const char *cstr_name = env->GetStringUTFChars(name, NULL);
  const char *cstr_src = env->GetStringUTFChars(src, NULL);
  bool result = ctx_->compile_cuda(cstr_src, cstr_name);
  env->ReleaseStringUTFChars(src, cstr_src);
  env->ReleaseStringUTFChars(name, cstr_name);
  return result;
}

JNIEXPORT jdouble JNICALL
Java_org_apache_sysds_runtime_codegen_SpoofNativeCUDA_execute_1d(
    JNIEnv *env, jobject jobj, jlong ctx, jstring name, jlongArray in_ptrs,
    jlongArray side_ptrs, jlong out_ptr, jdoubleArray scalars_, jlong m, jlong n, jlong grix) {

  SpoofCudaContext *ctx_ = reinterpret_cast<SpoofCudaContext *>(ctx);
  const char *cstr_name = env->GetStringUTFChars(name, NULL);

  double **inputs = reinterpret_cast<double **>(GET_ARRAY(env, in_ptrs));
  double **sides = reinterpret_cast<double **>(GET_ARRAY(env, side_ptrs));
  double *scalars = reinterpret_cast<double *>(GET_ARRAY(env, scalars_));

  double result = ctx_->execute_kernel(
      cstr_name, inputs, env->GetArrayLength(in_ptrs), sides, env->GetArrayLength(side_ptrs),
      reinterpret_cast<double*>(out_ptr), scalars, env->GetArrayLength(scalars_), m, n, grix);

  RELEASE_ARRAY(env, in_ptrs, inputs);
  RELEASE_ARRAY(env, side_ptrs, sides);
  RELEASE_ARRAY(env, scalars_, scalars);

  // FIXME: that release causes an error
  //std::cout << "releasing " << name_ << std::endl;
  env->ReleaseStringUTFChars(name, cstr_name);
  return result;
}

JNIEXPORT jfloat JNICALL
Java_org_apache_sysds_runtime_codegen_SpoofNativeCUDA_execute_1f(
    JNIEnv *env, jobject jobj, jlong ctx, jstring name, jlongArray in_ptrs,
    jlongArray side_ptrs, jlong out_ptr, jfloatArray scalars_, jlong m, jlong n, jlong grix) {

  SpoofCudaContext *ctx_ = reinterpret_cast<SpoofCudaContext *>(ctx);

  const char *cstr_name = env->GetStringUTFChars(name, NULL);

  float **inputs = reinterpret_cast<float**>(GET_ARRAY(env, in_ptrs));
  float **sides = reinterpret_cast<float **>(GET_ARRAY(env, side_ptrs));
  float *scalars = reinterpret_cast<float *>(GET_ARRAY(env, scalars_));

  float result = ctx_->execute_kernel(
      cstr_name, inputs, env->GetArrayLength(in_ptrs), sides, env->GetArrayLength(side_ptrs),
      reinterpret_cast<float *>(out_ptr), scalars, env->GetArrayLength(scalars_), m, n, grix);

  RELEASE_ARRAY(env, in_ptrs, inputs);
  RELEASE_ARRAY(env, side_ptrs, sides);
  RELEASE_ARRAY(env, scalars_, scalars);

  // FIXME: that release causes an error
  env->ReleaseStringUTFChars(name, cstr_name);
  return result;
}
