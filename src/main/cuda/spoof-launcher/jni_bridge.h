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

/* DO NOT EDIT THIS FILE - it is machine generated */

#pragma once
#ifndef JNI_BRIDGE_H
#define JNI_BRIDGE_H

#include <jni.h>
/* Header for class org_apache_sysds_hops_codegen_SpoofCompiler */

#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     org_apache_sysds_hops_codegen_SpoofCompiler
 * Method:    initialize_cuda_context
 * Signature: (I)J
 */
JNIEXPORT jlong JNICALL
Java_org_apache_sysds_hops_codegen_SpoofCompiler_initialize_1cuda_1context(
    JNIEnv *, jobject, jint, jstring);

/*
 * Class:     org_apache_sysds_hops_codegen_SpoofCompiler
 * Method:    destroy_cuda_context
 * Signature: (JI)V
 */
JNIEXPORT void JNICALL
Java_org_apache_sysds_hops_codegen_SpoofCompiler_destroy_1cuda_1context(
    JNIEnv *, jobject, jlong, jint);

/*
 * Class:     org_apache_sysds_hops_codegen_SpoofCompiler
 * Method:    compile_cuda_kernel
 * Signature: (JLjava/lang/String;Ljava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL
Java_org_apache_sysds_hops_codegen_SpoofCompiler_compile_1cuda_1kernel(
    JNIEnv *, jobject, jlong, jstring, jstring);

/*
 * Class:     org_apache_sysds_runtime_instructions_gpu_SpoofCUDAInstruction
 * Method:    execute_d
 * Signature: (...)Z
 */
JNIEXPORT jdouble JNICALL
Java_org_apache_sysds_runtime_codegen_SpoofCUDA_execute_1d(
    JNIEnv *, jobject, jlong, jstring, jlongArray, jlongArray, jlong, jdoubleArray, jlong, jlong, jlong, jlong);

///*
// * Class:     org_apache_sysds_runtime_instructions_gpu_SpoofCUDAInstruction
// * Method:    execute_f
// * Signature: (...)Z
// */
//JNIEXPORT jfloat JNICALL
//Java_org_apache_sysds_runtime_codegen_SpoofCUDA_execute_1f(
//    JNIEnv *, jobject, jlong, jstring, jlongArray, jlongArray, jlong, jfloatArray, jlong, jlong, jlong, jlong);

#ifdef __cplusplus
}
#endif

#endif // JNI_BRIDGE_H
