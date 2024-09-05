
//   Copyright 2024 SpiralDB, Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.


#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#define DTYPE_PRIMITIVE_U8 0

#define DTYPE_PRIMITIVE_U16 1

#define DTYPE_PRIMITIVE_U32 2

#define DTYPE_PRIMITIVE_U64 3

#define DTYPE_PRIMITIVE_I8 4

#define DTYPE_PRIMITIVE_I16 5

#define DTYPE_PRIMITIVE_I32 6

#define DTYPE_PRIMITIVE_I64 7

#define DTYPE_PRIMITIVE_F16 8

#define DTYPE_PRIMITIVE_F32 9

#define DTYPE_PRIMITIVE_F64 10

#define DTYPE_BOOL 11

#define DTYPE_BINARY 12

#define DTYPE_UTF8 13

#define DTYPE_STRUCT 14

#define DTYPE_LIST 15

#define DTYPE_EXTENSION 16

#define DTYPE_NULL 17

/**
 * Opaque wrapper around a Vortex array.
 */
typedef struct VortexArray VortexArray;

/**
 * Opaque wrapper over a Vortex DType.
 */
typedef struct VortexDType VortexDType;

/**
 * Create a new vortex array of primitive values.
 */
struct VortexArray *vortex_array_new_primitive(struct VortexDType *dtype,
                                               const void *ptr,
                                               uintptr_t length);

/**
 * Free the VortexDType memory.
 */
void vortex_dtype_free(struct VortexDType *dtype_ptr);

bool vortex_dtype_is_nullable(struct VortexDType *dtype_ptr);

uint8_t vortex_dtype_info(struct VortexDType *dtype);

/**
 *Create a new DType::Bool with optional nullability
 */
struct VortexDType *vortex_dtype_bool(bool nullable);

/**
 *Create a new DType::Binary with optional nullability
 */
struct VortexDType *vortex_dtype_binary(bool nullable);

/**
 *Create a new DType::Utf8 with optional nullability
 */
struct VortexDType *vortex_dtype_utf8(bool nullable);

/**
 *Create a new DType::Primitive(PType::U8) with optional nullability
 */
struct VortexDType *vortex_dtype_u8(bool nullable);

/**
 *Create a new DType::Primitive(PType::U16) with optional nullability
 */
struct VortexDType *vortex_dtype_u16(bool nullable);

/**
 *Create a new DType::Primitive(PType::U32) with optional nullability
 */
struct VortexDType *vortex_dtype_u32(bool nullable);

/**
 *Create a new DType::Primitive(PType::U64) with optional nullability
 */
struct VortexDType *vortex_dtype_u64(bool nullable);

/**
 *Create a new DType::Primitive(PType::I8) with optional nullability
 */
struct VortexDType *vortex_dtype_i8(bool nullable);

/**
 *Create a new DType::Primitive(PType::I16) with optional nullability
 */
struct VortexDType *vortex_dtype_i16(bool nullable);

/**
 *Create a new DType::Primitive(PType::I32) with optional nullability
 */
struct VortexDType *vortex_dtype_i32(bool nullable);

/**
 *Create a new DType::Primitive(PType::I64) with optional nullability
 */
struct VortexDType *vortex_dtype_i64(bool nullable);

/**
 *Create a new DType::Primitive(PType::F32) with optional nullability
 */
struct VortexDType *vortex_dtype_f32(bool nullable);

/**
 *Create a new DType::Primitive(PType::F64) with optional nullability
 */
struct VortexDType *vortex_dtype_f64(bool nullable);
