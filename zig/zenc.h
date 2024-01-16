#ifndef SPIRAL_ZIG_ENC_H
#define SPIRAL_ZIG_ENC_H

#include "stdint.h"
#include "stdbool.h"
#include "float.h"
#include "assert.h"

#if defined(__cplusplus)
extern "C" {
#endif

static_assert(sizeof(float) == 4, "float type must have 32 bits");
static_assert(sizeof(double) == 8, "double type must have 64 bits");
typedef uintptr_t expected_zig_usize_t; // for a comptime check in zig

// max
uint8_t zimd_max_u8(uint8_t const *const ptr, uintptr_t const len);
uint16_t zimd_max_u16(uint16_t const *const ptr, uintptr_t const len);
uint32_t zimd_max_u32(uint32_t const *const ptr, uintptr_t const len);
uint64_t zimd_max_u64(uint64_t const *const ptr, uintptr_t const len);
int8_t zimd_max_i8(int8_t const *const ptr, uintptr_t const len);
int16_t zimd_max_i16(int16_t const *const ptr, uintptr_t const len);
int32_t zimd_max_i32(int32_t const *const ptr, uintptr_t const len);
int64_t zimd_max_i64(int64_t const *const ptr, uintptr_t const len);
float zimd_max_f32(float const *const ptr, uintptr_t const len);
double zimd_max_f64(double const *const ptr, uintptr_t const len);

// min
uint8_t zimd_min_u8(uint8_t const *const ptr, uintptr_t const len);
uint16_t zimd_min_u16(uint16_t const *const ptr, uintptr_t const len);
uint32_t zimd_min_u32(uint32_t const *const ptr, uintptr_t const len);
uint64_t zimd_min_u64(uint64_t const *const ptr, uintptr_t const len);
int8_t zimd_min_i8(int8_t const *const ptr, uintptr_t const len);
int16_t zimd_min_i16(int16_t const *const ptr, uintptr_t const len);
int32_t zimd_min_i32(int32_t const *const ptr, uintptr_t const len);
int64_t zimd_min_i64(int64_t const *const ptr, uintptr_t const len);
float zimd_min_f32(float const *const ptr, uintptr_t const len);
double zimd_min_f64(double const *const ptr, uintptr_t const len);

// isSorted
bool zimd_isSorted_u8(uint8_t const *const ptr, uintptr_t const len);
bool zimd_isSorted_u16(uint16_t const *const ptr, uintptr_t const len);
bool zimd_isSorted_u32(uint32_t const *const ptr, uintptr_t const len);
bool zimd_isSorted_u64(uint64_t const *const ptr, uintptr_t const len);
bool zimd_isSorted_i8(int8_t const *const ptr, uintptr_t const len);
bool zimd_isSorted_i16(int16_t const *const ptr, uintptr_t const len);
bool zimd_isSorted_i32(int32_t const *const ptr, uintptr_t const len);
bool zimd_isSorted_i64(int64_t const *const ptr, uintptr_t const len);
bool zimd_isSorted_f32(float const *const ptr, uintptr_t const len);
bool zimd_isSorted_f64(double const *const ptr, uintptr_t const len);

// isConstant
bool zimd_isConstant_u8(uint8_t const *const ptr, uintptr_t const len);
bool zimd_isConstant_u16(uint16_t const *const ptr, uintptr_t const len);
bool zimd_isConstant_u32(uint32_t const *const ptr, uintptr_t const len);
bool zimd_isConstant_u64(uint64_t const *const ptr, uintptr_t const len);
bool zimd_isConstant_i8(int8_t const *const ptr, uintptr_t const len);
bool zimd_isConstant_i16(int16_t const *const ptr, uintptr_t const len);
bool zimd_isConstant_i32(int32_t const *const ptr, uintptr_t const len);
bool zimd_isConstant_i64(int64_t const *const ptr, uintptr_t const len);
bool zimd_isConstant_f32(float const *const ptr, uintptr_t const len);
bool zimd_isConstant_f64(double const *const ptr, uintptr_t const len);

// runLengthStats
typedef struct {
    uint64_t runCount;
    uint64_t runElementCount;
} RunLengthStats_t;

RunLengthStats_t zimd_runLengthStats_u8(uint8_t const *const ptr, uintptr_t const len);
RunLengthStats_t zimd_runLengthStats_u16(uint16_t const *const ptr, uintptr_t const len);
RunLengthStats_t zimd_runLengthStats_u32(uint32_t const *const ptr, uintptr_t const len);
RunLengthStats_t zimd_runLengthStats_u64(uint64_t const *const ptr, uintptr_t const len);
RunLengthStats_t zimd_runLengthStats_i8(int8_t const *const ptr, uintptr_t const len);
RunLengthStats_t zimd_runLengthStats_i16(int16_t const *const ptr, uintptr_t const len);
RunLengthStats_t zimd_runLengthStats_i32(int32_t const *const ptr, uintptr_t const len);
RunLengthStats_t zimd_runLengthStats_i64(int64_t const *const ptr, uintptr_t const len);
RunLengthStats_t zimd_runLengthStats_f32(float const *const ptr, uintptr_t const len);
RunLengthStats_t zimd_runLengthStats_f64(double const *const ptr, uintptr_t const len);

#ifndef SPIRAL_ALIGNMENT
#define SPIRAL_ALIGNMENT 128
#endif // SPIRAL_ALIGNMENT

// RunEndEncoding
typedef struct {
    uint8_t* ptr;
    uintptr_t len;
} ByteBuffer_t;

typedef struct {
    ByteBuffer_t buffer;
    uint8_t bitSizePerElement;
    uint64_t numElements;
    uint64_t inputBytesUsed;
} WrittenBuffer_t;

enum ResultStatus_t {
    Ok,
    // errors
    InvalidInput,
    IncorrectAlignment,
    EncodingFailed,
    OutputBufferTooSmall,
    OutOfMemory,
    UnknownCodecError, // catch-all, should never happen
};

typedef struct {
    enum ResultStatus_t status;
    WrittenBuffer_t buffer;
} OneBufferResult_t;

typedef struct {
    enum ResultStatus_t status;
    WrittenBuffer_t firstBuffer;
    WrittenBuffer_t secondBuffer;
} TwoBufferResult_t;

TwoBufferResult_t codecz_ree_encode_u8_u32(uint8_t const *const ptr, uintptr_t const len, ByteBuffer_t values_buf, ByteBuffer_t runends_buf);
TwoBufferResult_t codecz_ree_encode_u16_u32(uint16_t const *const ptr, uintptr_t const len, ByteBuffer_t values_buf, ByteBuffer_t runends_buf);
TwoBufferResult_t codecz_ree_encode_u32_u32(uint32_t const *const ptr, uintptr_t const len, ByteBuffer_t values_buf, ByteBuffer_t runends_buf);
TwoBufferResult_t codecz_ree_encode_u64_u32(uint64_t const *const ptr, uintptr_t const len, ByteBuffer_t values_buf, ByteBuffer_t runends_buf);
TwoBufferResult_t codecz_ree_encode_i8_u32(int8_t const *const ptr, uintptr_t const len, ByteBuffer_t values_buf, ByteBuffer_t runends_buf);
TwoBufferResult_t codecz_ree_encode_i16_u32(int16_t const *const ptr, uintptr_t const len, ByteBuffer_t values_buf, ByteBuffer_t runends_buf);
TwoBufferResult_t codecz_ree_encode_i32_u32(int32_t const *const ptr, uintptr_t const len, ByteBuffer_t values_buf, ByteBuffer_t runends_buf);
TwoBufferResult_t codecz_ree_encode_i64_u32(int64_t const *const ptr, uintptr_t const len, ByteBuffer_t values_buf, ByteBuffer_t runends_buf);
OneBufferResult_t codecz_ree_decode_u8_u32(const ByteBuffer_t values, const ByteBuffer_t runends, uintptr_t const numRuns, ByteBuffer_t out);
OneBufferResult_t codecz_ree_decode_u16_u32(const ByteBuffer_t values, const ByteBuffer_t runends, uintptr_t const numRuns, ByteBuffer_t out);
OneBufferResult_t codecz_ree_decode_u32_u32(const ByteBuffer_t values, const ByteBuffer_t runends, uintptr_t const numRuns, ByteBuffer_t out);
OneBufferResult_t codecz_ree_decode_u64_u32(const ByteBuffer_t values, const ByteBuffer_t runends, uintptr_t const numRuns, ByteBuffer_t out);
OneBufferResult_t codecz_ree_decode_i8_u32(const ByteBuffer_t values, const ByteBuffer_t runends, uintptr_t const numRuns, ByteBuffer_t out);
OneBufferResult_t codecz_ree_decode_i16_u32(const ByteBuffer_t values, const ByteBuffer_t runends, uintptr_t const numRuns, ByteBuffer_t out);
OneBufferResult_t codecz_ree_decode_i32_u32(const ByteBuffer_t values, const ByteBuffer_t runends, uintptr_t const numRuns, ByteBuffer_t out);
OneBufferResult_t codecz_ree_decode_i64_u32(const ByteBuffer_t values, const ByteBuffer_t runends, uintptr_t const numRuns, ByteBuffer_t out);

#if defined(__cplusplus)
} // extern "C"
#endif

#endif // SPIRAL_ZIG_ENC_H