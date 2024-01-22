#ifndef SPIRAL_CODECZ_H
#define SPIRAL_CODECZ_H

#include "stdint.h"
#include "float.h"

#if defined(__cplusplus)
extern "C" {
#endif

//
// codecs data structures
//
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

// Run End Encoding
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

// Adaptive Lossless Floating Point (ALP) Encoding
typedef struct {
    uint8_t e;
    uint8_t f;
} AlpExponents_t;

typedef struct {
    enum ResultStatus_t status;
    AlpExponents_t exponents;
} AlpExponentsResult_t;

AlpExponentsResult_t codecz_alp_sampleFindExponents_f32(float const *const ptr, uintptr_t const len);
AlpExponentsResult_t codecz_alp_sampleFindExponents_f64(double const *const ptr, uintptr_t const len);
TwoBufferResult_t codecz_alp_encode_f32(float const *const ptr, uintptr_t const len, AlpExponents_t const exponents, ByteBuffer_t enc_buf, ByteBuffer_t exc_idx_buf);
TwoBufferResult_t codecz_alp_encode_f64(double const *const ptr, uintptr_t const len, AlpExponents_t const exponents, ByteBuffer_t enc_buf, ByteBuffer_t exc_idx_buf);
OneBufferResult_t codecz_alp_decode_f32(int32_t const *const ptr, uintptr_t const len, AlpExponents_t const exponents, ByteBuffer_t out);
OneBufferResult_t codecz_alp_decode_f64(int64_t const *const ptr, uintptr_t const len, AlpExponents_t const exponents, ByteBuffer_t out);

#if defined(__cplusplus)
} // extern "C"
#endif

#endif // SPIRAL_CODECZ_H