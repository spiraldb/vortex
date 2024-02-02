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
    uint64_t len;
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
    WrittenBuffer_t buf;
} OneBufferResult_t;

typedef struct {
    enum ResultStatus_t status;
    WrittenBuffer_t first;
    WrittenBuffer_t second;
} TwoBufferResult_t;

//
// Run End Encoding
//
void codecz_ree_encode_u8_u32(uint8_t const *const ptr, uint64_t const len, TwoBufferResult_t *const out);
void codecz_ree_encode_u16_u32(uint16_t const *const ptr, uint64_t const len, TwoBufferResult_t *const out);
void codecz_ree_encode_u32_u32(uint32_t const *const ptr, uint64_t const len, TwoBufferResult_t *const out);
void codecz_ree_encode_u64_u32(uint64_t const *const ptr, uint64_t const len, TwoBufferResult_t *const out);
void codecz_ree_encode_i8_u32(int8_t const *const ptr, uint64_t const len, TwoBufferResult_t *const out);
void codecz_ree_encode_i16_u32(int16_t const *const ptr, uint64_t const len, TwoBufferResult_t *const out);
void codecz_ree_encode_i32_u32(int32_t const *const ptr, uint64_t const len, TwoBufferResult_t *const out);
void codecz_ree_encode_i64_u32(int64_t const *const ptr, uint64_t const len, TwoBufferResult_t *const out);
void codecz_ree_encode_f16_u32(int16_t const *const ptr, uint64_t const len, TwoBufferResult_t *const out);
void codecz_ree_encode_f32_u32(float const *const ptr, uint64_t const len, TwoBufferResult_t *const out);
void codecz_ree_encode_f64_u32(double const *const ptr, uint64_t const len, TwoBufferResult_t *const out);
void codecz_ree_decode_u8_u32(uint8_t const *const values, uint32_t const *const runends, uint64_t const len, OneBufferResult_t *const out);
void codecz_ree_decode_u16_u32(uint16_t const *const values, uint32_t const *const runends, uint64_t const len, OneBufferResult_t *const out);
void codecz_ree_decode_u32_u32(uint32_t const *const values, uint32_t const *const runends, uint64_t const len, OneBufferResult_t *const out);
void codecz_ree_decode_u64_u32(uint64_t const *const values, uint32_t const *const runends, uint64_t const len, OneBufferResult_t *const out);
void codecz_ree_decode_i8_u32(int8_t const *const values, uint32_t const *const runends, uint64_t const len, OneBufferResult_t *const out);
void codecz_ree_decode_i16_u32(int16_t const *const values, uint32_t const *const runends, uint64_t const len, OneBufferResult_t *const out);
void codecz_ree_decode_i32_u32(int32_t const *const values, uint32_t const *const runends, uint64_t const len, OneBufferResult_t *const out);
void codecz_ree_decode_i64_u32(int64_t const *const values, uint32_t const *const runends, uint64_t const len, OneBufferResult_t *const out);
void codecz_ree_decode_f16_u32(int16_t const *const values, uint32_t const *const runends, uint64_t const len, OneBufferResult_t *const out);
void codecz_ree_decode_f32_u32(float const *const values, uint32_t const *const runends, uint64_t const len, OneBufferResult_t *const out);
void codecz_ree_decode_f64_u32(double const *const values, uint32_t const *const runends, uint64_t const len, OneBufferResult_t *const out);

//
// Adaptive Lossless Floating Point (ALP) Encoding
//
typedef struct {
    uint8_t e;
    uint8_t f;
} AlpExponents_t;

typedef struct {
    enum ResultStatus_t status;
    AlpExponents_t exponents;
} AlpExponentsResult_t;

void codecz_alp_sampleFindExponents_f32(float const *const ptr, uint64_t const len, AlpExponentsResult_t *const out);
void codecz_alp_sampleFindExponents_f64(double const *const ptr, uint64_t const len, AlpExponentsResult_t *const out);
void codecz_alp_encode_f32(float const *const ptr, uint64_t const len, AlpExponents_t const *const exponents, TwoBufferResult_t *const out);
void codecz_alp_encode_f64(double const *const ptr, uint64_t const len, AlpExponents_t const *const exponents, TwoBufferResult_t *const out);
void codecz_alp_decode_f32(int32_t const *const ptr, uint64_t const len, AlpExponents_t const *const exponents, OneBufferResult_t *const out);
void codecz_alp_decode_f64(int64_t const *const ptr, uint64_t const len, AlpExponents_t const *const exponents, OneBufferResult_t *const out);

//
// ZigZag Encoding
//
void codecz_zz_encode_i8(int8_t const *const ptr, uint64_t const len, OneBufferResult_t *const out);
void codecz_zz_encode_i16(int16_t const *const ptr, uint64_t const len, OneBufferResult_t *const out);
void codecz_zz_encode_i32(int32_t const *const ptr, uint64_t const len, OneBufferResult_t *const out);
void codecz_zz_encode_i64(int64_t const *const ptr, uint64_t const len, OneBufferResult_t *const out);
void codecz_zz_decode_i8(uint8_t const *const ptr, uint64_t const len, OneBufferResult_t *const out);
void codecz_zz_decode_i16(uint16_t const *const ptr, uint64_t const len, OneBufferResult_t *const out);
void codecz_zz_decode_i32(uint32_t const *const ptr, uint64_t const len, OneBufferResult_t *const out);
void codecz_zz_decode_i64(uint64_t const *const ptr, uint64_t const len, OneBufferResult_t *const out);

//
// Fastlanes bitpacking
//

// buffer sizing functions are shared between PackedInts and FFoR
uint64_t codecz_flbp_encodedSizeInBytes_u8(uint64_t const len, uint8_t const num_bits);
uint64_t codecz_flbp_encodedSizeInBytes_u16(uint64_t const len, uint8_t const num_bits);
uint64_t codecz_flbp_encodedSizeInBytes_u32(uint64_t const len, uint8_t const num_bits);
uint64_t codecz_flbp_encodedSizeInBytes_u64(uint64_t const len, uint8_t const num_bits);
uint64_t codecz_flbp_encodedSizeInBytes_i8(uint64_t const len, uint8_t const num_bits);
uint64_t codecz_flbp_encodedSizeInBytes_i16(uint64_t const len, uint8_t const num_bits);
uint64_t codecz_flbp_encodedSizeInBytes_i32(uint64_t const len, uint8_t const num_bits);
uint64_t codecz_flbp_encodedSizeInBytes_i64(uint64_t const len, uint8_t const num_bits);

// Fastlanes Packed Ints Encoding
typedef struct {
    enum ResultStatus_t status;
    WrittenBuffer_t encoded;
    uint64_t num_exceptions;
} PackedIntsResult_t;

void codecz_flpi_encode_u8(uint8_t const *const ptr, uint64_t const len, uint8_t const num_bits, PackedIntsResult_t *const out);
void codecz_flpi_encode_u16(uint16_t const *const ptr, uint64_t const len, uint8_t const num_bits, PackedIntsResult_t *const out);
void codecz_flpi_encode_u32(uint32_t const *const ptr, uint64_t const len, uint8_t const num_bits, PackedIntsResult_t *const out);
void codecz_flpi_encode_u64(uint64_t const *const ptr, uint64_t const len, uint8_t const num_bits, PackedIntsResult_t *const out);
void codecz_flpi_collectExceptions_u8(uint8_t const *const ptr, uint64_t const len, uint8_t const num_bits, uint64_t num_exceptions, TwoBufferResult_t *const out);
void codecz_flpi_collectExceptions_u16(uint16_t const *const ptr, uint64_t const len, uint8_t const num_bits, uint64_t num_exceptions, TwoBufferResult_t *const out);
void codecz_flpi_collectExceptions_u32(uint32_t const *const ptr, uint64_t const len, uint8_t const num_bits, uint64_t num_exceptions, TwoBufferResult_t *const out);
void codecz_flpi_collectExceptions_u64(uint64_t const *const ptr, uint64_t const len, uint8_t const num_bits, uint64_t num_exceptions, TwoBufferResult_t *const out);
void codecz_flpi_decode_u8(ByteBuffer_t const *const bytes, uint64_t const num_elems, uint8_t const num_bits, OneBufferResult_t *const out);
void codecz_flpi_decode_u16(ByteBuffer_t const *const bytes, uint64_t const num_elems, uint8_t const num_bits, OneBufferResult_t *const out);
void codecz_flpi_decode_u32(ByteBuffer_t const *const bytes, uint64_t const num_elems, uint8_t const num_bits, OneBufferResult_t *const out);
void codecz_flpi_decode_u64(ByteBuffer_t const *const bytes, uint64_t const num_elems, uint8_t const num_bits, OneBufferResult_t *const out);

// Fastlanes Fused Frame of Reference (FFoR) Encoding
typedef struct {
    enum ResultStatus_t status;
    WrittenBuffer_t encoded;
    int64_t min_val;
    uint64_t num_exceptions;
} FforResult_t;

void codecz_ffor_encode_u8(uint8_t const *const ptr, uint64_t const len, uint8_t const num_bits, int64_t min_val, FforResult_t *const out);
void codecz_ffor_encode_u16(uint16_t const *const ptr, uint64_t const len, uint8_t const num_bits, int64_t min_val, FforResult_t *const out);
void codecz_ffor_encode_u32(uint32_t const *const ptr, uint64_t const len, uint8_t const num_bits, int64_t min_val, FforResult_t *const out);
void codecz_ffor_encode_u64(uint64_t const *const ptr, uint64_t const len, uint8_t const num_bits, int64_t min_val, FforResult_t *const out);
void codecz_ffor_encode_i8(int8_t const *const ptr, uint64_t const len, uint8_t const num_bits, int64_t min_val, FforResult_t *const out);
void codecz_ffor_encode_i16(int16_t const *const ptr, uint64_t const len, uint8_t const num_bits, int64_t min_val, FforResult_t *const out);
void codecz_ffor_encode_i32(int32_t const *const ptr, uint64_t const len, uint8_t const num_bits, int64_t min_val, FforResult_t *const out);
void codecz_ffor_encode_i64(int64_t const *const ptr, uint64_t const len, uint8_t const num_bits, int64_t min_val, FforResult_t *const out);
void codecz_ffor_collectExceptions_u8(uint8_t const *const ptr, uint64_t const len, uint8_t const num_bits, int64_t min_val, uint64_t num_exceptions, TwoBufferResult_t *const out);
void codecz_ffor_collectExceptions_u16(uint16_t const *const ptr, uint64_t const len, uint8_t const num_bits, int64_t min_val, uint64_t num_exceptions, TwoBufferResult_t *const out);
void codecz_ffor_collectExceptions_u32(uint32_t const *const ptr, uint64_t const len, uint8_t const num_bits, int64_t min_val, uint64_t num_exceptions, TwoBufferResult_t *const out);
void codecz_ffor_collectExceptions_u64(uint64_t const *const ptr, uint64_t const len, uint8_t const num_bits, int64_t min_val, uint64_t num_exceptions, TwoBufferResult_t *const out);
void codecz_ffor_collectExceptions_i8(int8_t const *const ptr, uint64_t const len, uint8_t const num_bits, int64_t min_val, uint64_t num_exceptions, TwoBufferResult_t *const out);
void codecz_ffor_collectExceptions_i16(int16_t const *const ptr, uint64_t const len, uint8_t const num_bits, int64_t min_val, uint64_t num_exceptions, TwoBufferResult_t *const out);
void codecz_ffor_collectExceptions_i32(int32_t const *const ptr, uint64_t const len, uint8_t const num_bits, int64_t min_val, uint64_t num_exceptions, TwoBufferResult_t *const out);
void codecz_ffor_collectExceptions_i64(int64_t const *const ptr, uint64_t const len, uint8_t const num_bits, int64_t min_val, uint64_t num_exceptions, TwoBufferResult_t *const out);
void codecz_ffor_decode_u8(ByteBuffer_t const *const bytes, uint64_t const num_elems, uint8_t const num_bits, int64_t min_val, OneBufferResult_t *const out);
void codecz_ffor_decode_u16(ByteBuffer_t const *const bytes, uint64_t const num_elems, uint8_t const num_bits, int64_t min_val, OneBufferResult_t *const out);
void codecz_ffor_decode_u32(ByteBuffer_t const *const bytes, uint64_t const num_elems, uint8_t const num_bits, int64_t min_val, OneBufferResult_t *const out);
void codecz_ffor_decode_u64(ByteBuffer_t const *const bytes, uint64_t const num_elems, uint8_t const num_bits, int64_t min_val, OneBufferResult_t *const out);
void codecz_ffor_decode_i8(ByteBuffer_t const *const bytes, uint64_t const num_elems, uint8_t const num_bits, int64_t min_val, OneBufferResult_t *const out);
void codecz_ffor_decode_i16(ByteBuffer_t const *const bytes, uint64_t const num_elems, uint8_t const num_bits, int64_t min_val, OneBufferResult_t *const out);
void codecz_ffor_decode_i32(ByteBuffer_t const *const bytes, uint64_t const num_elems, uint8_t const num_bits, int64_t min_val, OneBufferResult_t *const out);
void codecz_ffor_decode_i64(ByteBuffer_t const *const bytes, uint64_t const num_elems, uint8_t const num_bits, int64_t min_val, OneBufferResult_t *const out);

#if defined(__cplusplus)
} // extern "C"
#endif

#endif // SPIRAL_CODECZ_H