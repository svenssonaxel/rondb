/*
 * This header provides light-weight utilities for developing SIMD-enabled
 * functions made up of one implementation for each instruction set.
 *
 * It supports both C and C++, both GCC and LLVM, and both compile-time and
 * run-time dispatch. Compile-time dispatch is used to the extent possible.
 *
 * 1) Macros for determining and describing the compilation configuration.
 *
 * Four compilation configurations are recognized. The macro
 * RONDB_SIMD_COMPILATION_CONFIG is defined as a string describing it.
 *
 * ╔═════════╦══════════════════════╦═══════════════════════════╦══════════════════════════════════════╗
 * ║ Arch.   ║ Compiler flags       ║ Flag usable with #ifdef   ║ RONDB_SIMD_COMPILATION_CONFIG        ║
 * ╠═════════╬══════════════════════╬═══════════════════════════╬══════════════════════════════════════╣
 * ║ X86_64  ║                      ║ RONDB_SIMD_X86_64_SCALAR  ║ "X86_64"                             ║
 * ║ X86_64  ║ -mavx2               ║ RONDB_SIMD_X86_64_AVX2    ║ "X86_64 with AVX2"                   ║
 * ║ X86_64  ║ -mavx512f -mavx512bw ║ RONDB_SIMD_X86_64_AVX512  ║ "X86_64 with AVX-512F and AVX-512BW" ║
 * ║ AArch64 ║                      ║ RONDB_SIMD_AARCH64        ║ "AArch64"                            ║
 * ╚═════════╩══════════════════════╩═══════════════════════════╩══════════════════════════════════════╝
 *
 * 2) A function for checking that the processor fulfills the requirements.
 *
 * The function
 *   const char* rondb_simd_check_processor()
 * will return nullptr if the processor supports the compilation target, and
 * otherwise a string describing the error.
 *
 * 3) Macros for defining functions using different sets of SIMD intrinsics.
 *
 * There are four macros for defining functions:
 * - RONDB_SIMD_FUN_AVX2 defines a function that can use 256-bit Intel AVX2
 *   intrinsics, as well as preceding sets including AVX, SSE4.2, SSE4.1, SSSE3,
 *   SSE3, SSE2 and SSE. It is suitable for defining
 *   - The _avx2 variant of a dispatched function.
 *   - Helper functions called by other functions defined using the same macro.
 * - RONDB_SIMD_FUN_AVX512 defines a function that can use 512-bit Intel
 *   AVX-512F and AVX-512BW intrinsics, as well as all sets allowed by
 *   RONDB_SIMD_FUN_AVX2. This is suitable for defining:
 *   - The _avx512 variant of a dispatched function.
 *   - Helper functions called by other functions defined using the same macro.
 * - RONDB_SIMD_FUN_NEON defines a function that can use 128-bit ARM Advanced
 *   SIMD (NEON) intrinsics on ARMv8.
 *   - The _neon variant of a dispatched function.
 *     and RONDB_SIMD_DISPATCH_AVX2_NEON_SCALAR.
 *   - Helper functions called by other functions defined using the same macro.
 * - RONDB_SIMD_FUN_SCALAR defines a function with no SIMD intrinsics support.
 *   This is suitable for defining:
 *   - Fallback functions.
 *
 * The entire function definition is placed within parenthesis, for example:
 *
 * RONDB_SIMD_FUN_NEON(
 * uint64_t xor_128bit_neon(uint8_t* ptr) {
 *   uint64x2_t data = vreinterpret_u64_u8(vld1q_u8(ptr));
 *   return vget_lane_u64(data, 0) ^ vget_lane_u64(data, 1);
 * })
 *
 * Functions defined in this way must not call functions defined using a
 * different macro. They may call other code freely. However, in order to call
 * these functions from other code, a dispatch function must be defined.
 *
 * There are two macros for defining dispatch functions:
 * - RONDB_SIMD_DISPATCH_AVX2_AVX512_NEON_SCALAR defines a dispatch in terms of
 *   an avx2, an avx512, a neon and a scalar function. It requires one variant
 *   implemented using each of the following macros:
 *   - RONDB_SIMD_FUN_AVX2
 *   - RONDB_SIMD_FUN_AVX512
 *   - RONDB_SIMD_FUN_NEON
 *   - RONDB_SIMD_FUN_SCALAR
 * - RONDB_SIMD_DISPATCH_AVX2_NEON_SCALAR defines a dispatch in terms of an
 *   avx2, a neon and a scalar function. Use this macro if no AVX-512
 *   implementation exists; the avx2 variant will be used instead. It requires
 *   one variant implemented using each of the following macros:
 *   - RONDB_SIMD_FUN_AVX2
 *   - RONDB_SIMD_FUN_NEON
 *   - RONDB_SIMD_FUN_SCALAR
 *
 * The dispatch macro takes the return type, dispatch function name and
 * arguments, and expects SIMD functions to be defined using suffixes, for
 * example:
 *
 * RONDB_SIMD_DISPATCH_AVX2_NEON_SCALAR(uint64_t, xor_128bit, (uint8_t* ptr))
 *
 * This expects functions xor_128bit_avx2, xor_128bit_neon and xor_128bit_scalar
 * functions to already be defined.
 *
 */

// Architecture / instruction set dependent definitions
#if defined(__x86_64__)
#if defined(__aarch64__)
#error "This is weird"
#endif
#define _RONDB_SIMD_X86_64
#if defined(__AVX512F__) && defined(__AVX512BW__)
#define RONDB_SIMD_X86_64_AVX512
#define RONDB_SIMD_COMPILATION_CONFIG "X86_64 with AVX-512F and AVX-512BW"
#elif defined(__AVX2__)
#define RONDB_SIMD_X86_64_AVX2
#define RONDB_SIMD_COMPILATION_CONFIG "X86_64 with AVX2"
#else
#define RONDB_SIMD_X86_64_SCALAR
#define RONDB_SIMD_COMPILATION_CONFIG "X86_64"
#endif
#elif defined(__aarch64__)
#define RONDB_SIMD_AARCH64
#define RONDB_SIMD_COMPILATION_CONFIG "AArch64"
#if !defined(__ARM_NEON)
#error AArch64 without NEON should not be possible
#endif
#else
#error "Only x86_64 and AArch64 are supported"
#endif

// Compiler dependent definitions
#if defined(__clang__)
// Clang does not support function alias for static functions.
#define _RONDB_SIMD_ALIASABLE_STATIC
#elif defined(__GNUC__)
#define _RONDB_SIMD_ALIASABLE_STATIC static
#else
#error "Only GCC and LLVM are supported"
#endif

// Language dependent definitions
#ifdef __cplusplus
#define _RONDB_SIMD_EXTERNC(...) extern "C" { __VA_ARGS__ }
#define _RONDB_SIMD_CAST_TO_VOID_PTR(X) reinterpret_cast<void*>(X)
#define _RONDB_SIMD_FUN_ALIASABLE(...) \
    __attribute__((always_inline)) static inline \
    __VA_ARGS__
#define _RONDB_SIMD_FUN_ALIAS(RET, NAME, ARGS, SUFFIX) \
  constexpr auto NAME = NAME##SUFFIX;
#else
#define _RONDB_SIMD_EXTERNC(...) __VA_ARGS__
#define _RONDB_SIMD_CAST_TO_VOID_PTR(X) ((void*)(X))
#define _RONDB_SIMD_FUN_ALIASABLE(...) \
    __attribute__((always_inline)) _RONDB_SIMD_ALIASABLE_STATIC inline \
    __VA_ARGS__
#define _RONDB_SIMD_FUN_ALIAS(RET, NAME, ARGS, SUFFIX) \
    RET NAME ARGS __attribute__((alias(#NAME #SUFFIX)));
#endif

// Other helper macros
#define _RONDB_SIMD_FUN_SUPPRESS(...)
#define _RONDB_SIMD_FUN_STD(...) \
    __attribute__((always_inline)) static inline \
    __VA_ARGS__
#define _RONDB_SIMD_FUN_TARGET(TARGET, ...) \
  __attribute__((always_inline)) static inline \
  __attribute__((__target__(TARGET))) \
  __VA_ARGS__

// X86_64
#ifdef _RONDB_SIMD_X86_64
#include <cpuid.h>
#include <xmmintrin.h> // SSE intrinsics
#include <emmintrin.h> // SSE2 intrinsics
#include <pmmintrin.h> // SSE3 intrinsics
#include <tmmintrin.h> // SSSE3 intrinsics
#include <smmintrin.h> // SSE4.1 intrinsics
#include <nmmintrin.h> // SSE4.2 intrinsics
#include <immintrin.h> // Intrinsics for AVX, AVX2, etc.
#define RONDB_SIMD_FUN_NEON _RONDB_SIMD_FUN_SUPPRESS
#define _RONDB_SIMD_DISPATCH_HELPER_AVX(RET, NAME, ARGS, DEFAULT, ...) \
  _RONDB_SIMD_EXTERNC( \
    typedef RET (NAME##_function_type) ARGS; \
    void* NAME##_resolver() { \
      NAME##_function_type* ret = NAME##DEFAULT; \
      __VA_ARGS__ \
      return _RONDB_SIMD_CAST_TO_VOID_PTR(ret); \
    } \
  ) \
  __attribute__((ifunc(#NAME "_resolver"))) \
  RET NAME ARGS;
_RONDB_SIMD_FUN_STD(
bool _rondb_simd_cpuid_helper(
  unsigned int leaf,
  unsigned int subleaf,
  unsigned int reg, // 0=eax, 1=ebx, 2=ecx, 3=edx
  unsigned int requirement_mask)
{
  unsigned int r[4];
  if (__get_cpuid_count(leaf, subleaf, &r[0], &r[1], &r[2], &r[3]))
    return (r[reg] & requirement_mask) == requirement_mask;
  return false;
})
/* From "Features in %ebx for leaf 7 sub-leaf 0" in
 * https://clang.llvm.org/doxygen/cpuid_8h_source.html
 * #define bit_AVX2        0x00000020
 * #define bit_AVX512F     0x00010000
 * #define bit_AVX512DQ    0x00020000
 * #define bit_AVX512IFMA  0x00200000
 * #define bit_AVX512PF    0x04000000
 * #define bit_AVX512ER    0x08000000
 * #define bit_AVX512CD    0x10000000
 * #define bit_AVX512BW    0x40000000
 * #define bit_AVX512VL    0x80000000
 */
#define _RONDB_SIMD_CPU_SUPPORTS_AVX2 \
  _rondb_simd_cpuid_helper(7, 0, 1, 0x00000020)
#define _RONDB_SIMD_CPU_SUPPORTS_AVX512F \
  _rondb_simd_cpuid_helper(7, 0, 1, 0x00010000)
#define _RONDB_SIMD_CPU_SUPPORTS_AVX512BW \
  _rondb_simd_cpuid_helper(7, 0, 1, 0x40000000)
// This is our "selection" of AVX512 subsets. We will only use AVX512 on
// processors that support all of them.
#define _RONDB_SIMD_CPU_SUPPORTS_AVX512_SELECTION \
  (_RONDB_SIMD_CPU_SUPPORTS_AVX512F && \
   _RONDB_SIMD_CPU_SUPPORTS_AVX512BW)
#endif

// X86_64 compiled against AVX-512
#ifdef RONDB_SIMD_X86_64_AVX512
#define RONDB_SIMD_FUN_SCALAR _RONDB_SIMD_FUN_SUPPRESS
#define RONDB_SIMD_FUN_AVX2 _RONDB_SIMD_FUN_ALIASABLE
#define RONDB_SIMD_FUN_AVX512 _RONDB_SIMD_FUN_ALIASABLE
#define RONDB_SIMD_DISPATCH_AVX2_NEON_SCALAR(RET, NAME, ARGS) \
  _RONDB_SIMD_FUN_ALIAS(RET, NAME, ARGS, _avx2)
#define RONDB_SIMD_DISPATCH_AVX2_AVX512_NEON_SCALAR(RET, NAME, ARGS) \
  _RONDB_SIMD_FUN_ALIAS(RET, NAME, ARGS, _avx512)
#endif

// X86_64 compiled against AVX2, with run-time detection for AVX-512
#ifdef RONDB_SIMD_X86_64_AVX2
#define RONDB_SIMD_FUN_SCALAR _RONDB_SIMD_FUN_SUPPRESS
#define RONDB_SIMD_FUN_AVX2 _RONDB_SIMD_FUN_ALIASABLE
#define RONDB_SIMD_FUN_AVX512(...) _RONDB_SIMD_FUN_TARGET("avx512f,avx512bw", __VA_ARGS__)
#define RONDB_SIMD_DISPATCH_AVX2_NEON_SCALAR(RET, NAME, ARGS) \
  _RONDB_SIMD_FUN_ALIAS(RET, NAME, ARGS, _avx2)
#define RONDB_SIMD_DISPATCH_AVX2_AVX512_NEON_SCALAR(RET, NAME, ARGS) \
  _RONDB_SIMD_DISPATCH_HELPER_AVX(RET, NAME, ARGS, _avx2, \
      if (_RONDB_SIMD_CPU_SUPPORTS_AVX512_SELECTION) ret = NAME##_avx512;)
#endif

// X86_64 with run-time detection for AVX2 and AVX-512
#ifdef RONDB_SIMD_X86_64_SCALAR
#define RONDB_SIMD_FUN_SCALAR _RONDB_SIMD_FUN_STD
#define RONDB_SIMD_FUN_AVX2(...) _RONDB_SIMD_FUN_TARGET("avx2", __VA_ARGS__)
#define RONDB_SIMD_FUN_AVX512(...) _RONDB_SIMD_FUN_TARGET("avx512f,avx512bw", __VA_ARGS__)
#define RONDB_SIMD_DISPATCH_AVX2_NEON_SCALAR(RET, NAME, ARGS) \
  _RONDB_SIMD_DISPATCH_HELPER_AVX(RET, NAME, ARGS, _scalar, \
      if (_RONDB_SIMD_CPU_SUPPORTS_AVX2) ret = NAME##_avx2;)
#define RONDB_SIMD_DISPATCH_AVX2_AVX512_NEON_SCALAR(RET, NAME, ARGS) \
  _RONDB_SIMD_DISPATCH_HELPER_AVX(RET, NAME, ARGS, _scalar, \
      if (_RONDB_SIMD_CPU_SUPPORTS_AVX512_SELECTION) ret = NAME##_avx512; \
      else if (_RONDB_SIMD_CPU_SUPPORTS_AVX2) ret = NAME##_avx2;)
#endif

// AArch64, which guarantees that NEON is available
#ifdef RONDB_SIMD_AARCH64
#include <arm_neon.h>
#include <sys/auxv.h>
#include <linux/auxvec.h>
#include <asm/hwcap.h>
#define RONDB_SIMD_FUN_AVX2 _RONDB_SIMD_FUN_SUPPRESS
#define RONDB_SIMD_FUN_AVX512 _RONDB_SIMD_FUN_SUPPRESS
#define RONDB_SIMD_FUN_SCALAR _RONDB_SIMD_FUN_SUPPRESS
#define RONDB_SIMD_FUN_NEON _RONDB_SIMD_FUN_ALIASABLE
#define _RONDB_SIMD_NEON_ALIAS(RET, NAME, ARGS) _RONDB_SIMD_FUN_ALIAS(RET, NAME, ARGS, _neon)
#define RONDB_SIMD_DISPATCH_AVX2_AVX512_NEON_SCALAR _RONDB_SIMD_NEON_ALIAS
#define RONDB_SIMD_DISPATCH_AVX2_NEON_SCALAR _RONDB_SIMD_NEON_ALIAS
#if !defined(AT_HWCAP)
#error AT_HWCAP should be defined
#endif
#if defined(HWCAP_NEON)
#error HWCAP_NEON should not be defined when compiling for ARMv8.
#endif
#endif

const char* rondb_simd_check_processor() {
#if defined(RONDB_SIMD_X86_64_AVX512)
  if (!_RONDB_SIMD_CPU_SUPPORTS_AVX512F && !_RONDB_SIMD_CPU_SUPPORTS_AVX512BW)
    return "Compiled for X86_64 with AVX-512F and AVX-512BW but processor does"
      " not support either";
  if (!_RONDB_SIMD_CPU_SUPPORTS_AVX512F)
    return "Compiled for X86_64 with AVX-512F and AVX-512BW but processor does"
      " not support AVX-512F";
  if (!_RONDB_SIMD_CPU_SUPPORTS_AVX512BW)
    return "Compiled for X86_64 with AVX-512F and AVX-512BW but processor does"
      " not support AVX-512BW";
  if (!_RONDB_SIMD_CPU_SUPPORTS_AVX512_SELECTION)
    return "Bug in rondb_simd_check_processor";
#endif
#if defined(RONDB_SIMD_X86_64_AVX2)
  if (!_RONDB_SIMD_CPU_SUPPORTS_AVX2)
    return "Compiled for X86_64 with AVX2 but processor does not support AVX2";
#endif
// For AArch64, all checks are done compile-time.
  return nullptr;
}
