#include <iostream>
#include <cstring>
#include <cassert>
#include <chrono>

// Check whether we're on x86 or ARM
#if defined(__x86_64__)
#define ua_x86_64
#endif
#if defined(__aarch64__)
#define ua_arm
#endif
// Assert ua_x86_64 xor ua_arm
#if defined(ua_x86_64) && defined(ua_arm)
#error "This is weird"
#endif
#if !defined(ua_x86_64) && !defined(ua_arm)
#error "Only x86_64 and ARM are supported"
#endif

#ifdef ua_x86_64
#include <emmintrin.h> // SSE2 intrinsics
#include <immintrin.h> // AVX2 intrinsics
#endif
#ifdef ua_arm
#include <arm_neon.h>
#include <sys/auxv.h>
#include <linux/auxvec.h>
#include <asm/hwcap.h>
#endif

using std::cout;
using std::endl;

typedef unsigned char uchar;
constexpr bool likely(bool expr) { return __builtin_expect(expr, true); }
constexpr bool unlikely(bool expr) { return __builtin_expect(expr, false); }
typedef __SIZE_TYPE__ UintPtr;

//#define DBG(...) do { cout << "DBG: " __FILE__ ":" << __LINE__ << " " __VA_ARGS__ << endl; } while(0)
#define DBG(...) do { } while(0)

// Return true if c is in ranges 0x20-0x21, 0x23-5b and 0x5d-0x7e.
inline bool char_is_unescaped_ascii(char c) {
  return 0x20 <= c && c != 0x22 && c != 0x5c && c <= 0x7e;
}

// Return !char_is_unescaped_ascii(c)
inline bool char_is_not_unescaped_ascii(char c) {
  return c < 0x20 || c == 0x22 || c == 0x5c || 0x7e < c;
}

__attribute__((always_inline)) static inline
bool unescaped_ascii_fallback(const char* str, const char* end) {
  while (str < end) {
    uchar c = *((const uchar*)str);
    DBG("c:" << c);
    if (unlikely(char_is_not_unescaped_ascii(c))) {
      return false;
    }
    str++;
  }
  return true;
}

__attribute__((always_inline)) static inline
bool unescaped_ascii_correct(const char* str, const char* end) {
  return unescaped_ascii_fallback(str, end);
}

#ifdef ua_x86_64

__attribute__((always_inline)) static inline
__attribute__((__target__("avx2")))
int unescaped_ascii_avx2_helper_32(__m256i input) {
  return _mm256_movemask_epi8(
           _mm256_or_si256(
             _mm256_or_si256(
               _mm256_cmpgt_epi8(_mm256_set1_epi8(0x20), input),
               _mm256_cmpgt_epi8(input, _mm256_set1_epi8(0x7e))),
             _mm256_or_si256(
               _mm256_cmpeq_epi8(input, _mm256_set1_epi8(0x22)),
               _mm256_cmpeq_epi8(input, _mm256_set1_epi8(0x5c)))));
}

__attribute__((always_inline)) static inline
__attribute__((__target__("sse2,avx2")))
int unescaped_ascii_sse2_helper_16(__m128i input) {
  return _mm_movemask_epi8(
           _mm_or_si128(
             _mm_or_si128(
               _mm_cmpgt_epi8(_mm_set1_epi8(0x20), input),
               _mm_cmpgt_epi8(input, _mm_set1_epi8(0x7e))),
             _mm_or_si128(
               _mm_cmpeq_epi8(input, _mm_set1_epi8(0x22)),
               _mm_cmpeq_epi8(input, _mm_set1_epi8(0x5c)))));
}

__attribute__((always_inline)) static inline
__attribute__((__target__ ("sse2,avx2")))
bool unescaped_ascii_avx2(const char *str, const char *end) {
  unsigned int len = end - str;
  if (likely(len <= 255)) {
    // A lot of these values are read from possibly unaligned memory.
    // This is ok on all platforms supporting SSE2. (todo confirm)
    switch((unsigned char)(len)) {
    case 255: case 254: case 253: case 252: case 251: case 250: case 249:
    case 248: case 247: case 246: case 245: case 244: case 243: case 242:
    case 241: case 240: case 239: case 238: case 237: case 236: case 235:
    case 234: case 233: case 232: case 231: case 230: case 229: case 228:
    case 227: case 226: case 225:
      return (unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(end - 32))) |
              unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(str + 192))) |
              unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(str + 160))) |
              unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(str + 128))) |
              unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(str + 96))) |
              unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(str + 64))) |
              unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(str + 32))) |
              unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(str)))) == 0;
    case 224: case 223: case 222: case 221: case 220: case 219: case 218:
    case 217: case 216: case 215: case 214: case 213: case 212: case 211:
    case 210: case 209: case 208: case 207: case 206: case 205: case 204:
    case 203: case 202: case 201: case 200: case 199: case 198: case 197:
    case 196: case 195: case 194: case 193:
      return (unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(end - 32))) |
              unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(str + 160))) |
              unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(str + 128))) |
              unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(str + 96))) |
              unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(str + 64))) |
              unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(str + 32))) |
              unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(str)))) == 0;
    case 192: case 191: case 190: case 189: case 188: case 187: case 186:
    case 185: case 184: case 183: case 182: case 181: case 180: case 179:
    case 178: case 177: case 176: case 175: case 174: case 173: case 172:
    case 171: case 170: case 169: case 168: case 167: case 166: case 165:
    case 164: case 163: case 162: case 161:
      return (unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(end - 32))) |
              unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(str + 128))) |
              unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(str + 96))) |
              unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(str + 64))) |
              unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(str + 32))) |
              unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(str)))) == 0;
    case 160: case 159: case 158: case 157: case 156: case 155: case 154:
    case 153: case 152: case 151: case 150: case 149: case 148: case 147:
    case 146: case 145: case 144: case 143: case 142: case 141: case 140:
    case 139: case 138: case 137: case 136: case 135: case 134: case 133:
    case 132: case 131: case 130: case 129:
      return (unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(end - 32))) |
              unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(str + 96))) |
              unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(str + 64))) |
              unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(str + 32))) |
              unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(str)))) == 0;
    case 128: case 127: case 126: case 125: case 124: case 123: case 122:
    case 121: case 120: case 119: case 118: case 117: case 116: case 115:
    case 114: case 113: case 112: case 111: case 110: case 109: case 108:
    case 107: case 106: case 105: case 104: case 103: case 102: case 101:
    case 100: case 99: case 98: case 97:
      return (unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(end - 32))) |
              unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(str + 64))) |
              unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(str + 32))) |
              unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(str)))) == 0;
    case 96: case 95: case 94: case 93: case 92: case 91: case 90: case 89:
    case 88: case 87: case 86: case 85: case 84: case 83: case 82: case 81:
    case 80: case 79: case 78: case 77: case 76: case 75: case 74: case 73:
    case 72: case 71: case 70: case 69: case 68: case 67: case 66: case 65:
      return (unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(end - 32))) |
              unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(str + 32))) |
              unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(str)))) == 0;
    case 64: case 63: case 62: case 61: case 60: case 59: case 58: case 57:
    case 56: case 55: case 54: case 53: case 52: case 51: case 50: case 49:
    case 48: case 47: case 46: case 45: case 44: case 43: case 42: case 41:
    case 40: case 39: case 38: case 37: case 36: case 35: case 34: case 33:
      return (unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(end - 32))) |
              unescaped_ascii_avx2_helper_32(_mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(str)))) == 0;
    case 32:
      return unescaped_ascii_avx2_helper_32(
              _mm256_loadu_si256(reinterpret_cast<const __m256i*>(str))) == 0;
    case 31: case 30: case 29: case 28: case 27: case 26: case 25: case 24:
    case 23: case 22: case 21: case 20: case 19: case 18: case 17:
      return unescaped_ascii_avx2_helper_32(
              _mm256_set_m128i(
                _mm_loadu_si128(reinterpret_cast<const __m128i*>(end - 16)),
                _mm_loadu_si128(reinterpret_cast<const __m128i*>(str)))) == 0;
    case 16:
      return unescaped_ascii_sse2_helper_16(
               _mm_loadu_si128(reinterpret_cast<const __m128i*>(str))) == 0;
    case 15: case 14: case 13: case 12: case 11: case 10: case 9:
      return unescaped_ascii_sse2_helper_16(
              _mm_set_epi64x(
                *reinterpret_cast<const uint64_t*>(end - 8),
                *reinterpret_cast<const uint64_t*>(str))) == 0;
    case 8:
      return unescaped_ascii_sse2_helper_16(
               _mm_set_epi64x(
                 0x2020202020202020L,
                 *reinterpret_cast<const uint64_t*>(str))) == 0;
    case 7: case 6: case 5:
      return unescaped_ascii_sse2_helper_16(
               _mm_set_epi32(
                 0x20202020, 0x20202020,
                 *reinterpret_cast<const uint32_t*>(end - 4),
                 *reinterpret_cast<const uint32_t*>(str))) == 0;
    case 4:
      return unescaped_ascii_sse2_helper_16(
               _mm_set_epi32(
                 0x20202020, 0x20202020, 0x20202020,
                 *reinterpret_cast<const uint32_t*>(str))) == 0;
    case 3:
      if (unlikely(char_is_not_unescaped_ascii(str[2]))) return false;
    case 2:
      if (unlikely(char_is_not_unescaped_ascii(str[1]))) return false;
    case 1:
      if (unlikely(char_is_not_unescaped_ascii(str[0]))) return false;
    case 0:
      return true;
    default:
      abort();
    }
  }
  // Less specialized code for len >= 256
  const __m256i* section1 = reinterpret_cast<const __m256i*>
    ((reinterpret_cast<UintPtr>(str) + 32) & -32UL);
  const __m256i* section2 = reinterpret_cast<const __m256i*>
    ((reinterpret_cast<UintPtr>(end) - 1) & -32UL);
  UintPtr b = reinterpret_cast<UintPtr>(str);
  UintPtr e = reinterpret_cast<UintPtr>(end);
  UintPtr s1 = reinterpret_cast<UintPtr>(section1);
  UintPtr s2 = reinterpret_cast<UintPtr>(section2);
  assert((s1 & 0x1f) == 0 && (s2 & 0x1f) == 0);
  assert(s1 < s2);
  assert((b + 1) <= s1 && s1 <= (b + 32));
  assert((e - 32) <= s2 && s2 <= (e - 1));
  {
    if (unlikely(unescaped_ascii_avx2_helper_32(
                   _mm256_loadu_si256(
                     reinterpret_cast<const __m256i*>(str))))) {
      return false;
    }
  }
  for (const __m256i* aptr = section1; aptr < section2; aptr++) {
    if (unlikely(unescaped_ascii_avx2_helper_32(
                   _mm256_load_si256(aptr)))) {
      return false;
    }
  }
  {
    if (unlikely(unescaped_ascii_avx2_helper_32(
                   _mm256_loadu_si256(
                     reinterpret_cast<const __m256i*>(end - 32))))) {
      return false;
    }
  }
  return true;
}

#endif

#ifdef ua_arm

std::ostream& operator<<(std::ostream& os, const uint8x16_t& vec) {
    uint8_t data[16];
    vst1q_u8(data, vec); // Store the vector into an array
    for (size_t i = 0; i < 16; ++i) {
        os << ("0123456789abcdef"[data[i] >> 4])
           << ("0123456789abcdef"[data[i] & 0xf]);
        if (i < 15) os << " "; // Separate elements with spaces
    }
    return os;
}

__attribute__((always_inline)) static inline
uint64_t unescaped_ascii_asimd_helper_16(uint8x16_t input) {
  uint64x2_t res = vreinterpretq_u64_u8(
    vorrq_u8(
      vorrq_u8(
        vcgtq_u8(vdupq_n_u8(0x20), input),
        vcgtq_u8(input, vdupq_n_u8(0x7e))),
      vorrq_u8(
        vceqq_u8(input, vdupq_n_u8(0x22)),
        vceqq_u8(input, vdupq_n_u8(0x5c)))));
  return vgetq_lane_u64(res, 0) | vgetq_lane_u64(res, 1);
};

__attribute__((always_inline)) static inline
uint64_t unescaped_ascii_asimd_helper_16(const char* ptr) {
  return unescaped_ascii_asimd_helper_16(
           vld1q_u8(reinterpret_cast<const uint8_t*>(ptr)));
}

__attribute__((always_inline)) static inline
bool unescaped_ascii_asimd(const char* str, const char* end) {
  unsigned int len = end - str;
  const char *last = end - 0x10; // Computing this before switch() is slightly
                                 // faster.
  if (likely(len <= 0xff)) {
    // Hard code cases with small sizes, for a significant performance
    // improvement.
    switch((unsigned char)(len)) {
    case 0xff: case 0xfe: case 0xfd: case 0xfc: case 0xfb: case 0xfa: case 0xf9:
    case 0xf8: case 0xf7: case 0xf6: case 0xf5: case 0xf4: case 0xf3: case 0xf2:
    case 0xf1:
      return (unescaped_ascii_asimd_helper_16(last) |
              unescaped_ascii_asimd_helper_16(str + 0xe0) |
              unescaped_ascii_asimd_helper_16(str + 0xd0) |
              unescaped_ascii_asimd_helper_16(str + 0xc0) |
              unescaped_ascii_asimd_helper_16(str + 0xb0) |
              unescaped_ascii_asimd_helper_16(str + 0xa0) |
              unescaped_ascii_asimd_helper_16(str + 0x90) |
              unescaped_ascii_asimd_helper_16(str + 0x80) |
              unescaped_ascii_asimd_helper_16(str + 0x70) |
              unescaped_ascii_asimd_helper_16(str + 0x60) |
              unescaped_ascii_asimd_helper_16(str + 0x50) |
              unescaped_ascii_asimd_helper_16(str + 0x40) |
              unescaped_ascii_asimd_helper_16(str + 0x30) |
              unescaped_ascii_asimd_helper_16(str + 0x20) |
              unescaped_ascii_asimd_helper_16(str + 0x10) |
              unescaped_ascii_asimd_helper_16(str)) == 0;
    case 0xf0: case 0xef: case 0xee: case 0xed: case 0xec: case 0xeb: case 0xea:
    case 0xe9: case 0xe8: case 0xe7: case 0xe6: case 0xe5: case 0xe4: case 0xe3:
    case 0xe2: case 0xe1:
      return (unescaped_ascii_asimd_helper_16(last) |
              unescaped_ascii_asimd_helper_16(str + 0xd0) |
              unescaped_ascii_asimd_helper_16(str + 0xc0) |
              unescaped_ascii_asimd_helper_16(str + 0xb0) |
              unescaped_ascii_asimd_helper_16(str + 0xa0) |
              unescaped_ascii_asimd_helper_16(str + 0x90) |
              unescaped_ascii_asimd_helper_16(str + 0x80) |
              unescaped_ascii_asimd_helper_16(str + 0x70) |
              unescaped_ascii_asimd_helper_16(str + 0x60) |
              unescaped_ascii_asimd_helper_16(str + 0x50) |
              unescaped_ascii_asimd_helper_16(str + 0x40) |
              unescaped_ascii_asimd_helper_16(str + 0x30) |
              unescaped_ascii_asimd_helper_16(str + 0x20) |
              unescaped_ascii_asimd_helper_16(str + 0x10) |
              unescaped_ascii_asimd_helper_16(str)) == 0;
    case 0xe0: case 0xdf: case 0xde: case 0xdd: case 0xdc: case 0xdb: case 0xda:
    case 0xd9: case 0xd8: case 0xd7: case 0xd6: case 0xd5: case 0xd4: case 0xd3:
    case 0xd2: case 0xd1:
      return (unescaped_ascii_asimd_helper_16(last) |
              unescaped_ascii_asimd_helper_16(str + 0xc0) |
              unescaped_ascii_asimd_helper_16(str + 0xb0) |
              unescaped_ascii_asimd_helper_16(str + 0xa0) |
              unescaped_ascii_asimd_helper_16(str + 0x90) |
              unescaped_ascii_asimd_helper_16(str + 0x80) |
              unescaped_ascii_asimd_helper_16(str + 0x70) |
              unescaped_ascii_asimd_helper_16(str + 0x60) |
              unescaped_ascii_asimd_helper_16(str + 0x50) |
              unescaped_ascii_asimd_helper_16(str + 0x40) |
              unescaped_ascii_asimd_helper_16(str + 0x30) |
              unescaped_ascii_asimd_helper_16(str + 0x20) |
              unescaped_ascii_asimd_helper_16(str + 0x10) |
              unescaped_ascii_asimd_helper_16(str)) == 0;
    case 0xd0: case 0xcf: case 0xce: case 0xcd: case 0xcc: case 0xcb: case 0xca:
    case 0xc9: case 0xc8: case 0xc7: case 0xc6: case 0xc5: case 0xc4: case 0xc3:
    case 0xc2: case 0xc1:
      return (unescaped_ascii_asimd_helper_16(last) |
              unescaped_ascii_asimd_helper_16(str + 0xb0) |
              unescaped_ascii_asimd_helper_16(str + 0xa0) |
              unescaped_ascii_asimd_helper_16(str + 0x90) |
              unescaped_ascii_asimd_helper_16(str + 0x80) |
              unescaped_ascii_asimd_helper_16(str + 0x70) |
              unescaped_ascii_asimd_helper_16(str + 0x60) |
              unescaped_ascii_asimd_helper_16(str + 0x50) |
              unescaped_ascii_asimd_helper_16(str + 0x40) |
              unescaped_ascii_asimd_helper_16(str + 0x30) |
              unescaped_ascii_asimd_helper_16(str + 0x20) |
              unescaped_ascii_asimd_helper_16(str + 0x10) |
              unescaped_ascii_asimd_helper_16(str)) == 0;
    case 0xc0: case 0xbf: case 0xbe: case 0xbd: case 0xbc: case 0xbb: case 0xba:
    case 0xb9: case 0xb8: case 0xb7: case 0xb6: case 0xb5: case 0xb4: case 0xb3:
    case 0xb2: case 0xb1:
      return (unescaped_ascii_asimd_helper_16(last) |
              unescaped_ascii_asimd_helper_16(str + 0xa0) |
              unescaped_ascii_asimd_helper_16(str + 0x90) |
              unescaped_ascii_asimd_helper_16(str + 0x80) |
              unescaped_ascii_asimd_helper_16(str + 0x70) |
              unescaped_ascii_asimd_helper_16(str + 0x60) |
              unescaped_ascii_asimd_helper_16(str + 0x50) |
              unescaped_ascii_asimd_helper_16(str + 0x40) |
              unescaped_ascii_asimd_helper_16(str + 0x30) |
              unescaped_ascii_asimd_helper_16(str + 0x20) |
              unescaped_ascii_asimd_helper_16(str + 0x10) |
              unescaped_ascii_asimd_helper_16(str)) == 0;
    case 0xb0: case 0xaf: case 0xae: case 0xad: case 0xac: case 0xab: case 0xaa:
    case 0xa9: case 0xa8: case 0xa7: case 0xa6: case 0xa5: case 0xa4: case 0xa3:
    case 0xa2: case 0xa1:
      return (unescaped_ascii_asimd_helper_16(last) |
              unescaped_ascii_asimd_helper_16(str + 0x90) |
              unescaped_ascii_asimd_helper_16(str + 0x80) |
              unescaped_ascii_asimd_helper_16(str + 0x70) |
              unescaped_ascii_asimd_helper_16(str + 0x60) |
              unescaped_ascii_asimd_helper_16(str + 0x50) |
              unescaped_ascii_asimd_helper_16(str + 0x40) |
              unescaped_ascii_asimd_helper_16(str + 0x30) |
              unescaped_ascii_asimd_helper_16(str + 0x20) |
              unescaped_ascii_asimd_helper_16(str + 0x10) |
              unescaped_ascii_asimd_helper_16(str)) == 0;
    case 0xa0: case 0x9f: case 0x9e: case 0x9d: case 0x9c: case 0x9b: case 0x9a:
    case 0x99: case 0x98: case 0x97: case 0x96: case 0x95: case 0x94: case 0x93:
    case 0x92: case 0x91:
      return (unescaped_ascii_asimd_helper_16(last) |
              unescaped_ascii_asimd_helper_16(str + 0x80) |
              unescaped_ascii_asimd_helper_16(str + 0x70) |
              unescaped_ascii_asimd_helper_16(str + 0x60) |
              unescaped_ascii_asimd_helper_16(str + 0x50) |
              unescaped_ascii_asimd_helper_16(str + 0x40) |
              unescaped_ascii_asimd_helper_16(str + 0x30) |
              unescaped_ascii_asimd_helper_16(str + 0x20) |
              unescaped_ascii_asimd_helper_16(str + 0x10) |
              unescaped_ascii_asimd_helper_16(str)) == 0;
    case 0x90: case 0x8f: case 0x8e: case 0x8d: case 0x8c: case 0x8b: case 0x8a:
    case 0x89: case 0x88: case 0x87: case 0x86: case 0x85: case 0x84: case 0x83:
    case 0x82: case 0x81:
      return (unescaped_ascii_asimd_helper_16(last) |
              unescaped_ascii_asimd_helper_16(str + 0x70) |
              unescaped_ascii_asimd_helper_16(str + 0x60) |
              unescaped_ascii_asimd_helper_16(str + 0x50) |
              unescaped_ascii_asimd_helper_16(str + 0x40) |
              unescaped_ascii_asimd_helper_16(str + 0x30) |
              unescaped_ascii_asimd_helper_16(str + 0x20) |
              unescaped_ascii_asimd_helper_16(str + 0x10) |
              unescaped_ascii_asimd_helper_16(str)) == 0;
    case 0x80: case 0x7f: case 0x7e: case 0x7d: case 0x7c: case 0x7b: case 0x7a:
    case 0x79: case 0x78: case 0x77: case 0x76: case 0x75: case 0x74: case 0x73:
    case 0x72: case 0x71:
      return (unescaped_ascii_asimd_helper_16(last) |
              unescaped_ascii_asimd_helper_16(str + 0x60) |
              unescaped_ascii_asimd_helper_16(str + 0x50) |
              unescaped_ascii_asimd_helper_16(str + 0x40) |
              unescaped_ascii_asimd_helper_16(str + 0x30) |
              unescaped_ascii_asimd_helper_16(str + 0x20) |
              unescaped_ascii_asimd_helper_16(str + 0x10) |
              unescaped_ascii_asimd_helper_16(str)) == 0;
    case 0x70: case 0x6f: case 0x6e: case 0x6d: case 0x6c: case 0x6b: case 0x6a:
    case 0x69: case 0x68: case 0x67: case 0x66: case 0x65: case 0x64: case 0x63:
    case 0x62: case 0x61:
      return (unescaped_ascii_asimd_helper_16(last) |
              unescaped_ascii_asimd_helper_16(str + 0x50) |
              unescaped_ascii_asimd_helper_16(str + 0x40) |
              unescaped_ascii_asimd_helper_16(str + 0x30) |
              unescaped_ascii_asimd_helper_16(str + 0x20) |
              unescaped_ascii_asimd_helper_16(str + 0x10) |
              unescaped_ascii_asimd_helper_16(str)) == 0;
    case 0x60: case 0x5f: case 0x5e: case 0x5d: case 0x5c: case 0x5b: case 0x5a:
    case 0x59: case 0x58: case 0x57: case 0x56: case 0x55: case 0x54: case 0x53:
    case 0x52: case 0x51:
      return (unescaped_ascii_asimd_helper_16(last) |
              unescaped_ascii_asimd_helper_16(str + 0x40) |
              unescaped_ascii_asimd_helper_16(str + 0x30) |
              unescaped_ascii_asimd_helper_16(str + 0x20) |
              unescaped_ascii_asimd_helper_16(str + 0x10) |
              unescaped_ascii_asimd_helper_16(str)) == 0;
    case 0x50: case 0x4f: case 0x4e: case 0x4d: case 0x4c: case 0x4b: case 0x4a:
    case 0x49: case 0x48: case 0x47: case 0x46: case 0x45: case 0x44: case 0x43:
    case 0x42: case 0x41:
      return (unescaped_ascii_asimd_helper_16(last) |
              unescaped_ascii_asimd_helper_16(str + 0x30) |
              unescaped_ascii_asimd_helper_16(str + 0x20) |
              unescaped_ascii_asimd_helper_16(str + 0x10) |
              unescaped_ascii_asimd_helper_16(str)) == 0;
    case 0x40: case 0x3f: case 0x3e: case 0x3d: case 0x3c: case 0x3b: case 0x3a:
    case 0x39: case 0x38: case 0x37: case 0x36: case 0x35: case 0x34: case 0x33:
    case 0x32: case 0x31:
      return (unescaped_ascii_asimd_helper_16(last) |
              unescaped_ascii_asimd_helper_16(str + 0x20) |
              unescaped_ascii_asimd_helper_16(str + 0x10) |
              unescaped_ascii_asimd_helper_16(str)) == 0;
    case 0x30: case 0x2f: case 0x2e: case 0x2d: case 0x2c: case 0x2b: case 0x2a:
    case 0x29: case 0x28: case 0x27: case 0x26: case 0x25: case 0x24: case 0x23:
    case 0x22: case 0x21:
      return (unescaped_ascii_asimd_helper_16(last) |
              unescaped_ascii_asimd_helper_16(str + 0x10) |
              unescaped_ascii_asimd_helper_16(str)) == 0;
    case 0x20: case 0x1f: case 0x1e: case 0x1d: case 0x1c: case 0x1b: case 0x1a:
    case 0x19: case 0x18: case 0x17: case 0x16: case 0x15: case 0x14: case 0x13:
    case 0x12: case 0x11:
      return (unescaped_ascii_asimd_helper_16(last) |
              unescaped_ascii_asimd_helper_16(str)) == 0;
    case 0x10:
      return unescaped_ascii_asimd_helper_16(str) == 0;
    case 0xf: case 0xe: case 0xd: case 0xc: case 0xb: case 0xa: case 9:
      return unescaped_ascii_asimd_helper_16(
               vcombine_u8(
                 *reinterpret_cast<const uint8x8_t*>(end - 8),
                 *reinterpret_cast<const uint8x8_t*>(str))) == 0;
    case 8:
      return unescaped_ascii_asimd_helper_16(
               vcombine_u8(
                 vcreate_u8(uint64_t(0x2020202020202020L)),
                 vld1_u8(reinterpret_cast<const uint8_t*>(str)))) == 0;
    case 7: case 6: case 5:
      return unescaped_ascii_asimd_helper_16(
               vcombine_u8(
                 vcreate_u8(uint64_t(0x2020202020202020L)),
                 vcreate_u8((uint64_t(*reinterpret_cast<const uint32_t*>(end - 4)) << 32) |
                            *reinterpret_cast<const uint32_t*>(str)))) == 0;
    case 4:
      return unescaped_ascii_asimd_helper_16(
               vcombine_u8(
                 vcreate_u8(uint64_t(0x2020202020202020L)),
                 vcreate_u8(uint64_t(0x2020202000000000L) |
                            *reinterpret_cast<const uint32_t*>(str)))) == 0;
    case 3:
      if (unlikely(char_is_not_unescaped_ascii(str[2]))) return false;
    case 2:
      if (unlikely(char_is_not_unescaped_ascii(str[1]))) return false;
    case 1:
      if (unlikely(char_is_not_unescaped_ascii(str[0]))) return false;
    case 0:
      return true;
    default:
      abort();
    }
  }

  /*
   * One might think that loading most pointers aligned to 16-byte boundaries as
   * in the code below would be faster. In a simple test with constant input
   * alignment and size, it indeed is faster by a factor 2. However, when the
   * input alignment and size varies randomly, as is expected for VARCHAR data
   * types, this is instead 20% slower. When input alignment varies with
   * constant input size, as is expected for CHAR data types, this is slower by
   * a factor 2 or more. I cannot explain why.
  const uint8x16_t* section1 = reinterpret_cast<const uint8x16_t*>(
    (reinterpret_cast<uintptr_t>(str) + 16) & -16UL);
  const uint8x16_t* section2 = reinterpret_cast<const uint8x16_t*>(
    (reinterpret_cast<uintptr_t>(end) - 1) & -16UL);
  uintptr_t b = reinterpret_cast<uintptr_t>(str);
  uintptr_t e = reinterpret_cast<uintptr_t>(end);
  uintptr_t s1 = reinterpret_cast<uintptr_t>(section1);
  uintptr_t s2 = reinterpret_cast<uintptr_t>(section2);
  assert((s1 & 0xf) == 0 && (s2 & 0xf) == 0);
  assert(s1 < s2);
  assert((b + 1) <= s1 && s1 <= (b + 16));
  assert((e - 16) <= s2 && s2 <= (e - 1));
  // Check initial unaligned segment, possibly overlapping an aligned segment.
  if (unlikely(unescaped_ascii_asimd_helper_16(str))) {
    return false;
  }
  // Check aligned segments
  for (const uint8x16_t* aptr = section1; likely(aptr < section2); aptr++) {
    // todo alignment specifier
    if (unlikely(unescaped_ascii_asimd_helper_16(*aptr))) {
      return false;
    }
  }
  // Check final unaligned segment, possibly overlapping an aligned segment.
  if (unlikely(unescaped_ascii_asimd_helper_16(last))) {
    return false;
  }
  */

  /* Disregard memory alignment completely. For some reason this is faster than
   * making the effort to load mostly from aligned pointers, at least when input
   * alignment varies.
   */
  for (const char* aptr = str; aptr < last; aptr += 16) {
    if (unlikely(unescaped_ascii_asimd_helper_16(aptr))) {
      return false;
    }
  }
  if (unlikely(unescaped_ascii_asimd_helper_16(last))) {
    return false;
  }

  return true;
}

#endif

__attribute__((always_inline)) static inline
bool unescaped_ascii_simd(const char *str, const char *end)
{
  using T = decltype(unescaped_ascii_simd);
  static T* pointer = nullptr;
  if (unlikely(pointer == nullptr))
  {
#ifdef ua_x86_64
    if (__builtin_cpu_supports("sse2") &&
        __builtin_cpu_supports("avx2"))
      pointer = &unescaped_ascii_avx2;
#endif
#ifdef ua_arm
    if (getauxval(AT_HWCAP) & HWCAP_ASIMD)
      pointer = &unescaped_ascii_asimd;
#endif
    else
      pointer = &unescaped_ascii_fallback;
  }
  return pointer(str, end);
}

void test_correctness(bool (*testfun)(const char*, const char*), std::string fun_name) {
  uchar chars_to_test[] = { 0x00, 0x01, 0x1f, 0x20, 0x21, 0x22, 0x23, 0x5b,
                            0x5c, 0x5d, 0x7e, 0x7f, 0x80, 0xff};
  std::cerr << "Starting correctness test for function " << fun_name << std::endl;
  for (int len = 0; len < 300; len++) {
    int buflen = len + 15 + 2;
    uchar* buf = (uchar*)malloc(buflen);
    for (int ch1_idx = 0; ch1_idx < sizeof(chars_to_test); ch1_idx++) {
      uchar ch1 = chars_to_test[ch1_idx];
      for (int i = 0; i < buflen; i++) {
        buf[i] = ch1;
      }
      for (int ch2_idx = 0; ch2_idx < sizeof(chars_to_test); ch2_idx++) {
        uchar ch2 = chars_to_test[ch2_idx];
        for (int ch2_pos = 0; ch2_pos < buflen; ch2_pos++) {
          buf[ch2_pos] = ch2;
          for (int align = 0; align < 16; align++) {
            uchar* start = buf + 1 + align;
            uchar* end = start + len;
            bool result = testfun((char*)start, (char*)end);
            bool correct_result = unescaped_ascii_correct((char*)start, (char*)end);
            if (result != correct_result) {
              std::cerr << "Test failed: test " << fun_name
                        << ", actual " << result
                        << ", expected " << correct_result
                        << ", size " << len
                        << ", data:";
              for (int i = 0; i < len; i++) {
                std::cerr << " "
                          << ("0123456789abcdef"[start[i] >> 4])
                          << ("0123456789abcdef"[start[i] & 0xf]);
              }
              std::cerr << std::endl;
              abort();
            }
          }
          buf[ch2_pos] = ch1;
        }
      }
    }
  }
  std::cerr << "Function " << fun_name << " passed correctness test" << std::endl;
}

#include <algorithm>
#include <random>
void test_performance(bool (*testfun)(const char*, const char*),
                      std::string fun_name,
                      bool fixlen = false,
                      int len = 0) {
  const int max_size = 300;
  constexpr int data_size = max_size + 32;
  char data[data_size];
  for (int i=0; i < data_size; i++) {
    data[i] = 'A';
  }
  constexpr int nof_configs = 1000;
  int lengths[nof_configs];
  int alignments[nof_configs];
  for (int i=0; i < nof_configs; i++) {
    lengths[i] = fixlen ? len : i % max_size;
    alignments[i] = i % 32;
  }
  // Shuffle
  std::mt19937 g(0);
  std::shuffle(data, data + data_size, g);
  std::shuffle(lengths, lengths + nof_configs, g);
  std::shuffle(alignments, alignments + nof_configs, g);
  const int iterations = 1000;
  int total_size = 0;
  for(int i = 0; i < nof_configs; i++) {
    total_size += lengths[i];
  }
  auto start = std::chrono::high_resolution_clock::now();
  for(int i = 0; i < iterations; i++) {
    for(int config = 0; config < nof_configs; config++) {
      char* dstart = data + alignments[config];
      char* dend = dstart + lengths[config];
      testfun(dstart, dend);
    }
  }
  auto end = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> elapsed = end - start;
  double proc_speed_GiB = double(total_size) * iterations / elapsed.count() / 1024 / 1024 / 1024;
  std::cout << "Performance test for " << fun_name << " with ";
  if (fixlen) std::cout << "length " << len;
  else std::cout << "varying lengths";
  std::cout << ": " << (iterations * nof_configs / elapsed.count()) << " calls/s, "
            << proc_speed_GiB << " GiB/s" << endl;
}

int
main() {
  // Performance test
  test_performance(&unescaped_ascii_fallback, "unescaped_ascii_fallback");
  test_performance(&unescaped_ascii_simd, "unescaped_ascii_simd");
  test_performance(&unescaped_ascii_simd, "unescaped_ascii_simd", true, 7);
  test_performance(&unescaped_ascii_simd, "unescaped_ascii_simd", true, 15);
  test_performance(&unescaped_ascii_simd, "unescaped_ascii_simd", true, 31);
  test_performance(&unescaped_ascii_simd, "unescaped_ascii_simd", true, 70);
  test_performance(&unescaped_ascii_simd, "unescaped_ascii_simd", true, 128);
  test_performance(&unescaped_ascii_simd, "unescaped_ascii_simd", true, 255);
  test_performance(&unescaped_ascii_simd, "unescaped_ascii_simd", true, 256);
  test_performance(&unescaped_ascii_simd, "unescaped_ascii_simd", true, 16384);
  test_performance(&unescaped_ascii_simd, "unescaped_ascii_simd", true, 1048576);
  // Correctness test
  test_correctness(&unescaped_ascii_simd, "unescaped_ascii_simd");
  return 0;
}
