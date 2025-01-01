#include <iostream>
#include <cstring>
#include <cassert>
#include <chrono>

#include <emmintrin.h> // SSE2 intrinsics
#include <immintrin.h> // AVX2 intrinsics

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

__attribute__((always_inline)) static inline
bool unescaped_ascii_simd(const char *str, const char *end)
{
  using T = decltype(unescaped_ascii_simd);
  static T* pointer = nullptr;
  if (unlikely(pointer == nullptr))
  {
    if (__builtin_cpu_supports("sse2") &&
        __builtin_cpu_supports("avx2"))
        pointer = &unescaped_ascii_avx2;
    else
        pointer = &unescaped_ascii_fallback;
  }
  return pointer(str, end);
}

// Array of implementations and their names
struct {
  bool (*fun)(const char*, const char*);
  const char* name;
} funs[] = {
  {&unescaped_ascii_fallback, "fallback"},
  {&unescaped_ascii_simd, "simd"},
};

void test(const char* alphabet,
          char midch,
          int size,
          bool (*testfun)(const char*, const char*),
          std::string testname,
          bool test_perf) {
  char* data = (char*)malloc(size);
  assert(data);
  int ablen = strlen((const char*)alphabet);
  if (ablen == 0) ablen = 1;
  {
    int i = 0;
    while((i + ablen) < size) {
      memcpy(data + i, alphabet, ablen);
      i += ablen;
    }
    while(i < size) {
      int idx = i % ablen;
      data[i] = alphabet[idx];
      i++;
    }
  }
  data[size / 2] = midch;
  char* data_end = data + size;
  if (test_perf) {
    const int iterations = 100000000 / size;
    auto start = std::chrono::high_resolution_clock::now();
    for(int i = 0; i < iterations; i++) {
      testfun(data, data_end);
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    double proc_speed_GiB = double(size) * iterations / elapsed.count() / 1024 / 1024 / 1024;
    std::cout << "Test " << testname << " took "
              << elapsed.count() << " seconds, "
              << iterations / elapsed.count() << " calls/s, "
              << proc_speed_GiB << " GiB/s" << endl;
  } else {
    bool result = testfun(data, data_end);
    bool correct_result = unescaped_ascii_correct(data, data_end);
    if (result != correct_result) {
      std::cerr << "Test failed: test " << testname
                << ", actual " << result
                << ", expected " << correct_result
                << ", size " << size
                << ", data:";
      for (int i = 0; i < size; i++) {
        std::cerr << " "
                  << ("0123456789abcdef"[data[i] >> 4])
                  << ("0123456789abcdef"[data[i] & 0xf]);
      }
      std::cerr << std::endl;
      abort();
    }
  }
  free(data);
}

#include <algorithm>
#include <random>
void test_varied(bool (*testfun)(const char*, const char*),
                 std::string testname,
                 bool test_perf) {
  const int max_size = 300;
  const int nof_nonascii = 0;
  constexpr int data_size = max_size + 32;
  char data[data_size];
  for (int i=0; i < data_size; i++) {
    data[i] = i < nof_nonascii ? '\x80' : 'A';
  }
  constexpr int nof_configs = 1000;
  int lengths[nof_configs];
  int alignments[nof_configs];
  for (int i=0; i < nof_configs; i++) {
    lengths[i] = i % max_size;
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
  if (test_perf) {
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
    std::cout << "Test_varied " << testname << " took "
              << elapsed.count() << " seconds, "
              << iterations * nof_configs / elapsed.count() << " calls/s, "
              << proc_speed_GiB << " GiB/s" << endl;
  } else {
    for(int config = 0; config < nof_configs; config++) {
      char* dstart = data + alignments[config];
      char* dend = dstart + lengths[config];
      bool result = testfun(dstart, dend);
      bool correct_result = unescaped_ascii_correct(dstart, dend);
      if (result != correct_result) {
        std::cerr << "Test failed: test_varied " << testname
                  << ", actual " << result
                  << ", expected " << correct_result
                  << ", size " << lengths[config]
                  << ", data:";
        for (int i = 0; i < lengths[config]; i++) {
          std::cerr << " "
                    << ("0123456789abcdef"[dstart[i] >> 4])
                    << ("0123456789abcdef"[dstart[i] & 0xf]);
        }
        std::cerr << std::endl;
        abort();
      }
    }
  }
}

int
main() {
  // Correctness test
  for (int ch = 0; ch < 256; ch++) {
    for (int midch = 0; midch < 256; midch++) {
      char alphabet[2] = {char(ch), 0};
      for (unsigned int f = 0; f < sizeof(funs) / sizeof(funs[0]); f++) {
        auto fun = funs[f].fun;
        const char* fun_name = funs[f].name;
        for (int len = 0; len <= 100; len++) {
          //if (len == 8) std::cerr << "Testing " << fun_name << " ch " << ch << " midch " << midch << " len " << len << std::endl;
          test(alphabet, midch, len, fun, fun_name, false);
        }
      }
    }
  }
  // Performance test
  const char* alphabet = "ABCDEF !# GHIJKLMNO jklmnopqrstuvwxyz.";
  char midch = 0x41;
  for(int l = -1; l < 270; l++) {
    int len = l;
    if (l==-1) len = 53248;
    if (l==0) len = 1048576;
    for (unsigned int f = 0; f < sizeof(funs) / sizeof(funs[0]); f++) {
      auto fun = funs[f].fun;
      const char* fun_name = funs[f].name;
      test(alphabet, midch, len, fun, (std::string(fun_name) + " " + std::to_string(len)), true);
    }
    std::cout << "========================================" << std::endl;
  }
  // Varied test
  for (unsigned int f = 0; f < sizeof(funs) / sizeof(funs[0]); f++) {
    auto fun = funs[f].fun;
    const char* fun_name = funs[f].name;
    test_varied(fun, std::string(fun_name), false);
    test_varied(fun, std::string(fun_name), true);
  }
  return 0;
}
