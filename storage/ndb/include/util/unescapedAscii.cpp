#include <iostream>
#include <cstring>
#include <cassert>
#include <chrono>

using std::cout;
using std::endl;

typedef unsigned char uchar;
constexpr bool likely(bool expr) { return __builtin_expect(expr, true); }
constexpr bool unlikely(bool expr) { return __builtin_expect(expr, false); }
typedef __SIZE_TYPE__ UintPtr;

//#define DBG(...) do { cout << "DBG: " __FILE__ ":" << __LINE__ << " " __VA_ARGS__ << endl; } while(0)
#define DBG(...) do { } while(0)

std::ostream&
operator<<( std::ostream& dest, __int128_t value )
{
    std::ostream::sentry s( dest );
    if ( s ) {
        __uint128_t tmp = value < 0 ? -value : value;
        char buffer[ 128 ];
        char* d = std::end( buffer );
        do
        {
            -- d;
            *d = "0123456789abcdef"[ tmp % 16 ];
            tmp /= 16;
        } while ( tmp != 0 );
        if ( value < 0 ) {
            -- d;
            *d = '-';
        }
        int len = std::end( buffer ) - d;
        if ( dest.rdbuf()->sputn( d, len ) != len ) {
            dest.setstate( std::ios_base::badbit );
        }
    }
    return dest;
}

// Return true if ls contains only bytes in ranges 0x20-0x21, 0x23-5b and
// 0x5d-0x7e.

__attribute__((always_inline)) static inline
bool unescaped_ascii_fallback(const char* str, const char* end) {
  while (str < end) {
    uchar c = *((const uchar*)str);
    DBG("c:" << c);
    if (c < 0x20 || c == 0x22 || c == 0x5c || 0x7e < c) {
      return false;
    }
    str++;
  }
  return true;
}

bool unescaped_ascii_correct(const char* str, const char* end) {
  return unescaped_ascii_fallback(str, end);
}

#include <immintrin.h>
//__attribute__((__target__ ("avx2")))
[[gnu::target("avx2")]]
bool unescaped_ascii_avx2(const char *str, const char *end) {
  if (likely((end - str) <= 32)) {
    // Require at least 32 bytes of readable memory
    const __m256i input = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(str));
    const int lt_20 = _mm256_movemask_epi8(_mm256_cmpgt_epi8(_mm256_set1_epi8(0x20), input));
    const int gt_7e = _mm256_movemask_epi8(_mm256_cmpgt_epi8(input, _mm256_set1_epi8(0x7e)));
    const int eq_22 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(input, _mm256_set1_epi8(0x22)));
    const int eq_5c = _mm256_movemask_epi8(_mm256_cmpeq_epi8(input, _mm256_set1_epi8(0x5c)));
    const int nonascii = lt_20 | gt_7e | eq_22 | eq_5c;
    const int mask = (1 << (end - str)) - 1;
    return ((nonascii & mask) == 0);
  }
  const char* section1 = reinterpret_cast<const char*>
    ((reinterpret_cast<UintPtr>(str) + 32) & -32UL);
  const char* section2 = reinterpret_cast<const char*>
    ((reinterpret_cast<UintPtr>(end) - 1) & -32UL);
  assert((reinterpret_cast<UintPtr>(section1) & 0xf) == 0);
  assert((reinterpret_cast<UintPtr>(section2) & 0xf) == 0);
  assert(str <= section1);
  assert(section1 <= (str + 32));
  assert(section2 < end);
  assert((end - 32) <= section2);
  {
    const __m256i input = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(str));
    const int lt_20 = _mm256_movemask_epi8(_mm256_cmpgt_epi8(_mm256_set1_epi8(0x20), input));
    const int gt_7e = _mm256_movemask_epi8(_mm256_cmpgt_epi8(input, _mm256_set1_epi8(0x7e)));
    const int eq_22 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(input, _mm256_set1_epi8(0x22)));
    const int eq_5c = _mm256_movemask_epi8(_mm256_cmpeq_epi8(input, _mm256_set1_epi8(0x5c)));
    if (unlikely(lt_20 | gt_7e | eq_22 | eq_5c)) {
      return false;
    }
  }
  for (const __m256* aptr = reinterpret_cast<const __m256*>(section1);
       aptr < reinterpret_cast<const __m256*>(section2); aptr++) {
    const __m256i input = _mm256_load_si256(reinterpret_cast<const __m256i*>(aptr));
    const int lt_20 = _mm256_movemask_epi8(_mm256_cmpgt_epi8(_mm256_set1_epi8(0x20), input));
    const int gt_7e = _mm256_movemask_epi8(_mm256_cmpgt_epi8(input, _mm256_set1_epi8(0x7e)));
    const int eq_22 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(input, _mm256_set1_epi8(0x22)));
    const int eq_5c = _mm256_movemask_epi8(_mm256_cmpeq_epi8(input, _mm256_set1_epi8(0x5c)));
    if (unlikely(lt_20 | gt_7e | eq_22 | eq_5c)) {
      return false;
    }
  }
  {
    const __m256i input = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(end - 32));
    const int lt_20 = _mm256_movemask_epi8(_mm256_cmpgt_epi8(_mm256_set1_epi8(0x20), input));
    const int gt_7e = _mm256_movemask_epi8(_mm256_cmpgt_epi8(input, _mm256_set1_epi8(0x7e)));
    const int eq_22 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(input, _mm256_set1_epi8(0x22)));
    const int eq_5c = _mm256_movemask_epi8(_mm256_cmpeq_epi8(input, _mm256_set1_epi8(0x5c)));
    if (unlikely(lt_20 | gt_7e | eq_22 | eq_5c)) {
      return false;
    }
  }
  return true;
}

bool unescaped_ascii_simd(const char *str, const char *end)
{
  using T = decltype(unescaped_ascii_simd);
  static T* pointer = nullptr;
  if (unlikely(pointer == nullptr))
  {
    if (__builtin_cpu_supports("avx2"))
        pointer = &unescaped_ascii_avx2;
    else
        pointer = &unescaped_ascii_fallback;
  }
  return pointer(str, end);
}

// array of implementations ane their names
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
          int chunk,
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
    const char* e = data_end - chunk;
    auto start = std::chrono::high_resolution_clock::now();
    for(char* start = data; start <= e; start += chunk) {
      testfun(start, start + chunk);
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    double proc_speed_GiB = double(size) / elapsed.count() / 1024 / 1024 / 1024;
    std::cout << "Test " << testname << " took "
              << elapsed.count() << " seconds, "
              << proc_speed_GiB << " GiB/s" << endl;
  } else {
    const char* e = data_end - chunk;
    for(char* start = data; start < e; start += chunk) {
      bool result = testfun(start, start + chunk);
      bool correct_result = unescaped_ascii_correct(start, start + chunk);
      if (result != correct_result) {
        std::cerr << "Test failed: " << testname << " " << result << " " << correct_result << endl;
      }
    }
  }
  // Leak memory in `char* data` on purpose so the cache doesn't taint the
  // results.
}

int
main() {
  for (unsigned int f = 0; f < sizeof(funs) / sizeof(funs[0]); f++) {
    auto fun = funs[f].fun;
    const char* fun_name = funs[f].name;
    // Correctness test
    for (int ch = 0; ch < 256; ch++) {
      for (int midch = 0; midch < 256; midch++) {
        char alphabet[2] = {char(ch), 0};
        test(alphabet, midch, 1, 1, fun, fun_name, false);
        test(alphabet, midch, 100, 100, fun, fun_name, false);
      }
    }
    // Performance test
    const char* alphabet = "ABCDEF !# GHIJKLMNO jklmnopqrstuvwxyz.";
    char midch = 0x41;
    test(alphabet, midch, 104857600,    13, fun, (std::string(fun_name) + " 13B"), true);
    test(alphabet, midch, 104857600,    31, fun, (std::string(fun_name) + " 31B"), true);
    test(alphabet, midch, 104857600,    32, fun, (std::string(fun_name) + " 32B"), true);
    test(alphabet, midch, 104857600,    33, fun, (std::string(fun_name) + " 33B"), true);
    test(alphabet, midch, 104857600,    63, fun, (std::string(fun_name) + " 63B"), true);
    test(alphabet, midch, 104857600,    64, fun, (std::string(fun_name) + " 64B"), true);
    test(alphabet, midch, 104857600,    65, fun, (std::string(fun_name) + " 65B"), true);
    test(alphabet, midch, 104857600,    95, fun, (std::string(fun_name) + " 95B"), true);
    test(alphabet, midch, 104857600, 53248, fun, (std::string(fun_name) + " 52K"), true);
  }
  DBG();
  return 0;
}
