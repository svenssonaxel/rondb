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

bool unescaped_ascii_correct(const char* str, const char* end) {
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

//__attribute__((always_inline)) static inline
bool unescaped_ascii_fast1(const char* str, const char* end) {
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

bool unescaped_ascii_fast2(const char* str, const char* end) {
  if (likely((end - str) < 32)) {
    while (str < end) {
      uchar c = *str;
      DBG("c:" << c);
      if (c < 0x20 || c == 0x22 || c == 0x5c || 0x7e < c) {
        return false;
      }
      str++;
    }
    return true;
  }
  const char* section1 = reinterpret_cast<const char*>
    ((reinterpret_cast<UintPtr>(str) + 15) & -16UL);
  const char* section2 = reinterpret_cast<const char*>
    (reinterpret_cast<UintPtr>(end) & -16UL);
  assert((reinterpret_cast<UintPtr>(section1) & 0xf) == 0);
  assert((reinterpret_cast<UintPtr>(section2) & 0xf) == 0);
  assert(str <= section1);
  assert(section1 < (str + 16));
  assert(section1 <= section2);
  assert(section2 <= end);
  assert((end - 16) < section2);
  DBG("str:" << ((void*)str) << ", section1:" << ((void*)section1) << ", section2:" << ((void*)section2) << ", end:" << ((void*)end));
  for (const char* chptr = str; chptr < section1; chptr++) {
    uchar c = *str;
    DBG("c:" << c);
    if (unlikely(c < 0x20 || c == 0x22 || c == 0x5c || 0x7e < c)) {
      return false;
    }
  }
  typedef __int128_t B16;
  for (const B16* aptr = reinterpret_cast<const B16*>(section1);
       aptr < reinterpret_cast<const B16*>(section2); aptr++) {
    B16 ch = *aptr;
    DBG("aptr:" << aptr << ", ch:" << ch);
    constexpr B16 b01 = (B16(0x0101010101010101) << 64) | 0x0101010101010101;
    constexpr B16 b20 = (B16(0x2020202020202020) << 64) | 0x2020202020202020;
    constexpr B16 b80 = (B16(0x8080808080808080) << 64) | 0x8080808080808080;
    constexpr B16 ba3 = (B16(0xa3a3a3a3a3a3a3a3) << 64) | 0xa3a3a3a3a3a3a3a3;
    constexpr B16 bdd = (B16(0xdddddddddddddddd) << 64) | 0xdddddddddddddddd;
    B16 p22 = ch ^ bdd; // (bits[0..7] == 0xff) == (char == 0x22)
    B16 p5c = ch ^ ba3; // (bits[0..7] == 0xff) == (char == 0x5c)
    B16 p7f = ch ^ b80; // (bits[0..7] == 0xff) == (char == 0x7f)
    p22 &= p22 >> 4; // (bits[0..3] != 0) == (char == 0x22)
    p5c &= p5c >> 4; // (bits[0..3] != 0) == (char == 0x5c)
    p7f &= p7f >> 4; // (bits[0..3] != 0) == (char == 0x7f)
    p22 &= p22 >> 2; // (bits[0..1] != 0) == (char == 0x22)
    p5c &= p5c >> 2; // (bits[0..1] != 0) == (char == 0x5c)
    p7f &= p7f >> 2; // (bits[0..1] != 0) == (char == 0x7f)
    p22 &= p22 >> 1; // (bits[0] == 1) == (char == 0x22)
    p5c &= p5c >> 1; // (bits[0] == 1) == (char == 0x5c)
    p7f &= p7f >> 1; // (bits[0] == 1) == (char == 0x7f)
    B16 special = p22 | p5c | p7f; // (bits[0] == 1) == (char in [0x22, 0x5c, 0x7f])
    special &= b01; // (special == 0) == forall chars (char not in [0x22, 0x5c, 0x7f])
    B16 high = ch & b80; // (high == 0) == forall chars (char < 0x80)

    B16 noctrl = ch | (ch >> 1); // (bits[5] == 1) == ((0x20 <= char && char < 0x80) || 0xa0 <= char)
    B16 ctrl = noctrl ^ b20; // (bits[5] == 0) == ((0x20 <= char && char < 0x80) || 0xa0 <= char)
    ctrl &= b20; // (ctrl == 0) == forall chars (0x20 <= char && char < 0x80)
    B16 nonascii = high | ctrl | special;
    if (unlikely(nonascii)) {
      DBG();
      return false;
    }
  }
  for (const char* chptr = section2; chptr < end; chptr++) {
    uchar c = *chptr;
    DBG("c:" << c);
    if (unlikely(c < 0x20 || c == 0x22 || c == 0x5c || 0x7e < c)) {
      return false;
    }
  }
  DBG();
  return true;
}

void test(const char* alphabet,
          char midch,
          int size,
          int chunk,
          int testid) {
  assert((size % chunk) == 0);
  char* data = (char*)malloc(size);
  assert(data);
  int ablen = strlen((const char*)alphabet);
  if (ablen == 0) ablen = 1;
  for(int i = 0; i < size; i++) {
    int idx = i % ablen;
    data[i] = alphabet[idx];
  }
  data[size / 2] = midch;
  char* data_end = data + size;
  if (testid == -1) {
    for(char* start = data; start < data_end; start += chunk) {
      bool res_fast1 = unescaped_ascii_fast1(start, start + chunk);
      bool res_fast2 = unescaped_ascii_fast2(start, start + chunk);
      bool res_correct = unescaped_ascii_correct(start, start + chunk);
      if (res_fast1 != res_correct) {
        cout << "res_fast1:" << res_fast1 << ", res_correct:" << res_correct << endl;
        assert(false);
      }
      if (res_fast2 != res_correct) {
        cout << "res_fast2:" << res_fast2 << ", res_correct:" << res_correct << endl;
        assert(false);
      }
    }
  }
  if (testid == 0) {
    auto start = std::chrono::high_resolution_clock::now();
    bool res = false;
    for(char* start = data; start < data_end; start += chunk) {
      res = res != unescaped_ascii_correct(start, start + chunk);
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    std::cout << "unescaped_ascii_correct took "
              << elapsed.count() << " seconds" << (res ? " " : "") << endl;
  }
  if (testid == 1) {
    auto start = std::chrono::high_resolution_clock::now();
    bool res = false;
    for(char* start = data; start < data_end; start += chunk) {
      res = res != unescaped_ascii_fast1(start, start + chunk);
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    std::cout << "unescaped_ascii_fast1 took   "
              << elapsed.count() << " seconds" << (res ? " " : "") << endl;
  }
  if (testid == 2) {
    auto start = std::chrono::high_resolution_clock::now();
    bool res = false;
    for(char* start = data; start < data_end; start += chunk) {
      res = res != unescaped_ascii_fast2(start, start + chunk);
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    std::cout << "unescaped_ascii_fast2 took   "
              << elapsed.count() << " seconds" << (res ? " " : "") << endl;
  }
  // Leak memory in `char* data` on purpose so the cache doesn't taint the
  // results.
}

int
main() {
  for (int ch = 0; ch < 256; ch++) {
    for (int m = 0; m < 18; m++) {
      char alphabet[2] = {char(ch), 0};
      char midch = (m == 0) ? ch : (" \x00\x05\x1f\x20\x21\x22\x23\x41\x5b\x5c\x5d\x7e\x7f\x80\x85\xa0\xff")[m];
      test(alphabet, midch, 1, 1, -1);
      test(alphabet, midch, 100, 100, -1);
    }
  }
  const char* alphabet = "ABCDEF !# GHIJKLMNO jklmnopqrstuvwxyz.";
  char midch = 0x41;
  test(alphabet, midch,
       13 * 1024 * 1024,
       4 * 13,
       -1);
  test(alphabet, midch,
       10 * 13 * 1024 * 1024,
       4 * 13 * 1024,
       0);
  test(alphabet, midch,
       10 * 13 * 1024 * 1024,
       4 * 13 * 1024,
       1);
  test(alphabet, midch,
       10 * 13 * 1024 * 1024,
       4 * 13 * 1024,
       2);
  DBG();
  return 0;
}
