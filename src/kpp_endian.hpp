#include <cstdint>


namespace endian {

namespace {           

  template<typename T>
    constexpr T lshift_by(T val, std::uint_fast8_t n) {
      return val << (n * 8);
    }

  /*
   * rshift_by
   * Right shift 'val' by 'n' bytes
   */
  template<typename T>
    constexpr T rshift_by(T val, std::uint_fast8_t n) {
      return val >> (n * 8);
    }

  /*
   * mask_t
   * Mask 'n'th byte
   */
  template<typename T>
    constexpr T mask_at(std::uint_fast8_t n) {
      return lshift_by(T(0xFF), n);
    }

  /*
   * byte_at
   * Get 'n'th byte from 'val'
   */
  template<typename T>
    constexpr T byte_at(T val, std::uint_fast8_t n) {
      return rshift_by((val & mask_at<T>(n)), n);
    }

  /*
   * replace_at
   * Replace, in 'val', 'n'th byte with 'byte_val'
   */
  template<typename T>
    constexpr T replace_at(T val, std::uint8_t byte_val, std::uint_fast8_t n) {
      return (val & ~mask_at<T>(n)) | lshift_by(T(byte_val), n);
    }

  /*
   * copy
   * Copy, in 'val', byte at index 'from' to byte at index 'to'
   */
  template<typename T>
    constexpr T copy(T val, std::uint_fast8_t from, std::uint_fast8_t to) {
      return replace_at(val, byte_at(val, from), to);
    }

  /*
   * swap
   * Swap, in 'val', byte at index 'n1' with byte at index 'n2'
   */
  template<typename T>
    constexpr T swap(T val, std::uint_fast8_t n1, std::uint_fast8_t n2) {
      return replace_at(copy(val, n1, n2), byte_at(val, n2), n1);
    }

  /*
   * reverse_impl
   * Swap, in 'val', byte at index 'n1' with byte at index 'n2' in 'val', increment n1, decrement n2 until n1 > n2
   */
  template<typename T>
    constexpr T reverse_impl(T val, std::uint_fast8_t n1, std::uint_fast8_t n2) {
      return n1 > n2 ? val : reverse_impl(swap(val, n1, n2), n1 + 1, n2 - 1);
    }

  /*
   * reverse
   * Reverse 'val'
   */
  template<typename T> 
    constexpr T reverse(T val) {
      return reverse_impl(val, 0, sizeof(T) - 1);
    }

  /*
   * reverse_words_impl
   * Swap, in 'val', byte at index 'n' - 1 with byte at index 'n' - 2, decrement n by 2 until n == 0
   */
  template<typename T>
    constexpr T reverse_words_impl(T val, std::uint_fast8_t n) {
      return n == 0 ? val : reverse_words_impl(swap(val, n - 1, n - 2), n - 2);
    }

  /*
   * reverse_words
   * Reverse each word in 'val'
   */
  template<typename T>
    constexpr T reverse_words(T val) {
      return reverse_words_impl(val, sizeof(T));
    }

  static inline bool is_big_endian() {
    const uint16_t endianness = 256;
    return *reinterpret_cast<const uint8_t *>(&endianness);
  }
} 

template<typename T>
inline T hton(T val) {
if (is_big_endian())
  return val;
else
  return reverse(val);
}

template<typename T>
inline T ntoh(T val) {
if (is_big_endian())
  return val;
else
  return reverse(val);
}

}
