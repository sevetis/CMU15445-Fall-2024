//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog.cpp
//
// Identification: src/primer/hyperloglog.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/hyperloglog.h"

namespace bustub {

/** @brief Parameterized constructor. */
template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits)
    : bit_num_(static_cast<uint64_t>(n_bits > 0 ? n_bits : 0)),
      registers_(std::vector<uint8_t>(1 << std::max(n_bits, static_cast<int16_t>(0)))) {}

/**
 * @brief Function that computes binary.
 *
 * @param[in] hash
 * @returns binary of a given hash
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  /** @TODO(student) Implement this function! */
  return {hash};
}

/**
 * @brief Function that computes leading zeros.
 *
 * @param[in] bset - binary values of a given bitset
 * @returns leading zeros of given binary set
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  /** @TODO(student) Implement this function! */
  for (uint64_t position = BITSET_CAPACITY; position != 0; position--)
    if (bset[position - 1]) return BITSET_CAPACITY - position;
  return BITSET_CAPACITY;
}

/**
 * @brief Adds a value into the HyperLogLog.
 *
 * @param[in] val - value that's added into hyperloglog
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  auto bin = ComputeBinary(CalculateHash(val));
  uint64_t reg_i = 0;
  for (uint64_t i = BITSET_CAPACITY - 1; i >= BITSET_CAPACITY - bit_num_; i--) {
    reg_i = (reg_i << 1) | static_cast<uint64_t>(bin[i]);
    bin[i] = 0;
  }
  const auto p = static_cast<uint8_t>(PositionOfLeftmostOne(bin) - bit_num_ + 1);
  registers_[reg_i] = std::max(registers_[reg_i], p);
}

/**
 * @brief Function that computes cardinality.
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  const uint64_t m = registers_.size();
  double sigma = 0;
  for (auto r_j : registers_) {
    sigma += pow(2.0, -r_j);
  }
  cardinality_ = floor(CONSTANT * m * m / sigma);
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub
