//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog_presto.cpp
//
// Identification: src/primer/hyperloglog_presto.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/hyperloglog_presto.h"

namespace bustub {

/** @brief Parameterized constructor. */
template <typename KeyType>
HyperLogLogPresto<KeyType>::HyperLogLogPresto(int16_t n_leading_bits)
    : dense_bucket_(1 << std::max(n_leading_bits, static_cast<int16_t>(0)), std::bitset<DENSE_BUCKET_SIZE>{}),
      cardinality_(0),
      leading_bits_(static_cast<uint64_t>(n_leading_bits > 0 ? n_leading_bits : 0)) {}

/** @brief Element is added for HLL calculation. */
template <typename KeyType>
auto HyperLogLogPresto<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  const auto bin = std::bitset<BITSET_CAPACITY>{CalculateHash(val)};
  uint64_t reg_i = 0;
  for (uint64_t i = BITSET_CAPACITY - 1; i >= BITSET_CAPACITY - leading_bits_; i--)
    reg_i = (reg_i << 1) | static_cast<uint64_t>(bin[i]);

  uint64_t contiguous_zeros = 0;
  while (contiguous_zeros < BITSET_CAPACITY - leading_bits_) {
    if (bin[contiguous_zeros]) break;
    contiguous_zeros++;
  }

  uint64_t dense_bucket_index_ = dense_bucket_[reg_i].to_ulong();
  uint64_t overflow_bucket_index_ = overflow_bucket_[reg_i].to_ulong();
  if ((overflow_bucket_index_ << DENSE_BUCKET_SIZE) + dense_bucket_index_ < contiguous_zeros) {
    overflow_bucket_index_ = contiguous_zeros >> DENSE_BUCKET_SIZE;
    dense_bucket_index_ = (contiguous_zeros % (1 << DENSE_BUCKET_SIZE));
    overflow_bucket_[reg_i] = overflow_bucket_index_;
    dense_bucket_[reg_i] = dense_bucket_index_;
  }
}

/** @brief Function to compute cardinality. */
template <typename T>
auto HyperLogLogPresto<T>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  double sigma = 0;
  uint64_t m = (1 << leading_bits_);
  for (uint64_t i = 0; i < m; i++) {
    const auto num =
        static_cast<int64_t>((overflow_bucket_[i].to_ulong() << DENSE_BUCKET_SIZE) + dense_bucket_[i].to_ulong());
    sigma += pow(2, -num);
  }
  cardinality_ = std::floor(CONSTANT * m * m / sigma);
}

template class HyperLogLogPresto<int64_t>;
template class HyperLogLogPresto<std::string>;
}  // namespace bustub
