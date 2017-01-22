// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <cstdlib>
#include "exprs/bloom-filter.h"

namespace impala_udf
{

#define FORCE_INLINE inline __attribute__((always_inline))

  inline uint32_t rotl32(uint32_t x, int8_t r)
  {
    return (x << r) | (x >> (32 - r));
  }

  inline uint64_t rotl64(uint64_t x, int8_t r)
  {
    return (x << r) | (x >> (64 - r));
  }

#define ROTL32(x,y) rotl32(x,y)
#define ROTL64(x,y) rotl64(x,y)

#define BIG_CONSTANT(x) (x##LLU)

  FORCE_INLINE uint32_t fmix32(uint32_t h)
  {
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;

    return h;
  }

  FORCE_INLINE int64_t fmix64(uint64_t h)
  {
    h ^= h >> 33;
    h *= BIG_CONSTANT(0xff51afd7ed558ccd);
    h ^= h >> 33;
    h *= BIG_CONSTANT(0xc4ceb9fe1a85ec53);
    h ^= h >> 33;

    return h;
  }

  uint32_t BloomFilter::Murmur3::hash32(uint8_t* data, int len)
  {
    return hash32(data, len, DEFAULT_SEED);
  }

  uint32_t BloomFilter::Murmur3::hash32(uint8_t* data, int length, int seed)
  {
    uint32_t hash = seed;
    uint32_t nblocks = length >> 2;

    uint32_t idx;
    uint32_t k1;
    for (idx = 0; idx < nblocks; ++idx) {
      k1 = idx << 2;
      uint32_t k = (data[k1] & 0xff) | (data[k1 + 1] & 0xff) << 8
              | (data[k1 + 2] & 0xff) << 16 | (data[k1 + 3] & 0xff) << 24;
      k *= C1_32;
      k = ROTL32(k, R1_32);
      k *= C2_32;
      hash ^= k;
      hash = ROTL32(hash, R2_32) * M_32 + N_32;
    }

    idx = nblocks << 2;
    k1 = 0;

    switch (length - idx) {
    case 3:
      k1 ^= data[idx + 2] << 16;
    case 2:
      k1 ^= data[idx + 1] << 8;
    case 1:
      k1 ^= data[idx];
      k1 *= C1_32;
      k1 = ROTL32(k1, 15);
      k1 *= C2_32;
      hash ^= k1;
    }

    hash ^= length;
    hash = fmix32(hash);
    return hash;
  }

  uint64_t BloomFilter::Murmur3::hash64(uint8_t* data, int len)
  {
    return hash64(data, len, DEFAULT_SEED);
  }

  uint64_t BloomFilter::Murmur3::hash64(uint8_t* data, int length, int seed)
  {
    uint64_t hash = (uint64_t) seed;
    uint64_t nblocks = length >> 3;

    for (int i = 0; i < nblocks; ++i) {
      uint64_t i8 = i << 3;
      uint64_t k = ((uint64_t) data[i8] & 0xff) | ((uint64_t) data[i8 + 1] & 0xff) << 8
              | ((uint64_t) data[i8 + 2] & 0xff) << 16 | ((uint64_t) data[i8 + 3] & 0xff) << 24
              | ((uint64_t) data[i8 + 4] & 0xff) << 32 | ((uint64_t) data[i8 + 5] & 0xff) << 40
              | ((uint64_t) data[i8 + 6] & 0xff) << 48 | ((uint64_t) data[i8 + 7] & 0xff) << 56;
      k *= C1;
      k = ROTL64(k, R1);
      k *= C2;
      hash ^= k;
      hash = ROTL64(hash, R2) * M + N1;
    }

    uint64_t k1 = 0;
    uint64_t tailStart = nblocks << 3;
    switch (length - tailStart) {
    case 7:
      k1 ^= ((uint64_t) data[tailStart + 6] & 0xff) << 48;
    case 6:
      k1 ^= ((uint64_t) data[tailStart + 5] & 0xff) << 40;
    case 5:
      k1 ^= ((uint64_t) data[tailStart + 4] & 0xff) << 32;
    case 4:
      k1 ^= ((uint64_t) data[tailStart + 3] & 0xff) << 24;
    case 3:
      k1 ^= ((uint64_t) data[tailStart + 2] & 0xff) << 16;
    case 2:
      k1 ^= ((uint64_t) data[tailStart + 1] & 0xff) << 8;
    case 1:
      k1 ^= (uint64_t) data[tailStart] & 0xff;
      k1 *= C1;
      k1 = ROTL64(k1, R1);
      k1 *= C2;
      hash ^= k1;
    }

    hash ^= (uint64_t) length;
    hash = fmix64(hash);
    return hash;
  }

  bool BloomFilter::TestBytes(uint8_t* val, int len)
  {
    long hash64 = val == NULL ? Murmur3::NULL_HASHCODE : Murmur3::hash64(val, len);
    return TestHash(hash64);
  }

  bool BloomFilter::TestLong(int64_t val)
  {
    return TestHash(GetLongHash(val));
  }

  bool BloomFilter::TestDouble(double val)
  {
    return TestLong(*(long*) &val);
  }

  bool BloomFilter::TestFloat(float val)
  {
    return TestLong(*(long*) &val);
  }

  bool BloomFilter::TestInt(int val)
  {
    return TestLong(val);
  }

  long BloomFilter::GetLongHash(int64_t key)
  {
    key = (~key) + (key << 21); // key = (key << 21) - key - 1;
    key = key ^ (key >> 24);
    key = (key + (key << 3)) + (key << 8); // key * 265
    key = key ^ (key >> 14);
    key = (key + (key << 2)) + (key << 4); // key * 21
    key = key ^ (key >> 28);
    key = key + (key << 31);
    return key;
  }

  bool BloomFilter::TestHash(int64_t hash64)
  {
    int32_t hash1 = (int32_t) hash64;
    int32_t hash2 = (int32_t) ((uint64_t) hash64 >> 32);

    for (int i = 1; i <= numHashFunctions; i++) {
      int32_t combinedHash = hash1 + (i * hash2);
      // hashcode should be positive, flip all the bits if it's negative
      if (combinedHash < 0) {
        combinedHash = ~combinedHash;
      }
      int32_t pos = combinedHash % numBits;

      if (!TestBit(pos)) {
        return false;
      }
    }
    return true;
  }

  bool BloomFilter::TestBit(long index)
  {
    long bigendian = __builtin_bswap64(bitset[index >> 6]);
    return bigendian & ROTL64(1, index % 64);
  }
}