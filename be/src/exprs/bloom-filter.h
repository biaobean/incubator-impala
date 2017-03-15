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

#ifndef IMPALA_EXPRS_BLOOM_FILTER_H_
#define IMPALA_EXPRS_BLOOM_FILTER_H_
#include <cstdint>
#include <vector>

namespace impala_udf {
  class BloomFilter {
  public:
      class Murmur3 {
        public:
            static const uint64_t NULL_HASHCODE = 2862933555777941757LLU;
        private:
            static const int C1_32 = -862048943;
            static const int C2_32 = 461845907;
            static const int R1_32 = 15;
            static const int R2_32 = 13;
            static const int M_32 = 5;
            static const int N_32 = -430675100;
            static const long C1 = -8663945395140668459LL;
            static const long C2 = 5545529020109919103LL;
            static const int R1 = 31;
            static const int R2 = 27;
            static const int R3 = 33;
            static const int M = 5;
            static const int N1 = 1390208809;
            static const int N2 = 944331445;
            static const int DEFAULT_SEED = 104729;

        public:
            Murmur3() {}
            static uint32_t hash32(uint8_t* data, int length, int seed); 
            static uint32_t hash32(uint8_t* data, int len) ;

            static uint64_t hash64(uint8_t* data, int length, int seed);
            static uint64_t hash64(uint8_t* data, int len);

        };
    public:
        static constexpr double DEFAULT_FALSE_POSITIVE_PROBABILITY = 0.05;

        BloomFilter(int numBits, int numHashFunctions, const char* data) :
        numBits(numBits),
        numHashFunctions(numHashFunctions),
        bitset(reinterpret_cast<const long*>(data)) {
        }
        bool TestBytes(uint8_t* val, int len);
        bool TestLong(int64_t val);
        bool TestInt(int val);
        bool TestFloat(float val);
        bool TestDouble(double val);

        bool TestBit(long index) ;

    protected:
        int numBits;
        int numHashFunctions;
        const int64_t* bitset;

    public:
        bool TestHash(int64_t hash64);
        long GetLongHash(long key);
    };
}

#endif /* BLOOM_FILTER_H */

