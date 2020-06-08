/*__________________________________________________________________________________________

            (c) Hash(BEGIN(Satoshi[2010]), END(Sunny[2012])) == Videlicet[2014] ++

            (c) Copyright The Nexus Developers 2014 - 2019

            Distributed under the MIT software license, see the accompanying
            file COPYING or http://www.opensource.org/licenses/mit-license.php.

            "ad vocem populi" - To the Voice of the People

____________________________________________________________________________________________*/

#include <functional>

#include <LLD/hash/xxh3.h>
#include <LLD/templates/bloom.h>

#include <Util/include/filesystem.h>
#include <Util/templates/serialize.h>

namespace LLD
{

    /* Get the bucket for given value k and key. */
    uint64_t BloomFilter::get_bucket(const std::vector<uint8_t>& vKey, const uint32_t nK) const
    {
        /* Get an xxHash. */
        uint64_t nBucket = XXH64(&vKey[0], vKey.size(), nK);

        return static_cast<uint64_t>(nBucket % HASHMAP_TOTAL_BUCKETS);
    }


    /* Copy Constructor. */
    BloomFilter::BloomFilter(const BloomFilter& filter)
    : BitArray              (filter)
    , HASHMAP_TOTAL_BUCKETS (filter.HASHMAP_TOTAL_BUCKETS)
    , MUTEX                 ( )
    {
    }


    /* Move Constructor. */
    BloomFilter::BloomFilter(BloomFilter&& filter)
    : BitArray              (std::move(filter))
    , HASHMAP_TOTAL_BUCKETS (std::move(filter.HASHMAP_TOTAL_BUCKETS))
    , MUTEX                 ( )
    {
    }


    /* Copy assignment. */
    BloomFilter& BloomFilter::operator=(const BloomFilter& filter)
    {
        HASHMAP_TOTAL_BUCKETS       = filter.HASHMAP_TOTAL_BUCKETS;
        nModifiedBegin              = filter.nModifiedBegin;
        nModifiedEnd                = filter.nModifiedEnd;
        vRegisters                  = filter.vRegisters;

        fModified                   = filter.fModified.load();

        return *this;
    }


    /* Move assignment. */
    BloomFilter& BloomFilter::operator=(BloomFilter&& filter)
    {
        HASHMAP_TOTAL_BUCKETS       = std::move(filter.HASHMAP_TOTAL_BUCKETS);
        nModifiedBegin              = std::move(filter.nModifiedBegin);
        nModifiedEnd                = std::move(filter.nModifiedEnd);
        vRegisters                  = std::move(filter.vRegisters);

        fModified                   = filter.fModified.load();

        return *this;
    }


    /* Create bloom filter with given number of buckets. */
    BloomFilter::BloomFilter  (const uint64_t nBuckets)
    : BitArray              ((nBuckets * 3) / 0.693147)
    , HASHMAP_TOTAL_BUCKETS ((nBuckets * 3) / 0.693147) //n * k / ln(2) = m
    , MUTEX                 ( )
    {
    }


    /* Default Destructor. */
    BloomFilter::~BloomFilter()
    {
    }


    /* Add a new key to the bloom filter. */
    void BloomFilter::Insert(const std::vector<uint8_t>& vKey)
    {
        LOCK(MUTEX);

        /* Run over k hashes. */
        for(uint16_t nK = 0; nK < 3; ++nK)
        {
            /* Set the internal bit from the bitarray. */
            uint64_t nBucket = get_bucket(vKey, nK);
            set_bit(nBucket);
        }
    }


    /* Checks if a key exists in the bloom filter. */
    bool BloomFilter::Has(const std::vector<uint8_t>& vKey) const
    {
        LOCK(MUTEX);

        /* Run over k hashes. */
        for(uint16_t nK = 0; nK < 3; ++nK)
        {
            uint64_t nBucket = get_bucket(vKey, nK);
            if(!is_set(nBucket))
                return false;
        }

        return true;
    }
}
