/*__________________________________________________________________________________________

            (c) Hash(BEGIN(Satoshi[2010]), END(Sunny[2012])) == Videlicet[2014] ++

            (c) Copyright The Nexus Developers 2014 - 2019

            Distributed under the MIT software license, see the accompanying
            file COPYING or http://www.opensource.org/licenses/mit-license.php.

            "ad vocem populi" - To the Voice of the People

____________________________________________________________________________________________*/

#pragma once
#ifndef NEXUS_LLD_KEYCHAIN_HASHMAP_H
#define NEXUS_LLD_KEYCHAIN_HASHMAP_H

#include <LLD/keychain/keychain.h>
#include <LLD/cache/template_lru.h>
#include <LLD/include/enum.h>

#include <Util/templates/bitarray.h>

#include <cstdint>
#include <string>
#include <fstream>
#include <vector>
#include <mutex>

namespace LLD
{
    /** The maximum number of keys for linear probing for the hashmaps. **/
    const uint32_t HASHMAP_MAX_KEYS_LINEAR_PROBE = 16;


    //forward declaration
    class BloomFilter;


    /** HashMapFilter class
     *
     *  Handles hashmap for filtering available buckets.
     *
     **/
    class HashMapFilter : public BitArray
    {
    public:
        /** Default Constructor. **/
        HashMapFilter()                                    = delete;


        /** Copy Constructor. **/
        HashMapFilter(const HashMapFilter& filter)
        : BitArray(filter)
        {
        }


        /** Move Constructor. **/
        HashMapFilter(HashMapFilter&& filter)
        : BitArray(std::move(filter))
        {
        }


        /** Copy assignment. **/
        HashMapFilter& operator=(const HashMapFilter& filter)
        {
            nModifiedBegin = filter.nModifiedBegin;
            nModifiedEnd   = filter.nModifiedEnd;
            vRegisters     = filter.vRegisters;

            fModified      = filter.fModified.load();

            return *this;
        }


        /** Move assignment. **/
        HashMapFilter& operator=(HashMapFilter&& filter)
        {
            nModifiedBegin = std::move(filter.nModifiedBegin);
            nModifiedEnd   = std::move(filter.nModifiedEnd);
            vRegisters     = std::move(filter.vRegisters);

            fModified      = filter.fModified.load();

            return *this;
        }


        /** Default Destructor. **/
        ~HashMapFilter()
        {
        }


        /** Create bit array with given number of elements. **/
        HashMapFilter  (const uint64_t nElements)
        : BitArray(nElements)
        {
        }


        /** Has
         *
         *  Check if the given bucket is available.
         *
         *  @param[in] nBucket The bucket to check for.
         *
         *  @return true if the bucket is available.
         *
         **/
        bool Has(const uint64_t nBucket)
        {
            return is_set(nBucket);
        }


        /** Insert
         *
         *  Insert key into given bucket.
         *
         *  @param[in] nBucket The bucket to add key for.
         *
         **/
        void Insert(const uint64_t nBucket)
        {
            set_bit(nBucket);
        }


        /** Erase
         *
         *  Erase key from given bucket.
         *
         *  @param[in] nBucket The bucket to add key for.
         *
         **/
        void Erase(const uint64_t nBucket)
        {
            clear_bit(nBucket);
        }
    };



    /** BinaryHashMap
     *
     *  This class is responsible for managing the keys to the sector database.
     *
     *  It contains a Binary Hash Map with a minimum complexity of O(1).
     *  It uses a linked file list based on index to iterate trhough files and binary Positions
     *  when there is a collision that is found.
     *
     **/
    class BinaryHashMap : public Keychain
    {
    protected:

        /** Mutex for Thread Synchronization. **/
        mutable std::mutex KEY_MUTEX;


        /** The string to hold the database location. **/
        std::string strBaseLocation;


        /** Keychain stream object. **/
        TemplateLRU<uint16_t, std::fstream*>* pFileStreams;


        /** Bloom filter stream objects. **/
        TemplateLRU<uint16_t, std::fstream*>* pBloomStreams;


        /** The Maximum buckets allowed in the hashmap. */
        uint32_t HASHMAP_TOTAL_BUCKETS;


        /** The Maximum key size for static key sectors. **/
        uint16_t HASHMAP_MAX_KEY_SIZE;


        /** The total space that a key consumes. */
        uint16_t HASHMAP_KEY_ALLOCATION;


        /** The keychain flags. **/
        uint8_t HASHMAP_FLAGS;


        /* The key level locking hashmap. */
        mutable std::vector<std::mutex> RECORD_MUTEX;


        /** Set of filters for each hashmap file. **/
        std::vector< std::pair<BloomFilter, HashMapFilter> > vHashmaps;


        /** compress_key
         *
         *  Compresses a given key until it matches size criteria.
         *  This function is one way and efficient for reducing key sizes.
         *
         *  @param[out] vData The binary data of key to compress.
         *  @param[in] nSize The desired size of key after compression.
         *
         **/
        void compress_key(std::vector<uint8_t>& vData, uint16_t nSize = 32);


        /** get_bucket
         *
         *  Calculates a bucket to be used for the hashmap allocation.
         *
         *  @param[in] vKey The key object to calculate with.
         *
         *  @return The bucket assigned to the key.
         *
         **/
        uint32_t get_bucket(const std::vector<uint8_t>& vKey);


        /** write_key
         *
         *  Writes a given key to disk from particular hashmap file and binary position.
         *
         *  @param[in] vData The key data that is going to be written to disk.
         *  @param[in] nFile The file number that key is going to be written to.
         *  @param[in] nFilePos The current binary position of key in keychain.
         *
         *  @return true if the operation completed successfully.
         *
         **/
        bool write_key(const std::vector<uint8_t>& vData, const uint16_t nFile, const uint64_t nFilePos);


    public:


        /** Default Constructor. **/
        BinaryHashMap() = delete;


        /** The Database Constructor. To determine file location and the Bytes per Record. **/
        BinaryHashMap(const std::string& strBaseLocationIn, const uint8_t nFlagsIn = FLAGS::APPEND, const uint64_t nBucketsIn = 256 * 256 * 64);


        /** Copy Constructor **/
        BinaryHashMap(const BinaryHashMap& map);


        /** Move Constructor **/
        BinaryHashMap(BinaryHashMap&& map);


        /** Copy Assignment Operator **/
        BinaryHashMap& operator=(const BinaryHashMap& map);


        /** Move Assignment Operator **/
        BinaryHashMap& operator=(BinaryHashMap&& map);


        /** Default Destructor **/
        virtual ~BinaryHashMap();


        /** Initialize
         *
         *  Initialize the binary hash map keychain.
         *
         **/
        void Initialize();


        /** Get
         *
         *  Read a key index from the disk hashmaps.
         *
         *  @param[in] vKey The binary data of key.
         *  @param[out] cKey The key object to return.
         *
         *  @return True if the key was found, false otherwise.
         *
         **/
        bool Get(const std::vector<uint8_t>& vKey, SectorKey &cKey);


        /** Put
         *
         *  Write a key to the disk hashmaps.
         *
         *  @param[in] cKey The key object to write.
         *
         *  @return True if the key was written, false otherwise.
         *
         **/
        bool Put(const SectorKey& cKey);


        /** Flush
         *
         *  Flush all buffers to disk if using ACID transaction.
         *
         **/
        void Flush();


        /** Erase
         *
         *  Erase a key from the disk hashmaps.
         *
         *  @param[in] vKey the key to erase.
         *
         *  @return True if the key was erased, false otherwise.
         *
         **/
        bool Erase(const std::vector<uint8_t>& vKey);
    };
}

#endif
