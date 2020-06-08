/*__________________________________________________________________________________________

            (c) Hash(BEGIN(Satoshi[2010]), END(Sunny[2012])) == Videlicet[2014] ++

            (c) Copyright The Nexus Developers 2014 - 2019

            Distributed under the MIT software license, see the accompanying
            file COPYING or http://www.opensource.org/licenses/mit-license.php.

            "ad vocem populi" - To the Voice of the People

____________________________________________________________________________________________*/

#include <LLD/keychain/hashmap.h>
#include <LLD/include/enum.h>
#include <LLD/include/version.h>
#include <LLD/hash/xxh3.h>
#include <LLD/templates/bloom.h>

#include <Util/templates/datastream.h>
#include <Util/include/filesystem.h>
#include <Util/include/debug.h>
#include <Util/include/hex.h>

#include <iomanip>

namespace LLD
{
    /*  Compresses a given key until it matches size criteria. */
    void BinaryHashMap::compress_key(std::vector<uint8_t>& vData, uint16_t nSize)
    {
        /* Loop until key is of desired size. */
        while(vData.size() > nSize)
        {
            /* Loop half of the key to XOR elements. */
            uint64_t nSize2 = (vData.size() >> 1);
            for(uint64_t i = 0; i < nSize2; ++i)
            {
                uint64_t i2 = (i << 1);
                if(i2 < (nSize2 << 1))
                    vData[i] = vData[i] ^ vData[i2];
            }

            /* Resize the container to half its size. */
            vData.resize(std::max(uint16_t(nSize2), nSize));
        }
    }


    /* Calculates a bucket to be used for the hashmap allocation. */
    uint32_t BinaryHashMap::get_bucket(const std::vector<uint8_t>& vKey)
    {
        /* Get an xxHash. */
        uint64_t nBucket = XXH64(&vKey[0], vKey.size(), 0) / 7;

        return static_cast<uint32_t>(nBucket % HASHMAP_TOTAL_BUCKETS);
    }


    /* Writes a given key to disk from particular hashmap file and binary position. */
    bool BinaryHashMap::write_key(const std::vector<uint8_t>& vData, const uint16_t nFile, const uint64_t nFilePos)
    {
        /* Find the file stream for LRU cache. */
        std::fstream* pstream;
        if(!pFileStreams->Get(nFile, pstream))
        {
            std::string strFilename = debug::safe_printstr(strBaseLocation, "_hashmap.", std::setfill('0'), std::setw(5), nFile);

            /* Set the new stream pointer. */
            pstream = new std::fstream(strFilename, std::ios::in | std::ios::out | std::ios::binary);
            if(!pstream->is_open())
            {
                delete pstream;
                return debug::error(FUNCTION, "couldn't create hashmap object at: ", strFilename, " (", strerror(errno), ")");
            }

            /* If file not found add to LRU cache. */
            pFileStreams->Put(nFile, pstream);
        }

        /* Handle the disk writing operations. */
        pstream->seekp (nFilePos, std::ios::beg);
        pstream->write((char*)&vData[0], vData.size());
        pstream->flush();

        return true;
    }


    /* The Database Constructor. To determine file location and the Bytes per Record. */
    BinaryHashMap::BinaryHashMap(const std::string& strBaseLocationIn, const uint8_t nFlagsIn, const uint64_t nBucketsIn)
    : KEY_MUTEX              ( )
    , strBaseLocation        (strBaseLocationIn)
    , pFileStreams           (new TemplateLRU<uint16_t, std::fstream*>(8))
    , pBloomStreams          (new TemplateLRU<uint16_t, std::fstream*>(8))
    , HASHMAP_TOTAL_BUCKETS  (nBucketsIn)
    , HASHMAP_MAX_KEY_SIZE   (32)
    , HASHMAP_KEY_ALLOCATION (static_cast<uint16_t>(HASHMAP_MAX_KEY_SIZE + 13))
    , HASHMAP_FLAGS          (nFlagsIn)
    , RECORD_MUTEX           (1024)
    , vHashmaps              ( )
    {
        Initialize();
    }


    /* Copy Constructor */
    BinaryHashMap::BinaryHashMap(const BinaryHashMap& map)
    : KEY_MUTEX              ( )
    , strBaseLocation        (map.strBaseLocation)
    , pFileStreams           (map.pFileStreams)
    , pBloomStreams          (map.pBloomStreams)
    , HASHMAP_TOTAL_BUCKETS  (map.HASHMAP_TOTAL_BUCKETS)
    , HASHMAP_MAX_KEY_SIZE   (map.HASHMAP_MAX_KEY_SIZE)
    , HASHMAP_KEY_ALLOCATION (map.HASHMAP_KEY_ALLOCATION)
    , HASHMAP_FLAGS          (map.HASHMAP_FLAGS)
    , RECORD_MUTEX           (map.RECORD_MUTEX.size())
    , vHashmaps              (map.vHashmaps)
    {
        Initialize();
    }


    /* Move Constructor */
    BinaryHashMap::BinaryHashMap(BinaryHashMap&& map)
    : KEY_MUTEX              ( )
    , strBaseLocation        (std::move(map.strBaseLocation))
    , pFileStreams           (std::move(map.pFileStreams))
    , pBloomStreams          (std::move(map.pBloomStreams))
    , HASHMAP_TOTAL_BUCKETS  (std::move(map.HASHMAP_TOTAL_BUCKETS))
    , HASHMAP_MAX_KEY_SIZE   (std::move(map.HASHMAP_MAX_KEY_SIZE))
    , HASHMAP_KEY_ALLOCATION (std::move(map.HASHMAP_KEY_ALLOCATION))
    , HASHMAP_FLAGS          (std::move(map.HASHMAP_FLAGS))
    , RECORD_MUTEX           (map.RECORD_MUTEX.size())
    , vHashmaps              (std::move(map.vHashmaps))
    {
        Initialize();
    }


    /* Copy Assignment Operator */
    BinaryHashMap& BinaryHashMap::operator=(const BinaryHashMap& map)
    {
        strBaseLocation        = map.strBaseLocation;
        pFileStreams           = map.pFileStreams;
        pBloomStreams          = map.pBloomStreams;
        HASHMAP_TOTAL_BUCKETS  = map.HASHMAP_TOTAL_BUCKETS;
        HASHMAP_MAX_KEY_SIZE   = map.HASHMAP_MAX_KEY_SIZE;
        HASHMAP_KEY_ALLOCATION = map.HASHMAP_KEY_ALLOCATION;
        HASHMAP_FLAGS          = map.HASHMAP_FLAGS;

        vHashmaps              = map.vHashmaps;

        Initialize();

        return *this;
    }


    /* Move Assignment Operator */
    BinaryHashMap& BinaryHashMap::operator=(BinaryHashMap&& map)
    {
        strBaseLocation        = std::move(map.strBaseLocation);
        pFileStreams           = std::move(map.pFileStreams);
        pBloomStreams          = std::move(map.pBloomStreams);
        HASHMAP_TOTAL_BUCKETS  = std::move(map.HASHMAP_TOTAL_BUCKETS);
        HASHMAP_MAX_KEY_SIZE   = std::move(map.HASHMAP_MAX_KEY_SIZE);
        HASHMAP_KEY_ALLOCATION = std::move(map.HASHMAP_KEY_ALLOCATION);
        HASHMAP_FLAGS          = std::move(map.HASHMAP_FLAGS);

        vHashmaps              = std::move(map.vHashmaps);

        Initialize();

        return *this;
    }


    /* Default Destructor */
    BinaryHashMap::~BinaryHashMap()
    {
        if(pFileStreams)
            delete pFileStreams;

        if(pBloomStreams)
            delete pBloomStreams;
    }


    /* Read a key index from the disk hashmaps. */
    void BinaryHashMap::Initialize()
    {
        /* Keep track of total hashmaps. */
        uint32_t nTotalHashmaps = 0;

        /* Create directories if they don't exist yet. */
        if(!filesystem::exists(strBaseLocation) && filesystem::create_directories(strBaseLocation))
            debug::log(0, FUNCTION, "Generated Path ", strBaseLocation);

        /* Set the new stream pointer. */
        std::string strFilename = debug::safe_printstr(strBaseLocation, "_bloom.", std::setfill('0'), std::setw(5), 0u);
        if(!filesystem::exists(strFilename))
        {
            /* Read bloom filter into memory. */
            vHashmaps.emplace_back(std::make_pair(BloomFilter(HASHMAP_TOTAL_BUCKETS), HashMapFilter(HASHMAP_TOTAL_BUCKETS)));

            /* Write the new disk index .*/
            std::fstream bloom(strFilename, std::ios::out | std::ios::binary | std::ios::trunc);
            bloom.write((char*)vHashmaps[0].first.Bytes(),   vHashmaps[0].first.Size());
            bloom.write((char*)vHashmaps[0].second.Bytes(), vHashmaps[0].second.Size());
            bloom.close();

            /* Debug output showing generating of the hashmap file. */
            debug::log(0, FUNCTION, "Generated Bloom Filter 0 of ", vHashmaps[0].first.Size() + vHashmaps[0].second.Size(), " bytes");
        }

        /* Read the hashmap indexes. */
        else
        {
            /* Load the bloom filter disk images. */
            for(nTotalHashmaps = 0; ; ++nTotalHashmaps)
            {
                /* Find the file stream for LRU cache. */
                std::fstream *pstream;
                if(!pBloomStreams->Get(nTotalHashmaps, pstream))
                {
                    /* Set the new stream pointer. */
                    std::string strFilename = debug::safe_printstr(strBaseLocation, "_bloom.", std::setfill('0'), std::setw(5), nTotalHashmaps);
                    pstream = new std::fstream(strFilename, std::ios::in | std::ios::out | std::ios::binary);
                    if(!pstream->is_open())
                    {
                        delete pstream;
                        break;
                    }

                    /* If file not found add to LRU cache. */
                    pBloomStreams->Put(nTotalHashmaps, pstream);
                }

                /* Read bloom filter into memory. */
                vHashmaps.emplace_back(std::make_pair(BloomFilter(HASHMAP_TOTAL_BUCKETS), HashMapFilter(HASHMAP_TOTAL_BUCKETS)));

                /* Read data into bloom filter. */
                pstream->seekg(0, std::ios::beg);
                pstream->read((char*)vHashmaps[nTotalHashmaps].first.Bytes(),   vHashmaps[nTotalHashmaps].first.Size());
                pstream->read((char*)vHashmaps[nTotalHashmaps].second.Bytes(), vHashmaps[nTotalHashmaps].second.Size());
            }

            /* Debug output showing loading of disk index. */
            debug::log(0, FUNCTION, "Loaded Disk Indexes | ", nTotalHashmaps, " hashmaps");
        }

        /* Build the first hashmap index file if it doesn't exist. */
        std::string file = debug::safe_printstr(strBaseLocation, "_hashmap.", std::setfill('0'), std::setw(5), 0u);
        if(!filesystem::exists(file))
        {
            /* Build a vector with empty bytes to flush to disk. */
            std::vector<uint8_t> vSpace(HASHMAP_TOTAL_BUCKETS * HASHMAP_KEY_ALLOCATION, 0);

            /* Flush the empty keychain file to disk. */
            std::fstream stream(file, std::ios::out | std::ios::binary | std::ios::trunc);
            stream.write((char*)&vSpace[0], vSpace.size());
            stream.close();

            /* Debug output showing generating of the hashmap file. */
            debug::log(0, FUNCTION, "Generated Disk Hash Map 0 of ", vSpace.size(), " bytes");
        }

        /* Load the stream object into the stream LRU cache. */
        pFileStreams->Put(0, new std::fstream(file, std::ios::in | std::ios::out | std::ios::binary));
    }


    /* Read a key index from the disk hashmaps. */
    bool BinaryHashMap::Get(const std::vector<uint8_t>& vKey, SectorKey &cKey)
    {
        LOCK(KEY_MUTEX);

        /* Get the assigned bucket for the hashmap. */
        uint32_t nBucket = get_bucket(vKey);

        /* Get the file binary position. */
        uint32_t nFilePos = nBucket * HASHMAP_KEY_ALLOCATION;

        /* Set the cKey return value non compressed. */
        cKey.vKey = vKey;

        /* Compress any keys larger than max size. */
        std::vector<uint8_t> vKeyCompressed = vKey;
        compress_key(vKeyCompressed, HASHMAP_MAX_KEY_SIZE);

        /* Reverse iterate the linked file list from hashmap to get most recent keys first. */
        std::vector<uint8_t> vBucket(HASHMAP_KEY_ALLOCATION *
            std::min(uint32_t(HASHMAP_MAX_KEYS_LINEAR_PROBE), (HASHMAP_TOTAL_BUCKETS - nBucket)), 0);

        /* Check through all the hashmaps. */
        for(int16_t i = vHashmaps.size() - 1; i >= 0; --i)
        {
            /* Check the bloom filters for keys. */
            if(!vHashmaps[i].first.Has(cKey.vKey))
                continue;

            /* Find the file stream for LRU cache. */
            std::fstream* pstream;
            if(!pFileStreams->Get(i, pstream))
            {
                std::string strFilename = debug::safe_printstr(strBaseLocation, "_hashmap.", std::setfill('0'), std::setw(5), i);

                /* Set the new stream pointer. */
                pstream = new std::fstream(strFilename, std::ios::in | std::ios::out | std::ios::binary);
                if(!pstream->is_open())
                {
                    delete pstream;
                    return debug::error(FUNCTION, "couldn't create hashmap object at: ",
                        strFilename, " (", strerror(errno), ")");
                }

                /* If file not found add to LRU cache. */
                pFileStreams->Put(i, pstream);
            }

            /* Seek to the hashmap index in file. */
            pstream->seekg (nFilePos, std::ios::beg);

            /* Read the bucket binary data from file stream */
            pstream->read((char*) &vBucket[0], vBucket.size());

            /* Search through all of the keys read for linear probing. */
            uint32_t nMaxKeys = (vBucket.size() / HASHMAP_KEY_ALLOCATION);
            for(uint16_t nKey = 0; nKey < nMaxKeys; ++nKey)
            {
                /* Check that there is an active key. */
                if(!vHashmaps[i].second.Has(nBucket + nKey)) //checking the linear index offset
                    continue;

                /* The binary offset of particular key. */
                uint64_t nOffset = (nKey * HASHMAP_KEY_ALLOCATION);

                /* Check if this bucket has the key or is in an empty state. */
                if(std::equal(vBucket.begin() + nOffset + 13, vBucket.begin() + nOffset + 13 +
                vKeyCompressed.size(), vKeyCompressed.begin()))
                {
                    /* Deserialie key and return if found. */
                    DataStream ssKey(vBucket, SER_LLD, DATABASE_VERSION);

                    /* Seek to offset and deserialize the key. */
                    ssKey.SetPos(nOffset);
                    ssKey >> cKey;

                    /* Check if the key is ready. */
                    if(!cKey.Ready())
                        continue;

                    /* Debug Output of Sector Key Information. */
                    if(config::nVerbose >= 4)
                        debug::log(4, FUNCTION, "State: ", cKey.nState == STATE::READY ? "Valid" : "Invalid",
                            " | Length: ", cKey.nLength,
                            " | Bucket ", nBucket,
                            " | Location: ", nFilePos,
                            " | Offset: ", nOffset,
                            " | File: ", i,
                            " | Sector File: ", cKey.nSectorFile,
                            " | Sector Size: ", cKey.nSectorSize,
                            " | Sector Start: ", cKey.nSectorStart, "\n",
                            HexStr(vKeyCompressed.begin(), vKeyCompressed.end(), true));

                    return true;
                }
            }
        }

        return false;
    }


    /* Write a key to the disk hashmaps. */
    bool BinaryHashMap::Put(const SectorKey& cKey)
    {
        LOCK(KEY_MUTEX);

        /* Get the assigned bucket for the hashmap. */
        uint32_t nBucket = get_bucket(cKey.vKey);

        /* Get the file binary position. */
        uint32_t nFilePos = nBucket * HASHMAP_KEY_ALLOCATION;

        /* Compress any keys larger than max size. */
        std::vector<uint8_t> vKeyCompressed = cKey.vKey;
        compress_key(vKeyCompressed, HASHMAP_MAX_KEY_SIZE);

        /* Read the State and Size of Sector Header. */
        DataStream ssKey(SER_LLD, DATABASE_VERSION);
        ssKey << cKey;

        /* Serialize the key into the end of the vector. */
        ssKey.write((char*)&vKeyCompressed[0], vKeyCompressed.size());

        /* Handle if not in append mode which will update the key. */
        if(!(HASHMAP_FLAGS & FLAGS::APPEND))
        {
            /* Reverse iterate the linked file list from hashmap to get most recent keys first. */
            std::vector<uint8_t> vBucket(HASHMAP_KEY_ALLOCATION *
                std::min(uint32_t(HASHMAP_MAX_KEYS_LINEAR_PROBE), (HASHMAP_TOTAL_BUCKETS - nBucket)), 0);

            /* Check through all the hashmaps. */
            for(int16_t i = vHashmaps.size() - 1; i >= 0; --i)
            {
                /* Check the bloom filters for keys. */
                if(!vHashmaps[i].first.Has(cKey.vKey))
                    continue;

                /* Find the file stream for LRU cache. */
                std::fstream* pstream;
                if(!pFileStreams->Get(i, pstream))
                {
                    std::string strFilename = debug::safe_printstr(strBaseLocation, "_hashmap.", std::setfill('0'), std::setw(5), i);

                    /* Set the new stream pointer. */
                    pstream = new std::fstream(strFilename, std::ios::in | std::ios::out | std::ios::binary);
                    if(!pstream->is_open())
                    {
                        delete pstream;
                        return debug::error(FUNCTION, "couldn't create hashmap object at: ",
                            strFilename, " (", strerror(errno), ")");
                    }

                    /* If file not found add to LRU cache. */
                    pFileStreams->Put(i, pstream);
                }

                /* Seek to the hashmap index in file. */
                pstream->seekg (nFilePos, std::ios::beg);

                /* Read the bucket binary data from file stream */
                pstream->read((char*) &vBucket[0], vBucket.size());

                /* Search through all of the keys read for linear probing. */
                uint32_t nMaxKeys = (vBucket.size() / HASHMAP_KEY_ALLOCATION);
                for(uint16_t nKey = 0; nKey < nMaxKeys; ++nKey)
                {
                    /* Check that there is an active key. */
                    if(!vHashmaps[i].second.Has(nBucket + nKey)) //checking the linear offset
                        continue;

                    /* The binary offset of particular key. */
                    uint64_t nOffset = (nKey * HASHMAP_KEY_ALLOCATION);

                    /* Check if this bucket has the key or is in an empty state. */
                    if(vBucket[nKey] == STATE::EMPTY || std::equal(vBucket.begin() + nOffset + 13, vBucket.begin() + nOffset + 13 +
                    vKeyCompressed.size(), vKeyCompressed.begin()))
                    {
                        /* Write the key to disk. */
                        if(!write_key(ssKey.Bytes(), i, nFilePos + nOffset))
                            return false;

                        /* Debug Output of Sector Key Information. */
                        if(config::nVerbose >= 4)
                            debug::log(4, FUNCTION, "State: ", cKey.nState == STATE::READY ? "Valid" : "Invalid",
                                " | Length: ", cKey.nLength,
                                " | Bucket: ", nBucket,
                                " | Location: ", nFilePos,
                                " | Offset: ", nOffset,
                                " | File: ", i,
                                " | Sector File: ", cKey.nSectorFile,
                                " | Sector Size: ", cKey.nSectorSize,
                                " | Sector Start: ", cKey.nSectorStart, "\n",
                                HexStr(vKeyCompressed.begin(), vKeyCompressed.end(), true));

                        return true;
                    }
                }
            }
        }

        /* Check through all the hashmaps from oldest to newest for an available slot. */
        for(int16_t i = 0; i < vHashmaps.size(); ++i)
        {
            /* Search through all of the keys read for linear probing. */
            uint32_t nMaxKeys = std::min(uint32_t(HASHMAP_MAX_KEYS_LINEAR_PROBE), uint32_t(HASHMAP_TOTAL_BUCKETS - nBucket));
            for(uint16_t nKey = 0; nKey < nMaxKeys; ++nKey)
            {
                /* Check that there is an active key. */
                if(vHashmaps[i].second.Has(nBucket + nKey)) //checking the linear offset
                    continue;

                /* The binary offset of particular key. */
                uint64_t nOffset = (nKey * HASHMAP_KEY_ALLOCATION);

                /* Update the bloom filter with new key. */
                vHashmaps[i].first. Insert(cKey.vKey);
                vHashmaps[i].second.Insert(nBucket + nKey);

                /* Write the key to disk. */
                if(!write_key(ssKey.Bytes(), i, nFilePos + nOffset))
                    return false;

                /* Debug Output of Sector Key Information. */
                if(config::nVerbose >= 4)
                    debug::log(4, FUNCTION, "State: ", cKey.nState == STATE::READY ? "Valid" : "Invalid",
                        " | Length: ", cKey.nLength,
                        " | Bucket: ", nBucket,
                        " | Location: ", nFilePos,
                        " | Offset: ", nOffset,
                        " | File: ", i,
                        " | Sector File: ", cKey.nSectorFile,
                        " | Sector Size: ", cKey.nSectorSize,
                        " | Sector Start: ", cKey.nSectorStart, "\n",
                        HexStr(vKeyCompressed.begin(), vKeyCompressed.end(), true));

                return true;
            }
        }

        /* Create a new disk hashmap object in linked list if it doesn't exist. */
        std::string strHashmap = debug::safe_printstr(strBaseLocation, "_hashmap.", std::setfill('0'), std::setw(5), vHashmaps.size());
        if(!filesystem::exists(strHashmap))
        {
            /* Blank vector to write empty space in new disk file. */
            std::vector<uint8_t> vSpace(HASHMAP_KEY_ALLOCATION, 0);

            /* Write the blank data to the new file handle. */
            std::ofstream stream(strHashmap, std::ios::out | std::ios::binary | std::ios::app);
            if(!stream)
                return debug::error(FUNCTION, strerror(errno));

            /* Flush the stream to disk in smaller chunks. */
            for(uint32_t i = 0; i < HASHMAP_TOTAL_BUCKETS; ++i)
                stream.write((char*)&vSpace[0], vSpace.size());

            //stream.flush();
            stream.close();
        }

        /* Set the new stream pointer. */
        std::string strBloom = debug::safe_printstr(strBaseLocation, "_bloom.", std::setfill('0'), std::setw(5), vHashmaps.size());
        if(!filesystem::exists(strBloom))
        {
            /* Read bloom filter into memory. */
            vHashmaps.emplace_back(std::make_pair(BloomFilter(HASHMAP_TOTAL_BUCKETS), HashMapFilter(HASHMAP_TOTAL_BUCKETS)));

            /* Write the new disk index .*/
            std::ofstream stream(strBloom, std::ios::out | std::ios::binary | std::ios::trunc);
            if(!stream)
                return debug::error(FUNCTION, strerror(errno));

            /* Write a blank bloom filter to the disk. */
            stream.write((char*)vHashmaps.back().first .Bytes(), vHashmaps.back().first.Size());
            stream.write((char*)vHashmaps.back().second.Bytes(), vHashmaps.back().second.Size());
            stream.close();
        }

        /* Grab the total hashmaps for now. */
        uint32_t nLastIndex = vHashmaps.size() - 1;
        vHashmaps[nLastIndex].first. Insert(cKey.vKey);
        vHashmaps[nLastIndex].second.Insert(nBucket);

        /* Write the key to disk. */
        if(!write_key(ssKey.Bytes(), nLastIndex, nFilePos))
            return false;

        /* Debug Output of Sector Key Information. */
        if(config::nVerbose >= 4)
            debug::log(4, FUNCTION, "State: ", cKey.nState == STATE::READY ? "Valid" : "Invalid",
                " | Length: ", cKey.nLength,
                " | Bucket ", nBucket,
                " | Hashmap ", vHashmaps.size() - 1,
                " | Location: ", nFilePos,
                " | File: ", nLastIndex,
                " | Sector File: ", cKey.nSectorFile,
                " | Sector Size: ", cKey.nSectorSize,
                " | Sector Start: ", cKey.nSectorStart,
                " | Key: ",  HexStr(vKeyCompressed.begin(), vKeyCompressed.end()));

        return true;
    }


    /* Flush all buffers to disk if using ACID transaction. */
    void BinaryHashMap::Flush()
    {
        LOCK(KEY_MUTEX);

        /* Flush the bloom filters to disk. */
        for(auto& hashmap : vHashmaps)
        {
            /* Check if the bloom filter needs to be updated. */
            if(hashmap.first.fModified.load())
            {
                /* Unset the modified state once flushed. */
                hashmap.first.fModified.store(false);
            }

            /* Check if the hashmap filter needs to be updated. */
            if(hashmap.second.fModified.load())
            {
                /* Unset the modified state once flushed. */
                hashmap.second.fModified.store(false);
            }
        }

        debug::log(0, "Update Bloom Filter Disk Images");
    }


    /*  Erase a key from the disk hashmaps. */
    bool BinaryHashMap::Erase(const std::vector<uint8_t> &vKey)
    {
        LOCK(KEY_MUTEX);

        /* Get the assigned bucket for the hashmap. */
        uint32_t nBucket = get_bucket(vKey);

        /* Get the file binary position. */
        uint32_t nFilePos = nBucket * HASHMAP_KEY_ALLOCATION;

        /* Compress any keys larger than max size. */
        std::vector<uint8_t> vKeyCompressed = vKey;
        compress_key(vKeyCompressed, HASHMAP_MAX_KEY_SIZE);

        /* Reverse iterate the linked file list from hashmap to get most recent keys first. */
        std::vector<uint8_t> vBucket(HASHMAP_KEY_ALLOCATION *
            std::min(uint32_t(HASHMAP_MAX_KEYS_LINEAR_PROBE), (HASHMAP_TOTAL_BUCKETS - nBucket)), 0);

        /* Check through all the hashmaps. */
        for(int16_t i = vHashmaps.size() - 1; i >= 0; --i)
        {
            /* Check the bloom filters for keys. */
            if(!vHashmaps[i].first.Has(vKey))
                continue;

            /* Find the file stream for LRU cache. */
            std::fstream* pstream;
            if(!pFileStreams->Get(i, pstream))
            {
                std::string strFilename = debug::safe_printstr(strBaseLocation, "_hashmap.", std::setfill('0'), std::setw(5), i);

                /* Set the new stream pointer. */
                pstream = new std::fstream(strFilename, std::ios::in | std::ios::out | std::ios::binary);
                if(!pstream->is_open())
                {
                    delete pstream;
                    return debug::error(FUNCTION, "couldn't create hashmap object at: ",
                        strFilename, " (", strerror(errno), ")");
                }

                /* If file not found add to LRU cache. */
                pFileStreams->Put(i, pstream);
            }

            /* Seek to the hashmap index in file. */
            pstream->seekg (nFilePos, std::ios::beg);
            pstream->read((char*) &vBucket[0], vBucket.size());

            /* Search through all of the keys read for linear probing. */
            uint32_t nMaxKeys = (vBucket.size() / HASHMAP_KEY_ALLOCATION);
            for(uint16_t nKey = 0; nKey < nMaxKeys; ++nKey)
            {
                /* Check that there is an active key. */
                if(!vHashmaps[i].second.Has(nBucket + nKey)) //checking the linear offset
                    continue;

                /* The binary offset of particular key. */
                uint64_t nOffset = (nKey * HASHMAP_KEY_ALLOCATION);

                /* Check if this bucket has the key or is in an empty state. */
                if(vBucket[nKey] == STATE::EMPTY || std::equal(vBucket.begin() + nOffset + 13, vBucket.begin() + nOffset + 13 +
                vKeyCompressed.size(), vKeyCompressed.begin()))
                {
                    /* Keep this in static memory so we don't have to re-initialize it over and over. */
                    const static std::vector<uint8_t> vEmpty(HASHMAP_KEY_ALLOCATION, 0);

                    /* Update the bloom filter with new key. */
                    vHashmaps[i].second.Erase(nBucket + nKey);

                    /* Write the key to disk. */
                    if(!write_key(vEmpty, i, nFilePos + nOffset))
                        return false;

                    /* Debug Output of Sector Key Information. */
                    if(config::nVerbose >= 4)
                        debug::log(4, FUNCTION, "ERASE",
                            " | Bucket ", nBucket,
                            " | Location: ", nFilePos,
                            " | File: ", i,
                            " | Key: ", HexStr(vKeyCompressed.begin(), vKeyCompressed.end()));

                    return true;
                }
            }
        }

        return false;
    }
}
