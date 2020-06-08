/*__________________________________________________________________________________________

            (c) Hash(BEGIN(Satoshi[2010]), END(Sunny[2012])) == Videlicet[2014] ++

            (c) Copyright The Nexus Developers 2014 - 2019

            Distributed under the MIT software license, see the accompanying
            file COPYING or http://www.opensource.org/licenses/mit-license.php.

            "ad vocem populi" - To the Voice of the People

____________________________________________________________________________________________*/

#pragma once
#ifndef NEXUS_UTIL_TEMPLATES_BITARRAY_H
#define NEXUS_UTIL_TEMPLATES_BITARRAY_H

#include <cstdint>
#include <vector>
#include <atomic>
#include <bitset>


/** Bit Array Class
 *
 *  Class to act as a container for keys.
 *  This class operates in O(1) for insert and access
 *
 *  It has internal modification pointers for selecting the range of modified registers.
 *
 **/
class BitArray
{
protected:

    /** Keep track of first register that was modified. **/
    uint64_t nModifiedBegin;


    /** Keep track of the last register that was modified. **/
    uint64_t nModifiedEnd;


    /** is_set
     *
     *  Check if a particular bit is set in the Bit Array.
     *
     *  @param[in] nIndex The index to check bit for.
     *
     *  @return true if the bit is set.
     *
     **/
    bool is_set(const uint64_t nIndex) const
    {
        return (vRegisters[nIndex / 64] & (uint64_t(1) << (nIndex % 64)));
    }


    /** set_bit
     *
     *  Set a bit in the Bit Array for a given index.
     *
     *  @param[in] nIndex The index to set bit for.
     *
     **/
    void set_bit(const uint64_t nIndex)
    {
        /* Grab the register. */
        uint32_t nRegister = (nIndex / 64);
        if(!fModified.load())
        {
            nModifiedBegin = nRegister;
            nModifiedEnd   = nRegister + 1; //allocate one register for first modification

            fModified.store(true);
        }
        else
        {
            /* If we need to get closer to the zero-index. */
            if(nRegister < nModifiedBegin)
                nModifiedBegin = nRegister;

            /* If we need to extend the ending index. */
            if(nRegister >= nModifiedEnd)
                nModifiedEnd = (nRegister + 1);
        }

        vRegisters[nRegister] |= (uint64_t(1) << (nIndex % 64));
    }


    /** clear_bit
     *
     *  Clear a bit in the Bit Array for a given index.
     *
     *  @param[in] nIndex The index to set bit for.
     *
     **/
    void clear_bit(const uint64_t nIndex)
    {
        /* Grab the register. */
        uint32_t nRegister = (nIndex / 64);
        if(!fModified.load())
        {
            nModifiedBegin = nRegister;
            nModifiedEnd   = nRegister + 1; //allocate one register for first modification

            fModified.store(true);
        }
        else
        {
            /* If we need to get closer to the zero-index. */
            if(nRegister < nModifiedBegin)
                nModifiedBegin = nRegister;

            /* If we need to extend the ending index. */
            if(nRegister >= nModifiedEnd)
                nModifiedEnd = (nRegister + 1);
        }

        vRegisters[nRegister] &= ~(uint64_t(1) << (nIndex % 64));
    }


    /** The bitarray using 64 bit registers. **/
    std::vector<uint64_t> vRegisters;


public:

    /** Flag to track if container has been modified. **/
    std::atomic<bool> fModified;


    /** Default Constructor. **/
    BitArray()                                    = delete;


    /** Copy Constructor. **/
    BitArray(const BitArray& filter)
    : nModifiedBegin (filter.nModifiedBegin)
    , nModifiedEnd   (filter.nModifiedEnd)
    , vRegisters     (filter.vRegisters)
    , fModified      (filter.fModified.load())
    {
    }


    /** Move Constructor. **/
    BitArray(BitArray&& filter)
    : nModifiedBegin (std::move(filter.nModifiedBegin))
    , nModifiedEnd   (std::move(filter.nModifiedEnd))
    , vRegisters     (std::move(filter.vRegisters))
    , fModified      (filter.fModified.load())
    {
    }


    /** Copy assignment. **/
    BitArray& operator=(const BitArray& filter)
    {
        nModifiedBegin = filter.nModifiedBegin;
        nModifiedEnd   = filter.nModifiedEnd;
        vRegisters     = filter.vRegisters;

        fModified      = filter.fModified.load();

        return *this;
    }


    /** Move assignment. **/
    BitArray& operator=(BitArray&& filter)
    {
        nModifiedBegin = std::move(filter.nModifiedBegin);
        nModifiedEnd   = std::move(filter.nModifiedEnd);
        vRegisters     = std::move(filter.vRegisters);

        fModified      = filter.fModified.load();

        return *this;
    }


    /** Default Destructor. **/
    ~BitArray()
    {
    }


    /** Create bit array with given number of elements. **/
    BitArray  (const uint64_t nElements)
    : nModifiedBegin (0)
    , nModifiedEnd   (0)
    , vRegisters     ((nElements / 64) + 1, 0)
    , fModified      (false)
    {
    }


    /** Count
     *
     *  The total number of elements set to 1 in the bit array.
     *
     **/
    uint32_t Count() const
    {
        uint32_t nTotal = 0;
        for(const auto& nRegister : vRegisters)
            nTotal += __builtin_popcountl(nRegister);

        return nTotal;
    }


    /** Bytes
     *
     *  Get the beginning memory location of the bit array.
     *
     **/
    uint8_t* Bytes() const
    {
        return (uint8_t*)&vRegisters[0];
    }


    /** Size
     *
     *  Get the size (in bytes) of the bit array.
     *
     **/
    uint64_t Size() const
    {
        return vRegisters.size() * 8;
    }


    /** ModifiedBytes
     *
     *  Get the beginning modified memory location of the bit array.
     *
     **/
    uint8_t* ModifiedBytes() const
    {
        return (uint8_t*)&vRegisters[nModifiedBegin];
    }


    /** ModifiedSize
     *
     *  Get the size (in bytes) of the modified space in the bit array.
     *
     **/
    uint64_t ModifiedSize() const
    {
        return (nModifiedEnd - nModifiedBegin) * 8;
    }


    /** ModifiedOffset
     *
     *  Get the current starting offset (in bytes) of the modified space.
     *
     **/
    uint64_t ModifiedOffset() const
    {
        return (nModifiedBegin * 8);
    }
};

#endif
