/*__________________________________________________________________________________________

            (c) Hash(BEGIN(Satoshi[2010]), END(Sunny[2012])) == Videlicet[2014] ++

            (c) Copyright The Nexus Developers 2014 - 2018

            Distributed under the MIT software license, see the accompanying
            file COPYING or http://www.opensource.org/licenses/mit-license.php.

            "ad vocem populi" - To the Voice of the People

____________________________________________________________________________________________*/

#ifndef NEXUS_UTIL_INCLUDE_ALLOCATORS_H
#define NEXUS_UTIL_INCLUDE_ALLOCATORS_H

#include <cstring>
#include <string>

#ifdef WIN32
#ifdef _WIN32_WINNT
#undef _WIN32_WINNT
#endif
#define _WIN32_WINNT 0x0501
#define WIN32_LEAN_AND_MEAN 1
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
/** This is used to attempt to keep keying material out of swap
 *  Note that VirtualLock does not provide this as a guarantee on Windows,
 *  but, in practice, memory that has been VirtualLock'd almost never gets written to
 *  the pagefile except in rare circumstances where memory is extremely low.
 **/
#define mlock(p, n) VirtualLock((p), (n));
#define munlock(p, n) VirtualUnlock((p), (n));
#else
#include <sys/mman.h>
#include <limits.h>
/* This comes from limits.h if it's not defined there set a sane default */
#ifndef PAGESIZE
#include <unistd.h>
#define PAGESIZE sysconf(_SC_PAGESIZE)
#endif
#define mlock(a,b) \
mlock(((void *)(((size_t)(a)) & (~((PAGESIZE)-1)))),\
(((((size_t)(a)) + (b) - 1) | ((PAGESIZE) - 1)) + 1) - (((size_t)(a)) & (~((PAGESIZE) - 1))))
#define munlock(a,b) \
munlock(((void *)(((size_t)(a)) & (~((PAGESIZE)-1)))),\
(((((size_t)(a)) + (b) - 1) | ((PAGESIZE) - 1)) + 1) - (((size_t)(a)) & (~((PAGESIZE) - 1))))
#endif

/**
 *
 * Allocator that locks its contents from being paged
 * out of memory and clears its contents before deletion.
 *
 **/
template<typename T>
struct secure_allocator : public std::allocator<T>
{
    /* MSVC8 default copy constructor is broken */
    typedef std::allocator<T> base;
    typedef typename base::size_type size_type;
    typedef typename base::difference_type  difference_type;
    typedef typename base::pointer pointer;
    typedef typename base::const_pointer const_pointer;
    typedef typename base::reference reference;
    typedef typename base::const_reference const_reference;
    typedef typename base::value_type value_type;
    secure_allocator() throw() {}
    secure_allocator(const secure_allocator& a) throw() : base(a) {}
    template <typename U>
    secure_allocator(const secure_allocator<U>& a) throw() : base(a) {}
    ~secure_allocator() throw() {}
    template<typename _Other> struct rebind
    { typedef secure_allocator<_Other> other; };

    /** allocate
     *
     *  allocates n elements of type T
     *
     **/
    T* allocate(std::size_t n, const void *hint = 0)
    {
        T *p;
        p = std::allocator<T>::allocate(n, hint);
        if (p != NULL)
            mlock(p, sizeof(T) * n);
        return p;
    }


    /** deallocate
     *
     *  frees n elements of type T from pointer p. clears contents before deletion
     *
     **/
    void deallocate(T* p, std::size_t n)
    {
        if (p != NULL)
        {
            memset(p, 0, sizeof(T) * n);
            munlock(p, sizeof(T) * n);
        }
        std::allocator<T>::deallocate(p, n);
    }
};


/**
 *
 * Allocator that clears its contents before deletion.
 *
 **/
template<typename T>
struct zero_after_free_allocator : public std::allocator<T>
{
    /* MSVC8 default copy constructor is broken */
    typedef std::allocator<T> base;
    typedef typename base::size_type size_type;
    typedef typename base::difference_type  difference_type;
    typedef typename base::pointer pointer;
    typedef typename base::const_pointer const_pointer;
    typedef typename base::reference reference;
    typedef typename base::const_reference const_reference;
    typedef typename base::value_type value_type;
    zero_after_free_allocator() throw() {}
    zero_after_free_allocator(const zero_after_free_allocator& a) throw() : base(a) {}
    template <typename U>
    zero_after_free_allocator(const zero_after_free_allocator<U>& a) throw() : base(a) {}
    ~zero_after_free_allocator() throw() {}
    template<typename _Other> struct rebind
    { typedef zero_after_free_allocator<_Other> other; };

    /** deallocate
     *
     *  frees n elements of type T from pointer p. clears contents before deletion
     *
     **/
    void deallocate(T* p, std::size_t n)
    {
        if (p != NULL)
            memset(p, 0, sizeof(T) * n);
        std::allocator<T>::deallocate(p, n);
    }
};

/* This is exactly like std::string, but with a custom allocator. */
typedef std::basic_string<char, std::char_traits<char>, secure_allocator<char> > SecureString;

#endif
