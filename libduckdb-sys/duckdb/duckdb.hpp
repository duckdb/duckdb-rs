/*
Copyright 2018 DuckDB Contributors (see https://github.com/duckdb/duckdb/graphs/contributors)

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

#pragma once
#define DUCKDB_AMALGAMATION 1
#define DUCKDB_SOURCE_ID "d4d56bab1"
#define DUCKDB_VERSION "0.2.8-dev332"
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/connection.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/profiler_format.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/constants.hpp
//
//
//===----------------------------------------------------------------------===//



#include <memory>
#include <cstdint>
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/string.hpp
//
//
//===----------------------------------------------------------------------===//



#include <string>
#include <sstream>

namespace duckdb {
using std::string;
}


namespace duckdb {

//! inline std directives that we use frequently
using std::move;
using std::shared_ptr;
using std::unique_ptr;
using std::weak_ptr;
using data_ptr = unique_ptr<char[]>;
using std::make_shared;

// NOTE: there is a copy of this in the Postgres' parser grammar (gram.y)
#define DEFAULT_SCHEMA "main"
#define TEMP_SCHEMA    "temp"
#define INVALID_SCHEMA ""

//! a saner size_t for loop indices etc
typedef uint64_t idx_t;

//! The type used for row identifiers
typedef int64_t row_t;

//! The type used for hashes
typedef uint64_t hash_t;

//! The value used to signify an invalid index entry
extern const idx_t INVALID_INDEX;

//! data pointers
typedef uint8_t data_t;
typedef data_t *data_ptr_t;
typedef const data_t *const_data_ptr_t;

//! Type used for the selection vector
typedef uint32_t sel_t;
//! Type used for transaction timestamps
typedef idx_t transaction_t;

//! Type used for column identifiers
typedef idx_t column_t;
//! Special value used to signify the ROW ID of a table
extern const column_t COLUMN_IDENTIFIER_ROW_ID;

//! The maximum row identifier used in tables
extern const row_t MAX_ROW_ID;

extern const transaction_t TRANSACTION_ID_START;
extern const transaction_t MAXIMUM_QUERY_ID;
extern const transaction_t NOT_DELETED_ID;

extern const double PI;

struct Storage {
	//! The size of a hard disk sector, only really needed for Direct IO
	constexpr static int SECTOR_SIZE = 4096;
	//! Block header size for blocks written to the storage
	constexpr static int BLOCK_HEADER_SIZE = sizeof(uint64_t);
	// Size of a memory slot managed by the StorageManager. This is the quantum of allocation for Blocks on DuckDB. We
	// default to 256KB. (1 << 18)
	constexpr static int BLOCK_ALLOC_SIZE = 262144;
	//! The actual memory space that is available within the blocks
	constexpr static int BLOCK_SIZE = BLOCK_ALLOC_SIZE - BLOCK_HEADER_SIZE;
	//! The size of the headers. This should be small and written more or less atomically by the hard disk. We default
	//! to the page size, which is 4KB. (1 << 12)
	constexpr static int FILE_HEADER_SIZE = 4096;
};

uint64_t NextPowerOfTwo(uint64_t v);

} // namespace duckdb


namespace duckdb {

enum class ProfilerPrintFormat : uint8_t { NONE, QUERY_TREE, JSON, QUERY_TREE_OPTIMIZER };

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer/buffered_file_writer.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/common.hpp
//
//
//===----------------------------------------------------------------------===//




//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/helper.hpp
//
//
//===----------------------------------------------------------------------===//




#include <string.h>

#ifdef _MSC_VER
#define suint64_t int64_t
#endif

namespace duckdb {
#if !defined(_MSC_VER) && (__cplusplus < 201402L)
template <typename T, typename... Args>
unique_ptr<T> make_unique(Args &&... args) {
	return unique_ptr<T>(new T(std::forward<Args>(args)...));
}
#else // Visual Studio has make_unique
using std::make_unique;
#endif
template <typename S, typename T, typename... Args>
unique_ptr<S> make_unique_base(Args &&... args) {
	return unique_ptr<S>(new T(std::forward<Args>(args)...));
}

template <typename T, typename S>
unique_ptr<S> unique_ptr_cast(unique_ptr<T> src) {
	return unique_ptr<S>(static_cast<S *>(src.release()));
}

struct SharedConstructor {
	template <class T, typename... ARGS>
	static shared_ptr<T> Create(ARGS &&...args) {
		return make_shared<T>(std::forward<ARGS>(args)...);
	}
};

struct UniqueConstructor {
	template <class T, typename... ARGS>
	static unique_ptr<T> Create(ARGS &&...args) {
		return make_unique<T>(std::forward<ARGS>(args)...);
	}
};

template <typename T>
T MaxValue(T a, T b) {
	return a > b ? a : b;
}

template <typename T>
T MinValue(T a, T b) {
	return a < b ? a : b;
}

template <typename T>
const T Load(const_data_ptr_t ptr) {
	T ret;
	memcpy(&ret, ptr, sizeof(ret));
	return ret;
}

template <typename T>
void Store(const T val, data_ptr_t ptr) {
	memcpy(ptr, (void *)&val, sizeof(val));
}

//! This assigns a shared pointer, but ONLY assigns if "target" is not equal to "source"
//! If this is often the case, this manner of assignment is significantly faster (~20X faster)
//! Since it avoids the need of an atomic incref/decref at the cost of a single pointer comparison
//! Benchmark: https://gist.github.com/Mytherin/4db3faa8e233c4a9b874b21f62bb4b96
//! If the shared pointers are not the same, the penalty is very low (on the order of 1%~ slower)
//! This method should always be preferred if there is a (reasonable) chance that the pointers are the same
template<class T>
void AssignSharedPointer(shared_ptr<T> &target, const shared_ptr<T> &source) {
	if (target.get() != source.get()) {
		target = source;
	}
}

} // namespace duckdb


//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/exception.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/assert.hpp
//
//
//===----------------------------------------------------------------------===//



#if (defined(DUCKDB_USE_STANDARD_ASSERT) || !defined(DEBUG)) && !defined(DUCKDB_FORCE_ASSERT)

#include <assert.h>
#define D_ASSERT assert
#else
namespace duckdb {
void DuckDBAssertInternal(bool condition, const char *condition_name, const char *file, int linenr);
}

#define D_ASSERT(condition) duckdb::DuckDBAssertInternal(bool(condition), #condition, __FILE__, __LINE__)

#endif

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector.hpp
//
//
//===----------------------------------------------------------------------===//



#include <vector>

namespace duckdb {
using std::vector;
}

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/exception_format_value.hpp
//
//
//===----------------------------------------------------------------------===//




//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types.hpp
//
//
//===----------------------------------------------------------------------===//





//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/single_thread_ptr.hpp
//
//
//===----------------------------------------------------------------------===//



class RefCounter {
public:
	uint32_t pn;
	RefCounter() : pn(1) {
	}
	void inc() {
		++pn;
	}
	void dec() {
		--pn;
	}
	uint32_t getPn() const {
		return pn;
	}
	virtual ~RefCounter() {
	}
};

namespace duckdb {
template <typename T>
class single_thread_ptr {
public:
	T *ptr;                // contained pointer
	RefCounter *ref_count; // reference counter

public:
	// Default constructor, constructs an empty single_thread_ptr.
	constexpr single_thread_ptr() : ptr(nullptr), ref_count(nullptr) {
	}
	// Construct empty single_thread_ptr.
	constexpr single_thread_ptr(std::nullptr_t) : ptr(nullptr), ref_count(nullptr) {
	}
	// Construct a single_thread_ptr that wraps raw pointer.

	single_thread_ptr(RefCounter *r, T *p) {
		ptr = p;
		ref_count = r;
	}

	template <class U>
	single_thread_ptr(RefCounter *r, U *p) {
		ptr = p;
		ref_count = r;
	}

	// Copy  constructor.
	single_thread_ptr(const single_thread_ptr &sp) : ptr(nullptr), ref_count(nullptr) {
		if (sp.ptr) {
			ptr = sp.ptr;
			ref_count = sp.ref_count;
			ref_count->inc();
		}
	}

	// Conversion constructor.
	template <typename U>
	single_thread_ptr(const single_thread_ptr<U> &sp) : ptr(nullptr), ref_count(nullptr) {
		if (sp.ptr) {
			ptr = sp.ptr;
			ref_count = sp.ref_count;
			ref_count->inc();
		}
	}

	// move  constructor.
	single_thread_ptr(single_thread_ptr &&sp) noexcept : ptr {sp.ptr}, ref_count {sp.ref_count} {
		sp.ptr = nullptr;
		sp.ref_count = nullptr;
	}

	// move  constructor.
	template <class U>
	single_thread_ptr(single_thread_ptr<U> &&sp) noexcept : ptr {sp.ptr}, ref_count {sp.ref_count} {
		sp.ptr = nullptr;
		sp.ref_count = nullptr;
	}

	// No effect if single_thread_ptr is empty or use_count() > 1, otherwise release the resources.
	~single_thread_ptr() {
		release();
	}

	void release() {
		if (ptr && ref_count) {
			ref_count->dec();
			if ((ref_count->getPn()) == 0) {
				delete ref_count;
			}
		}
		ref_count = nullptr;
		ptr = nullptr;
	}

	// Copy assignment.
	single_thread_ptr &operator=(single_thread_ptr sp) noexcept {
		std::swap(this->ptr, sp.ptr);
		std::swap(this->ref_count, sp.ref_count);
		return *this;
	}

	// Dereference pointer to managed object.
	T &operator*() const noexcept {
		return *ptr;
	}
	T *operator->() const noexcept {
		return ptr;
	}

	// Return the contained pointer.
	T *get() const noexcept {
		return ptr;
	}

	// Return use count (use count == 0 if single_thread_ptr is empty).
	long use_count() const noexcept {
		if (ptr)
			return ref_count->getPn();
		else
			return 0;
	}

	// Check if there is an associated managed object.
	explicit operator bool() const noexcept {
		return (ptr);
	}

	// Resets single_thread_ptr to empty.
	void reset() noexcept {
		release();
	}
};

template <class T>
struct _object_and_block : public RefCounter {
	T object;

	template <class... Args>
	explicit _object_and_block(Args &&... args) : object(std::forward<Args>(args)...) {
	}
};

// Operator overloading.
template <typename T, typename U>
inline bool operator==(const single_thread_ptr<T> &sp1, const single_thread_ptr<U> &sp2) {
	return sp1.get() == sp2.get();
}

template <typename T>
inline bool operator==(const single_thread_ptr<T> &sp, std::nullptr_t) noexcept {
	return !sp;
}

template <typename T, typename U>
inline bool operator!=(const single_thread_ptr<T> &sp1, const single_thread_ptr<U> &sp2) {
	return sp1.get() != sp2.get();
}

template <typename T>
inline bool operator!=(const single_thread_ptr<T> &sp, std::nullptr_t) noexcept {
	return sp.get();
}

template <typename T>
inline bool operator!=(std::nullptr_t, const single_thread_ptr<T> &sp) noexcept {
	return sp.get();
}

template <class T, class... Args>
single_thread_ptr<T> single_thread_make_shared(Args &&... args) {
	auto tmp_object = new _object_and_block<T>(std::forward<Args>(args)...);
	return single_thread_ptr<T>(tmp_object, &(tmp_object->object));
}
} // namespace duckdb



#ifndef DUCKDB_API
#ifdef _WIN32
#if defined(DUCKDB_BUILD_LIBRARY) && !defined(DUCKDB_BUILD_LOADABLE_EXTENSION)
#define DUCKDB_API __declspec(dllexport)
#else
#define DUCKDB_API __declspec(dllimport)
#endif
#else
#define DUCKDB_API
#endif
#endif

#ifdef DUCKDB_BUILD_LOADABLE_EXTENSION
#ifdef _WIN32
#define DUCKDB_EXTENSION_API __declspec(dllexport)
#else
#define DUCKDB_EXTENSION_API
#endif
#endif


namespace duckdb {

class Serializer;
class Deserializer;

//! Type used to represent dates (days since 1970-01-01)
struct date_t {
	int32_t days;

	date_t() = default;
	explicit inline date_t(int32_t days_p) : days(days_p) {}

	// explicit conversion
	explicit inline operator int32_t() const {return days;}

	// comparison operators
	inline bool operator==(const date_t &rhs) const {return days == rhs.days;};
	inline bool operator!=(const date_t &rhs) const {return days != rhs.days;};
	inline bool operator<=(const date_t &rhs) const {return days <= rhs.days;};
	inline bool operator<(const date_t &rhs) const {return days < rhs.days;};
	inline bool operator>(const date_t &rhs) const {return days > rhs.days;};
	inline bool operator>=(const date_t &rhs) const {return days >= rhs.days;};

	// arithmetic operators
	inline date_t operator+(const int32_t &days) const {return date_t(this->days + days);};
	inline date_t operator-(const int32_t &days) const {return date_t(this->days - days);};

	// in-place operators
	inline date_t &operator+=(const int32_t &days) {this->days += days; return *this;};
	inline date_t &operator-=(const int32_t &days) {this->days -= days; return *this;};
};

//! Type used to represent time (microseconds)
struct dtime_t {
    int64_t micros;

	dtime_t() = default;
	explicit inline dtime_t(int64_t micros_p) : micros(micros_p) {}
	inline dtime_t& operator=(int64_t micros_p) {micros = micros_p; return *this;}

	// explicit conversion
	explicit inline operator int64_t() const {return micros;}
	explicit inline operator double() const {return micros;}

	// comparison operators
	inline bool operator==(const dtime_t &rhs) const {return micros == rhs.micros;};
	inline bool operator!=(const dtime_t &rhs) const {return micros != rhs.micros;};
	inline bool operator<=(const dtime_t &rhs) const {return micros <= rhs.micros;};
	inline bool operator<(const dtime_t &rhs) const {return micros < rhs.micros;};
	inline bool operator>(const dtime_t &rhs) const {return micros > rhs.micros;};
	inline bool operator>=(const dtime_t &rhs) const {return micros >= rhs.micros;};

	// arithmetic operators
	inline dtime_t operator+(const int64_t &micros) const {return dtime_t(this->micros + micros);};
	inline dtime_t operator+(const double &micros) const {return dtime_t(this->micros + int64_t(micros));};
	inline dtime_t operator-(const int64_t &micros) const {return dtime_t(this->micros - micros);};
	inline dtime_t operator*(const idx_t &copies) const {return dtime_t(this->micros * copies);};
	inline dtime_t operator/(const idx_t &copies) const {return dtime_t(this->micros / copies);};
	inline int64_t operator-(const dtime_t &other) const {return this->micros - other.micros;};

	// in-place operators
	inline dtime_t &operator+=(const int64_t &micros) {this->micros += micros; return *this;};
	inline dtime_t &operator-=(const int64_t &micros) {this->micros -= micros; return *this;};
	inline dtime_t &operator+=(const dtime_t &other) {this->micros += other.micros; return *this;};
};

//! Type used to represent timestamps (seconds,microseconds,milliseconds or nanoseconds since 1970-01-01)
struct timestamp_t {
    int64_t value;

	timestamp_t() = default;
	explicit inline timestamp_t(int64_t value_p) : value(value_p) {}
	inline timestamp_t& operator=(int64_t value_p) {value = value_p; return *this;}

	// explicit conversion
	explicit inline operator int64_t() const {return value;}

	// comparison operators
	inline bool operator==(const timestamp_t &rhs) const {return value == rhs.value;};
	inline bool operator!=(const timestamp_t &rhs) const {return value != rhs.value;};
	inline bool operator<=(const timestamp_t &rhs) const {return value <= rhs.value;};
	inline bool operator<(const timestamp_t &rhs) const {return value < rhs.value;};
	inline bool operator>(const timestamp_t &rhs) const {return value > rhs.value;};
	inline bool operator>=(const timestamp_t &rhs) const {return value >= rhs.value;};

	// arithmetic operators
	inline timestamp_t operator+(const double &value) const {return timestamp_t(this->value + int64_t(value));};
	inline int64_t operator-(const timestamp_t &other) const {return this->value - other.value;};

	// in-place operators
	inline timestamp_t &operator+=(const int64_t &value) {this->value += value; return *this;};
	inline timestamp_t &operator-=(const int64_t &value) {this->value -= value; return *this;};
};

struct interval_t {
	int32_t months;
	int32_t days;
	int64_t micros;

	inline bool operator==(const interval_t &rhs) const {
		return this->days == rhs.days && this->months == rhs.months && this->micros == rhs.micros;
	}
};

struct hugeint_t {
public:
	uint64_t lower;
	int64_t upper;

public:
	hugeint_t() = default;
	hugeint_t(int64_t value); // NOLINT: Allow implicit conversion from `int64_t`
	hugeint_t(const hugeint_t &rhs) = default;
	hugeint_t(hugeint_t &&rhs) = default;
	hugeint_t &operator=(const hugeint_t &rhs) = default;
	hugeint_t &operator=(hugeint_t &&rhs) = default;

	string ToString() const;

	// comparison operators
	bool operator==(const hugeint_t &rhs) const;
	bool operator!=(const hugeint_t &rhs) const;
	bool operator<=(const hugeint_t &rhs) const;
	bool operator<(const hugeint_t &rhs) const;
	bool operator>(const hugeint_t &rhs) const;
	bool operator>=(const hugeint_t &rhs) const;

	// arithmetic operators
	hugeint_t operator+(const hugeint_t &rhs) const;
	hugeint_t operator-(const hugeint_t &rhs) const;
	hugeint_t operator*(const hugeint_t &rhs) const;
	hugeint_t operator/(const hugeint_t &rhs) const;
	hugeint_t operator%(const hugeint_t &rhs) const;
	hugeint_t operator-() const;

	// bitwise operators
	hugeint_t operator>>(const hugeint_t &rhs) const;
	hugeint_t operator<<(const hugeint_t &rhs) const;
	hugeint_t operator&(const hugeint_t &rhs) const;
	hugeint_t operator|(const hugeint_t &rhs) const;
	hugeint_t operator^(const hugeint_t &rhs) const;
	hugeint_t operator~() const;

	// in-place operators
	hugeint_t &operator+=(const hugeint_t &rhs);
	hugeint_t &operator-=(const hugeint_t &rhs);
	hugeint_t &operator*=(const hugeint_t &rhs);
	hugeint_t &operator/=(const hugeint_t &rhs);
	hugeint_t &operator%=(const hugeint_t &rhs);
	hugeint_t &operator>>=(const hugeint_t &rhs);
	hugeint_t &operator<<=(const hugeint_t &rhs);
	hugeint_t &operator&=(const hugeint_t &rhs);
	hugeint_t &operator|=(const hugeint_t &rhs);
	hugeint_t &operator^=(const hugeint_t &rhs);
};

struct string_t;

template <class T>
using child_list_t = std::vector<std::pair<std::string, T>>;
// we should be using single_thread_ptr here but cross-thread access to ChunkCollections currently prohibits this.
template <class T>
using buffer_ptr = shared_ptr<T>;

template <class T, typename... Args>
buffer_ptr<T> make_buffer(Args &&...args) {
	return make_shared<T>(std::forward<Args>(args)...);
}

struct list_entry_t {
	list_entry_t() = default;
	list_entry_t(uint64_t offset, uint64_t length) : offset(offset), length(length) {
	}

	uint64_t offset;
	uint64_t length;
};

//===--------------------------------------------------------------------===//
// Internal Types
//===--------------------------------------------------------------------===//

// taken from arrow's type.h
enum class PhysicalType : uint8_t {
	/// A NULL type having no physical storage
	NA = 0,

	/// Boolean as 8 bit "bool" value
	BOOL = 1,

	/// Unsigned 8-bit little-endian integer
	UINT8 = 2,

	/// Signed 8-bit little-endian integer
	INT8 = 3,

	/// Unsigned 16-bit little-endian integer
	UINT16 = 4,

	/// Signed 16-bit little-endian integer
	INT16 = 5,

	/// Unsigned 32-bit little-endian integer
	UINT32 = 6,

	/// Signed 32-bit little-endian integer
	INT32 = 7,

	/// Unsigned 64-bit little-endian integer
	UINT64 = 8,

	/// Signed 64-bit little-endian integer
	INT64 = 9,

	/// 2-byte floating point value
	HALF_FLOAT = 10,

	/// 4-byte floating point value
	FLOAT = 11,

	/// 8-byte floating point value
	DOUBLE = 12,

	/// UTF8 variable-length string as List<Char>
	STRING = 13,

	/// Variable-length bytes (no guarantee of UTF8-ness)
	BINARY = 14,

	/// Fixed-size binary. Each value occupies the same number of bytes
	FIXED_SIZE_BINARY = 15,

	/// int32_t days since the UNIX epoch
	DATE32 = 16,

	/// int64_t milliseconds since the UNIX epoch
	DATE64 = 17,

	/// Exact timestamp encoded with int64 since UNIX epoch
	/// Default unit millisecond
	TIMESTAMP = 18,

	/// Time as signed 32-bit integer, representing either seconds or
	/// milliseconds since midnight
	TIME32 = 19,

	/// Time as signed 64-bit integer, representing either microseconds or
	/// nanoseconds since midnight
	TIME64 = 20,

	/// YEAR_MONTH or DAY_TIME interval in SQL style
	INTERVAL = 21,

	/// Precision- and scale-based decimal type. Storage type depends on the
	/// parameters.
	// DECIMAL = 22,

	/// A list of some logical data type
	LIST = 23,

	/// Struct of logical types
	STRUCT = 24,

	/// Unions of logical types
	UNION = 25,

	/// Dictionary-encoded type, also called "categorical" or "factor"
	/// in other programming languages. Holds the dictionary value
	/// type but not the dictionary itself, which is part of the
	/// ArrayData struct
	DICTIONARY = 26,

	/// Map, a repeated struct logical type
	MAP = 27,

	/// Custom data type, implemented by user
	EXTENSION = 28,

	/// Fixed size list of some logical type
	FIXED_SIZE_LIST = 29,

	/// Measure of elapsed time in either seconds, milliseconds, microseconds
	/// or nanoseconds.
	DURATION = 30,

	/// Like STRING, but with 64-bit offsets
	LARGE_STRING = 31,

	/// Like BINARY, but with 64-bit offsets
	LARGE_BINARY = 32,

	/// Like LIST, but with 64-bit offsets
	LARGE_LIST = 33,

	// DuckDB Extensions
	VARCHAR = 200, // our own string representation, different from STRING and LARGE_STRING above
	POINTER = 202,
	HASH = 203,
	INT128 = 204, // 128-bit integers

	/// Boolean as 1 bit, LSB bit-packed ordering
	BIT = 205,

	INVALID = 255
};

//===--------------------------------------------------------------------===//
// SQL Types
//===--------------------------------------------------------------------===//
enum class LogicalTypeId : uint8_t {
	INVALID = 0,
	SQLNULL = 1, /* NULL type, used for constant NULL */
	UNKNOWN = 2, /* unknown type, used for parameter expressions */
	ANY = 3,     /* ANY type, used for functions that accept any type as parameter */

	BOOLEAN = 10,
	TINYINT = 11,
	SMALLINT = 12,
	INTEGER = 13,
	BIGINT = 14,
	DATE = 15,
	TIME = 16,
	TIMESTAMP_SEC = 17,
	TIMESTAMP_MS = 18,
	TIMESTAMP = 19, //! us
	TIMESTAMP_NS = 20,
	DECIMAL = 21,
	FLOAT = 22,
	DOUBLE = 23,
	CHAR = 24,
	VARCHAR = 25,
	BLOB = 26,
	INTERVAL = 27,
	UTINYINT = 28,
	USMALLINT = 29,
	UINTEGER = 30,
	UBIGINT = 31,


	HUGEINT = 50,
	POINTER = 51,
	HASH = 52,
	VALIDITY = 53,

	STRUCT = 100,
	LIST = 101,
	MAP = 102,
	TABLE = 103
};

struct ExtraTypeInfo;

struct LogicalType {
	DUCKDB_API LogicalType();
	DUCKDB_API LogicalType(LogicalTypeId id); // NOLINT: Allow implicit conversion from `LogicalTypeId`
	DUCKDB_API LogicalType(LogicalTypeId id, shared_ptr<ExtraTypeInfo> type_info);
	DUCKDB_API LogicalType(const LogicalType &other) :
		id_(other.id_), physical_type_(other.physical_type_), type_info_(other.type_info_) {}

	DUCKDB_API LogicalType(LogicalType &&other) :
		id_(other.id_), physical_type_(other.physical_type_), type_info_(move(other.type_info_)) {}

	DUCKDB_API ~LogicalType();

	LogicalTypeId id() const {
		return id_;
	}
	PhysicalType InternalType() const {
		return physical_type_;
	}
	const ExtraTypeInfo *AuxInfo() const {
		return type_info_.get();
	}

	// copy assignment
	LogicalType& operator=(const LogicalType &other) {
		id_ = other.id_;
		physical_type_ = other.physical_type_;
		type_info_ = other.type_info_;
		return *this;
	}
	// move assignment
	LogicalType& operator=(LogicalType&& other) {
		id_ = other.id_;
		physical_type_ = other.physical_type_;
		type_info_ = move(other.type_info_);
		return *this;
	}

	bool operator==(const LogicalType &rhs) const;
	bool operator!=(const LogicalType &rhs) const {
		return !(*this == rhs);
	}

	//! Serializes a LogicalType to a stand-alone binary blob
	DUCKDB_API void Serialize(Serializer &serializer) const;
	//! Deserializes a blob back into an LogicalType
	DUCKDB_API static LogicalType Deserialize(Deserializer &source);

	DUCKDB_API string ToString() const;
	DUCKDB_API bool IsIntegral() const;
	DUCKDB_API bool IsNumeric() const;
	DUCKDB_API bool IsMoreGenericThan(LogicalType &other) const;
	DUCKDB_API hash_t Hash() const;

	DUCKDB_API static LogicalType MaxLogicalType(const LogicalType &left, const LogicalType &right);

	//! Gets the decimal properties of a numeric type. Fails if the type is not numeric.
	DUCKDB_API bool GetDecimalProperties(uint8_t &width, uint8_t &scale) const;

	DUCKDB_API void Verify() const;

private:
	LogicalTypeId id_;
	PhysicalType physical_type_;
	shared_ptr<ExtraTypeInfo> type_info_;

private:
	PhysicalType GetInternalType();

public:
	DUCKDB_API static const LogicalType SQLNULL;
	DUCKDB_API static const LogicalType BOOLEAN;
	DUCKDB_API static const LogicalType TINYINT;
	DUCKDB_API static const LogicalType UTINYINT;
	DUCKDB_API static const LogicalType SMALLINT;
	DUCKDB_API static const LogicalType USMALLINT;
	DUCKDB_API static const LogicalType INTEGER;
	DUCKDB_API static const LogicalType UINTEGER;
	DUCKDB_API static const LogicalType BIGINT;
	DUCKDB_API static const LogicalType UBIGINT;
	DUCKDB_API static const LogicalType FLOAT;
	DUCKDB_API static const LogicalType DOUBLE;
	DUCKDB_API static const LogicalType DATE;
	DUCKDB_API static const LogicalType TIMESTAMP;
	DUCKDB_API static const LogicalType TIMESTAMP_S;
	DUCKDB_API static const LogicalType TIMESTAMP_MS;
	DUCKDB_API static const LogicalType TIMESTAMP_NS;
	DUCKDB_API static const LogicalType TIME;
	DUCKDB_API static const LogicalType VARCHAR;
	DUCKDB_API static const LogicalType ANY;
	DUCKDB_API static const LogicalType BLOB;
	DUCKDB_API static const LogicalType INTERVAL;
	DUCKDB_API static const LogicalType HUGEINT;
	DUCKDB_API static const LogicalType HASH;
	DUCKDB_API static const LogicalType POINTER;
	DUCKDB_API static const LogicalType TABLE;
	DUCKDB_API static const LogicalType INVALID;

	// explicitly allowing these functions to be capitalized to be in-line with the remaining functions
	DUCKDB_API static LogicalType DECIMAL(int width, int scale);                 // NOLINT
	DUCKDB_API static LogicalType VARCHAR_COLLATION(string collation);           // NOLINT
	DUCKDB_API static LogicalType LIST(LogicalType child);                       // NOLINT
	DUCKDB_API static LogicalType STRUCT(child_list_t<LogicalType> children);    // NOLINT
	DUCKDB_API static LogicalType MAP(child_list_t<LogicalType> children);       // NOLINT

	//! A list of all NUMERIC types (integral and floating point types)
	DUCKDB_API static const vector<LogicalType> NUMERIC;
	//! A list of all INTEGRAL types
	DUCKDB_API static const vector<LogicalType> INTEGRAL;
	//! A list of ALL SQL types
	DUCKDB_API static const vector<LogicalType> ALL_TYPES;
};

struct DecimalType {
	DUCKDB_API static uint8_t GetWidth(const LogicalType &type);
	DUCKDB_API static uint8_t GetScale(const LogicalType &type);
};

struct StringType {
	DUCKDB_API static string GetCollation(const LogicalType &type);
};

struct ListType {
	DUCKDB_API static const LogicalType &GetChildType(const LogicalType &type);
};

struct StructType {
	DUCKDB_API static const child_list_t<LogicalType> &GetChildTypes(const LogicalType &type);
	DUCKDB_API static const LogicalType &GetChildType(const LogicalType &type, idx_t index);
	DUCKDB_API static const string &GetChildName(const LogicalType &type, idx_t index);
	DUCKDB_API static idx_t GetChildCount(const LogicalType &type);
};


string LogicalTypeIdToString(LogicalTypeId type);

LogicalTypeId TransformStringToLogicalType(const string &str);

//! Returns the PhysicalType for the given type
template <class T>
PhysicalType GetTypeId() {
	if (std::is_same<T, bool>()) {
		return PhysicalType::BOOL;
	} else if (std::is_same<T, int8_t>()) {
		return PhysicalType::INT8;
	} else if (std::is_same<T, int16_t>()) {
		return PhysicalType::INT16;
	} else if (std::is_same<T, int32_t>()) {
		return PhysicalType::INT32;
	} else if (std::is_same<T, int64_t>()) {
		return PhysicalType::INT64;
	} else if (std::is_same<T, uint8_t>()) {
		return PhysicalType::UINT8;
	} else if (std::is_same<T, uint16_t>()) {
		return PhysicalType::UINT16;
	} else if (std::is_same<T, uint32_t>()) {
		return PhysicalType::UINT32;
	} else if (std::is_same<T, uint64_t>()) {
		return PhysicalType::UINT64;
	} else if (std::is_same<T, hugeint_t>()) {
		return PhysicalType::INT128;
	} else if (std::is_same<T, uint64_t>()) {
		return PhysicalType::HASH;
	} else if (std::is_same<T, uintptr_t>()) {
		return PhysicalType::POINTER;
	} else if (std::is_same<T, float>()) {
		return PhysicalType::FLOAT;
	} else if (std::is_same<T, double>()) {
		return PhysicalType::DOUBLE;
	} else if (std::is_same<T, const char *>() || std::is_same<T, char *>()) {
		return PhysicalType::VARCHAR;
	} else if (std::is_same<T, interval_t>()) {
		return PhysicalType::INTERVAL;
	} else {
		return PhysicalType::INVALID;
	}
}

template <class T>
bool IsValidType() {
	return GetTypeId<T>() != PhysicalType::INVALID;
}

//! The PhysicalType used by the row identifiers column
extern const LogicalType LOGICAL_ROW_TYPE;
extern const PhysicalType ROW_TYPE;

string TypeIdToString(PhysicalType type);
idx_t GetTypeIdSize(PhysicalType type);
bool TypeIsConstantSize(PhysicalType type);
bool TypeIsIntegral(PhysicalType type);
bool TypeIsNumeric(PhysicalType type);
bool TypeIsInteger(PhysicalType type);

template <class T>
bool IsIntegerType() {
	return TypeIsIntegral(GetTypeId<T>());
}

bool ApproxEqual(float l, float r);
bool ApproxEqual(double l, double r);

} // namespace duckdb


namespace duckdb {

enum class ExceptionFormatValueType : uint8_t {
	FORMAT_VALUE_TYPE_DOUBLE,
	FORMAT_VALUE_TYPE_INTEGER,
	FORMAT_VALUE_TYPE_STRING
};

struct ExceptionFormatValue {
	ExceptionFormatValue(double dbl_val) // NOLINT
	    : type(ExceptionFormatValueType::FORMAT_VALUE_TYPE_DOUBLE), dbl_val(dbl_val) {
	}
	ExceptionFormatValue(int64_t int_val) // NOLINT
	    : type(ExceptionFormatValueType::FORMAT_VALUE_TYPE_INTEGER), int_val(int_val) {
	}
	ExceptionFormatValue(string str_val) // NOLINT
	    : type(ExceptionFormatValueType::FORMAT_VALUE_TYPE_STRING), str_val(move(str_val)) {
	}

	ExceptionFormatValueType type;

	double dbl_val;
	int64_t int_val;
	string str_val;

public:
	template <class T>
	static ExceptionFormatValue CreateFormatValue(T value) {
		return int64_t(value);
	}
	static string Format(const string &msg, vector<ExceptionFormatValue> &values);
};

template <>
ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(PhysicalType value);
template <>
ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(LogicalType value);
template <>
ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(float value);
template <>
ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(double value);
template <>
ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(string value);
template <>
ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(const char *value);
template <>
ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(char *value);

} // namespace duckdb


#include <stdexcept>

namespace duckdb {
enum class PhysicalType : uint8_t;
struct LogicalType;
struct hugeint_t;

inline void assert_restrict_function(void *left_start, void *left_end, void *right_start, void *right_end,
                                     const char *fname, int linenr) {
	// assert that the two pointers do not overlap
#ifdef DEBUG
	if (!(left_end <= right_start || right_end <= left_start)) {
		printf("ASSERT RESTRICT FAILED: %s:%d\n", fname, linenr);
		D_ASSERT(0);
	}
#endif
}

#define ASSERT_RESTRICT(left_start, left_end, right_start, right_end)                                                  \
	assert_restrict_function(left_start, left_end, right_start, right_end, __FILE__, __LINE__)

//===--------------------------------------------------------------------===//
// Exception Types
//===--------------------------------------------------------------------===//

enum class ExceptionType {
	INVALID = 0,          // invalid type
	OUT_OF_RANGE = 1,     // value out of range error
	CONVERSION = 2,       // conversion/casting error
	UNKNOWN_TYPE = 3,     // unknown type
	DECIMAL = 4,          // decimal related
	MISMATCH_TYPE = 5,    // type mismatch
	DIVIDE_BY_ZERO = 6,   // divide by 0
	OBJECT_SIZE = 7,      // object size exceeded
	INVALID_TYPE = 8,     // incompatible for operation
	SERIALIZATION = 9,    // serialization
	TRANSACTION = 10,     // transaction management
	NOT_IMPLEMENTED = 11, // method not implemented
	EXPRESSION = 12,      // expression parsing
	CATALOG = 13,         // catalog related
	PARSER = 14,          // parser related
	PLANNER = 15,         // planner related
	SCHEDULER = 16,       // scheduler related
	EXECUTOR = 17,        // executor related
	CONSTRAINT = 18,      // constraint related
	INDEX = 19,           // index related
	STAT = 20,            // stat related
	CONNECTION = 21,      // connection related
	SYNTAX = 22,          // syntax related
	SETTINGS = 23,        // settings related
	BINDER = 24,          // binder related
	NETWORK = 25,         // network related
	OPTIMIZER = 26,       // optimizer related
	NULL_POINTER = 27,    // nullptr exception
	IO = 28,              // IO exception
	INTERRUPT = 29,       // interrupt
	FATAL = 30, // Fatal exception: fatal exceptions are non-recoverable, and render the entire DB in an unusable state
	INTERNAL =
	    31, // Internal exception: exception that indicates something went wrong internally (i.e. bug in the code base)
	INVALID_INPUT = 32 // Input or arguments error
};

class Exception : public std::exception {
public:
	explicit Exception(const string &msg);
	Exception(ExceptionType exception_type, const string &message);

	ExceptionType type;

public:
	const char *what() const noexcept override;

	string ExceptionTypeToString(ExceptionType type);

	template <typename... Args>
	static string ConstructMessage(const string &msg, Args... params) {
		vector<ExceptionFormatValue> values;
		return ConstructMessageRecursive(msg, values, params...);
	}

	static string ConstructMessageRecursive(const string &msg, vector<ExceptionFormatValue> &values);

	template <class T, typename... Args>
	static string ConstructMessageRecursive(const string &msg, vector<ExceptionFormatValue> &values, T param,
	                                        Args... params) {
		values.push_back(ExceptionFormatValue::CreateFormatValue<T>(param));
		return ConstructMessageRecursive(msg, values, params...);
	}

private:
	string exception_message_;
};

//===--------------------------------------------------------------------===//
// Exception derived classes
//===--------------------------------------------------------------------===//

//! Exceptions that are StandardExceptions do NOT invalidate the current transaction when thrown
class StandardException : public Exception {
public:
	StandardException(ExceptionType exception_type, string message) : Exception(exception_type, message) {
	}
};

class CatalogException : public StandardException {
public:
	explicit CatalogException(const string &msg);

	template <typename... Args>
	explicit CatalogException(const string &msg, Args... params) : CatalogException(ConstructMessage(msg, params...)) {
	}
};

class ParserException : public StandardException {
public:
	explicit ParserException(const string &msg);

	template <typename... Args>
	explicit ParserException(const string &msg, Args... params) : ParserException(ConstructMessage(msg, params...)) {
	}
};

class BinderException : public StandardException {
public:
	explicit BinderException(const string &msg);

	template <typename... Args>
	explicit BinderException(const string &msg, Args... params) : BinderException(ConstructMessage(msg, params...)) {
	}
};

class ConversionException : public Exception {
public:
	explicit ConversionException(const string &msg);

	template <typename... Args>
	explicit ConversionException(const string &msg, Args... params)
	    : ConversionException(ConstructMessage(msg, params...)) {
	}
};

class TransactionException : public Exception {
public:
	explicit TransactionException(const string &msg);

	template <typename... Args>
	explicit TransactionException(const string &msg, Args... params)
	    : TransactionException(ConstructMessage(msg, params...)) {
	}
};

class NotImplementedException : public Exception {
public:
	explicit NotImplementedException(const string &msg);

	template <typename... Args>
	explicit NotImplementedException(const string &msg, Args... params)
	    : NotImplementedException(ConstructMessage(msg, params...)) {
	}
};

class OutOfRangeException : public Exception {
public:
	explicit OutOfRangeException(const string &msg);

	template <typename... Args>
	explicit OutOfRangeException(const string &msg, Args... params)
	    : OutOfRangeException(ConstructMessage(msg, params...)) {
	}
};

class SyntaxException : public Exception {
public:
	explicit SyntaxException(const string &msg);

	template <typename... Args>
	explicit SyntaxException(const string &msg, Args... params) : SyntaxException(ConstructMessage(msg, params...)) {
	}
};

class ConstraintException : public Exception {
public:
	explicit ConstraintException(const string &msg);

	template <typename... Args>
	explicit ConstraintException(const string &msg, Args... params)
	    : ConstraintException(ConstructMessage(msg, params...)) {
	}
};

class IOException : public Exception {
public:
	explicit IOException(const string &msg);

	template <typename... Args>
	explicit IOException(const string &msg, Args... params) : IOException(ConstructMessage(msg, params...)) {
	}
};

class SerializationException : public Exception {
public:
	explicit SerializationException(const string &msg);

	template <typename... Args>
	explicit SerializationException(const string &msg, Args... params)
	    : SerializationException(ConstructMessage(msg, params...)) {
	}
};

class SequenceException : public Exception {
public:
	explicit SequenceException(const string &msg);

	template <typename... Args>
	explicit SequenceException(const string &msg, Args... params)
	    : SequenceException(ConstructMessage(msg, params...)) {
	}
};

class InterruptException : public Exception {
public:
	InterruptException();
};

class FatalException : public Exception {
public:
	explicit FatalException(const string &msg);

	template <typename... Args>
	explicit FatalException(const string &msg, Args... params) : FatalException(ConstructMessage(msg, params...)) {
	}
};

class InternalException : public Exception {
public:
	explicit InternalException(const string &msg);

	template <typename... Args>
	explicit InternalException(const string &msg, Args... params)
	    : InternalException(ConstructMessage(msg, params...)) {
	}
};

class InvalidInputException : public Exception {
public:
	explicit InvalidInputException(const string &msg);

	template <typename... Args>
	explicit InvalidInputException(const string &msg, Args... params)
	    : InvalidInputException(ConstructMessage(msg, params...)) {
	}
};

class CastException : public Exception {
public:
	CastException(const PhysicalType origType, const PhysicalType newType);
	CastException(const LogicalType &origType, const LogicalType &newType);
};

class InvalidTypeException : public Exception {
public:
	InvalidTypeException(PhysicalType type, const string &msg);
	InvalidTypeException(const LogicalType &type, const string &msg);
};

class TypeMismatchException : public Exception {
public:
	TypeMismatchException(const PhysicalType type_1, const PhysicalType type_2, const string &msg);
	TypeMismatchException(const LogicalType &type_1, const LogicalType &type_2, const string &msg);
};

class ValueOutOfRangeException : public Exception {
public:
	ValueOutOfRangeException(const int64_t value, const PhysicalType origType, const PhysicalType newType);
	ValueOutOfRangeException(const hugeint_t value, const PhysicalType origType, const PhysicalType newType);
	ValueOutOfRangeException(const double value, const PhysicalType origType, const PhysicalType newType);
	ValueOutOfRangeException(const PhysicalType varType, const idx_t length);
};

} // namespace duckdb



namespace duckdb {

//! The Serialize class is a base class that can be used to serializing objects into a binary buffer
class Serializer {
public:
	virtual ~Serializer() {
	}

	virtual void WriteData(const_data_ptr_t buffer, idx_t write_size) = 0;

	template <class T>
	void Write(T element) {
		WriteData((const_data_ptr_t)&element, sizeof(T));
	}

	//! Write data from a string buffer directly (wihtout length prefix)
	void WriteBufferData(const string &str) {
		WriteData((const_data_ptr_t)str.c_str(), str.size());
	}
	//! Write a string with a length prefix
	void WriteString(const string &val) {
		Write<uint32_t>((uint32_t)val.size());
		if (val.size() > 0) {
			WriteData((const_data_ptr_t)val.c_str(), val.size());
		}
	}
	void WriteStringLen(const_data_ptr_t val, idx_t len) {
		Write<uint32_t>((uint32_t)len);
		if (len > 0) {
			WriteData(val, len);
		}
	}

	template <class T>
	void WriteList(vector<unique_ptr<T>> &list) {
		Write<uint32_t>((uint32_t)list.size());
		for (auto &child : list) {
			child->Serialize(*this);
		}
	}

	void WriteStringVector(const vector<string> &list) {
		Write<uint32_t>((uint32_t)list.size());
		for (auto &child : list) {
			WriteString(child);
		}
	}

	template <class T>
	void WriteOptional(const unique_ptr<T> &element) {
		Write<bool>(element ? true : false);
		if (element) {
			element->Serialize(*this);
		}
	}
};

//! The Deserializer class assists in deserializing a binary blob back into an
//! object
class Deserializer {
public:
	virtual ~Deserializer() {
	}

	//! Reads [read_size] bytes into the buffer
	virtual void ReadData(data_ptr_t buffer, idx_t read_size) = 0;

	template <class T>
	T Read() {
		T value;
		ReadData((data_ptr_t)&value, sizeof(T));
		return value;
	}
	template <class T>
	void ReadList(vector<unique_ptr<T>> &list) {
		auto select_count = Read<uint32_t>();
		for (uint32_t i = 0; i < select_count; i++) {
			auto child = T::Deserialize(*this);
			list.push_back(move(child));
		}
	}

	template <class T, class RETURN_TYPE = T>
	unique_ptr<RETURN_TYPE> ReadOptional() {
		auto has_entry = Read<bool>();
		if (has_entry) {
			return T::Deserialize(*this);
		}
		return nullptr;
	}

	void ReadStringVector(vector<string> &list);
};

template <>
string Deserializer::Read();

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/file_system.hpp
//
//
//===----------------------------------------------------------------------===//




//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/file_buffer.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {
class Allocator;
struct FileHandle;

enum class FileBufferType : uint8_t { BLOCK = 1, MANAGED_BUFFER = 2 };

//! The FileBuffer represents a buffer that can be read or written to a Direct IO FileHandle.
class FileBuffer {
public:
	//! Allocates a buffer of the specified size that is sector-aligned. bufsiz must be a multiple of
	//! FileSystemConstants::FILE_BUFFER_BLOCK_SIZE. The content in this buffer can be written to FileHandles that have
	//! been opened with DIRECT_IO on all operating systems, however, the entire buffer must be written to the file.
	//! Note that the returned size is 8 bytes less than the allocation size to account for the checksum.
	FileBuffer(Allocator &allocator, FileBufferType type, uint64_t bufsiz);
	virtual ~FileBuffer();

	Allocator &allocator;
	//! The type of the buffer
	FileBufferType type;
	//! The buffer that users can write to
	data_ptr_t buffer;
	//! The size of the portion that users can write to, this is equivalent to internal_size - BLOCK_HEADER_SIZE
	uint64_t size;

public:
	//! Read into the FileBuffer from the specified location. Automatically verifies the checksum, and throws an
	//! exception if the checksum does not match correctly.
	void Read(FileHandle &handle, uint64_t location);
	//! Write the contents of the FileBuffer to the specified location. Automatically adds a checksum of the contents of
	//! the filebuffer in front of the written data.
	void Write(FileHandle &handle, uint64_t location);

	void Clear();

	uint64_t AllocSize() {
		return internal_size;
	}

private:
	//! The pointer to the internal buffer that will be read or written, including the buffer header
	data_ptr_t internal_buffer;
	//! The aligned size as passed to the constructor. This is the size that is read or written to disk.
	uint64_t internal_size;

	//! The buffer that was actually malloc'd, i.e. the pointer that must be freed when the FileBuffer is destroyed
	data_ptr_t malloced_buffer;
	uint64_t malloced_size;
};

} // namespace duckdb


//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/unordered_map.hpp
//
//
//===----------------------------------------------------------------------===//



#include <unordered_map>

namespace duckdb {
using std::unordered_map;
}


//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/file_compression_type.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

enum class FileCompressionType : uint8_t { AUTO_DETECT = 0, UNCOMPRESSED = 1, GZIP = 2 };

} // namespace duckdb


#include <functional>

#undef CreateDirectory
#undef MoveFile
#undef RemoveDirectory

namespace duckdb {
class ClientContext;
class DatabaseInstance;
class FileSystem;

struct FileHandle {
public:
	FileHandle(FileSystem &file_system, string path) : file_system(file_system), path(path) {
	}
	FileHandle(const FileHandle &) = delete;
	virtual ~FileHandle() {
	}

	int64_t Read(void *buffer, idx_t nr_bytes);
	int64_t Write(void *buffer, idx_t nr_bytes);
	void Read(void *buffer, idx_t nr_bytes, idx_t location);
	void Write(void *buffer, idx_t nr_bytes, idx_t location);
	void Seek(idx_t location);
	void Reset();
	idx_t SeekPosition();
	void Sync();
	void Truncate(int64_t new_size);
	string ReadLine();

	bool CanSeek();
	bool OnDiskFile();
	idx_t GetFileSize();

protected:
	virtual void Close() = 0;

public:
	FileSystem &file_system;
	string path;
};

enum class FileLockType : uint8_t { NO_LOCK = 0, READ_LOCK = 1, WRITE_LOCK = 2 };

class FileFlags {
public:
	//! Open file with read access
	static constexpr uint8_t FILE_FLAGS_READ = 1 << 0;
	//! Open file with read/write access
	static constexpr uint8_t FILE_FLAGS_WRITE = 1 << 1;
	//! Use direct IO when reading/writing to the file
	static constexpr uint8_t FILE_FLAGS_DIRECT_IO = 1 << 2;
	//! Create file if not exists, can only be used together with WRITE
	static constexpr uint8_t FILE_FLAGS_FILE_CREATE = 1 << 3;
	//! Always create a new file. If a file exists, the file is truncated. Cannot be used together with CREATE.
	static constexpr uint8_t FILE_FLAGS_FILE_CREATE_NEW = 1 << 4;
	//! Open file in append mode
	static constexpr uint8_t FILE_FLAGS_APPEND = 1 << 5;
};

class FileSystem {
public:
	virtual ~FileSystem() {
	}

public:
	static FileSystem &GetFileSystem(ClientContext &context);
	static FileSystem &GetFileSystem(DatabaseInstance &db);

	virtual unique_ptr<FileHandle> OpenFile(const string &path, uint8_t flags,
	                                        FileLockType lock = FileLockType::NO_LOCK,
	                                        FileCompressionType compression = FileCompressionType::UNCOMPRESSED);

	//! Read exactly nr_bytes from the specified location in the file. Fails if nr_bytes could not be read. This is
	//! equivalent to calling SetFilePointer(location) followed by calling Read().
	virtual void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location);
	//! Write exactly nr_bytes to the specified location in the file. Fails if nr_bytes could not be read. This is
	//! equivalent to calling SetFilePointer(location) followed by calling Write().
	virtual void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location);
	//! Read nr_bytes from the specified file into the buffer, moving the file pointer forward by nr_bytes. Returns the
	//! amount of bytes read.
	virtual int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes);
	//! Write nr_bytes from the buffer into the file, moving the file pointer forward by nr_bytes.
	virtual int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes);

	//! Returns the file size of a file handle, returns -1 on error
	virtual int64_t GetFileSize(FileHandle &handle);
	//! Returns the file last modified time of a file handle, returns timespec with zero on all attributes on error
	virtual time_t GetLastModifiedTime(FileHandle &handle);
	//! Truncate a file to a maximum size of new_size, new_size should be smaller than or equal to the current size of
	//! the file
	virtual void Truncate(FileHandle &handle, int64_t new_size);

	//! Check if a directory exists
	virtual bool DirectoryExists(const string &directory);
	//! Create a directory if it does not exist
	virtual void CreateDirectory(const string &directory);
	//! Recursively remove a directory and all files in it
	virtual void RemoveDirectory(const string &directory);
	//! List files in a directory, invoking the callback method for each one with (filename, is_dir)
	virtual bool ListFiles(const string &directory, const std::function<void(string, bool)> &callback);
	//! Move a file from source path to the target, StorageManager relies on this being an atomic action for ACID
	//! properties
	virtual void MoveFile(const string &source, const string &target);
	//! Check if a file exists
	virtual bool FileExists(const string &filename);
	//! Remove a file from disk
	virtual void RemoveFile(const string &filename);
	//! Path separator for the current file system
	virtual string PathSeparator();
	//! Join two paths together
	virtual string JoinPath(const string &a, const string &path);
	//! Convert separators in a path to the local separators (e.g. convert "/" into \\ on windows)
	virtual string ConvertSeparators(const string &path);
	//! Extract the base name of a file (e.g. if the input is lib/example.dll the base name is example)
	virtual string ExtractBaseName(const string &path);
	//! Sync a file handle to disk
	virtual void FileSync(FileHandle &handle);

	//! Sets the working directory
	virtual void SetWorkingDirectory(const string &path);
	//! Gets the working directory
	virtual string GetWorkingDirectory();
	//! Gets the users home directory
	virtual string GetHomeDirectory();

	//! Runs a glob on the file system, returning a list of matching files
	virtual vector<string> Glob(const string &path);

	//! Returns the system-available memory in bytes
	virtual idx_t GetAvailableMemory();

	//! registers a sub-file system to handle certain file name prefixes, e.g. http:// etc.
	virtual void RegisterSubSystem(unique_ptr<FileSystem> sub_fs) {
		throw NotImplementedException("Can't register a sub system on a non-virtual file system");
	}

	virtual bool CanHandleFile(const string &fpath) {
		//! Whether or not a sub-system can handle a specific file path
		return false;
	}

	//! Set the file pointer of a file handle to a specified location. Reads and writes will happen from this location
	virtual void Seek(FileHandle &handle, idx_t location);
	//! Reset a file to the beginning (equivalent to Seek(handle, 0) for simple files)
	virtual void Reset(FileHandle &handle);
	virtual idx_t SeekPosition(FileHandle &handle);

	//! Whether or not we can seek into the file
	virtual bool CanSeek();
	//! Whether or not the FS handles plain files on disk. This is relevant for certain optimizations, as random reads
	//! in a file on-disk are much cheaper than e.g. random reads in a file over the network
	virtual bool OnDiskFile(FileHandle &handle);

private:
	//! Set the file pointer of a file handle to a specified location. Reads and writes will happen from this location
	void SetFilePointer(FileHandle &handle, idx_t location);
	virtual idx_t GetFilePointer(FileHandle &handle);
};

// bunch of wrappers to allow registering protocol handlers
class VirtualFileSystem : public FileSystem {
public:
	unique_ptr<FileHandle> OpenFile(const string &path, uint8_t flags, FileLockType lock = FileLockType::NO_LOCK,
	                                FileCompressionType compression = FileCompressionType::UNCOMPRESSED) override;

	virtual void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		handle.file_system.Read(handle, buffer, nr_bytes, location);
	};

	virtual void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		handle.file_system.Write(handle, buffer, nr_bytes, location);
	}

	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override {
		return handle.file_system.Read(handle, buffer, nr_bytes);
	}

	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override {
		return handle.file_system.Write(handle, buffer, nr_bytes);
	}

	int64_t GetFileSize(FileHandle &handle) override {
		return handle.file_system.GetFileSize(handle);
	}
	time_t GetLastModifiedTime(FileHandle &handle) override {
		return handle.file_system.GetLastModifiedTime(handle);
	}

	void Truncate(FileHandle &handle, int64_t new_size) override {
		handle.file_system.Truncate(handle, new_size);
	}

	void FileSync(FileHandle &handle) override {
		handle.file_system.FileSync(handle);
	}

	// need to look up correct fs for this
	bool DirectoryExists(const string &directory) override {
		return FindFileSystem(directory)->DirectoryExists(directory);
	}
	void CreateDirectory(const string &directory) override {
		FindFileSystem(directory)->CreateDirectory(directory);
	}

	void RemoveDirectory(const string &directory) override {
		FindFileSystem(directory)->RemoveDirectory(directory);
	}

	bool ListFiles(const string &directory, const std::function<void(string, bool)> &callback) override {
		return FindFileSystem(directory)->ListFiles(directory, callback);
	}

	void MoveFile(const string &source, const string &target) override {
		FindFileSystem(source)->MoveFile(source, target);
	}

	bool FileExists(const string &filename) override {
		return FindFileSystem(filename)->FileExists(filename);
	}

	virtual void RemoveFile(const string &filename) override {
		FindFileSystem(filename)->RemoveFile(filename);
	}

	vector<string> Glob(const string &path) override {
		return FindFileSystem(path)->Glob(path);
	}

	// these goes to the default fs
	void SetWorkingDirectory(const string &path) override {
		default_fs.SetWorkingDirectory(path);
	}

	string GetWorkingDirectory() override {
		return default_fs.GetWorkingDirectory();
	}

	string GetHomeDirectory() override {
		return default_fs.GetWorkingDirectory();
	}

	idx_t GetAvailableMemory() override {
		return default_fs.GetAvailableMemory();
	}

	void RegisterSubSystem(unique_ptr<FileSystem> fs) override {
		sub_systems.push_back(move(fs));
	}

private:
	FileSystem *FindFileSystem(const string &path) {
		for (auto &sub_system : sub_systems) {
			if (sub_system->CanHandleFile(path)) {
				return sub_system.get();
			}
		}
		return &default_fs;
	}

private:
	vector<unique_ptr<FileSystem>> sub_systems;
	FileSystem default_fs;
};

} // namespace duckdb


namespace duckdb {

#define FILE_BUFFER_SIZE 4096

class BufferedFileWriter : public Serializer {
public:
	//! Serializes to a buffer allocated by the serializer, will expand when
	//! writing past the initial threshold
	BufferedFileWriter(FileSystem &fs, const string &path,
	                   uint8_t open_flags = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);

	FileSystem &fs;
	unique_ptr<data_t[]> data;
	idx_t offset;
	idx_t total_written;
	unique_ptr<FileHandle> handle;

public:
	void WriteData(const_data_ptr_t buffer, uint64_t write_size) override;
	//! Flush the buffer to disk and sync the file to ensure writing is completed
	void Sync();
	//! Flush the buffer to the file (without sync)
	void Flush();
	//! Returns the current size of the file
	int64_t GetFileSize();
	//! Truncate the size to a previous size (given that size <= GetFileSize())
	void Truncate(int64_t size);

	idx_t GetTotalWritten();
};

} // namespace duckdb


//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/udf_function.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar_function.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/binary_executor.hpp
//
//
//===----------------------------------------------------------------------===//




//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/vector.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/bitset.hpp
//
//
//===----------------------------------------------------------------------===//



#include <bitset>

namespace duckdb {
using std::bitset;
}


//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/selection_vector.hpp
//
//
//===----------------------------------------------------------------------===//





//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_size.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

//! The vector size used in the execution engine
#ifndef STANDARD_VECTOR_SIZE
#define STANDARD_VECTOR_SIZE 1024
#endif

#if ((STANDARD_VECTOR_SIZE & (STANDARD_VECTOR_SIZE - 1)) != 0)
#error Vector size should be a power of two
#endif

//! Zero selection vector: completely filled with the value 0 [READ ONLY]
extern const sel_t ZERO_VECTOR[STANDARD_VECTOR_SIZE];

} // namespace duckdb


namespace duckdb {
class VectorBuffer;

struct SelectionData {
	explicit SelectionData(idx_t count) {
		owned_data = unique_ptr<sel_t[]>(new sel_t[count]);
	}

	unique_ptr<sel_t[]> owned_data;
};

struct SelectionVector {
	SelectionVector() : sel_vector(nullptr) {
	}
	explicit SelectionVector(sel_t *sel) {
		Initialize(sel);
	}
	explicit SelectionVector(idx_t count) {
		Initialize(count);
	}
	SelectionVector(idx_t start, idx_t count) {
		Initialize(STANDARD_VECTOR_SIZE);
		for (idx_t i = 0; i < count; i++) {
			set_index(i, start + i);
		}
	}
	SelectionVector(const SelectionVector &sel_vector) {
		Initialize(sel_vector);
	}
	explicit SelectionVector(buffer_ptr<SelectionData> data) {
		Initialize(move(data));
	}

public:
	void Initialize(sel_t *sel) {
		selection_data.reset();
		sel_vector = sel;
	}
	void Initialize(idx_t count = STANDARD_VECTOR_SIZE) {
		selection_data = make_buffer<SelectionData>(count);
		sel_vector = selection_data->owned_data.get();
	}
	void Initialize(buffer_ptr<SelectionData> data) {
		selection_data = move(data);
		sel_vector = selection_data->owned_data.get();
	}
	void Initialize(const SelectionVector &other) {
		selection_data = other.selection_data;
		sel_vector = other.sel_vector;
	}

	bool empty() const {
		return !sel_vector;
	}
	void set_index(idx_t idx, idx_t loc) {
		sel_vector[idx] = loc;
	}
	void swap(idx_t i, idx_t j) {
		sel_t tmp = sel_vector[i];
		sel_vector[i] = sel_vector[j];
		sel_vector[j] = tmp;
	}
	idx_t get_index(idx_t idx) const {
		return sel_vector[idx];
	}
	sel_t *data() {
		return sel_vector;
	}
	buffer_ptr<SelectionData> sel_data() {
		return selection_data;
	}
	buffer_ptr<SelectionData> Slice(const SelectionVector &sel, idx_t count) const;

	string ToString(idx_t count = 0) const;
	void Print(idx_t count = 0) const;

	sel_t &operator[](idx_t index) {
		return sel_vector[index];
	}

private:
	sel_t *sel_vector;
	buffer_ptr<SelectionData> selection_data;
};

class OptionalSelection {
public:
	explicit inline OptionalSelection(SelectionVector *sel_p) : sel(sel_p) {

		if (sel) {
			vec.Initialize(sel->data());
			sel = &vec;
		}
	}

	inline operator SelectionVector *() {
		return sel;
	}

	inline void Append(idx_t &count, const idx_t idx) {
		if (sel) {
			sel->set_index(count, idx);
		}
		++count;
	}

	inline void Advance(idx_t completed) {
		if (sel) {
			sel->Initialize(sel->data() + completed);
		}
	}

private:
	SelectionVector *sel;
	SelectionVector vec;
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/value.hpp
//
//
//===----------------------------------------------------------------------===//








namespace duckdb {

class Deserializer;
class Serializer;

//! The Value object holds a single arbitrary value of any type that can be
//! stored in the database.
class Value {
	friend class Vector;

public:
	//! Create an empty NULL value of the specified type
	explicit Value(LogicalType type = LogicalType::SQLNULL);
	//! Create an INTEGER value
	Value(int32_t val); // NOLINT: Allow implicit conversion from `int32_t`
	//! Create a BIGINT value
	Value(int64_t val); // NOLINT: Allow implicit conversion from `int64_t`
	//! Create a FLOAT value
	Value(float val); // NOLINT: Allow implicit conversion from `float`
	//! Create a DOUBLE value
	Value(double val); // NOLINT: Allow implicit conversion from `double`
	//! Create a VARCHAR value
	Value(const char *val); // NOLINT: Allow implicit conversion from `const char *`
	//! Create a NULL value
	Value(std::nullptr_t val); // NOLINT: Allow implicit conversion from `nullptr_t`
	//! Create a VARCHAR value
	Value(string_t val); // NOLINT: Allow implicit conversion from `string_t`
	//! Create a VARCHAR value
	Value(string val); // NOLINT: Allow implicit conversion from `string`

	const LogicalType &type() const {
		return type_;
	}

	//! Create the lowest possible value of a given type (numeric only)
	static Value MinimumValue(const LogicalType &type);
	//! Create the highest possible value of a given type (numeric only)
	static Value MaximumValue(const LogicalType &type);
	//! Create a Numeric value of the specified type with the specified value
	static Value Numeric(const LogicalType &type, int64_t value);
	static Value Numeric(const LogicalType &type, hugeint_t value);

	//! Create a tinyint Value from a specified value
	static Value BOOLEAN(int8_t value);
	//! Create a tinyint Value from a specified value
	static Value TINYINT(int8_t value);
	//! Create a smallint Value from a specified value
	static Value SMALLINT(int16_t value);
	//! Create an integer Value from a specified value
	static Value INTEGER(int32_t value);
	//! Create a bigint Value from a specified value
	static Value BIGINT(int64_t value);
	//! Create an unsigned tinyint Value from a specified value
	static Value UTINYINT(uint8_t value);
	//! Create an unsigned smallint Value from a specified value
	static Value USMALLINT(uint16_t value);
	//! Create an unsigned integer Value from a specified value
	static Value UINTEGER(uint32_t value);
	//! Create an unsigned bigint Value from a specified value
	static Value UBIGINT(uint64_t value);
	//! Create a hugeint Value from a specified value
	static Value HUGEINT(hugeint_t value);
	//! Create a hash Value from a specified value
	static Value HASH(hash_t value);
	//! Create a pointer Value from a specified value
	static Value POINTER(uintptr_t value);
	//! Create a date Value from a specified date
	static Value DATE(date_t date);
	//! Create a date Value from a specified date
	static Value DATE(int32_t year, int32_t month, int32_t day);
	//! Create a time Value from a specified time
	static Value TIME(dtime_t time);
	//! Create a time Value from a specified time
	static Value TIME(int32_t hour, int32_t min, int32_t sec, int32_t micros);
	//! Create a timestamp Value from a specified date/time combination
	static Value TIMESTAMP(date_t date, dtime_t time);
	//! Create a timestamp Value from a specified timestamp
	static Value TIMESTAMP(timestamp_t timestamp);
	static Value TimestampNs(timestamp_t timestamp);
	static Value TimestampMs(timestamp_t timestamp);
	static Value TimestampSec(timestamp_t timestamp);
	//! Create a timestamp Value from a specified timestamp in separate values
	static Value TIMESTAMP(int32_t year, int32_t month, int32_t day, int32_t hour, int32_t min, int32_t sec,
	                       int32_t micros);
	static Value INTERVAL(int32_t months, int32_t days, int64_t micros);
	static Value INTERVAL(interval_t interval);

	// Decimal values
	static Value DECIMAL(int16_t value, uint8_t width, uint8_t scale);
	static Value DECIMAL(int32_t value, uint8_t width, uint8_t scale);
	static Value DECIMAL(int64_t value, uint8_t width, uint8_t scale);
	static Value DECIMAL(hugeint_t value, uint8_t width, uint8_t scale);
	//! Create a float Value from a specified value
	static Value FLOAT(float value);
	//! Create a double Value from a specified value
	static Value DOUBLE(double value);
	//! Create a struct value with given list of entries
	static Value STRUCT(child_list_t<Value> values);
	//! Create a list value with the given entries
	static Value LIST(vector<Value> values);
	//! Creat a map value from a (key, value) pair
	static Value MAP(Value key, Value value);

	//! Create a blob Value from a data pointer and a length: no bytes are interpreted
	static Value BLOB(const_data_ptr_t data, idx_t len);
	static Value BLOB_RAW(const string &data) {
		return Value::BLOB((const_data_ptr_t)data.c_str(), data.size());
	}
	//! Creates a blob by casting a specified string to a blob (i.e. interpreting \x characters)
	static Value BLOB(const string &data);

	template <class T>
	T GetValue() const {
		throw NotImplementedException("Unimplemented template type for Value::GetValue");
	}
	template <class T>
	static Value CreateValue(T value) {
		throw NotImplementedException("Unimplemented template type for Value::CreateValue");
	}
	// Returns the internal value. Unlike GetValue(), this method does not perform casting, and assumes T matches the
	// type of the value. Only use this if you know what you are doing.
	template <class T>
	T &GetValueUnsafe() {
		throw NotImplementedException("Unimplemented template type for Value::GetValueUnsafe");
	}

	//! Return a copy of this value
	Value Copy() const {
		return Value(*this);
	}

	//! Convert this value to a string
	DUCKDB_API string ToString() const;

	DUCKDB_API uintptr_t GetPointer() const;

	//! Cast this value to another type
	DUCKDB_API Value CastAs(const LogicalType &target_type, bool strict = false) const;
	//! Tries to cast value to another type, throws exception if its not possible
	DUCKDB_API bool TryCastAs(const LogicalType &target_type, bool strict = false);

	//! Serializes a Value to a stand-alone binary blob
	DUCKDB_API void Serialize(Serializer &serializer);
	//! Deserializes a Value from a blob
	DUCKDB_API static Value Deserialize(Deserializer &source);

	//===--------------------------------------------------------------------===//
	// Numeric Operators
	//===--------------------------------------------------------------------===//
	Value operator+(const Value &rhs) const;
	Value operator-(const Value &rhs) const;
	Value operator*(const Value &rhs) const;
	Value operator/(const Value &rhs) const;
	Value operator%(const Value &rhs) const;

	//===--------------------------------------------------------------------===//
	// Comparison Operators
	//===--------------------------------------------------------------------===//
	bool operator==(const Value &rhs) const;
	bool operator!=(const Value &rhs) const;
	bool operator<(const Value &rhs) const;
	bool operator>(const Value &rhs) const;
	bool operator<=(const Value &rhs) const;
	bool operator>=(const Value &rhs) const;

	bool operator==(const int64_t &rhs) const;
	bool operator!=(const int64_t &rhs) const;
	bool operator<(const int64_t &rhs) const;
	bool operator>(const int64_t &rhs) const;
	bool operator<=(const int64_t &rhs) const;
	bool operator>=(const int64_t &rhs) const;

	static bool FloatIsValid(float value);
	static bool DoubleIsValid(double value);
	static bool StringIsValid(const char *str, idx_t length);
	static bool StringIsValid(const string &str) {
		return StringIsValid(str.c_str(), str.size());
	}

	template <class T>
	static bool IsValid(T value) {
		return true;
	}

	//! Returns true if the values are (approximately) equivalent. Note this is NOT the SQL equivalence. For this
	//! function, NULL values are equivalent and floating point values that are close are equivalent.
	static bool ValuesAreEqual(const Value &result_value, const Value &value);

	friend std::ostream &operator<<(std::ostream &out, const Value &val) {
		out << val.ToString();
		return out;
	}
	void Print();

private:
	//! The logical of the value
	LogicalType type_;

public:
	//! Whether or not the value is NULL
	bool is_null;

	//! The value of the object, if it is of a constant size Type
	union Val {
		int8_t boolean;
		int8_t tinyint;
		int16_t smallint;
		int32_t integer;
		int64_t bigint;
		uint8_t utinyint;
		uint16_t usmallint;
		uint32_t uinteger;
		uint64_t ubigint;
		hugeint_t hugeint;
		float float_;
		double double_;
		uintptr_t pointer;
		uint64_t hash;
		date_t date;
		dtime_t time;
		timestamp_t timestamp;
		interval_t interval;
	} value_;

	//! The value of the object, if it is of a variable size type
	string str_value;

	vector<Value> struct_value;
	vector<Value> list_value;

private:
	template <class T>
	T GetValueInternal() const;
	//! Templated helper function for casting
	template <class DST, class OP>
	static DST _cast(const Value &v);

	//! Templated helper function for binary operations
	template <class OP>
	static void _templated_binary_operation(const Value &left, const Value &right, Value &result, bool ignore_null);

	//! Templated helper function for boolean operations
	template <class OP>
	static bool _templated_boolean_operation(const Value &left, const Value &right);
};

template <>
Value DUCKDB_API Value::CreateValue(bool value);
template <>
Value DUCKDB_API Value::CreateValue(uint8_t value);
template <>
Value DUCKDB_API Value::CreateValue(uint16_t value);
template <>
Value DUCKDB_API Value::CreateValue(uint32_t value);
template <>
Value DUCKDB_API Value::CreateValue(uint64_t value);
template <>
Value DUCKDB_API Value::CreateValue(int8_t value);
template <>
Value DUCKDB_API Value::CreateValue(int16_t value);
template <>
Value DUCKDB_API Value::CreateValue(int32_t value);
template <>
Value DUCKDB_API Value::CreateValue(int64_t value);
template <>
Value DUCKDB_API Value::CreateValue(hugeint_t value);
template <>
Value DUCKDB_API Value::CreateValue(date_t value);
template <>
Value DUCKDB_API Value::CreateValue(dtime_t value);
template <>
Value DUCKDB_API Value::CreateValue(timestamp_t value);
template <>
Value DUCKDB_API Value::CreateValue(const char *value);
template <>
Value DUCKDB_API Value::CreateValue(string value);
template <>
Value DUCKDB_API Value::CreateValue(string_t value);
template <>
Value DUCKDB_API Value::CreateValue(float value);
template <>
Value DUCKDB_API Value::CreateValue(double value);
template <>
Value DUCKDB_API Value::CreateValue(interval_t value);
template <>
Value DUCKDB_API Value::CreateValue(Value value);

template <>
DUCKDB_API bool Value::GetValue() const;
template <>
DUCKDB_API int8_t Value::GetValue() const;
template <>
DUCKDB_API int16_t Value::GetValue() const;
template <>
DUCKDB_API int32_t Value::GetValue() const;
template <>
DUCKDB_API int64_t Value::GetValue() const;
template <>
DUCKDB_API uint8_t Value::GetValue() const;
template <>
DUCKDB_API uint16_t Value::GetValue() const;
template <>
DUCKDB_API uint32_t Value::GetValue() const;
template <>
DUCKDB_API uint64_t Value::GetValue() const;
template <>
DUCKDB_API hugeint_t Value::GetValue() const;
template <>
DUCKDB_API string Value::GetValue() const;
template <>
DUCKDB_API float Value::GetValue() const;
template <>
DUCKDB_API double Value::GetValue() const;
template <>
DUCKDB_API date_t Value::GetValue() const;
template <>
DUCKDB_API dtime_t Value::GetValue() const;
template <>
DUCKDB_API timestamp_t Value::GetValue() const;

template <>
DUCKDB_API int8_t &Value::GetValueUnsafe();
template <>
DUCKDB_API int16_t &Value::GetValueUnsafe();
template <>
DUCKDB_API int32_t &Value::GetValueUnsafe();
template <>
DUCKDB_API int64_t &Value::GetValueUnsafe();
template <>
DUCKDB_API hugeint_t &Value::GetValueUnsafe();
template <>
DUCKDB_API uint8_t &Value::GetValueUnsafe();
template <>
DUCKDB_API uint16_t &Value::GetValueUnsafe();
template <>
DUCKDB_API uint32_t &Value::GetValueUnsafe();
template <>
DUCKDB_API uint64_t &Value::GetValueUnsafe();
template <>
DUCKDB_API string &Value::GetValueUnsafe();
template <>
DUCKDB_API float &Value::GetValueUnsafe();
template <>
DUCKDB_API double &Value::GetValueUnsafe();
template <>
DUCKDB_API date_t &Value::GetValueUnsafe();
template <>
DUCKDB_API dtime_t &Value::GetValueUnsafe();
template <>
DUCKDB_API timestamp_t &Value::GetValueUnsafe();
template <>
DUCKDB_API interval_t &Value::GetValueUnsafe();

template <>
DUCKDB_API bool Value::IsValid(float value);
template <>
DUCKDB_API bool Value::IsValid(double value);

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/vector_type.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

enum class VectorType : uint8_t {
	FLAT_VECTOR,       // Flat vectors represent a standard uncompressed vector
	CONSTANT_VECTOR,   // Constant vector represents a single constant
	DICTIONARY_VECTOR, // Dictionary vector represents a selection vector on top of another vector
	SEQUENCE_VECTOR    // Sequence vector represents a sequence with a start point and an increment
};

string VectorTypeToString(VectorType type);

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/vector_buffer.hpp
//
//
//===----------------------------------------------------------------------===//




//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/string_heap.hpp
//
//
//===----------------------------------------------------------------------===//






namespace duckdb {
//! A string heap is the owner of a set of strings, strings can be inserted into
//! it On every insert, a pointer to the inserted string is returned The
//! returned pointer will remain valid until the StringHeap is destroyed
class StringHeap {
public:
	StringHeap();

	void Destroy() {
		tail = nullptr;
		chunk = nullptr;
	}

	void Move(StringHeap &other) {
		D_ASSERT(!other.chunk);
		other.tail = tail;
		other.chunk = move(chunk);
		tail = nullptr;
	}

	//! Add a string to the string heap, returns a pointer to the string
	string_t AddString(const char *data, idx_t len);
	//! Add a string to the string heap, returns a pointer to the string
	string_t AddString(const char *data);
	//! Add a string to the string heap, returns a pointer to the string
	string_t AddString(const string &data);
	//! Add a string to the string heap, returns a pointer to the string
	string_t AddString(const string_t &data);
	//! Add a blob to the string heap; blobs can be non-valid UTF8
	string_t AddBlob(const char *data, idx_t len);
	//! Allocates space for an empty string of size "len" on the heap
	string_t EmptyString(idx_t len);
	//! Add all strings from a different string heap to this string heap
	void MergeHeap(StringHeap &heap);

private:
	struct StringChunk {
		explicit StringChunk(idx_t size) : current_position(0), maximum_size(size) {
			data = unique_ptr<char[]>(new char[maximum_size]);
		}
		~StringChunk() {
			if (prev) {
				auto current_prev = move(prev);
				while (current_prev) {
					current_prev = move(current_prev->prev);
				}
			}
		}

		unique_ptr<char[]> data;
		idx_t current_position;
		idx_t maximum_size;
		unique_ptr<StringChunk> prev;
	};
	StringChunk *tail;
	unique_ptr<StringChunk> chunk;
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/string_type.hpp
//
//
//===----------------------------------------------------------------------===//






#include <cstring>

namespace duckdb {

struct string_t {
	friend struct StringComparisonOperators;
	friend class StringSegment;

public:
	static constexpr idx_t PREFIX_LENGTH = 4 * sizeof(char);
	static constexpr idx_t INLINE_LENGTH = 12;

	string_t() = default;
	explicit string_t(uint32_t len) {
		value.inlined.length = len;
	}
	string_t(const char *data, uint32_t len) {
		value.inlined.length = len;
		D_ASSERT(data || GetSize() == 0);
		if (IsInlined()) {
			// zero initialize the prefix first
			// this makes sure that strings with length smaller than 4 still have an equal prefix
			memset(value.inlined.inlined, 0, INLINE_LENGTH);
			if (GetSize() == 0) {
				return;
			}
			// small string: inlined
			/* Note: this appears to write out-of bounds on `prefix` if `length` > `PREFIX_LENGTH`
			 but this is not the case because the `value_` union `inlined` char array directly
			 follows it with 8 more chars to use for the string value.
			 */
			memcpy(value.inlined.inlined, data, GetSize());
		} else {
			// large string: store pointer
			memcpy(value.pointer.prefix, data, PREFIX_LENGTH);
			value.pointer.ptr = (char *)data;
		}
	}
	string_t(const char *data) : string_t(data, strlen(data)) { // NOLINT: Allow implicit conversion from `const char*`
	}
	string_t(const string &value)
	    : string_t(value.c_str(), value.size()) { // NOLINT: Allow implicit conversion from `const char*`
	}

	bool IsInlined() const {
		return GetSize() <= INLINE_LENGTH;
	}

	//! this is unsafe since the string will not be terminated at the end
	const char *GetDataUnsafe() const {
		return IsInlined() ? (const char *)value.inlined.inlined : value.pointer.ptr;
	}

	char *GetDataWriteable() const {
		return IsInlined() ? (char *)value.inlined.inlined : value.pointer.ptr;
	}

	const char *GetPrefix() const {
		return value.pointer.prefix;
	}

	idx_t GetSize() const {
		return value.inlined.length;
	}

	string GetString() const {
		return string(GetDataUnsafe(), GetSize());
	}

	explicit operator string() const {
		return GetString();
	}

	void Finalize() {
		// set trailing NULL byte
		auto dataptr = (char *)GetDataUnsafe();
		if (GetSize() <= INLINE_LENGTH) {
			// fill prefix with zeros if the length is smaller than the prefix length
			for (idx_t i = GetSize(); i < INLINE_LENGTH; i++) {
				value.inlined.inlined[i] = '\0';
			}
		} else {
			// copy the data into the prefix
			memcpy(value.pointer.prefix, dataptr, PREFIX_LENGTH);
		}
	}

	void Verify();
	void VerifyNull();
	bool operator<(const string_t &r) const {
		auto this_str = this->GetString();
		auto r_str = r.GetString();
		return this_str < r_str;
	}

private:
	union {
		struct {
			uint32_t length;
			char prefix[4];
			char *ptr;
		} pointer;
		struct {
			uint32_t length;
			char inlined[12];
		} inlined;
	} value;
};

} // namespace duckdb



namespace duckdb {

class BufferHandle;
class VectorBuffer;
class Vector;
class ChunkCollection;

enum class VectorBufferType : uint8_t {
	STANDARD_BUFFER,     // standard buffer, holds a single array of data
	DICTIONARY_BUFFER,   // dictionary buffer, holds a selection vector
	VECTOR_CHILD_BUFFER, // vector child buffer: holds another vector
	STRING_BUFFER,       // string buffer, holds a string heap
	STRUCT_BUFFER,       // struct buffer, holds a ordered mapping from name to child vector
	LIST_BUFFER,         // list buffer, holds a single flatvector child
	MANAGED_BUFFER,      // managed buffer, holds a buffer managed by the buffermanager
	OPAQUE_BUFFER        // opaque buffer, can be created for example by the parquet reader
};

//! The VectorBuffer is a class used by the vector to hold its data
class VectorBuffer {
public:
	explicit VectorBuffer(VectorBufferType type) : buffer_type(type) {
	}
	explicit VectorBuffer(idx_t data_size) : buffer_type(VectorBufferType::STANDARD_BUFFER) {
		if (data_size > 0) {
			data = unique_ptr<data_t[]>(new data_t[data_size]);
		}
	}
	explicit VectorBuffer(unique_ptr<data_t[]> data_p)
	    : buffer_type(VectorBufferType::STANDARD_BUFFER), data(move(data_p)) {
	}
	virtual ~VectorBuffer() {
	}
	VectorBuffer() {
	}

public:
	data_ptr_t GetData() {
		return data.get();
	}
	void SetData(unique_ptr<data_t[]> new_data) {
		data = move(new_data);
	}

	static buffer_ptr<VectorBuffer> CreateStandardVector(PhysicalType type, idx_t capacity = STANDARD_VECTOR_SIZE);
	static buffer_ptr<VectorBuffer> CreateConstantVector(PhysicalType type);
	static buffer_ptr<VectorBuffer> CreateConstantVector(const LogicalType &logical_type);
	static buffer_ptr<VectorBuffer> CreateStandardVector(const LogicalType &logical_type,
	                                                     idx_t capacity = STANDARD_VECTOR_SIZE);

	inline VectorBufferType GetBufferType() const {
		return buffer_type;
	}

protected:
	VectorBufferType buffer_type;
	unique_ptr<data_t[]> data;
};

//! The DictionaryBuffer holds a selection vector
class DictionaryBuffer : public VectorBuffer {
public:
	explicit DictionaryBuffer(const SelectionVector &sel)
	    : VectorBuffer(VectorBufferType::DICTIONARY_BUFFER), sel_vector(sel) {
	}
	explicit DictionaryBuffer(buffer_ptr<SelectionData> data)
	    : VectorBuffer(VectorBufferType::DICTIONARY_BUFFER), sel_vector(move(data)) {
	}
	explicit DictionaryBuffer(idx_t count = STANDARD_VECTOR_SIZE)
	    : VectorBuffer(VectorBufferType::DICTIONARY_BUFFER), sel_vector(count) {
	}

public:
	const SelectionVector &GetSelVector() const {
		return sel_vector;
	}
	SelectionVector &GetSelVector() {
		return sel_vector;
	}
	void SetSelVector(const SelectionVector &vector) {
		this->sel_vector.Initialize(vector);
	}

private:
	SelectionVector sel_vector;
};

class VectorStringBuffer : public VectorBuffer {
public:
	VectorStringBuffer();

public:
	string_t AddString(const char *data, idx_t len) {
		return heap.AddString(data, len);
	}
	string_t AddString(string_t data) {
		return heap.AddString(data);
	}
	string_t AddBlob(string_t data) {
		return heap.AddBlob(data.GetDataUnsafe(), data.GetSize());
	}
	string_t EmptyString(idx_t len) {
		return heap.EmptyString(len);
	}

	void AddHeapReference(buffer_ptr<VectorBuffer> heap) {
		references.push_back(move(heap));
	}

private:
	//! The string heap of this buffer
	StringHeap heap;
	// References to additional vector buffers referenced by this string buffer
	vector<buffer_ptr<VectorBuffer>> references;
};

class VectorStructBuffer : public VectorBuffer {
public:
	VectorStructBuffer();
	VectorStructBuffer(const LogicalType &struct_type, idx_t capacity = STANDARD_VECTOR_SIZE);
	~VectorStructBuffer() override;

public:
	const vector<unique_ptr<Vector>> &GetChildren() const {
		return children;
	}
	vector<unique_ptr<Vector>> &GetChildren() {
		return children;
	}

private:
	//! child vectors used for nested data
	vector<unique_ptr<Vector>> children;
};

class VectorListBuffer : public VectorBuffer {
public:
	VectorListBuffer(unique_ptr<Vector> vector, idx_t initial_capacity = STANDARD_VECTOR_SIZE);
	VectorListBuffer(const LogicalType &list_type, idx_t initial_capacity = STANDARD_VECTOR_SIZE);
	~VectorListBuffer() override;

public:
	Vector &GetChild() {
		return *child;
	}
	void Reserve(idx_t to_reserve);

	void Append(const Vector &to_append, idx_t to_append_size, idx_t source_offset = 0);
	void Append(const Vector &to_append, const SelectionVector &sel, idx_t to_append_size, idx_t source_offset = 0);

	void PushBack(Value &insert);

	idx_t capacity = 0;
	idx_t size = 0;

private:
	//! child vectors used for nested data
	unique_ptr<Vector> child;
};

//! The ManagedVectorBuffer holds a buffer handle
class ManagedVectorBuffer : public VectorBuffer {
public:
	explicit ManagedVectorBuffer(unique_ptr<BufferHandle> handle);
	~ManagedVectorBuffer() override;

private:
	unique_ptr<BufferHandle> handle;
};

} // namespace duckdb


//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/validity_mask.hpp
//
//
//===----------------------------------------------------------------------===//






//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/to_string.hpp
//
//
//===----------------------------------------------------------------------===//



namespace duckdb {
using std::to_string;
}


namespace duckdb {
struct ValidityMask;

template <typename V>
struct TemplatedValidityData {
	static constexpr const int BITS_PER_VALUE = sizeof(V) * 8;
	static constexpr const V MAX_ENTRY = ~V(0);

public:
	explicit TemplatedValidityData(idx_t count) {
		auto entry_count = EntryCount(count);
		owned_data = unique_ptr<V[]>(new V[entry_count]);
		for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
			owned_data[entry_idx] = MAX_ENTRY;
		}
	}
	TemplatedValidityData(const V *validity_mask, idx_t count) {
		D_ASSERT(validity_mask);
		auto entry_count = EntryCount(count);
		owned_data = unique_ptr<V[]>(new V[entry_count]);
		for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
			owned_data[entry_idx] = validity_mask[entry_idx];
		}
	}

	unique_ptr<V[]> owned_data;

public:
	static inline idx_t EntryCount(idx_t count) {
		return (count + (BITS_PER_VALUE - 1)) / BITS_PER_VALUE;
	}
};

using validity_t = uint64_t;

struct ValidityData : TemplatedValidityData<validity_t> {
public:
	DUCKDB_API explicit ValidityData(idx_t count);
	DUCKDB_API ValidityData(const ValidityMask &original, idx_t count);
};

//! Type used for validity masks
template <typename V>
struct TemplatedValidityMask {
	using ValidityBuffer = TemplatedValidityData<V>;

public:
	static constexpr const int BITS_PER_VALUE = ValidityBuffer::BITS_PER_VALUE;
	static constexpr const int STANDARD_ENTRY_COUNT = (STANDARD_VECTOR_SIZE + (BITS_PER_VALUE - 1)) / BITS_PER_VALUE;
	static constexpr const int STANDARD_MASK_SIZE = STANDARD_ENTRY_COUNT * sizeof(validity_t);

public:
	TemplatedValidityMask() : validity_mask(nullptr) {
	}
	explicit TemplatedValidityMask(idx_t max_count) {
		Initialize(max_count);
	}
	explicit TemplatedValidityMask(V *ptr) : validity_mask(ptr) {
	}
	TemplatedValidityMask(const TemplatedValidityMask &original, idx_t count) {
		Copy(original, count);
	}

	static inline idx_t ValidityMaskSize(idx_t count = STANDARD_VECTOR_SIZE) {
		return ValidityBuffer::EntryCount(count) * sizeof(V);
	}
	inline bool AllValid() const {
		return !validity_mask;
	}
	bool CheckAllValid(idx_t count) const {
		if (AllValid()) {
			return true;
		}
		idx_t entry_count = ValidityBuffer::EntryCount(count);
		idx_t valid_count = 0;
		for (idx_t i = 0; i < entry_count; i++) {
			valid_count += validity_mask[i] == ValidityBuffer::MAX_ENTRY;
		}
		return valid_count == entry_count;
	}

	bool CheckAllValid(idx_t to, idx_t from) const {
		if (AllValid()) {
			return true;
		}
		for (idx_t i = from; i < to; i++) {
			if (!RowIsValid(i)) {
				return false;
			}
		}
		return true;
	}

	inline V *GetData() const {
		return validity_mask;
	}
	void Reset() {
		validity_mask = nullptr;
		validity_data.reset();
	}

	static inline idx_t EntryCount(idx_t count) {
		return ValidityBuffer::EntryCount(count);
	}
	inline V GetValidityEntry(idx_t entry_idx) const {
		if (!validity_mask) {
			return ValidityBuffer::MAX_ENTRY;
		}
		return validity_mask[entry_idx];
	}
	static inline bool AllValid(V entry) {
		return entry == ValidityBuffer::MAX_ENTRY;
	}
	static inline bool NoneValid(V entry) {
		return entry == 0;
	}
	static inline bool RowIsValid(V entry, idx_t idx_in_entry) {
		return entry & (V(1) << V(idx_in_entry));
	}
	static inline void GetEntryIndex(idx_t row_idx, idx_t &entry_idx, idx_t &idx_in_entry) {
		entry_idx = row_idx / BITS_PER_VALUE;
		idx_in_entry = row_idx % BITS_PER_VALUE;
	}

	//! RowIsValidUnsafe should only be used if AllValid() is false: it achieves the same as RowIsValid but skips a
	//! not-null check
	inline bool RowIsValidUnsafe(idx_t row_idx) const {
		D_ASSERT(validity_mask);
		idx_t entry_idx, idx_in_entry;
		GetEntryIndex(row_idx, entry_idx, idx_in_entry);
		auto entry = GetValidityEntry(entry_idx);
		return RowIsValid(entry, idx_in_entry);
	}

	//! Returns true if a row is valid (i.e. not null), false otherwise
	inline bool RowIsValid(idx_t row_idx) const {
		if (!validity_mask) {
			return true;
		}
		return RowIsValidUnsafe(row_idx);
	}

	//! Same as SetValid, but skips a null check on validity_mask
	inline void SetValidUnsafe(idx_t row_idx) {
		D_ASSERT(validity_mask);
		idx_t entry_idx, idx_in_entry;
		GetEntryIndex(row_idx, entry_idx, idx_in_entry);
		validity_mask[entry_idx] |= (V(1) << V(idx_in_entry));
	}

	//! Marks the entry at the specified row index as valid (i.e. not-null)
	inline void SetValid(idx_t row_idx) {
		if (!validity_mask) {
			// if AllValid() we don't need to do anything
			// the row is already valid
			return;
		}
		SetValidUnsafe(row_idx);
	}

	//! Marks the bit at the specified entry as invalid (i.e. null)
	inline void SetInvalidUnsafe(idx_t entry_idx, idx_t idx_in_entry) {
		D_ASSERT(validity_mask);
		validity_mask[entry_idx] &= ~(V(1) << V(idx_in_entry));
	}

	//! Marks the bit at the specified row index as invalid (i.e. null)
	inline void SetInvalidUnsafe(idx_t row_idx) {
		idx_t entry_idx, idx_in_entry;
		GetEntryIndex(row_idx, entry_idx, idx_in_entry);
		SetInvalidUnsafe(entry_idx, idx_in_entry);
	}

	//! Marks the entry at the specified row index as invalid (i.e. null)
	inline void SetInvalid(idx_t row_idx) {
		if (!validity_mask) {
			D_ASSERT(row_idx <= STANDARD_VECTOR_SIZE);
			Initialize(STANDARD_VECTOR_SIZE);
		}
		SetInvalidUnsafe(row_idx);
	}

	//! Mark the entrry at the specified index as either valid or invalid (non-null or null)
	inline void Set(idx_t row_idx, bool valid) {
		if (valid) {
			SetValid(row_idx);
		} else {
			SetInvalid(row_idx);
		}
	}

	//! Ensure the validity mask is writable, allocating space if it is not initialized
	inline void EnsureWritable() {
		if (!validity_mask) {
			Initialize();
		}
	}

	//! Marks "count" entries in the validity mask as invalid (null)
	inline void SetAllInvalid(idx_t count) {
		EnsureWritable();
		for (idx_t i = 0; i < ValidityBuffer::EntryCount(count); i++) {
			validity_mask[i] = 0;
		}
	}

	//! Marks "count" entries in the validity mask as valid (not null)
	inline void SetAllValid(idx_t count) {
		EnsureWritable();
		for (idx_t i = 0; i < ValidityBuffer::EntryCount(count); i++) {
			validity_mask[i] = ValidityBuffer::MAX_ENTRY;
		}
	}

	inline bool IsMaskSet() const {
		if (validity_mask) {
			return true;
		}
		return false;
	}

public:
	void Initialize(validity_t *validity) {
		validity_data.reset();
		validity_mask = validity;
	}
	void Initialize(const TemplatedValidityMask &other) {
		validity_mask = other.validity_mask;
		validity_data = other.validity_data;
	}
	void Initialize(idx_t count = STANDARD_VECTOR_SIZE) {
		validity_data = make_buffer<ValidityBuffer>(count);
		validity_mask = validity_data->owned_data.get();
	}
	void Copy(const TemplatedValidityMask &other, idx_t count) {
		if (other.AllValid()) {
			validity_data = nullptr;
			validity_mask = nullptr;
		} else {
			validity_data = make_buffer<ValidityBuffer>(other.validity_mask, count);
			validity_mask = validity_data->owned_data.get();
		}
	}

protected:
	V *validity_mask;
	buffer_ptr<ValidityBuffer> validity_data;
};

struct ValidityMask : public TemplatedValidityMask<validity_t> {
public:
	ValidityMask() : TemplatedValidityMask(nullptr) {
	}
	explicit ValidityMask(idx_t max_count) : TemplatedValidityMask(max_count) {
	}
	explicit ValidityMask(validity_t *ptr) : TemplatedValidityMask(ptr) {
	}
	ValidityMask(const ValidityMask &original, idx_t count) : TemplatedValidityMask(original, count) {
	}

public:
	void Resize(idx_t old_size, idx_t new_size);

	void Slice(const ValidityMask &other, idx_t offset);
	void Combine(const ValidityMask &other, idx_t count);
	string ToString(idx_t count) const;
};

} // namespace duckdb


namespace duckdb {

struct VectorData {
	const SelectionVector *sel;
	data_ptr_t data;
	ValidityMask validity;
	SelectionVector owned_sel;
};

class VectorCache;
class VectorStructBuffer;
class VectorListBuffer;
class ChunkCollection;

struct SelCache;

//!  Vector of values of a specified PhysicalType.
class Vector {
	friend struct ConstantVector;
	friend struct DictionaryVector;
	friend struct FlatVector;
	friend struct ListVector;
	friend struct StringVector;
	friend struct StructVector;
	friend struct SequenceVector;

	friend class DataChunk;
	friend class VectorCacheBuffer;

public:
	//! Create a vector that references the other vector
	explicit Vector(Vector &other);
	//! Create a vector that slices another vector
	explicit Vector(Vector &other, const SelectionVector &sel, idx_t count);
	//! Create a vector that slices another vector starting from a specific offset
	explicit Vector(Vector &other, idx_t offset);
	//! Create a vector of size one holding the passed on value
	explicit Vector(const Value &value);
	//! Create an empty standard vector with a type, equivalent to calling Vector(type, true, false)
	explicit Vector(LogicalType type, idx_t capacity = STANDARD_VECTOR_SIZE);
	//! Create an empty standard vector with a type, equivalent to calling Vector(type, true, false)
	explicit Vector(const VectorCache &cache);
	//! Create a non-owning vector that references the specified data
	Vector(LogicalType type, data_ptr_t dataptr);
	//! Create an owning vector that holds at most STANDARD_VECTOR_SIZE entries.
	/*!
	    Create a new vector
	    If create_data is true, the vector will be an owning empty vector.
	    If zero_data is true, the allocated data will be zero-initialized.
	*/
	Vector(LogicalType type, bool create_data, bool zero_data, idx_t capacity = STANDARD_VECTOR_SIZE);
	// implicit copying of Vectors is not allowed
	Vector(const Vector &) = delete;
	// but moving of vectors is allowed
	Vector(Vector &&other) noexcept;

public:
	//! Create a vector that references the specified value.
	void Reference(const Value &value);
	//! Causes this vector to reference the data held by the other vector.
	//! The type of the "other" vector should match the type of this vector
	void Reference(Vector &other);
	//! Reinterpret the data of the other vector as the type of this vector
	//! Note that this takes the data of the other vector as-is and places it in this vector
	//! Without changing the type of this vector
	void Reinterpret(Vector &other);

	//! Resets a vector from a vector cache.
	//! This turns the vector back into an empty FlatVector with STANDARD_VECTOR_SIZE entries.
	//! The VectorCache is used so this can be done without requiring any allocations.
	void ResetFromCache(const VectorCache &cache);

	//! Creates a reference to a slice of the other vector
	void Slice(Vector &other, idx_t offset);
	//! Creates a reference to a slice of the other vector
	void Slice(Vector &other, const SelectionVector &sel, idx_t count);
	//! Turns the vector into a dictionary vector with the specified dictionary
	void Slice(const SelectionVector &sel, idx_t count);
	//! Slice the vector, keeping the result around in a cache or potentially using the cache instead of slicing
	void Slice(const SelectionVector &sel, idx_t count, SelCache &cache);

	//! Creates the data of this vector with the specified type. Any data that
	//! is currently in the vector is destroyed.
	void Initialize(bool zero_data = false, idx_t capacity = STANDARD_VECTOR_SIZE);

	//! Converts this Vector to a printable string representation
	string ToString(idx_t count) const;
	void Print(idx_t count);

	string ToString() const;
	void Print();

	//! Flatten the vector, removing any compression and turning it into a FLAT_VECTOR
	DUCKDB_API void Normalify(idx_t count);
	DUCKDB_API void Normalify(const SelectionVector &sel, idx_t count);
	//! Obtains a selection vector and data pointer through which the data of this vector can be accessed
	DUCKDB_API void Orrify(idx_t count, VectorData &data);

	//! Turn the vector into a sequence vector
	void Sequence(int64_t start, int64_t increment);

	//! Verify that the Vector is in a consistent, not corrupt state. DEBUG
	//! FUNCTION ONLY!
	void Verify(idx_t count);
	void Verify(const SelectionVector &sel, idx_t count);
	void UTFVerify(idx_t count);
	void UTFVerify(const SelectionVector &sel, idx_t count);

	//! Returns the [index] element of the Vector as a Value.
	Value GetValue(idx_t index) const;
	//! Sets the [index] element of the Vector to the specified Value.
	void SetValue(idx_t index, const Value &val);

	void SetAuxiliary(buffer_ptr<VectorBuffer> new_buffer) {
		auxiliary = std::move(new_buffer);
	};

	//! This functions resizes the vector
	void Resize(idx_t cur_size, idx_t new_size);

	//! Serializes a Vector to a stand-alone binary blob
	void Serialize(idx_t count, Serializer &serializer);
	//! Deserializes a blob back into a Vector
	void Deserialize(idx_t count, Deserializer &source);

	// Getters
	inline VectorType GetVectorType() const {
		return vector_type;
	}
	inline const LogicalType &GetType() const {
		return type;
	}
	inline data_ptr_t GetData() {
		return data;
	}

	buffer_ptr<VectorBuffer> GetAuxiliary() {
		return auxiliary;
	}

	buffer_ptr<VectorBuffer> GetBuffer() {
		return buffer;
	}

	// Setters
	DUCKDB_API void SetVectorType(VectorType vector_type);

protected:
	//! The vector type specifies how the data of the vector is physically stored (i.e. if it is a single repeated
	//! constant, if it is compressed)
	VectorType vector_type;
	//! The type of the elements stored in the vector (e.g. integer, float)
	LogicalType type;
	//! A pointer to the data.
	data_ptr_t data;
	//! The validity mask of the vector
	ValidityMask validity;
	//! The main buffer holding the data of the vector
	buffer_ptr<VectorBuffer> buffer;
	//! The buffer holding auxiliary data of the vector
	//! e.g. a string vector uses this to store strings
	buffer_ptr<VectorBuffer> auxiliary;
};

//! The DictionaryBuffer holds a selection vector
class VectorChildBuffer : public VectorBuffer {
public:
	VectorChildBuffer(Vector vector) : VectorBuffer(VectorBufferType::VECTOR_CHILD_BUFFER), data(move(vector)) {
	}

public:
	Vector data;
};

struct ConstantVector {
	static inline const_data_ptr_t GetData(const Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::CONSTANT_VECTOR ||
		         vector.GetVectorType() == VectorType::FLAT_VECTOR);
		return vector.data;
	}
	static inline data_ptr_t GetData(Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::CONSTANT_VECTOR ||
		         vector.GetVectorType() == VectorType::FLAT_VECTOR);
		return vector.data;
	}
	template <class T>
	static inline const T *GetData(const Vector &vector) {
		return (const T *)ConstantVector::GetData(vector);
	}
	template <class T>
	static inline T *GetData(Vector &vector) {
		return (T *)ConstantVector::GetData(vector);
	}
	static inline bool IsNull(const Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
		return !vector.validity.RowIsValid(0);
	}
	DUCKDB_API static void SetNull(Vector &vector, bool is_null);
	static inline ValidityMask &Validity(Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
		return vector.validity;
	}
	DUCKDB_API static const SelectionVector *ZeroSelectionVector(idx_t count, SelectionVector &owned_sel);
	//! Turns "vector" into a constant vector by referencing a value within the source vector
	DUCKDB_API static void Reference(Vector &vector, Vector &source, idx_t position, idx_t count);

	static const sel_t ZERO_VECTOR[STANDARD_VECTOR_SIZE];
	static const SelectionVector ZERO_SELECTION_VECTOR;
};

struct DictionaryVector {
	static inline const SelectionVector &SelVector(const Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::DICTIONARY_VECTOR);
		return ((const DictionaryBuffer &)*vector.buffer).GetSelVector();
	}
	static inline SelectionVector &SelVector(Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::DICTIONARY_VECTOR);
		return ((DictionaryBuffer &)*vector.buffer).GetSelVector();
	}
	static inline const Vector &Child(const Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::DICTIONARY_VECTOR);
		return ((const VectorChildBuffer &)*vector.auxiliary).data;
	}
	static inline Vector &Child(Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::DICTIONARY_VECTOR);
		return ((VectorChildBuffer &)*vector.auxiliary).data;
	}
};

struct FlatVector {
	static inline data_ptr_t GetData(Vector &vector) {
		return ConstantVector::GetData(vector);
	}
	template <class T>
	static inline const T *GetData(const Vector &vector) {
		return ConstantVector::GetData<T>(vector);
	}
	template <class T>
	static inline T *GetData(Vector &vector) {
		return ConstantVector::GetData<T>(vector);
	}
	static inline void SetData(Vector &vector, data_ptr_t data) {
		D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
		vector.data = data;
	}
	template <class T>
	static inline T GetValue(Vector &vector, idx_t idx) {
		D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
		return FlatVector::GetData<T>(vector)[idx];
	}
	static inline const ValidityMask &Validity(const Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
		return vector.validity;
	}
	static inline ValidityMask &Validity(Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
		return vector.validity;
	}
	static inline void SetValidity(Vector &vector, ValidityMask &new_validity) {
		D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
		vector.validity.Initialize(new_validity);
	}
	static inline void SetNull(Vector &vector, idx_t idx, bool is_null) {
		D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
		vector.validity.Set(idx, !is_null);
	}
	static inline bool IsNull(const Vector &vector, idx_t idx) {
		D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
		return !vector.validity.RowIsValid(idx);
	}
	DUCKDB_API static const SelectionVector *IncrementalSelectionVector(idx_t count, SelectionVector &owned_sel);

	static const sel_t INCREMENTAL_VECTOR[STANDARD_VECTOR_SIZE];
	static const SelectionVector INCREMENTAL_SELECTION_VECTOR;
};

struct ListVector {
	//! Gets a reference to the underlying child-vector of a list
	DUCKDB_API static const Vector &GetEntry(const Vector &vector);
	//! Gets a reference to the underlying child-vector of a list
	DUCKDB_API static Vector &GetEntry(Vector &vector);
	//! Gets the total size of the underlying child-vector of a list
	DUCKDB_API static idx_t GetListSize(const Vector &vector);
	//! Sets the total size of the underlying child-vector of a list
	DUCKDB_API static void SetListSize(Vector &vec, idx_t size);
	DUCKDB_API static void Reserve(Vector &vec, idx_t required_capacity);
	DUCKDB_API static void Append(Vector &target, const Vector &source, idx_t source_size, idx_t source_offset = 0);
	DUCKDB_API static void Append(Vector &target, const Vector &source, const SelectionVector &sel, idx_t source_size,
	                              idx_t source_offset = 0);
	DUCKDB_API static void PushBack(Vector &target, Value &insert);
	DUCKDB_API static vector<idx_t> Search(Vector &list, Value &key, idx_t row);
	DUCKDB_API static Value GetValuesFromOffsets(Vector &list, vector<idx_t> &offsets);
	//! Share the entry of the other list vector
	DUCKDB_API static void ReferenceEntry(Vector &vector, Vector &other);
};

struct StringVector {
	//! Add a string to the string heap of the vector (auxiliary data)
	DUCKDB_API static string_t AddString(Vector &vector, const char *data, idx_t len);
	//! Add a string or a blob to the string heap of the vector (auxiliary data)
	//! This function is the same as ::AddString, except the added data does not need to be valid UTF8
	DUCKDB_API static string_t AddStringOrBlob(Vector &vector, const char *data, idx_t len);
	//! Add a string to the string heap of the vector (auxiliary data)
	DUCKDB_API static string_t AddString(Vector &vector, const char *data);
	//! Add a string to the string heap of the vector (auxiliary data)
	DUCKDB_API static string_t AddString(Vector &vector, string_t data);
	//! Add a string to the string heap of the vector (auxiliary data)
	DUCKDB_API static string_t AddString(Vector &vector, const string &data);
	//! Add a string or a blob to the string heap of the vector (auxiliary data)
	//! This function is the same as ::AddString, except the added data does not need to be valid UTF8
	DUCKDB_API static string_t AddStringOrBlob(Vector &vector, string_t data);
	//! Allocates an empty string of the specified size, and returns a writable pointer that can be used to store the
	//! result of an operation
	DUCKDB_API static string_t EmptyString(Vector &vector, idx_t len);
	//! Adds a reference to a handle that stores strings of this vector
	DUCKDB_API static void AddHandle(Vector &vector, unique_ptr<BufferHandle> handle);
	//! Adds a reference to an unspecified vector buffer that stores strings of this vector
	DUCKDB_API static void AddBuffer(Vector &vector, buffer_ptr<VectorBuffer> buffer);
	//! Add a reference from this vector to the string heap of the provided vector
	DUCKDB_API static void AddHeapReference(Vector &vector, Vector &other);
};

struct StructVector {
	DUCKDB_API static const vector<unique_ptr<Vector>> &GetEntries(const Vector &vector);
	DUCKDB_API static vector<unique_ptr<Vector>> &GetEntries(Vector &vector);
};

struct SequenceVector {
	static void GetSequence(const Vector &vector, int64_t &start, int64_t &increment) {
		D_ASSERT(vector.GetVectorType() == VectorType::SEQUENCE_VECTOR);
		auto data = (int64_t *)vector.buffer->GetData();
		start = data[0];
		increment = data[1];
	}
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/vector_operations.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/data_chunk.hpp
//
//
//===----------------------------------------------------------------------===//







struct ArrowArray;

namespace duckdb {
class VectorCache;

//!  A Data Chunk represents a set of vectors.
/*!
    The data chunk class is the intermediate representation used by the
   execution engine of DuckDB. It effectively represents a subset of a relation.
   It holds a set of vectors that all have the same length.

    DataChunk is initialized using the DataChunk::Initialize function by
   providing it with a vector of TypeIds for the Vector members. By default,
   this function will also allocate a chunk of memory in the DataChunk for the
   vectors and all the vectors will be referencing vectors to the data owned by
   the chunk. The reason for this behavior is that the underlying vectors can
   become referencing vectors to other chunks as well (i.e. in the case an
   operator does not alter the data, such as a Filter operator which only adds a
   selection vector).

    In addition to holding the data of the vectors, the DataChunk also owns the
   selection vector that underlying vectors can point to.
*/
class DataChunk {
public:
	//! Creates an empty DataChunk
	DataChunk();
	~DataChunk();

	//! The vectors owned by the DataChunk.
	vector<Vector> data;

public:
	DUCKDB_API idx_t size() const {
		return count;
	}
	DUCKDB_API idx_t ColumnCount() const {
		return data.size();
	}
	void SetCardinality(idx_t count_p) {
		D_ASSERT(count <= STANDARD_VECTOR_SIZE);
		this->count = count_p;
	}
	void SetCardinality(const DataChunk &other) {
		this->count = other.size();
	}

	DUCKDB_API Value GetValue(idx_t col_idx, idx_t index) const;
	DUCKDB_API void SetValue(idx_t col_idx, idx_t index, const Value &val);

	//! Set the DataChunk to reference another data chunk
	DUCKDB_API void Reference(DataChunk &chunk);
	//! Set the DataChunk to own the data of data chunk, destroying the other chunk in the process
	DUCKDB_API void Move(DataChunk &chunk);

	//! Initializes the DataChunk with the specified types to an empty DataChunk
	//! This will create one vector of the specified type for each LogicalType in the
	//! types list. The vector will be referencing vector to the data owned by
	//! the DataChunk.
	void Initialize(const vector<LogicalType> &types);
	//! Initializes an empty DataChunk with the given types. The vectors will *not* have any data allocated for them.
	void InitializeEmpty(const vector<LogicalType> &types);
	//! Append the other DataChunk to this one. The column count and types of
	//! the two DataChunks have to match exactly. Throws an exception if there
	//! is not enough space in the chunk.
	DUCKDB_API void Append(const DataChunk &other);
	//! Destroy all data and columns owned by this DataChunk
	DUCKDB_API void Destroy();

	//! Copies the data from this vector to another vector.
	DUCKDB_API void Copy(DataChunk &other, idx_t offset = 0) const;
	DUCKDB_API void Copy(DataChunk &other, const SelectionVector &sel, const idx_t source_count,
	                     const idx_t offset = 0) const;

	//! Turn all the vectors from the chunk into flat vectors
	DUCKDB_API void Normalify();

	DUCKDB_API unique_ptr<VectorData[]> Orrify();

	DUCKDB_API void Slice(const SelectionVector &sel_vector, idx_t count);
	DUCKDB_API void Slice(DataChunk &other, const SelectionVector &sel, idx_t count, idx_t col_offset = 0);

	//! Resets the DataChunk to its state right after the DataChunk::Initialize
	//! function was called. This sets the count to 0, and resets each member
	//! Vector to point back to the data owned by this DataChunk.
	DUCKDB_API void Reset();

	//! Serializes a DataChunk to a stand-alone binary blob
	DUCKDB_API void Serialize(Serializer &serializer);
	//! Deserializes a blob back into a DataChunk
	DUCKDB_API void Deserialize(Deserializer &source);

	//! Hashes the DataChunk to the target vector
	DUCKDB_API void Hash(Vector &result);

	//! Returns a list of types of the vectors of this data chunk
	DUCKDB_API vector<LogicalType> GetTypes();

	//! Converts this DataChunk to a printable string representation
	DUCKDB_API string ToString() const;
	DUCKDB_API void Print();

	DataChunk(const DataChunk &) = delete;

	//! Verify that the DataChunk is in a consistent, not corrupt state. DEBUG
	//! FUNCTION ONLY!
	DUCKDB_API void Verify();

	//! export data chunk as a arrow struct array that can be imported as arrow record batch
	DUCKDB_API void ToArrowArray(ArrowArray *out_array);

private:
	//! The amount of tuples stored in the data chunk
	idx_t count;
	//! Vector caches, used to store data when ::Initialize is called
	vector<VectorCache> vector_caches;
};
} // namespace duckdb



#include <functional>

namespace duckdb {

// VectorOperations contains a set of operations that operate on sets of
// vectors. In general, the operators must all have the same type, otherwise an
// exception is thrown. Note that the functions underneath use restrict
// pointers, hence the data that the vectors point to (and hence the vector
// themselves) should not be equal! For example, if you call the function Add(A,
// B, A) then ASSERT_RESTRICT will be triggered. Instead call AddInPlace(A, B)
// or Add(A, B, C)
struct VectorOperations {
	//===--------------------------------------------------------------------===//
	// In-Place Operators
	//===--------------------------------------------------------------------===//
	//! A += B
	static void AddInPlace(Vector &A, int64_t B, idx_t count);

	//===--------------------------------------------------------------------===//
	// NULL Operators
	//===--------------------------------------------------------------------===//
	//! result = IS NOT NULL(A)
	static void IsNotNull(Vector &A, Vector &result, idx_t count);
	//! result = IS NULL (A)
	static void IsNull(Vector &A, Vector &result, idx_t count);
	// Returns whether or not a vector has a NULL value
	static bool HasNull(Vector &A, idx_t count);
	static bool HasNotNull(Vector &A, idx_t count);

	//===--------------------------------------------------------------------===//
	// Boolean Operations
	//===--------------------------------------------------------------------===//
	// result = A && B
	static void And(Vector &A, Vector &B, Vector &result, idx_t count);
	// result = A || B
	static void Or(Vector &A, Vector &B, Vector &result, idx_t count);
	// result = NOT(A)
	static void Not(Vector &A, Vector &result, idx_t count);

	//===--------------------------------------------------------------------===//
	// Comparison Operations
	//===--------------------------------------------------------------------===//
	// result = A == B
	static void Equals(Vector &A, Vector &B, Vector &result, idx_t count);
	// result = A != B
	static void NotEquals(Vector &A, Vector &B, Vector &result, idx_t count);
	// result = A > B
	static void GreaterThan(Vector &A, Vector &B, Vector &result, idx_t count);
	// result = A >= B
	static void GreaterThanEquals(Vector &A, Vector &B, Vector &result, idx_t count);
	// result = A < B
	static void LessThan(Vector &A, Vector &B, Vector &result, idx_t count);
	// result = A <= B
	static void LessThanEquals(Vector &A, Vector &B, Vector &result, idx_t count);

	// result = A != B with nulls being equal
	static void DistinctFrom(Vector &left, Vector &right, Vector &result, idx_t count);
	// result := A == B with nulls being equal
	static void NotDistinctFrom(Vector &left, Vector &right, Vector &result, idx_t count);
	// result := A > B with nulls being maximal
	static void DistinctGreaterThan(Vector &left, Vector &right, Vector &result, idx_t count);
	// result := A >= B with nulls being maximal
	static void DistinctGreaterThanEquals(Vector &left, Vector &right, Vector &result, idx_t count);
	// result := A < B with nulls being maximal
	static void DistinctLessThan(Vector &left, Vector &right, Vector &result, idx_t count);
	// result := A <= B with nulls being maximal
	static void DistinctLessThanEquals(Vector &left, Vector &right, Vector &result, idx_t count);

	//===--------------------------------------------------------------------===//
	// Select Comparisons
	//===--------------------------------------------------------------------===//
	static idx_t Equals(Vector &left, Vector &right, const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
	                    SelectionVector *false_sel);
	static idx_t NotEquals(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
	                       SelectionVector *true_sel, SelectionVector *false_sel);
	static idx_t GreaterThan(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
	                         SelectionVector *true_sel, SelectionVector *false_sel);
	static idx_t GreaterThanEquals(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
	                               SelectionVector *true_sel, SelectionVector *false_sel);
	static idx_t LessThan(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
	                      SelectionVector *true_sel, SelectionVector *false_sel);
	static idx_t LessThanEquals(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
	                            SelectionVector *true_sel, SelectionVector *false_sel);

	// true := A != B with nulls being equal
	static idx_t DistinctFrom(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
	                          SelectionVector *true_sel, SelectionVector *false_sel);
	// true := A == B with nulls being equal
	static idx_t NotDistinctFrom(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
	                             SelectionVector *true_sel, SelectionVector *false_sel);
	// true := A > B with nulls being maximal
	static idx_t DistinctGreaterThan(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
	                                 SelectionVector *true_sel, SelectionVector *false_sel);
	// true := A >= B with nulls being maximal
	static idx_t DistinctGreaterThanEquals(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
	                                       SelectionVector *true_sel, SelectionVector *false_sel);
	// true := A < B with nulls being maximal
	static idx_t DistinctLessThan(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
	                              SelectionVector *true_sel, SelectionVector *false_sel);
	// true := A <= B with nulls being maximal
	static idx_t DistinctLessThanEquals(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
	                                    SelectionVector *true_sel, SelectionVector *false_sel);

	//===--------------------------------------------------------------------===//
	// Hash functions
	//===--------------------------------------------------------------------===//
	// result = HASH(A)
	static void Hash(Vector &input, Vector &hashes, idx_t count);
	static void Hash(Vector &input, Vector &hashes, const SelectionVector &rsel, idx_t count);
	// A ^= HASH(B)
	static void CombineHash(Vector &hashes, Vector &B, idx_t count);
	static void CombineHash(Vector &hashes, Vector &B, const SelectionVector &rsel, idx_t count);

	//===--------------------------------------------------------------------===//
	// Generate functions
	//===--------------------------------------------------------------------===//
	static void GenerateSequence(Vector &result, idx_t count, int64_t start = 0, int64_t increment = 1);
	static void GenerateSequence(Vector &result, idx_t count, const SelectionVector &sel, int64_t start = 0,
	                             int64_t increment = 1);
	//===--------------------------------------------------------------------===//
	// Helpers
	//===--------------------------------------------------------------------===//
	// Cast the data from the source type to the target type
	static void Cast(Vector &source, Vector &result, idx_t count, bool strict = false);

	// Copy the data of <source> to the target vector
	static void Copy(const Vector &source, Vector &target, idx_t source_count, idx_t source_offset,
	                 idx_t target_offset);
	static void Copy(const Vector &source, Vector &target, const SelectionVector &sel, idx_t source_count,
	                 idx_t source_offset, idx_t target_offset);

	// Copy the data of <source> to the target location, setting null values to
	// NullValue<T>. Used to store data without separate NULL mask.
	static void WriteToStorage(Vector &source, idx_t count, data_ptr_t target);
	// Reads the data of <source> to the target vector, setting the nullmask
	// for any NullValue<T> of source. Used to go back from storage to a proper vector
	static void ReadFromStorage(data_ptr_t source, idx_t count, Vector &result);
};
} // namespace duckdb


#include <functional>

namespace duckdb {

struct DefaultNullCheckOperator {
	template <class LEFT_TYPE, class RIGHT_TYPE>
	static inline bool Operation(LEFT_TYPE left, RIGHT_TYPE right) {
		return false;
	}
};

struct BinaryStandardOperatorWrapper {
	template <class FUNC, class OP, class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, LEFT_TYPE left, RIGHT_TYPE right, ValidityMask &mask, idx_t idx) {
		return OP::template Operation<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(left, right);
	}

	static bool AddsNulls() {
		return false;
	}
};

struct BinarySingleArgumentOperatorWrapper {
	template <class FUNC, class OP, class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, LEFT_TYPE left, RIGHT_TYPE right, ValidityMask &mask, idx_t idx) {
		return OP::template Operation<LEFT_TYPE>(left, right);
	}

	static bool AddsNulls() {
		return false;
	}
};

struct BinaryLambdaWrapper {
	template <class FUNC, class OP, class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, LEFT_TYPE left, RIGHT_TYPE right, ValidityMask &mask, idx_t idx) {
		return fun(left, right);
	}

	static bool AddsNulls() {
		return false;
	}
};

struct BinaryExecutor {
	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC,
	          bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
	static void ExecuteFlatLoop(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
	                            RESULT_TYPE *__restrict result_data, idx_t count, ValidityMask &mask, FUNC fun) {
		if (!LEFT_CONSTANT) {
			ASSERT_RESTRICT(ldata, ldata + count, result_data, result_data + count);
		}
		if (!RIGHT_CONSTANT) {
			ASSERT_RESTRICT(rdata, rdata + count, result_data, result_data + count);
		}

		if (!mask.AllValid()) {
			idx_t base_idx = 0;
			auto entry_count = ValidityMask::EntryCount(count);
			for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
				auto validity_entry = mask.GetValidityEntry(entry_idx);
				idx_t next = MinValue<idx_t>(base_idx + ValidityMask::BITS_PER_VALUE, count);
				if (ValidityMask::AllValid(validity_entry)) {
					// all valid: perform operation
					for (; base_idx < next; base_idx++) {
						auto lentry = ldata[LEFT_CONSTANT ? 0 : base_idx];
						auto rentry = rdata[RIGHT_CONSTANT ? 0 : base_idx];
						result_data[base_idx] =
						    OPWRAPPER::template Operation<FUNC, OP, LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(
						        fun, lentry, rentry, mask, base_idx);
					}
				} else if (ValidityMask::NoneValid(validity_entry)) {
					// nothing valid: skip all
					base_idx = next;
					continue;
				} else {
					// partially valid: need to check individual elements for validity
					idx_t start = base_idx;
					for (; base_idx < next; base_idx++) {
						if (ValidityMask::RowIsValid(validity_entry, base_idx - start)) {
							auto lentry = ldata[LEFT_CONSTANT ? 0 : base_idx];
							auto rentry = rdata[RIGHT_CONSTANT ? 0 : base_idx];
							result_data[base_idx] =
							    OPWRAPPER::template Operation<FUNC, OP, LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(
							        fun, lentry, rentry, mask, base_idx);
						}
					}
				}
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				auto lentry = ldata[LEFT_CONSTANT ? 0 : i];
				auto rentry = rdata[RIGHT_CONSTANT ? 0 : i];
				result_data[i] = OPWRAPPER::template Operation<FUNC, OP, LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(
				    fun, lentry, rentry, mask, i);
			}
		}
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC>
	static void ExecuteConstant(Vector &left, Vector &right, Vector &result, FUNC fun) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);

		auto ldata = ConstantVector::GetData<LEFT_TYPE>(left);
		auto rdata = ConstantVector::GetData<RIGHT_TYPE>(right);
		auto result_data = ConstantVector::GetData<RESULT_TYPE>(result);

		if (ConstantVector::IsNull(left) || ConstantVector::IsNull(right)) {
			ConstantVector::SetNull(result, true);
			return;
		}
		*result_data = OPWRAPPER::template Operation<FUNC, OP, LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(
		    fun, *ldata, *rdata, ConstantVector::Validity(result), 0);
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC,
	          bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
	static void ExecuteFlat(Vector &left, Vector &right, Vector &result, idx_t count, FUNC fun) {
		auto ldata = FlatVector::GetData<LEFT_TYPE>(left);
		auto rdata = FlatVector::GetData<RIGHT_TYPE>(right);

		if ((LEFT_CONSTANT && ConstantVector::IsNull(left)) || (RIGHT_CONSTANT && ConstantVector::IsNull(right))) {
			// either left or right is constant NULL: result is constant NULL
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
			return;
		}

		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto result_data = FlatVector::GetData<RESULT_TYPE>(result);
		auto &result_validity = FlatVector::Validity(result);
		if (LEFT_CONSTANT) {
			if (OPWRAPPER::AddsNulls()) {
				result_validity.Copy(FlatVector::Validity(right), count);
			} else {
				FlatVector::SetValidity(result, FlatVector::Validity(right));
			}
		} else if (RIGHT_CONSTANT) {
			if (OPWRAPPER::AddsNulls()) {
				result_validity.Copy(FlatVector::Validity(left), count);
			} else {
				FlatVector::SetValidity(result, FlatVector::Validity(left));
			}
		} else {
			if (OPWRAPPER::AddsNulls()) {
				result_validity.Copy(FlatVector::Validity(left), count);
				if (result_validity.AllValid()) {
					result_validity.Copy(FlatVector::Validity(right), count);
				} else {
					result_validity.Combine(FlatVector::Validity(right), count);
				}
			} else {
				FlatVector::SetValidity(result, FlatVector::Validity(left));
				result_validity.Combine(FlatVector::Validity(right), count);
			}
		}
		ExecuteFlatLoop<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, LEFT_CONSTANT, RIGHT_CONSTANT>(
		    ldata, rdata, result_data, count, result_validity, fun);
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC>
	static void ExecuteGenericLoop(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
	                               RESULT_TYPE *__restrict result_data, const SelectionVector *__restrict lsel,
	                               const SelectionVector *__restrict rsel, idx_t count, ValidityMask &lvalidity,
	                               ValidityMask &rvalidity, ValidityMask &result_validity, FUNC fun) {
		if (!lvalidity.AllValid() || !rvalidity.AllValid()) {
			for (idx_t i = 0; i < count; i++) {
				auto lindex = lsel->get_index(i);
				auto rindex = rsel->get_index(i);
				if (lvalidity.RowIsValid(lindex) && rvalidity.RowIsValid(rindex)) {
					auto lentry = ldata[lindex];
					auto rentry = rdata[rindex];
					result_data[i] = OPWRAPPER::template Operation<FUNC, OP, LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(
					    fun, lentry, rentry, result_validity, i);
				} else {
					result_validity.SetInvalid(i);
				}
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				auto lentry = ldata[lsel->get_index(i)];
				auto rentry = rdata[rsel->get_index(i)];
				result_data[i] = OPWRAPPER::template Operation<FUNC, OP, LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(
				    fun, lentry, rentry, result_validity, i);
			}
		}
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC>
	static void ExecuteGeneric(Vector &left, Vector &right, Vector &result, idx_t count, FUNC fun) {
		VectorData ldata, rdata;

		left.Orrify(count, ldata);
		right.Orrify(count, rdata);

		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto result_data = FlatVector::GetData<RESULT_TYPE>(result);
		ExecuteGenericLoop<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC>(
		    (LEFT_TYPE *)ldata.data, (RIGHT_TYPE *)rdata.data, result_data, ldata.sel, rdata.sel, count, ldata.validity,
		    rdata.validity, FlatVector::Validity(result), fun);
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC>
	static void ExecuteSwitch(Vector &left, Vector &right, Vector &result, idx_t count, FUNC fun) {
		auto left_vector_type = left.GetVectorType();
		auto right_vector_type = right.GetVectorType();
		if (left_vector_type == VectorType::CONSTANT_VECTOR && right_vector_type == VectorType::CONSTANT_VECTOR) {
			ExecuteConstant<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC>(left, right, result, fun);
		} else if (left_vector_type == VectorType::FLAT_VECTOR && right_vector_type == VectorType::CONSTANT_VECTOR) {
			ExecuteFlat<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, false, true>(left, right, result,
			                                                                                  count, fun);
		} else if (left_vector_type == VectorType::CONSTANT_VECTOR && right_vector_type == VectorType::FLAT_VECTOR) {
			ExecuteFlat<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, true, false>(left, right, result,
			                                                                                  count, fun);
		} else if (left_vector_type == VectorType::FLAT_VECTOR && right_vector_type == VectorType::FLAT_VECTOR) {
			ExecuteFlat<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC, false, false>(left, right, result,
			                                                                                   count, fun);
		} else {
			ExecuteGeneric<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC>(left, right, result, count, fun);
		}
	}

public:
	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE,
	          class FUNC = std::function<RESULT_TYPE(LEFT_TYPE, RIGHT_TYPE)>>
	static void Execute(Vector &left, Vector &right, Vector &result, idx_t count, FUNC fun) {
		ExecuteSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, BinaryLambdaWrapper, bool, FUNC>(left, right, result, count,
		                                                                                   fun);
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP,
	          class OPWRAPPER = BinarySingleArgumentOperatorWrapper>
	static void Execute(Vector &left, Vector &right, Vector &result, idx_t count) {
		ExecuteSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OPWRAPPER, OP, bool>(left, right, result, count, false);
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP>
	static void ExecuteStandard(Vector &left, Vector &right, Vector &result, idx_t count) {
		ExecuteSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, BinaryStandardOperatorWrapper, OP, bool>(left, right, result,
		                                                                                           count, false);
	}

public:
	template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
	static idx_t SelectConstant(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
	                            SelectionVector *true_sel, SelectionVector *false_sel) {
		auto ldata = ConstantVector::GetData<LEFT_TYPE>(left);
		auto rdata = ConstantVector::GetData<RIGHT_TYPE>(right);

		// both sides are constant, return either 0 or the count
		// in this case we do not fill in the result selection vector at all
		if (ConstantVector::IsNull(left) || ConstantVector::IsNull(right) || !OP::Operation(*ldata, *rdata)) {
			if (false_sel) {
				for (idx_t i = 0; i < count; i++) {
					false_sel->set_index(i, sel->get_index(i));
				}
			}
			return 0;
		} else {
			if (true_sel) {
				for (idx_t i = 0; i < count; i++) {
					true_sel->set_index(i, sel->get_index(i));
				}
			}
			return count;
		}
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool LEFT_CONSTANT, bool RIGHT_CONSTANT, bool HAS_TRUE_SEL,
	          bool HAS_FALSE_SEL>
	static inline idx_t SelectFlatLoop(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
	                                   const SelectionVector *sel, idx_t count, ValidityMask &validity_mask,
	                                   SelectionVector *true_sel, SelectionVector *false_sel) {
		idx_t true_count = 0, false_count = 0;
		idx_t base_idx = 0;
		auto entry_count = ValidityMask::EntryCount(count);
		for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
			auto validity_entry = validity_mask.GetValidityEntry(entry_idx);
			idx_t next = MinValue<idx_t>(base_idx + ValidityMask::BITS_PER_VALUE, count);
			if (ValidityMask::AllValid(validity_entry)) {
				// all valid: perform operation
				for (; base_idx < next; base_idx++) {
					idx_t result_idx = sel->get_index(base_idx);
					idx_t lidx = LEFT_CONSTANT ? 0 : base_idx;
					idx_t ridx = RIGHT_CONSTANT ? 0 : base_idx;
					bool comparison_result = OP::Operation(ldata[lidx], rdata[ridx]);
					if (HAS_TRUE_SEL) {
						true_sel->set_index(true_count, result_idx);
						true_count += comparison_result;
					}
					if (HAS_FALSE_SEL) {
						false_sel->set_index(false_count, result_idx);
						false_count += !comparison_result;
					}
				}
			} else if (ValidityMask::NoneValid(validity_entry)) {
				// nothing valid: skip all
				if (HAS_FALSE_SEL) {
					for (; base_idx < next; base_idx++) {
						idx_t result_idx = sel->get_index(base_idx);
						false_sel->set_index(false_count, result_idx);
						false_count++;
					}
				}
				base_idx = next;
				continue;
			} else {
				// partially valid: need to check individual elements for validity
				idx_t start = base_idx;
				for (; base_idx < next; base_idx++) {
					idx_t result_idx = sel->get_index(base_idx);
					idx_t lidx = LEFT_CONSTANT ? 0 : base_idx;
					idx_t ridx = RIGHT_CONSTANT ? 0 : base_idx;
					bool comparison_result = ValidityMask::RowIsValid(validity_entry, base_idx - start) &&
					                         OP::Operation(ldata[lidx], rdata[ridx]);
					if (HAS_TRUE_SEL) {
						true_sel->set_index(true_count, result_idx);
						true_count += comparison_result;
					}
					if (HAS_FALSE_SEL) {
						false_sel->set_index(false_count, result_idx);
						false_count += !comparison_result;
					}
				}
			}
		}
		if (HAS_TRUE_SEL) {
			return true_count;
		} else {
			return count - false_count;
		}
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
	static inline idx_t SelectFlatLoopSwitch(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
	                                         const SelectionVector *sel, idx_t count, ValidityMask &mask,
	                                         SelectionVector *true_sel, SelectionVector *false_sel) {
		if (true_sel && false_sel) {
			return SelectFlatLoop<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT, true, true>(
			    ldata, rdata, sel, count, mask, true_sel, false_sel);
		} else if (true_sel) {
			return SelectFlatLoop<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT, true, false>(
			    ldata, rdata, sel, count, mask, true_sel, false_sel);
		} else {
			D_ASSERT(false_sel);
			return SelectFlatLoop<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT, false, true>(
			    ldata, rdata, sel, count, mask, true_sel, false_sel);
		}
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
	static idx_t SelectFlat(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
	                        SelectionVector *true_sel, SelectionVector *false_sel) {
		auto ldata = FlatVector::GetData<LEFT_TYPE>(left);
		auto rdata = FlatVector::GetData<RIGHT_TYPE>(right);

		if (LEFT_CONSTANT && ConstantVector::IsNull(left)) {
			return 0;
		}
		if (RIGHT_CONSTANT && ConstantVector::IsNull(right)) {
			return 0;
		}

		if (LEFT_CONSTANT) {
			return SelectFlatLoopSwitch<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT>(
			    ldata, rdata, sel, count, FlatVector::Validity(right), true_sel, false_sel);
		} else if (RIGHT_CONSTANT) {
			return SelectFlatLoopSwitch<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT>(
			    ldata, rdata, sel, count, FlatVector::Validity(left), true_sel, false_sel);
		} else {
			ValidityMask combined_mask = FlatVector::Validity(left);
			combined_mask.Combine(FlatVector::Validity(right), count);
			return SelectFlatLoopSwitch<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT>(
			    ldata, rdata, sel, count, combined_mask, true_sel, false_sel);
		}
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool NO_NULL, bool HAS_TRUE_SEL, bool HAS_FALSE_SEL>
	static inline idx_t
	SelectGenericLoop(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata, const SelectionVector *__restrict lsel,
	                  const SelectionVector *__restrict rsel, const SelectionVector *__restrict result_sel, idx_t count,
	                  ValidityMask &lvalidity, ValidityMask &rvalidity, SelectionVector *true_sel,
	                  SelectionVector *false_sel) {
		idx_t true_count = 0, false_count = 0;
		for (idx_t i = 0; i < count; i++) {
			auto result_idx = result_sel->get_index(i);
			auto lindex = lsel->get_index(i);
			auto rindex = rsel->get_index(i);
			if ((NO_NULL || (lvalidity.RowIsValid(lindex) && rvalidity.RowIsValid(rindex))) &&
			    OP::Operation(ldata[lindex], rdata[rindex])) {
				if (HAS_TRUE_SEL) {
					true_sel->set_index(true_count++, result_idx);
				}
			} else {
				if (HAS_FALSE_SEL) {
					false_sel->set_index(false_count++, result_idx);
				}
			}
		}
		if (HAS_TRUE_SEL) {
			return true_count;
		} else {
			return count - false_count;
		}
	}
	template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool NO_NULL>
	static inline idx_t
	SelectGenericLoopSelSwitch(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
	                           const SelectionVector *__restrict lsel, const SelectionVector *__restrict rsel,
	                           const SelectionVector *__restrict result_sel, idx_t count, ValidityMask &lvalidity,
	                           ValidityMask &rvalidity, SelectionVector *true_sel, SelectionVector *false_sel) {
		if (true_sel && false_sel) {
			return SelectGenericLoop<LEFT_TYPE, RIGHT_TYPE, OP, NO_NULL, true, true>(
			    ldata, rdata, lsel, rsel, result_sel, count, lvalidity, rvalidity, true_sel, false_sel);
		} else if (true_sel) {
			return SelectGenericLoop<LEFT_TYPE, RIGHT_TYPE, OP, NO_NULL, true, false>(
			    ldata, rdata, lsel, rsel, result_sel, count, lvalidity, rvalidity, true_sel, false_sel);
		} else {
			D_ASSERT(false_sel);
			return SelectGenericLoop<LEFT_TYPE, RIGHT_TYPE, OP, NO_NULL, false, true>(
			    ldata, rdata, lsel, rsel, result_sel, count, lvalidity, rvalidity, true_sel, false_sel);
		}
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
	static inline idx_t
	SelectGenericLoopSwitch(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
	                        const SelectionVector *__restrict lsel, const SelectionVector *__restrict rsel,
	                        const SelectionVector *__restrict result_sel, idx_t count, ValidityMask &lvalidity,
	                        ValidityMask &rvalidity, SelectionVector *true_sel, SelectionVector *false_sel) {
		if (!lvalidity.AllValid() || !rvalidity.AllValid()) {
			return SelectGenericLoopSelSwitch<LEFT_TYPE, RIGHT_TYPE, OP, false>(
			    ldata, rdata, lsel, rsel, result_sel, count, lvalidity, rvalidity, true_sel, false_sel);
		} else {
			return SelectGenericLoopSelSwitch<LEFT_TYPE, RIGHT_TYPE, OP, true>(
			    ldata, rdata, lsel, rsel, result_sel, count, lvalidity, rvalidity, true_sel, false_sel);
		}
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
	static idx_t SelectGeneric(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
	                           SelectionVector *true_sel, SelectionVector *false_sel) {
		VectorData ldata, rdata;

		left.Orrify(count, ldata);
		right.Orrify(count, rdata);

		return SelectGenericLoopSwitch<LEFT_TYPE, RIGHT_TYPE, OP>((LEFT_TYPE *)ldata.data, (RIGHT_TYPE *)rdata.data,
		                                                          ldata.sel, rdata.sel, sel, count, ldata.validity,
		                                                          rdata.validity, true_sel, false_sel);
	}

	template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
	static idx_t Select(Vector &left, Vector &right, const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
	                    SelectionVector *false_sel) {
		if (!sel) {
			sel = &FlatVector::INCREMENTAL_SELECTION_VECTOR;
		}
		if (left.GetVectorType() == VectorType::CONSTANT_VECTOR &&
		    right.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			return SelectConstant<LEFT_TYPE, RIGHT_TYPE, OP>(left, right, sel, count, true_sel, false_sel);
		} else if (left.GetVectorType() == VectorType::CONSTANT_VECTOR &&
		           right.GetVectorType() == VectorType::FLAT_VECTOR) {
			return SelectFlat<LEFT_TYPE, RIGHT_TYPE, OP, true, false>(left, right, sel, count, true_sel, false_sel);
		} else if (left.GetVectorType() == VectorType::FLAT_VECTOR &&
		           right.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			return SelectFlat<LEFT_TYPE, RIGHT_TYPE, OP, false, true>(left, right, sel, count, true_sel, false_sel);
		} else if (left.GetVectorType() == VectorType::FLAT_VECTOR &&
		           right.GetVectorType() == VectorType::FLAT_VECTOR) {
			return SelectFlat<LEFT_TYPE, RIGHT_TYPE, OP, false, false>(left, right, sel, count, true_sel, false_sel);
		} else {
			return SelectGeneric<LEFT_TYPE, RIGHT_TYPE, OP>(left, right, sel, count, true_sel, false_sel);
		}
	}
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/ternary_executor.hpp
//
//
//===----------------------------------------------------------------------===//







#include <functional>

namespace duckdb {

struct TernaryExecutor {
private:
	template <class A_TYPE, class B_TYPE, class C_TYPE, class RESULT_TYPE, class FUN>
	static inline void ExecuteLoop(A_TYPE *__restrict adata, B_TYPE *__restrict bdata, C_TYPE *__restrict cdata,
	                               RESULT_TYPE *__restrict result_data, idx_t count, const SelectionVector &asel,
	                               const SelectionVector &bsel, const SelectionVector &csel, ValidityMask &avalidity,
	                               ValidityMask &bvalidity, ValidityMask &cvalidity, ValidityMask &result_validity,
	                               FUN fun) {
		if (!avalidity.AllValid() || !bvalidity.AllValid() || !cvalidity.AllValid()) {
			for (idx_t i = 0; i < count; i++) {
				auto aidx = asel.get_index(i);
				auto bidx = bsel.get_index(i);
				auto cidx = csel.get_index(i);
				if (avalidity.RowIsValid(aidx) && bvalidity.RowIsValid(bidx) && cvalidity.RowIsValid(cidx)) {
					result_data[i] = fun(adata[aidx], bdata[bidx], cdata[cidx]);
				} else {
					result_validity.SetInvalid(i);
				}
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				auto aidx = asel.get_index(i);
				auto bidx = bsel.get_index(i);
				auto cidx = csel.get_index(i);
				result_data[i] = fun(adata[aidx], bdata[bidx], cdata[cidx]);
			}
		}
	}

public:
	template <class A_TYPE, class B_TYPE, class C_TYPE, class RESULT_TYPE,
	          class FUN = std::function<RESULT_TYPE(A_TYPE, B_TYPE, C_TYPE)>>
	static void Execute(Vector &a, Vector &b, Vector &c, Vector &result, idx_t count, FUN fun) {
		if (a.GetVectorType() == VectorType::CONSTANT_VECTOR && b.GetVectorType() == VectorType::CONSTANT_VECTOR &&
		    c.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			if (ConstantVector::IsNull(a) || ConstantVector::IsNull(b) || ConstantVector::IsNull(c)) {
				ConstantVector::SetNull(result, true);
			} else {
				auto adata = ConstantVector::GetData<A_TYPE>(a);
				auto bdata = ConstantVector::GetData<B_TYPE>(b);
				auto cdata = ConstantVector::GetData<C_TYPE>(c);
				auto result_data = ConstantVector::GetData<RESULT_TYPE>(result);
				result_data[0] = fun(*adata, *bdata, *cdata);
			}
		} else {
			result.SetVectorType(VectorType::FLAT_VECTOR);

			VectorData adata, bdata, cdata;
			a.Orrify(count, adata);
			b.Orrify(count, bdata);
			c.Orrify(count, cdata);

			ExecuteLoop<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE>(
			    (A_TYPE *)adata.data, (B_TYPE *)bdata.data, (C_TYPE *)cdata.data,
			    FlatVector::GetData<RESULT_TYPE>(result), count, *adata.sel, *bdata.sel, *cdata.sel, adata.validity,
			    bdata.validity, cdata.validity, FlatVector::Validity(result), fun);
		}
	}

private:
	template <class A_TYPE, class B_TYPE, class C_TYPE, class OP, bool NO_NULL, bool HAS_TRUE_SEL, bool HAS_FALSE_SEL>
	static inline idx_t SelectLoop(A_TYPE *__restrict adata, B_TYPE *__restrict bdata, C_TYPE *__restrict cdata,
	                               const SelectionVector *result_sel, idx_t count, const SelectionVector &asel,
	                               const SelectionVector &bsel, const SelectionVector &csel, ValidityMask &avalidity,
	                               ValidityMask &bvalidity, ValidityMask &cvalidity, SelectionVector *true_sel,
	                               SelectionVector *false_sel) {
		idx_t true_count = 0, false_count = 0;
		for (idx_t i = 0; i < count; i++) {
			auto result_idx = result_sel->get_index(i);
			auto aidx = asel.get_index(i);
			auto bidx = bsel.get_index(i);
			auto cidx = csel.get_index(i);
			bool comparison_result =
			    (NO_NULL || (avalidity.RowIsValid(aidx) && bvalidity.RowIsValid(bidx) && cvalidity.RowIsValid(cidx))) &&
			    OP::Operation(adata[aidx], bdata[bidx], cdata[cidx]);
			if (HAS_TRUE_SEL) {
				true_sel->set_index(true_count, result_idx);
				true_count += comparison_result;
			}
			if (HAS_FALSE_SEL) {
				false_sel->set_index(false_count, result_idx);
				false_count += !comparison_result;
			}
		}
		if (HAS_TRUE_SEL) {
			return true_count;
		} else {
			return count - false_count;
		}
	}

	template <class A_TYPE, class B_TYPE, class C_TYPE, class OP, bool NO_NULL>
	static inline idx_t SelectLoopSelSwitch(VectorData &adata, VectorData &bdata, VectorData &cdata,
	                                        const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
	                                        SelectionVector *false_sel) {
		if (true_sel && false_sel) {
			return SelectLoop<A_TYPE, B_TYPE, C_TYPE, OP, NO_NULL, true, true>(
			    (A_TYPE *)adata.data, (B_TYPE *)bdata.data, (C_TYPE *)cdata.data, sel, count, *adata.sel, *bdata.sel,
			    *cdata.sel, adata.validity, bdata.validity, cdata.validity, true_sel, false_sel);
		} else if (true_sel) {
			return SelectLoop<A_TYPE, B_TYPE, C_TYPE, OP, NO_NULL, true, false>(
			    (A_TYPE *)adata.data, (B_TYPE *)bdata.data, (C_TYPE *)cdata.data, sel, count, *adata.sel, *bdata.sel,
			    *cdata.sel, adata.validity, bdata.validity, cdata.validity, true_sel, false_sel);
		} else {
			D_ASSERT(false_sel);
			return SelectLoop<A_TYPE, B_TYPE, C_TYPE, OP, NO_NULL, false, true>(
			    (A_TYPE *)adata.data, (B_TYPE *)bdata.data, (C_TYPE *)cdata.data, sel, count, *adata.sel, *bdata.sel,
			    *cdata.sel, adata.validity, bdata.validity, cdata.validity, true_sel, false_sel);
		}
	}

	template <class A_TYPE, class B_TYPE, class C_TYPE, class OP>
	static inline idx_t SelectLoopSwitch(VectorData &adata, VectorData &bdata, VectorData &cdata,
	                                     const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
	                                     SelectionVector *false_sel) {
		if (!adata.validity.AllValid() || !bdata.validity.AllValid() || !cdata.validity.AllValid()) {
			return SelectLoopSelSwitch<A_TYPE, B_TYPE, C_TYPE, OP, false>(adata, bdata, cdata, sel, count, true_sel,
			                                                              false_sel);
		} else {
			return SelectLoopSelSwitch<A_TYPE, B_TYPE, C_TYPE, OP, true>(adata, bdata, cdata, sel, count, true_sel,
			                                                             false_sel);
		}
	}

public:
	template <class A_TYPE, class B_TYPE, class C_TYPE, class OP>
	static idx_t Select(Vector &a, Vector &b, Vector &c, const SelectionVector *sel, idx_t count,
	                    SelectionVector *true_sel, SelectionVector *false_sel) {
		if (!sel) {
			sel = &FlatVector::INCREMENTAL_SELECTION_VECTOR;
		}
		VectorData adata, bdata, cdata;
		a.Orrify(count, adata);
		b.Orrify(count, bdata);
		c.Orrify(count, cdata);

		return SelectLoopSwitch<A_TYPE, B_TYPE, C_TYPE, OP>(adata, bdata, cdata, sel, count, true_sel, false_sel);
	}
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/unary_executor.hpp
//
//
//===----------------------------------------------------------------------===//







#include <functional>

namespace duckdb {

struct UnaryOperatorWrapper {
	template <class FUNC, class OP, class INPUT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, INPUT_TYPE input, ValidityMask &mask, idx_t idx) {
		return OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input);
	}

	static bool AddsNulls() {
		return false;
	}
};

struct UnaryLambdaWrapper {
	template <class FUNC, class OP, class INPUT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, INPUT_TYPE input, ValidityMask &mask, idx_t idx) {
		return fun(input);
	}

	static bool AddsNulls() {
		return false;
	}
};

struct UnaryExecutor {
private:
	template <class INPUT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC>
	static inline void ExecuteLoop(INPUT_TYPE *__restrict ldata, RESULT_TYPE *__restrict result_data, idx_t count,
	                               const SelectionVector *__restrict sel_vector, ValidityMask &mask,
	                               ValidityMask &result_mask, FUNC fun) {
		ASSERT_RESTRICT(ldata, ldata + count, result_data, result_data + count);

		if (!mask.AllValid()) {
			result_mask.EnsureWritable();
			for (idx_t i = 0; i < count; i++) {
				auto idx = sel_vector->get_index(i);
				if (mask.RowIsValidUnsafe(idx)) {
					result_data[i] = OPWRAPPER::template Operation<FUNC, OP, INPUT_TYPE, RESULT_TYPE>(fun, ldata[idx],
					                                                                                  result_mask, i);
				} else {
					result_mask.SetInvalid(i);
				}
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				auto idx = sel_vector->get_index(i);
				result_data[i] =
				    OPWRAPPER::template Operation<FUNC, OP, INPUT_TYPE, RESULT_TYPE>(fun, ldata[idx], result_mask, i);
			}
		}
	}

	template <class INPUT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC>
	static inline void ExecuteFlat(INPUT_TYPE *__restrict ldata, RESULT_TYPE *__restrict result_data, idx_t count,
	                               ValidityMask &mask, ValidityMask &result_mask, FUNC fun) {
		ASSERT_RESTRICT(ldata, ldata + count, result_data, result_data + count);

		if (!mask.AllValid()) {
			if (!OPWRAPPER::AddsNulls()) {
				result_mask.Initialize(mask);
			} else {
				result_mask.Copy(mask, count);
			}
			idx_t base_idx = 0;
			auto entry_count = ValidityMask::EntryCount(count);
			for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
				auto validity_entry = mask.GetValidityEntry(entry_idx);
				idx_t next = MinValue<idx_t>(base_idx + ValidityMask::BITS_PER_VALUE, count);
				if (ValidityMask::AllValid(validity_entry)) {
					// all valid: perform operation
					for (; base_idx < next; base_idx++) {
						result_data[base_idx] = OPWRAPPER::template Operation<FUNC, OP, INPUT_TYPE, RESULT_TYPE>(
						    fun, ldata[base_idx], result_mask, base_idx);
					}
				} else if (ValidityMask::NoneValid(validity_entry)) {
					// nothing valid: skip all
					base_idx = next;
					continue;
				} else {
					// partially valid: need to check individual elements for validity
					idx_t start = base_idx;
					for (; base_idx < next; base_idx++) {
						if (ValidityMask::RowIsValid(validity_entry, base_idx - start)) {
							D_ASSERT(mask.RowIsValid(base_idx));
							result_data[base_idx] = OPWRAPPER::template Operation<FUNC, OP, INPUT_TYPE, RESULT_TYPE>(
							    fun, ldata[base_idx], result_mask, base_idx);
						}
					}
				}
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				result_data[i] =
				    OPWRAPPER::template Operation<FUNC, OP, INPUT_TYPE, RESULT_TYPE>(fun, ldata[i], result_mask, i);
			}
		}
	}

	template <class INPUT_TYPE, class RESULT_TYPE, class OPWRAPPER, class OP, class FUNC>
	static inline void ExecuteStandard(Vector &input, Vector &result, idx_t count, FUNC fun) {
		switch (input.GetVectorType()) {
		case VectorType::CONSTANT_VECTOR: {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			auto result_data = ConstantVector::GetData<RESULT_TYPE>(result);
			auto ldata = ConstantVector::GetData<INPUT_TYPE>(input);

			if (ConstantVector::IsNull(input)) {
				ConstantVector::SetNull(result, true);
			} else {
				ConstantVector::SetNull(result, false);
				*result_data = OPWRAPPER::template Operation<FUNC, OP, INPUT_TYPE, RESULT_TYPE>(
				    fun, *ldata, ConstantVector::Validity(result), 0);
			}
			break;
		}
		case VectorType::FLAT_VECTOR: {
			result.SetVectorType(VectorType::FLAT_VECTOR);
			auto result_data = FlatVector::GetData<RESULT_TYPE>(result);
			auto ldata = FlatVector::GetData<INPUT_TYPE>(input);

			ExecuteFlat<INPUT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC>(
			    ldata, result_data, count, FlatVector::Validity(input), FlatVector::Validity(result), fun);
			break;
		}
		default: {
			VectorData vdata;
			input.Orrify(count, vdata);

			result.SetVectorType(VectorType::FLAT_VECTOR);
			auto result_data = FlatVector::GetData<RESULT_TYPE>(result);
			auto ldata = (INPUT_TYPE *)vdata.data;

			ExecuteLoop<INPUT_TYPE, RESULT_TYPE, OPWRAPPER, OP, FUNC>(
			    ldata, result_data, count, vdata.sel, vdata.validity, FlatVector::Validity(result), fun);
			break;
		}
		}
	}

public:
	template <class INPUT_TYPE, class RESULT_TYPE, class OP, class OPWRAPPER = UnaryOperatorWrapper>
	static void Execute(Vector &input, Vector &result, idx_t count) {
		ExecuteStandard<INPUT_TYPE, RESULT_TYPE, OPWRAPPER, OP, bool>(input, result, count, false);
	}

	template <class INPUT_TYPE, class RESULT_TYPE, class FUNC = std::function<RESULT_TYPE(INPUT_TYPE)>>
	static void Execute(Vector &input, Vector &result, idx_t count, FUNC fun) {
		ExecuteStandard<INPUT_TYPE, RESULT_TYPE, UnaryLambdaWrapper, bool, FUNC>(input, result, count, fun);
	}
};

} // namespace duckdb


//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/expression_executor_state.hpp
//
//
//===----------------------------------------------------------------------===//





//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/cycle_counter.hpp
//
//
//===----------------------------------------------------------------------===//




//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/chrono.hpp
//
//
//===----------------------------------------------------------------------===//



#include <chrono>

namespace duckdb {
using std::chrono::duration;
using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;
using std::chrono::system_clock;
using std::chrono::time_point;
} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/random_engine.hpp
//
//
//===----------------------------------------------------------------------===//




//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/limits.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

template <class T>
struct NumericLimits {
	static T Minimum();
	static T Maximum();
};

template <>
struct NumericLimits<int8_t> {
	static int8_t Minimum();
	static int8_t Maximum();
};
template <>
struct NumericLimits<int16_t> {
	static int16_t Minimum();
	static int16_t Maximum();
};
template <>
struct NumericLimits<int32_t> {
	static int32_t Minimum();
	static int32_t Maximum();
};
template <>
struct NumericLimits<int64_t> {
	static int64_t Minimum();
	static int64_t Maximum();
};
template <>
struct NumericLimits<hugeint_t> {
	static hugeint_t Minimum();
	static hugeint_t Maximum();
};
template <>
struct NumericLimits<uint8_t> {
	static uint8_t Minimum();
	static uint8_t Maximum();
};
template <>
struct NumericLimits<uint16_t> {
	static uint16_t Minimum();
	static uint16_t Maximum();
};
template <>
struct NumericLimits<uint32_t> {
	static uint32_t Minimum();
	static uint32_t Maximum();
};
template <>
struct NumericLimits<uint64_t> {
	static uint64_t Minimum();
	static uint64_t Maximum();
};
template <>
struct NumericLimits<float> {
	static float Minimum();
	static float Maximum();
};
template <>
struct NumericLimits<double> {
	static double Minimum();
	static double Maximum();
};

//! Returns the minimal type that guarantees an integer value from not
//! overflowing
PhysicalType MinimalType(int64_t value);

} // namespace duckdb

#include <random>

namespace duckdb {

struct RandomEngine {
	std::mt19937 random_engine;
	explicit RandomEngine(int64_t seed) {
		if (seed < 0) {
			std::random_device rd;
			random_engine.seed(rd());
		} else {
			random_engine.seed(seed);
		}
	}

	//! Generate a random number between min and max
	double NextRandom(double min, double max) {
		std::uniform_real_distribution<double> dist(min, max);
		return dist(random_engine);
	}
	//! Generate a random number between 0 and 1
	double NextRandom() {
		return NextRandom(0, 1);
	}
	uint32_t NextRandomInteger() {
		std::uniform_int_distribution<uint32_t> dist(0, NumericLimits<uint32_t>::Maximum());
		return dist(random_engine);
	}
};

} // namespace duckdb


namespace duckdb {

//! The cycle counter can be used to measure elapsed cycles for a function, expression and ...
//! Optimized by sampling mechanism. Once per 100 times.
//! //Todo Can be optimized further by calling RDTSC once per sample
class CycleCounter {
	friend struct ExpressionInfo;
	friend struct ExpressionRootInfo;
	static constexpr int SAMPLING_RATE = 50;
	static constexpr int SAMPLING_VARIANCE = 100;

public:
	CycleCounter() : random(-1) {
	}
	// Next_sample determines if a sample needs to be taken, if so start the profiler
	void BeginSample() {
		if (current_count >= next_sample) {
			tmp = Tick();
		}
	}

	// End the sample
	void EndSample(int chunk_size) {
		if (current_count >= next_sample) {
			time += Tick() - tmp;
		}
		if (current_count >= next_sample) {
			next_sample = SAMPLING_RATE + random.NextRandomInteger() % SAMPLING_VARIANCE;
			++sample_count;
			sample_tuples_count += chunk_size;
			current_count = 0;
		} else {
			++current_count;
		}
		tuples_count += chunk_size;
	}

private:
	uint64_t Tick() const;
	// current number on RDT register
	uint64_t tmp;
	// Elapsed cycles
	uint64_t time = 0;
	//! Count the number of time the executor called since last sampling
	uint64_t current_count = 0;
	//! Show the next sample
	uint64_t next_sample = 0;
	//! Count the number of samples
	uint64_t sample_count = 0;
	//! Count the number of tuples sampled
	uint64_t sample_tuples_count = 0;
	//! Count the number of ALL tuples
	uint64_t tuples_count = 0;
	//! the random number generator used for sampling
	RandomEngine random;
};

} // namespace duckdb



namespace duckdb {
class Expression;
class ExpressionExecutor;
struct ExpressionExecutorState;

struct ExpressionState {
	ExpressionState(const Expression &expr, ExpressionExecutorState &root);
	virtual ~ExpressionState() {
	}

	const Expression &expr;
	ExpressionExecutorState &root;
	vector<unique_ptr<ExpressionState>> child_states;
	vector<LogicalType> types;
	DataChunk intermediate_chunk;
	string name;
	CycleCounter profiler;

public:
	void AddChild(Expression *expr);
	void Finalize();
};

struct ExpressionExecutorState {
	explicit ExpressionExecutorState(const string &name);
	unique_ptr<ExpressionState> root_state;
	ExpressionExecutor *executor;
	CycleCounter profiler;
	string name;
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/function.hpp
//
//
//===----------------------------------------------------------------------===//





//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/unordered_set.hpp
//
//
//===----------------------------------------------------------------------===//



#include <unordered_set>

namespace duckdb {
using std::unordered_set;
}

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/column_definition.hpp
//
//
//===----------------------------------------------------------------------===//





//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_expression.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/base_expression.hpp
//
//
//===----------------------------------------------------------------------===//




//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/expression_type.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

//===--------------------------------------------------------------------===//
// Predicate Expression Operation Types
//===--------------------------------------------------------------------===//
enum class ExpressionType : uint8_t {
	INVALID = 0,

	// explicitly cast left as right (right is integer in ValueType enum)
	OPERATOR_CAST = 12,
	// logical not operator
	OPERATOR_NOT = 13,
	// is null operator
	OPERATOR_IS_NULL = 14,
	// is not null operator
	OPERATOR_IS_NOT_NULL = 15,

	// -----------------------------
	// Comparison Operators
	// -----------------------------
	// equal operator between left and right
	COMPARE_EQUAL = 25,
	// compare initial boundary
	COMPARE_BOUNDARY_START = COMPARE_EQUAL,
	// inequal operator between left and right
	COMPARE_NOTEQUAL = 26,
	// less than operator between left and right
	COMPARE_LESSTHAN = 27,
	// greater than operator between left and right
	COMPARE_GREATERTHAN = 28,
	// less than equal operator between left and right
	COMPARE_LESSTHANOREQUALTO = 29,
	// greater than equal operator between left and right
	COMPARE_GREATERTHANOREQUALTO = 30,
	// IN operator [left IN (right1, right2, ...)]
	COMPARE_IN = 35,
	// NOT IN operator [left NOT IN (right1, right2, ...)]
	COMPARE_NOT_IN = 36,
	// IS DISTINCT FROM operator
	COMPARE_DISTINCT_FROM = 37,

	COMPARE_BETWEEN = 38,
	COMPARE_NOT_BETWEEN = 39,
	// IS NOT DISTINCT FROM operator
	COMPARE_NOT_DISTINCT_FROM = 40,
	// compare final boundary
	COMPARE_BOUNDARY_END = COMPARE_NOT_DISTINCT_FROM,

	// -----------------------------
	// Conjunction Operators
	// -----------------------------
	CONJUNCTION_AND = 50,
	CONJUNCTION_OR = 51,

	// -----------------------------
	// Values
	// -----------------------------
	VALUE_CONSTANT = 75,
	VALUE_PARAMETER = 76,
	VALUE_TUPLE = 77,
	VALUE_TUPLE_ADDRESS = 78,
	VALUE_NULL = 79,
	VALUE_VECTOR = 80,
	VALUE_SCALAR = 81,
	VALUE_DEFAULT = 82,

	// -----------------------------
	// Aggregates
	// -----------------------------
	AGGREGATE = 100,
	BOUND_AGGREGATE = 101,

	// -----------------------------
	// Window Functions
	// -----------------------------
	WINDOW_AGGREGATE = 110,

	WINDOW_RANK = 120,
	WINDOW_RANK_DENSE = 121,
	WINDOW_NTILE = 122,
	WINDOW_PERCENT_RANK = 123,
	WINDOW_CUME_DIST = 124,
	WINDOW_ROW_NUMBER = 125,

	WINDOW_FIRST_VALUE = 130,
	WINDOW_LAST_VALUE = 131,
	WINDOW_LEAD = 132,
	WINDOW_LAG = 133,

	// -----------------------------
	// Functions
	// -----------------------------
	FUNCTION = 140,
	BOUND_FUNCTION = 141,

	// -----------------------------
	// Operators
	// -----------------------------
	CASE_EXPR = 150,
	OPERATOR_NULLIF = 151,
	OPERATOR_COALESCE = 152,
	ARRAY_EXTRACT = 153,
	ARRAY_SLICE = 154,
	STRUCT_EXTRACT = 155,
	ARRAY_CONSTRUCTOR = 156,

	// -----------------------------
	// Subquery IN/EXISTS
	// -----------------------------
	SUBQUERY = 175,

	// -----------------------------
	// Parser
	// -----------------------------
	STAR = 200,
	TABLE_STAR = 201,
	PLACEHOLDER = 202,
	COLUMN_REF = 203,
	FUNCTION_REF = 204,
	TABLE_REF = 205,

	// -----------------------------
	// Miscellaneous
	// -----------------------------
	CAST = 225,
	BOUND_REF = 227,
	BOUND_COLUMN_REF = 228,
	BOUND_UNNEST = 229,
	COLLATE = 230,
	LAMBDA = 231,
	POSITIONAL_REFERENCE = 232
};

//===--------------------------------------------------------------------===//
// Expression Class
//===--------------------------------------------------------------------===//
enum class ExpressionClass : uint8_t {
	INVALID = 0,
	//===--------------------------------------------------------------------===//
	// Parsed Expressions
	//===--------------------------------------------------------------------===//
	AGGREGATE = 1,
	CASE = 2,
	CAST = 3,
	COLUMN_REF = 4,
	COMPARISON = 5,
	CONJUNCTION = 6,
	CONSTANT = 7,
	DEFAULT = 8,
	FUNCTION = 9,
	OPERATOR = 10,
	STAR = 11,
	TABLE_STAR = 12,
	SUBQUERY = 13,
	WINDOW = 14,
	PARAMETER = 15,
	COLLATE = 16,
	LAMBDA = 17,
	POSITIONAL_REFERENCE = 18,
	BETWEEN = 19,
	//===--------------------------------------------------------------------===//
	// Bound Expressions
	//===--------------------------------------------------------------------===//
	BOUND_AGGREGATE = 25,
	BOUND_CASE = 26,
	BOUND_CAST = 27,
	BOUND_COLUMN_REF = 28,
	BOUND_COMPARISON = 29,
	BOUND_CONJUNCTION = 30,
	BOUND_CONSTANT = 31,
	BOUND_DEFAULT = 32,
	BOUND_FUNCTION = 33,
	BOUND_OPERATOR = 34,
	BOUND_PARAMETER = 35,
	BOUND_REF = 36,
	BOUND_SUBQUERY = 37,
	BOUND_WINDOW = 38,
	BOUND_BETWEEN = 39,
	BOUND_UNNEST = 40,
	//===--------------------------------------------------------------------===//
	// Miscellaneous
	//===--------------------------------------------------------------------===//
	BOUND_EXPRESSION = 50
};

string ExpressionTypeToString(ExpressionType type);
string ExpressionTypeToOperator(ExpressionType type);

//! Negate a comparison expression, turning e.g. = into !=, or < into >=
ExpressionType NegateComparisionExpression(ExpressionType type);
//! Flip a comparison expression, turning e.g. < into >, or = into =
ExpressionType FlipComparisionExpression(ExpressionType type);

} // namespace duckdb


namespace duckdb {

//!  The BaseExpression class is a base class that can represent any expression
//!  part of a SQL statement.
class BaseExpression {
public:
	//! Create an Expression
	BaseExpression(ExpressionType type, ExpressionClass expression_class)
	    : type(type), expression_class(expression_class) {
	}
	virtual ~BaseExpression() {
	}

	//! Returns the type of the expression
	ExpressionType GetExpressionType() {
		return type;
	}
	//! Returns the class of the expression
	ExpressionClass GetExpressionClass() {
		return expression_class;
	}

	//! Type of the expression
	ExpressionType type;
	//! The expression class of the node
	ExpressionClass expression_class;
	//! The alias of the expression,
	string alias;

public:
	//! Returns true if this expression is an aggregate or not.
	/*!
	 Examples:

	 (1) SUM(a) + 1 -- True

	 (2) a + 1 -- False
	 */
	virtual bool IsAggregate() const = 0;
	//! Returns true if the expression has a window function or not
	virtual bool IsWindow() const = 0;
	//! Returns true if the query contains a subquery
	virtual bool HasSubquery() const = 0;
	//! Returns true if expression does not contain a group ref or col ref or parameter
	virtual bool IsScalar() const = 0;
	//! Returns true if the expression has a parameter
	virtual bool HasParameter() const = 0;

	//! Get the name of the expression
	virtual string GetName() const;
	//! Convert the Expression to a String
	virtual string ToString() const = 0;
	//! Print the expression to stdout
	void Print();

	//! Creates a hash value of this expression. It is important that if two expressions are identical (i.e.
	//! Expression::Equals() returns true), that their hash value is identical as well.
	virtual hash_t Hash() const = 0;
	//! Returns true if this expression is equal to another expression
	virtual bool Equals(const BaseExpression *other) const;

	static bool Equals(BaseExpression *left, BaseExpression *right) {
		if (left == right) {
			return true;
		}
		if (!left || !right) {
			return false;
		}
		return left->Equals(right);
	}
	bool operator==(const BaseExpression &rhs) {
		return this->Equals(&rhs);
	}
};

} // namespace duckdb


namespace duckdb {
class Serializer;
class Deserializer;

//!  The ParsedExpression class is a base class that can represent any expression
//!  part of a SQL statement.
/*!
 The ParsedExpression class is a base class that can represent any expression
 part of a SQL statement. This is, for example, a column reference in a SELECT
 clause, but also operators, aggregates or filters. The Expression is emitted by the parser and does not contain any
 information about bindings to the catalog or to the types. ParsedExpressions are transformed into regular Expressions
 in the Binder.
 */
class ParsedExpression : public BaseExpression {
public:
	//! Create an Expression
	ParsedExpression(ExpressionType type, ExpressionClass expression_class) : BaseExpression(type, expression_class) {
	}

	//! The location in the query (if any)
	idx_t query_location = INVALID_INDEX;

public:
	bool IsAggregate() const override;
	bool IsWindow() const override;
	bool HasSubquery() const override;
	bool IsScalar() const override;
	bool HasParameter() const override;

	bool Equals(const BaseExpression *other) const override;
	hash_t Hash() const override;

	//! Create a copy of this expression
	virtual unique_ptr<ParsedExpression> Copy() const = 0;

	//! Serializes an Expression to a stand-alone binary blob
	virtual void Serialize(Serializer &serializer);
	//! Deserializes a blob back into an Expression [CAN THROW:
	//! SerializationException]
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &source);

protected:
	//! Copy base Expression properties from another expression to this one,
	//! used in Copy method
	void CopyProperties(const ParsedExpression &other) {
		type = other.type;
		expression_class = other.expression_class;
		alias = other.alias;
	}
};

} // namespace duckdb


namespace duckdb {

//! A column of a table.
class ColumnDefinition {
public:
	ColumnDefinition(string name, LogicalType type) : name(name), type(type) {
	}
	ColumnDefinition(string name, LogicalType type, unique_ptr<ParsedExpression> default_value)
	    : name(name), type(type), default_value(move(default_value)) {
	}

	//! The name of the entry
	string name;
	//! The index of the column in the table
	idx_t oid;
	//! The type of the column
	LogicalType type;
	//! The default value of the column (if any)
	unique_ptr<ParsedExpression> default_value;

public:
	ColumnDefinition Copy() const;

	void Serialize(Serializer &serializer) const;
	static ColumnDefinition Deserialize(Deserializer &source);
};

} // namespace duckdb


namespace duckdb {
class CatalogEntry;
class Catalog;
class ClientContext;
class Expression;
class ExpressionExecutor;
class Transaction;

class AggregateFunction;
class AggregateFunctionSet;
class CopyFunction;
class PragmaFunction;
class ScalarFunctionSet;
class ScalarFunction;
class TableFunctionSet;
class TableFunction;

struct PragmaInfo;

struct FunctionData {
	virtual ~FunctionData() {
	}

	virtual unique_ptr<FunctionData> Copy() {
		return make_unique<FunctionData>();
	};
	virtual bool Equals(FunctionData &other) {
		return true;
	}
	static bool Equals(FunctionData *left, FunctionData *right) {
		if (left == right) {
			return true;
		}
		if (!left || !right) {
			return false;
		}
		return left->Equals(*right);
	}
};

struct TableFunctionData : public FunctionData {
	unique_ptr<FunctionData> Copy() override {
		throw NotImplementedException("Copy not required for table-producing function");
	}
	// used to pass on projections to table functions that support them. NB, can contain COLUMN_IDENTIFIER_ROW_ID
	vector<idx_t> column_ids;
};

struct FunctionParameters {
	vector<Value> values;
	unordered_map<string, Value> named_parameters;
};

//! Function is the base class used for any type of function (scalar, aggregate or simple function)
class Function {
public:
	explicit Function(string name) : name(name) {
	}
	virtual ~Function() {
	}

	//! The name of the function
	string name;

public:
	//! Returns the formatted string name(arg1, arg2, ...)
	DUCKDB_API static string CallToString(const string &name, const vector<LogicalType> &arguments);
	//! Returns the formatted string name(arg1, arg2..) -> return_type
	DUCKDB_API static string CallToString(const string &name, const vector<LogicalType> &arguments,
	                                      const LogicalType &return_type);
	//! Returns the formatted string name(arg1, arg2.., np1=a, np2=b, ...)
	DUCKDB_API static string CallToString(const string &name, const vector<LogicalType> &arguments,
	                                      const unordered_map<string, LogicalType> &named_parameters);

	//! Bind a scalar function from the set of functions and input arguments. Returns the index of the chosen function,
	//! returns INVALID_INDEX and sets error if none could be found
	static idx_t BindFunction(const string &name, vector<ScalarFunction> &functions, vector<LogicalType> &arguments,
	                          string &error);
	static idx_t BindFunction(const string &name, vector<ScalarFunction> &functions,
	                          vector<unique_ptr<Expression>> &arguments, string &error);
	//! Bind an aggregate function from the set of functions and input arguments. Returns the index of the chosen
	//! function, returns INVALID_INDEX and sets error if none could be found
	static idx_t BindFunction(const string &name, vector<AggregateFunction> &functions, vector<LogicalType> &arguments,
	                          string &error);
	static idx_t BindFunction(const string &name, vector<AggregateFunction> &functions,
	                          vector<unique_ptr<Expression>> &arguments, string &error);
	//! Bind a table function from the set of functions and input arguments. Returns the index of the chosen
	//! function, returns INVALID_INDEX and sets error if none could be found
	static idx_t BindFunction(const string &name, vector<TableFunction> &functions, vector<LogicalType> &arguments,
	                          string &error);
	static idx_t BindFunction(const string &name, vector<TableFunction> &functions,
	                          vector<unique_ptr<Expression>> &arguments, string &error);
	//! Bind a pragma function from the set of functions and input arguments
	static idx_t BindFunction(const string &name, vector<PragmaFunction> &functions, PragmaInfo &info, string &error);
};

class SimpleFunction : public Function {
public:
	SimpleFunction(string name, vector<LogicalType> arguments,
	               LogicalType varargs = LogicalType(LogicalTypeId::INVALID))
	    : Function(name), arguments(move(arguments)), varargs(varargs) {
	}
	~SimpleFunction() override {
	}

	//! The set of arguments of the function
	vector<LogicalType> arguments;
	//! The type of varargs to support, or LogicalTypeId::INVALID if the function does not accept variable length
	//! arguments
	LogicalType varargs;

public:
	virtual string ToString() {
		return Function::CallToString(name, arguments);
	}

	bool HasVarArgs() const {
		return varargs.id() != LogicalTypeId::INVALID;
	}
};

class SimpleNamedParameterFunction : public SimpleFunction {
public:
	SimpleNamedParameterFunction(string name, vector<LogicalType> arguments,
	                             LogicalType varargs = LogicalType(LogicalTypeId::INVALID))
	    : SimpleFunction(name, move(arguments), varargs) {
	}
	~SimpleNamedParameterFunction() override {
	}

	//! The named parameters of the function
	unordered_map<string, LogicalType> named_parameters;

public:
	string ToString() override {
		return Function::CallToString(name, arguments, named_parameters);
	}

	bool HasNamedParameters() {
		return named_parameters.size() != 0;
	}

	void EvaluateInputParameters(vector<LogicalType> &arguments, vector<Value> &parameters,
	                             unordered_map<string, Value> &named_parameters,
	                             vector<unique_ptr<ParsedExpression>> &children);
};

class BaseScalarFunction : public SimpleFunction {
public:
	BaseScalarFunction(string name, vector<LogicalType> arguments, LogicalType return_type, bool has_side_effects,
	                   LogicalType varargs = LogicalType(LogicalTypeId::INVALID))
	    : SimpleFunction(move(name), move(arguments), move(varargs)), return_type(return_type),
	      has_side_effects(has_side_effects) {
	}
	~BaseScalarFunction() override {
	}

	//! Return type of the function
	LogicalType return_type;
	//! Whether or not the function has side effects (e.g. sequence increments, random() functions, NOW()). Functions
	//! with side-effects cannot be constant-folded.
	bool has_side_effects;

public:
	hash_t Hash() const;

	//! Cast a set of expressions to the arguments of this function
	void CastToFunctionArguments(vector<unique_ptr<Expression>> &children);

	string ToString() override {
		return Function::CallToString(name, arguments, return_type);
	}
};

class BuiltinFunctions {
public:
	BuiltinFunctions(ClientContext &transaction, Catalog &catalog);

	//! Initialize a catalog with all built-in functions
	void Initialize();

public:
	void AddFunction(AggregateFunctionSet set);
	void AddFunction(AggregateFunction function);
	void AddFunction(ScalarFunctionSet set);
	void AddFunction(PragmaFunction function);
	void AddFunction(const string &name, vector<PragmaFunction> functions);
	void AddFunction(ScalarFunction function);
	void AddFunction(const vector<string> &names, ScalarFunction function);
	void AddFunction(TableFunctionSet set);
	void AddFunction(TableFunction function);
	void AddFunction(CopyFunction function);

	void AddCollation(string name, ScalarFunction function, bool combinable = false,
	                  bool not_required_for_equality = false);

private:
	ClientContext &context;
	Catalog &catalog;

private:
	template <class T>
	void Register() {
		T::RegisterFunction(*this);
	}

	// table-producing functions
	void RegisterSQLiteFunctions();
	void RegisterReadFunctions();
	void RegisterTableFunctions();
	void RegisterArrowFunctions();

	// aggregates
	void RegisterAlgebraicAggregates();
	void RegisterDistributiveAggregates();
	void RegisterNestedAggregates();
	void RegisterHolisticAggregates();
	void RegisterRegressiveAggregates();

	// scalar functions
	void RegisterDateFunctions();
	void RegisterGenericFunctions();
	void RegisterMathFunctions();
	void RegisterOperators();
	void RegisterStringFunctions();
	void RegisterNestedFunctions();
	void RegisterSequenceFunctions();
	void RegisterTrigonometricsFunctions();

	// pragmas
	void RegisterPragmaFunctions();
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/base_statistics.hpp
//
//
//===----------------------------------------------------------------------===//





//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/comparison_operators.hpp
//
//
//===----------------------------------------------------------------------===//





//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/hugeint.hpp
//
//
//===----------------------------------------------------------------------===//






namespace duckdb {

//! The Hugeint class contains static operations for the INT128 type
class Hugeint {
public:
	//! Convert a string to a hugeint object
	static bool FromString(string str, hugeint_t &result);
	//! Convert a string to a hugeint object
	static bool FromCString(const char *str, idx_t len, hugeint_t &result);
	//! Convert a hugeint object to a string
	static string ToString(hugeint_t input);

	static hugeint_t FromString(string str) {
		hugeint_t result;
		FromString(str, result);
		return result;
	}

	template <class T>
	static bool TryCast(hugeint_t input, T &result);

	template <class T>
	static T Cast(hugeint_t input) {
		T value;
		TryCast(input, value);
		return value;
	}

	template <class T>
	static hugeint_t Convert(T value);

	static void NegateInPlace(hugeint_t &input) {
		input.lower = NumericLimits<uint64_t>::Maximum() - input.lower + 1;
		input.upper = -1 - input.upper + (input.lower == 0);
	}
	static hugeint_t Negate(hugeint_t input) {
		NegateInPlace(input);
		return input;
	}

	static bool TryMultiply(hugeint_t lhs, hugeint_t rhs, hugeint_t &result);

	static hugeint_t Add(hugeint_t lhs, hugeint_t rhs);
	static hugeint_t Subtract(hugeint_t lhs, hugeint_t rhs);
	static hugeint_t Multiply(hugeint_t lhs, hugeint_t rhs);
	static hugeint_t Divide(hugeint_t lhs, hugeint_t rhs);
	static hugeint_t Modulo(hugeint_t lhs, hugeint_t rhs);

	// DivMod -> returns the result of the division (lhs / rhs), and fills up the remainder
	static hugeint_t DivMod(hugeint_t lhs, hugeint_t rhs, hugeint_t &remainder);
	// DivMod but lhs MUST be positive, and rhs is a uint64_t
	static hugeint_t DivModPositive(hugeint_t lhs, uint64_t rhs, uint64_t &remainder);

	static bool AddInPlace(hugeint_t &lhs, hugeint_t rhs);
	static bool SubtractInPlace(hugeint_t &lhs, hugeint_t rhs);

	// comparison operators
	// note that everywhere here we intentionally use bitwise ops
	// this is because they seem to be consistently much faster (benchmarked on a Macbook Pro)
	static bool Equals(hugeint_t lhs, hugeint_t rhs) {
		int lower_equals = lhs.lower == rhs.lower;
		int upper_equals = lhs.upper == rhs.upper;
		return lower_equals & upper_equals;
	}
	static bool NotEquals(hugeint_t lhs, hugeint_t rhs) {
		int lower_not_equals = lhs.lower != rhs.lower;
		int upper_not_equals = lhs.upper != rhs.upper;
		return lower_not_equals | upper_not_equals;
	}
	static bool GreaterThan(hugeint_t lhs, hugeint_t rhs) {
		int upper_bigger = lhs.upper > rhs.upper;
		int upper_equal = lhs.upper == rhs.upper;
		int lower_bigger = lhs.lower > rhs.lower;
		return upper_bigger | (upper_equal & lower_bigger);
	}
	static bool GreaterThanEquals(hugeint_t lhs, hugeint_t rhs) {
		int upper_bigger = lhs.upper > rhs.upper;
		int upper_equal = lhs.upper == rhs.upper;
		int lower_bigger_equals = lhs.lower >= rhs.lower;
		return upper_bigger | (upper_equal & lower_bigger_equals);
	}
	static bool LessThan(hugeint_t lhs, hugeint_t rhs) {
		int upper_smaller = lhs.upper < rhs.upper;
		int upper_equal = lhs.upper == rhs.upper;
		int lower_smaller = lhs.lower < rhs.lower;
		return upper_smaller | (upper_equal & lower_smaller);
	}
	static bool LessThanEquals(hugeint_t lhs, hugeint_t rhs) {
		int upper_smaller = lhs.upper < rhs.upper;
		int upper_equal = lhs.upper == rhs.upper;
		int lower_smaller_equals = lhs.lower <= rhs.lower;
		return upper_smaller | (upper_equal & lower_smaller_equals);
	}
	static const hugeint_t POWERS_OF_TEN[40];
};

template <>
bool Hugeint::TryCast(hugeint_t input, int8_t &result);
template <>
bool Hugeint::TryCast(hugeint_t input, int16_t &result);
template <>
bool Hugeint::TryCast(hugeint_t input, int32_t &result);
template <>
bool Hugeint::TryCast(hugeint_t input, int64_t &result);
template <>
bool Hugeint::TryCast(hugeint_t input, uint8_t &result);
template <>
bool Hugeint::TryCast(hugeint_t input, uint16_t &result);
template <>
bool Hugeint::TryCast(hugeint_t input, uint32_t &result);
template <>
bool Hugeint::TryCast(hugeint_t input, uint64_t &result);
template <>
bool Hugeint::TryCast(hugeint_t input, hugeint_t &result);
template <>
bool Hugeint::TryCast(hugeint_t input, float &result);
template <>
bool Hugeint::TryCast(hugeint_t input, double &result);

template <>
hugeint_t Hugeint::Convert(int8_t value);
template <>
hugeint_t Hugeint::Convert(int16_t value);
template <>
hugeint_t Hugeint::Convert(int32_t value);
template <>
hugeint_t Hugeint::Convert(int64_t value);
template <>
hugeint_t Hugeint::Convert(uint8_t value);
template <>
hugeint_t Hugeint::Convert(uint16_t value);
template <>
hugeint_t Hugeint::Convert(uint32_t value);
template <>
hugeint_t Hugeint::Convert(uint64_t value);
template <>
hugeint_t Hugeint::Convert(float value);
template <>
hugeint_t Hugeint::Convert(double value);
} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/interval.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

//! The Interval class is a static class that holds helper functions for the Interval
//! type.
class Interval {
public:
	static constexpr const int32_t MONTHS_PER_MILLENIUM = 12000;
	static constexpr const int32_t MONTHS_PER_CENTURY = 1200;
	static constexpr const int32_t MONTHS_PER_DECADE = 120;
	static constexpr const int32_t MONTHS_PER_YEAR = 12;
	static constexpr const int32_t MONTHS_PER_QUARTER = 3;
	static constexpr const int32_t DAYS_PER_WEEK = 7;
	static constexpr const int64_t DAYS_PER_MONTH =
	    30; // only used for interval comparison/ordering purposes, in which case a month counts as 30 days
	static constexpr const int64_t MSECS_PER_SEC = 1000;
	static constexpr const int32_t SECS_PER_MINUTE = 60;
	static constexpr const int32_t MINS_PER_HOUR = 60;
	static constexpr const int32_t HOURS_PER_DAY = 24;
	static constexpr const int32_t SECS_PER_DAY = SECS_PER_MINUTE * MINS_PER_HOUR * HOURS_PER_DAY;

	static constexpr const int64_t MICROS_PER_MSEC = 1000;
	static constexpr const int64_t MICROS_PER_SEC = MICROS_PER_MSEC * MSECS_PER_SEC;
	static constexpr const int64_t MICROS_PER_MINUTE = MICROS_PER_SEC * SECS_PER_MINUTE;
	static constexpr const int64_t MICROS_PER_HOUR = MICROS_PER_MINUTE * MINS_PER_HOUR;
	static constexpr const int64_t MICROS_PER_DAY = MICROS_PER_HOUR * HOURS_PER_DAY;
	static constexpr const int64_t MICROS_PER_MONTH = MICROS_PER_DAY * DAYS_PER_MONTH;

public:
	//! Convert a string to an interval object
	static bool FromString(const string &str, interval_t &result);
	//! Convert a string to an interval object
	static bool FromCString(const char *str, idx_t len, interval_t &result);
	//! Convert an interval object to a string
	static string ToString(interval_t date);

	//! Get Interval in milliseconds
	static int64_t GetMilli(interval_t val);

	//! Returns the difference between two timestamps
	static interval_t GetDifference(timestamp_t timestamp_1, timestamp_t timestamp_2);

	//! Comparison operators
	static bool Equals(interval_t left, interval_t right);
	static bool GreaterThan(interval_t left, interval_t right);
	static bool GreaterThanEquals(interval_t left, interval_t right);
};
} // namespace duckdb



#include <cstring>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Comparison Operations
//===--------------------------------------------------------------------===//
struct Equals {
	template <class T>
	static inline bool Operation(T left, T right) {
		return left == right;
	}
};
struct NotEquals {
	template <class T>
	static inline bool Operation(T left, T right) {
		return left != right;
	}
};
struct GreaterThan {
	template <class T>
	static inline bool Operation(T left, T right) {
		return left > right;
	}
};
struct GreaterThanEquals {
	template <class T>
	static inline bool Operation(T left, T right) {
		return left >= right;
	}
};

struct LessThan {
	template <class T>
	static inline bool Operation(T left, T right) {
		return left < right;
	}
};
struct LessThanEquals {
	template <class T>
	static inline bool Operation(T left, T right) {
		return left <= right;
	}
};

// Distinct semantics are from Postgres record sorting. NULL = NULL and not-NULL < NULL
// Deferring to the non-distinct operations removes the need for further specialisation.
// TODO: To reverse the semantics, swap left_null and right_null for comparisons
struct DistinctFrom {
	template <class T>
	static inline bool Operation(T left, T right, bool left_null, bool right_null) {
		return (left_null != right_null) || (!left_null && !right_null && (left != right));
	}
};

struct NotDistinctFrom {
	template <class T>
	static inline bool Operation(T left, T right, bool left_null, bool right_null) {
		return (left_null && right_null) || (!left_null && !right_null && (left == right));
	}
};

struct DistinctGreaterThan {
	template <class T>
	static inline bool Operation(T left, T right, bool left_null, bool right_null) {
		return GreaterThan::Operation(left_null, right_null) ||
		       (!left_null && !right_null && GreaterThan::Operation(left, right));
	}
};

struct DistinctGreaterThanEquals {
	template <class T>
	static inline bool Operation(T left, T right, bool left_null, bool right_null) {
		return left_null || (!left_null && !right_null && GreaterThanEquals::Operation(left, right));
	}
};

struct DistinctLessThan {
	template <class T>
	static inline bool Operation(T left, T right, bool left_null, bool right_null) {
		return LessThan::Operation(left_null, right_null) ||
		       (!left_null && !right_null && LessThan::Operation(left, right));
	}
};

struct DistinctLessThanEquals {
	template <class T>
	static inline bool Operation(T left, T right, bool left_null, bool right_null) {
		return right_null || (!left_null && !right_null && LessThanEquals::Operation(left, right));
	}
};

//===--------------------------------------------------------------------===//
// Specialized Boolean Comparison Operators
//===--------------------------------------------------------------------===//
template <>
inline bool GreaterThan::Operation(bool left, bool right) {
	return !right && left;
}
template <>
inline bool LessThan::Operation(bool left, bool right) {
	return !left && right;
}
//===--------------------------------------------------------------------===//
// Specialized String Comparison Operations
//===--------------------------------------------------------------------===//
struct StringComparisonOperators {
	template <bool INVERSE>
	static inline bool EqualsOrNot(const string_t a, const string_t b) {
		if (a.IsInlined()) {
			// small string: compare entire string
			if (memcmp(&a, &b, sizeof(string_t)) == 0) {
				// entire string is equal
				return INVERSE ? false : true;
			}
		} else {
			// large string: first check prefix and length
			if (memcmp(&a, &b, sizeof(uint32_t) + string_t::PREFIX_LENGTH) == 0) {
				// prefix and length are equal: check main string
				if (memcmp(a.value.pointer.ptr, b.value.pointer.ptr, a.GetSize()) == 0) {
					// entire string is equal
					return INVERSE ? false : true;
				}
			}
		}
		// not equal
		return INVERSE ? true : false;
	}
};

template <>
inline bool Equals::Operation(string_t left, string_t right) {
	return StringComparisonOperators::EqualsOrNot<false>(left, right);
}
template <>
inline bool NotEquals::Operation(string_t left, string_t right) {
	return StringComparisonOperators::EqualsOrNot<true>(left, right);
}

template <>
inline bool NotDistinctFrom::Operation(string_t left, string_t right, bool left_null, bool right_null) {
	return (left_null && right_null) ||
	       (!left_null && !right_null && StringComparisonOperators::EqualsOrNot<false>(left, right));
}
template <>
inline bool DistinctFrom::Operation(string_t left, string_t right, bool left_null, bool right_null) {
	return (left_null != right_null) ||
	       (!left_null && !right_null && StringComparisonOperators::EqualsOrNot<true>(left, right));
}

// compare up to shared length. if still the same, compare lengths
template <class OP>
static bool templated_string_compare_op(string_t left, string_t right) {
	auto memcmp_res =
	    memcmp(left.GetDataUnsafe(), right.GetDataUnsafe(), MinValue<idx_t>(left.GetSize(), right.GetSize()));
	auto final_res = memcmp_res == 0 ? OP::Operation(left.GetSize(), right.GetSize()) : OP::Operation(memcmp_res, 0);
	return final_res;
}

template <>
inline bool GreaterThan::Operation(string_t left, string_t right) {
	return templated_string_compare_op<GreaterThan>(left, right);
}

template <>
inline bool GreaterThanEquals::Operation(string_t left, string_t right) {
	return templated_string_compare_op<GreaterThanEquals>(left, right);
}

template <>
inline bool LessThan::Operation(string_t left, string_t right) {
	return templated_string_compare_op<LessThan>(left, right);
}

template <>
inline bool LessThanEquals::Operation(string_t left, string_t right) {
	return templated_string_compare_op<LessThanEquals>(left, right);
}
//===--------------------------------------------------------------------===//
// Specialized Interval Comparison Operators
//===--------------------------------------------------------------------===//
template <>
inline bool Equals::Operation(interval_t left, interval_t right) {
	return Interval::Equals(left, right);
}
template <>
inline bool NotEquals::Operation(interval_t left, interval_t right) {
	return !Equals::Operation(left, right);
}
template <>
inline bool GreaterThan::Operation(interval_t left, interval_t right) {
	return Interval::GreaterThan(left, right);
}
template <>
inline bool GreaterThanEquals::Operation(interval_t left, interval_t right) {
	return Interval::GreaterThanEquals(left, right);
}
template <>
inline bool LessThan::Operation(interval_t left, interval_t right) {
	return GreaterThan::Operation(right, left);
}
template <>
inline bool LessThanEquals::Operation(interval_t left, interval_t right) {
	return GreaterThanEquals::Operation(right, left);
}

template <>
inline bool NotDistinctFrom::Operation(interval_t left, interval_t right, bool left_null, bool right_null) {
	return (left_null && right_null) || (!left_null && !right_null && Interval::Equals(left, right));
}
template <>
inline bool DistinctFrom::Operation(interval_t left, interval_t right, bool left_null, bool right_null) {
	return (left_null != right_null) || (!left_null && !right_null && !Equals::Operation(left, right));
}
inline bool operator<(const interval_t &lhs, const interval_t &rhs) {
	return LessThan::Operation(lhs, rhs);
}

//===--------------------------------------------------------------------===//
// Specialized Hugeint Comparison Operators
//===--------------------------------------------------------------------===//
template <>
inline bool Equals::Operation(hugeint_t left, hugeint_t right) {
	return Hugeint::Equals(left, right);
}
template <>
inline bool NotEquals::Operation(hugeint_t left, hugeint_t right) {
	return Hugeint::NotEquals(left, right);
}
template <>
inline bool GreaterThan::Operation(hugeint_t left, hugeint_t right) {
	return Hugeint::GreaterThan(left, right);
}
template <>
inline bool GreaterThanEquals::Operation(hugeint_t left, hugeint_t right) {
	return Hugeint::GreaterThanEquals(left, right);
}
template <>
inline bool LessThan::Operation(hugeint_t left, hugeint_t right) {
	return Hugeint::LessThan(left, right);
}
template <>
inline bool LessThanEquals::Operation(hugeint_t left, hugeint_t right) {
	return Hugeint::LessThanEquals(left, right);
}
} // namespace duckdb




namespace duckdb {
class Serializer;
class Deserializer;
class Vector;
class ValidityStatistics;

class BaseStatistics {
public:
	explicit BaseStatistics(LogicalType type);
	virtual ~BaseStatistics();

	//! The type of the logical segment
	LogicalType type;
	//! The validity stats of the column (if any)
	unique_ptr<BaseStatistics> validity_stats;

public:
	bool CanHaveNull();

	static unique_ptr<BaseStatistics> CreateEmpty(LogicalType type);

	virtual void Merge(const BaseStatistics &other);
	virtual unique_ptr<BaseStatistics> Copy();
	virtual void Serialize(Serializer &serializer);
	static unique_ptr<BaseStatistics> Deserialize(Deserializer &source, LogicalType type);
	//! Verify that a vector does not violate the statistics
	virtual void Verify(Vector &vector, idx_t count);

	virtual string ToString();
};

} // namespace duckdb


namespace duckdb {
class BoundFunctionExpression;
class ScalarFunctionCatalogEntry;

//! The type used for scalar functions
typedef std::function<void(DataChunk &, ExpressionState &, Vector &)> scalar_function_t;
//! Binds the scalar function and creates the function data
typedef unique_ptr<FunctionData> (*bind_scalar_function_t)(ClientContext &context, ScalarFunction &bound_function,
                                                           vector<unique_ptr<Expression>> &arguments);
typedef unique_ptr<BaseStatistics> (*function_statistics_t)(ClientContext &context, BoundFunctionExpression &expr,
                                                            FunctionData *bind_data,
                                                            vector<unique_ptr<BaseStatistics>> &child_stats);
//! Adds the dependencies of this BoundFunctionExpression to the set of dependencies
typedef void (*dependency_function_t)(BoundFunctionExpression &expr, unordered_set<CatalogEntry *> &dependencies);

class ScalarFunction : public BaseScalarFunction {
public:
	ScalarFunction(string name, vector<LogicalType> arguments, LogicalType return_type, scalar_function_t function,
	               bool has_side_effects = false, bind_scalar_function_t bind = nullptr,
	               dependency_function_t dependency = nullptr, function_statistics_t statistics = nullptr,
	               LogicalType varargs = LogicalType(LogicalTypeId::INVALID))
	    : BaseScalarFunction(name, arguments, return_type, has_side_effects, varargs), function(function), bind(bind),
	      dependency(dependency), statistics(statistics) {
	}

	ScalarFunction(vector<LogicalType> arguments, LogicalType return_type, scalar_function_t function,
	               bool has_side_effects = false, bind_scalar_function_t bind = nullptr,
	               dependency_function_t dependency = nullptr, function_statistics_t statistics = nullptr,
	               LogicalType varargs = LogicalType(LogicalTypeId::INVALID))
	    : ScalarFunction(string(), arguments, return_type, function, has_side_effects, bind, dependency, statistics,
	                     varargs) {
	}

	//! The main scalar function to execute
	scalar_function_t function;
	//! The bind function (if any)
	bind_scalar_function_t bind;
	// The dependency function (if any)
	dependency_function_t dependency;
	//! The statistics propagation function (if any)
	function_statistics_t statistics;

	static unique_ptr<BoundFunctionExpression> BindScalarFunction(ClientContext &context, const string &schema,
	                                                              const string &name,
	                                                              vector<unique_ptr<Expression>> children,
	                                                              string &error, bool is_operator = false);
	static unique_ptr<BoundFunctionExpression> BindScalarFunction(ClientContext &context,
	                                                              ScalarFunctionCatalogEntry &function,
	                                                              vector<unique_ptr<Expression>> children,
	                                                              string &error, bool is_operator = false);

	static unique_ptr<BoundFunctionExpression> BindScalarFunction(ClientContext &context, ScalarFunction bound_function,
	                                                              vector<unique_ptr<Expression>> children,
	                                                              bool is_operator = false);

	bool operator==(const ScalarFunction &rhs) const {
		return CompareScalarFunctionT(rhs.function) && bind == rhs.bind && dependency == rhs.dependency &&
		       statistics == rhs.statistics;
	}
	bool operator!=(const ScalarFunction &rhs) const {
		return !(*this == rhs);
	}

	bool Equal(const ScalarFunction &rhs) const {
		// number of types
		if (this->arguments.size() != rhs.arguments.size()) {
			return false;
		}
		// argument types
		for (idx_t i = 0; i < this->arguments.size(); ++i) {
			if (this->arguments[i] != rhs.arguments[i]) {
				return false;
			}
		}
		// return type
		if (this->return_type != rhs.return_type) {
			return false;
		}
		// varargs
		if (this->varargs != rhs.varargs) {
			return false;
		}

		return true; // they are equal
	}

private:
	bool CompareScalarFunctionT(const scalar_function_t other) const {
		typedef void(funcTypeT)(DataChunk &, ExpressionState &, Vector &);

		funcTypeT **func_ptr = (funcTypeT **)function.template target<funcTypeT *>();
		funcTypeT **other_ptr = (funcTypeT **)other.template target<funcTypeT *>();

		// Case the functions were created from lambdas the target will return a nullptr
		if (func_ptr == nullptr || other_ptr == nullptr) {
			// scalar_function_t (std::functions) from lambdas cannot be compared
			return false;
		}
		return ((size_t)*func_ptr == (size_t)*other_ptr);
	}

public:
	static void NopFunction(DataChunk &input, ExpressionState &state, Vector &result) {
		D_ASSERT(input.ColumnCount() >= 1);
		result.Reference(input.data[0]);
	}

	template <class TA, class TR, class OP>
	static void UnaryFunction(DataChunk &input, ExpressionState &state, Vector &result) {
		D_ASSERT(input.ColumnCount() >= 1);
		UnaryExecutor::Execute<TA, TR, OP>(input.data[0], result, input.size());
	}

	template <class TA, class TB, class TR, class OP>
	static void BinaryFunction(DataChunk &input, ExpressionState &state, Vector &result) {
		D_ASSERT(input.ColumnCount() == 2);
		BinaryExecutor::ExecuteStandard<TA, TB, TR, OP>(input.data[0], input.data[1], result, input.size());
	}

public:
	template <class OP>
	static scalar_function_t GetScalarUnaryFunction(LogicalType type) {
		scalar_function_t function;
		switch (type.id()) {
		case LogicalTypeId::TINYINT:
			function = &ScalarFunction::UnaryFunction<int8_t, int8_t, OP>;
			break;
		case LogicalTypeId::SMALLINT:
			function = &ScalarFunction::UnaryFunction<int16_t, int16_t, OP>;
			break;
		case LogicalTypeId::INTEGER:
			function = &ScalarFunction::UnaryFunction<int32_t, int32_t, OP>;
			break;
		case LogicalTypeId::BIGINT:
			function = &ScalarFunction::UnaryFunction<int64_t, int64_t, OP>;
			break;
		case LogicalTypeId::UTINYINT:
			function = &ScalarFunction::UnaryFunction<uint8_t, uint8_t, OP>;
			break;
		case LogicalTypeId::USMALLINT:
			function = &ScalarFunction::UnaryFunction<uint16_t, uint16_t, OP>;
			break;
		case LogicalTypeId::UINTEGER:
			function = &ScalarFunction::UnaryFunction<uint32_t, uint32_t, OP>;
			break;
		case LogicalTypeId::UBIGINT:
			function = &ScalarFunction::UnaryFunction<uint64_t, uint64_t, OP>;
			break;
		case LogicalTypeId::HUGEINT:
			function = &ScalarFunction::UnaryFunction<hugeint_t, hugeint_t, OP>;
			break;
		case LogicalTypeId::FLOAT:
			function = &ScalarFunction::UnaryFunction<float, float, OP>;
			break;
		case LogicalTypeId::DOUBLE:
			function = &ScalarFunction::UnaryFunction<double, double, OP>;
			break;
		default:
			throw NotImplementedException("Unimplemented type for GetScalarUnaryFunction");
		}
		return function;
	}

	template <class TR, class OP>
	static scalar_function_t GetScalarUnaryFunctionFixedReturn(LogicalType type) {
		scalar_function_t function;
		switch (type.id()) {
		case LogicalTypeId::TINYINT:
			function = &ScalarFunction::UnaryFunction<int8_t, TR, OP>;
			break;
		case LogicalTypeId::SMALLINT:
			function = &ScalarFunction::UnaryFunction<int16_t, TR, OP>;
			break;
		case LogicalTypeId::INTEGER:
			function = &ScalarFunction::UnaryFunction<int32_t, TR, OP>;
			break;
		case LogicalTypeId::BIGINT:
			function = &ScalarFunction::UnaryFunction<int64_t, TR, OP>;
			break;
		case LogicalTypeId::UTINYINT:
			function = &ScalarFunction::UnaryFunction<uint8_t, TR, OP>;
			break;
		case LogicalTypeId::USMALLINT:
			function = &ScalarFunction::UnaryFunction<uint16_t, TR, OP>;
			break;
		case LogicalTypeId::UINTEGER:
			function = &ScalarFunction::UnaryFunction<uint32_t, TR, OP>;
			break;
		case LogicalTypeId::UBIGINT:
			function = &ScalarFunction::UnaryFunction<uint64_t, TR, OP>;
			break;
		case LogicalTypeId::HUGEINT:
			function = &ScalarFunction::UnaryFunction<hugeint_t, TR, OP>;
			break;
		case LogicalTypeId::FLOAT:
			function = &ScalarFunction::UnaryFunction<float, TR, OP>;
			break;
		case LogicalTypeId::DOUBLE:
			function = &ScalarFunction::UnaryFunction<double, TR, OP>;
			break;
		default:
			throw NotImplementedException("Unimplemented type for GetScalarUnaryFunctionFixedReturn");
		}
		return function;
	}
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/aggregate_function.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/aggregate_executor.hpp
//
//
//===----------------------------------------------------------------------===//








namespace duckdb {
struct FunctionData;
typedef std::pair<idx_t, idx_t> FrameBounds;

class AggregateExecutor {
private:
	template <class STATE_TYPE, class OP>
	static inline void NullaryFlatLoop(STATE_TYPE **__restrict states, FunctionData *bind_data, idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			OP::template Operation<STATE_TYPE, OP>(states[i], bind_data, i);
		}
	}

	template <class STATE_TYPE, class OP>
	static inline void NullaryScatterLoop(STATE_TYPE **__restrict states, FunctionData *bind_data,
	                                      const SelectionVector &ssel, idx_t count) {

		for (idx_t i = 0; i < count; i++) {
			auto sidx = ssel.get_index(i);
			OP::template Operation<STATE_TYPE, OP>(states[sidx], bind_data, sidx);
		}
	}

	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static inline void UnaryFlatLoop(INPUT_TYPE *__restrict idata, FunctionData *bind_data,
	                                 STATE_TYPE **__restrict states, ValidityMask &mask, idx_t count) {
		if (!mask.AllValid()) {
			idx_t base_idx = 0;
			auto entry_count = ValidityMask::EntryCount(count);
			for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
				auto validity_entry = mask.GetValidityEntry(entry_idx);
				idx_t next = MinValue<idx_t>(base_idx + ValidityMask::BITS_PER_VALUE, count);
				if (!OP::IgnoreNull() || ValidityMask::AllValid(validity_entry)) {
					// all valid: perform operation
					for (; base_idx < next; base_idx++) {
						OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(states[base_idx], bind_data, idata, mask,
						                                                   base_idx);
					}
				} else if (ValidityMask::NoneValid(validity_entry)) {
					// nothing valid: skip all
					base_idx = next;
					continue;
				} else {
					// partially valid: need to check individual elements for validity
					idx_t start = base_idx;
					for (; base_idx < next; base_idx++) {
						if (ValidityMask::RowIsValid(validity_entry, base_idx - start)) {
							OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(states[base_idx], bind_data, idata, mask,
							                                                   base_idx);
						}
					}
				}
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(states[i], bind_data, idata, mask, i);
			}
		}
	}

	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static inline void UnaryScatterLoop(INPUT_TYPE *__restrict idata, FunctionData *bind_data,
	                                    STATE_TYPE **__restrict states, const SelectionVector &isel,
	                                    const SelectionVector &ssel, ValidityMask &mask, idx_t count) {
		if (OP::IgnoreNull() && !mask.AllValid()) {
			// potential NULL values and NULL values are ignored
			for (idx_t i = 0; i < count; i++) {
				auto idx = isel.get_index(i);
				auto sidx = ssel.get_index(i);
				if (mask.RowIsValid(idx)) {
					OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(states[sidx], bind_data, idata, mask, idx);
				}
			}
		} else {
			// quick path: no NULL values or NULL values are not ignored
			for (idx_t i = 0; i < count; i++) {
				auto idx = isel.get_index(i);
				auto sidx = ssel.get_index(i);
				OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(states[sidx], bind_data, idata, mask, idx);
			}
		}
	}

	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static inline void UnaryFlatUpdateLoop(INPUT_TYPE *__restrict idata, FunctionData *bind_data,
	                                       STATE_TYPE *__restrict state, idx_t count, ValidityMask &mask) {
		idx_t base_idx = 0;
		auto entry_count = ValidityMask::EntryCount(count);
		for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
			auto validity_entry = mask.GetValidityEntry(entry_idx);
			idx_t next = MinValue<idx_t>(base_idx + ValidityMask::BITS_PER_VALUE, count);
			if (!OP::IgnoreNull() || ValidityMask::AllValid(validity_entry)) {
				// all valid: perform operation
				for (; base_idx < next; base_idx++) {
					OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(state, bind_data, idata, mask, base_idx);
				}
			} else if (ValidityMask::NoneValid(validity_entry)) {
				// nothing valid: skip all
				base_idx = next;
				continue;
			} else {
				// partially valid: need to check individual elements for validity
				idx_t start = base_idx;
				for (; base_idx < next; base_idx++) {
					if (ValidityMask::RowIsValid(validity_entry, base_idx - start)) {
						OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(state, bind_data, idata, mask, base_idx);
					}
				}
			}
		}
	}

	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static inline void UnaryUpdateLoop(INPUT_TYPE *__restrict idata, FunctionData *bind_data,
	                                   STATE_TYPE *__restrict state, idx_t count, ValidityMask &mask,
	                                   const SelectionVector &__restrict sel_vector) {
		if (OP::IgnoreNull() && !mask.AllValid()) {
			// potential NULL values and NULL values are ignored
			for (idx_t i = 0; i < count; i++) {
				auto idx = sel_vector.get_index(i);
				if (mask.RowIsValid(idx)) {
					OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(state, bind_data, idata, mask, idx);
				}
			}
		} else {
			// quick path: no NULL values or NULL values are not ignored
			for (idx_t i = 0; i < count; i++) {
				auto idx = sel_vector.get_index(i);
				OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(state, bind_data, idata, mask, idx);
			}
		}
	}

	template <class STATE_TYPE, class A_TYPE, class B_TYPE, class OP>
	static inline void BinaryScatterLoop(A_TYPE *__restrict adata, FunctionData *bind_data, B_TYPE *__restrict bdata,
	                                     STATE_TYPE **__restrict states, idx_t count, const SelectionVector &asel,
	                                     const SelectionVector &bsel, const SelectionVector &ssel,
	                                     ValidityMask &avalidity, ValidityMask &bvalidity) {
		if (OP::IgnoreNull() && (!avalidity.AllValid() || !bvalidity.AllValid())) {
			// potential NULL values and NULL values are ignored
			for (idx_t i = 0; i < count; i++) {
				auto aidx = asel.get_index(i);
				auto bidx = bsel.get_index(i);
				auto sidx = ssel.get_index(i);
				if (avalidity.RowIsValid(aidx) && bvalidity.RowIsValid(bidx)) {
					OP::template Operation<A_TYPE, B_TYPE, STATE_TYPE, OP>(states[sidx], bind_data, adata, bdata,
					                                                       avalidity, bvalidity, aidx, bidx);
				}
			}
		} else {
			// quick path: no NULL values or NULL values are not ignored
			for (idx_t i = 0; i < count; i++) {
				auto aidx = asel.get_index(i);
				auto bidx = bsel.get_index(i);
				auto sidx = ssel.get_index(i);
				OP::template Operation<A_TYPE, B_TYPE, STATE_TYPE, OP>(states[sidx], bind_data, adata, bdata, avalidity,
				                                                       bvalidity, aidx, bidx);
			}
		}
	}

	template <class STATE_TYPE, class A_TYPE, class B_TYPE, class OP>
	static inline void BinaryUpdateLoop(A_TYPE *__restrict adata, FunctionData *bind_data, B_TYPE *__restrict bdata,
	                                    STATE_TYPE *__restrict state, idx_t count, const SelectionVector &asel,
	                                    const SelectionVector &bsel, ValidityMask &avalidity, ValidityMask &bvalidity) {
		if (OP::IgnoreNull() && (!avalidity.AllValid() || !bvalidity.AllValid())) {
			// potential NULL values and NULL values are ignored
			for (idx_t i = 0; i < count; i++) {
				auto aidx = asel.get_index(i);
				auto bidx = bsel.get_index(i);
				if (avalidity.RowIsValid(aidx) && bvalidity.RowIsValid(bidx)) {
					OP::template Operation<A_TYPE, B_TYPE, STATE_TYPE, OP>(state, bind_data, adata, bdata, avalidity,
					                                                       bvalidity, aidx, bidx);
				}
			}
		} else {
			// quick path: no NULL values or NULL values are not ignored
			for (idx_t i = 0; i < count; i++) {
				auto aidx = asel.get_index(i);
				auto bidx = bsel.get_index(i);
				OP::template Operation<A_TYPE, B_TYPE, STATE_TYPE, OP>(state, bind_data, adata, bdata, avalidity,
				                                                       bvalidity, aidx, bidx);
			}
		}
	}

public:
	template <class STATE_TYPE, class OP>
	static void NullaryScatter(Vector &states, FunctionData *bind_data, idx_t count) {
		if (states.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			auto sdata = ConstantVector::GetData<STATE_TYPE *>(states);
			OP::template ConstantOperation<STATE_TYPE, OP>(*sdata, bind_data, count);
		} else if (states.GetVectorType() == VectorType::FLAT_VECTOR) {
			auto sdata = FlatVector::GetData<STATE_TYPE *>(states);
			NullaryFlatLoop<STATE_TYPE, OP>(sdata, bind_data, count);
		} else {
			VectorData sdata;
			states.Orrify(count, sdata);
			NullaryScatterLoop<STATE_TYPE, OP>((STATE_TYPE **)sdata.data, bind_data, *sdata.sel, count);
		}
	}

	template <class STATE_TYPE, class OP>
	static void NullaryUpdate(data_ptr_t state, FunctionData *bind_data, idx_t count) {
		OP::template ConstantOperation<STATE_TYPE, OP>((STATE_TYPE *)state, bind_data, count);
	}

	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static void UnaryScatter(Vector &input, Vector &states, FunctionData *bind_data, idx_t count) {
		if (input.GetVectorType() == VectorType::CONSTANT_VECTOR &&
		    states.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			if (OP::IgnoreNull() && ConstantVector::IsNull(input)) {
				// constant NULL input in function that ignores NULL values
				return;
			}
			// regular constant: get first state
			auto idata = ConstantVector::GetData<INPUT_TYPE>(input);
			auto sdata = ConstantVector::GetData<STATE_TYPE *>(states);
			OP::template ConstantOperation<INPUT_TYPE, STATE_TYPE, OP>(*sdata, bind_data, idata,
			                                                           ConstantVector::Validity(input), count);
		} else if (input.GetVectorType() == VectorType::FLAT_VECTOR &&
		           states.GetVectorType() == VectorType::FLAT_VECTOR) {
			auto idata = FlatVector::GetData<INPUT_TYPE>(input);
			auto sdata = FlatVector::GetData<STATE_TYPE *>(states);
			UnaryFlatLoop<STATE_TYPE, INPUT_TYPE, OP>(idata, bind_data, sdata, FlatVector::Validity(input), count);
		} else {
			VectorData idata, sdata;
			input.Orrify(count, idata);
			states.Orrify(count, sdata);
			UnaryScatterLoop<STATE_TYPE, INPUT_TYPE, OP>((INPUT_TYPE *)idata.data, bind_data, (STATE_TYPE **)sdata.data,
			                                             *idata.sel, *sdata.sel, idata.validity, count);
		}
	}

	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static void UnaryUpdate(Vector &input, FunctionData *bind_data, data_ptr_t state, idx_t count) {
		switch (input.GetVectorType()) {
		case VectorType::CONSTANT_VECTOR: {
			if (OP::IgnoreNull() && ConstantVector::IsNull(input)) {
				return;
			}
			auto idata = ConstantVector::GetData<INPUT_TYPE>(input);
			OP::template ConstantOperation<INPUT_TYPE, STATE_TYPE, OP>((STATE_TYPE *)state, bind_data, idata,
			                                                           ConstantVector::Validity(input), count);
			break;
		}
		case VectorType::FLAT_VECTOR: {
			auto idata = FlatVector::GetData<INPUT_TYPE>(input);
			UnaryFlatUpdateLoop<STATE_TYPE, INPUT_TYPE, OP>(idata, bind_data, (STATE_TYPE *)state, count,
			                                                FlatVector::Validity(input));
			break;
		}
		default: {
			VectorData idata;
			input.Orrify(count, idata);
			UnaryUpdateLoop<STATE_TYPE, INPUT_TYPE, OP>((INPUT_TYPE *)idata.data, bind_data, (STATE_TYPE *)state, count,
			                                            idata.validity, *idata.sel);
			break;
		}
		}
	}

	template <class STATE_TYPE, class A_TYPE, class B_TYPE, class OP>
	static void BinaryScatter(FunctionData *bind_data, Vector &a, Vector &b, Vector &states, idx_t count) {
		VectorData adata, bdata, sdata;

		a.Orrify(count, adata);
		b.Orrify(count, bdata);
		states.Orrify(count, sdata);

		BinaryScatterLoop<STATE_TYPE, A_TYPE, B_TYPE, OP>((A_TYPE *)adata.data, bind_data, (B_TYPE *)bdata.data,
		                                                  (STATE_TYPE **)sdata.data, count, *adata.sel, *bdata.sel,
		                                                  *sdata.sel, adata.validity, bdata.validity);
	}

	template <class STATE_TYPE, class A_TYPE, class B_TYPE, class OP>
	static void BinaryUpdate(FunctionData *bind_data, Vector &a, Vector &b, data_ptr_t state, idx_t count) {
		VectorData adata, bdata;

		a.Orrify(count, adata);
		b.Orrify(count, bdata);

		BinaryUpdateLoop<STATE_TYPE, A_TYPE, B_TYPE, OP>((A_TYPE *)adata.data, bind_data, (B_TYPE *)bdata.data,
		                                                 (STATE_TYPE *)state, count, *adata.sel, *bdata.sel,
		                                                 adata.validity, bdata.validity);
	}

	template <class STATE_TYPE, class OP>
	static void Combine(Vector &source, Vector &target, idx_t count) {
		D_ASSERT(source.GetType().id() == LogicalTypeId::POINTER && target.GetType().id() == LogicalTypeId::POINTER);
		auto sdata = FlatVector::GetData<const STATE_TYPE *>(source);
		auto tdata = FlatVector::GetData<STATE_TYPE *>(target);

		for (idx_t i = 0; i < count; i++) {
			OP::template Combine<STATE_TYPE, OP>(*sdata[i], tdata[i]);
		}
	}

	template <class STATE_TYPE, class RESULT_TYPE, class OP>
	static void Finalize(Vector &states, FunctionData *bind_data, Vector &result, idx_t count) {
		if (states.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);

			auto sdata = ConstantVector::GetData<STATE_TYPE *>(states);
			auto rdata = ConstantVector::GetData<RESULT_TYPE>(result);
			OP::template Finalize<RESULT_TYPE, STATE_TYPE>(result, bind_data, *sdata, rdata,
			                                               ConstantVector::Validity(result), 0);
		} else {
			D_ASSERT(states.GetVectorType() == VectorType::FLAT_VECTOR);
			result.SetVectorType(VectorType::FLAT_VECTOR);

			auto sdata = FlatVector::GetData<STATE_TYPE *>(states);
			auto rdata = FlatVector::GetData<RESULT_TYPE>(result);
			for (idx_t i = 0; i < count; i++) {
				OP::template Finalize<RESULT_TYPE, STATE_TYPE>(result, bind_data, sdata[i], rdata,
				                                               FlatVector::Validity(result), i);
			}
		}
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP>
	static void UnaryWindow(Vector &input, FunctionData *bind_data, data_ptr_t state, const FrameBounds &frame,
	                        const FrameBounds &prev, Vector &result) {

		auto idata = FlatVector::GetData<const INPUT_TYPE>(input) - MinValue(frame.first, prev.first);
		const auto &ivalid = FlatVector::Validity(input);
		auto rdata = ConstantVector::GetData<RESULT_TYPE>(result);
		auto &rvalid = ConstantVector::Validity(result);
		OP::template Window<STATE, INPUT_TYPE, RESULT_TYPE>(idata, ivalid, bind_data, (STATE *)state, frame, prev,
		                                                    rdata, rvalid);
	}

	template <class STATE_TYPE, class OP>
	static void Destroy(Vector &states, idx_t count) {
		auto sdata = FlatVector::GetData<STATE_TYPE *>(states);
		for (idx_t i = 0; i < count; i++) {
			OP::template Destroy<STATE_TYPE>(sdata[i]);
		}
	}
};

} // namespace duckdb



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/node_statistics.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

class NodeStatistics {
public:
	NodeStatistics() : has_estimated_cardinality(false), has_max_cardinality(false) {
	}
	explicit NodeStatistics(idx_t estimated_cardinality)
	    : has_estimated_cardinality(true), estimated_cardinality(estimated_cardinality), has_max_cardinality(false) {
	}
	NodeStatistics(idx_t estimated_cardinality, idx_t max_cardinality)
	    : has_estimated_cardinality(true), estimated_cardinality(estimated_cardinality), has_max_cardinality(true),
	      max_cardinality(max_cardinality) {
	}

	//! Whether or not the node has an estimated cardinality specified
	bool has_estimated_cardinality;
	//! The estimated cardinality at the specified node
	idx_t estimated_cardinality;
	//! Whether or not the node has a maximum cardinality specified
	bool has_max_cardinality;
	//! The max possible cardinality at the specified node
	idx_t max_cardinality;
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression.hpp
//
//
//===----------------------------------------------------------------------===//






namespace duckdb {
class BaseStatistics;

//!  The Expression class represents a bound Expression with a return type
class Expression : public BaseExpression {
public:
	Expression(ExpressionType type, ExpressionClass expression_class, LogicalType return_type);
	~Expression() override;

	//! The return type of the expression
	LogicalType return_type;
	//! Expression statistics (if any)
	unique_ptr<BaseStatistics> stats;

public:
	bool IsAggregate() const override;
	bool IsWindow() const override;
	bool HasSubquery() const override;
	bool IsScalar() const override;
	bool HasParameter() const override;
	virtual bool HasSideEffects() const;
	virtual bool IsFoldable() const;

	hash_t Hash() const override;

	bool Equals(const BaseExpression *other) const override {
		if (!BaseExpression::Equals(other)) {
			return false;
		}
		return return_type == ((Expression *)other)->return_type;
	}

	static bool Equals(Expression *left, Expression *right) {
		return BaseExpression::Equals((BaseExpression *)left, (BaseExpression *)right);
	}
	//! Create a copy of this expression
	virtual unique_ptr<Expression> Copy() = 0;

protected:
	//! Copy base Expression properties from another expression to this one,
	//! used in Copy method
	void CopyProperties(Expression &other) {
		type = other.type;
		expression_class = other.expression_class;
		alias = other.alias;
		return_type = other.return_type;
	}
};

} // namespace duckdb


namespace duckdb {

class BoundAggregateExpression;

//! The type used for sizing hashed aggregate function states
typedef idx_t (*aggregate_size_t)();
//! The type used for initializing hashed aggregate function states
typedef void (*aggregate_initialize_t)(data_ptr_t state);
//! The type used for updating hashed aggregate functions
typedef void (*aggregate_update_t)(Vector inputs[], FunctionData *bind_data, idx_t input_count, Vector &state,
                                   idx_t count);
//! The type used for combining hashed aggregate states (optional)
typedef void (*aggregate_combine_t)(Vector &state, Vector &combined, idx_t count);
//! The type used for finalizing hashed aggregate function payloads
typedef void (*aggregate_finalize_t)(Vector &state, FunctionData *bind_data, Vector &result, idx_t count);
//! The type used for propagating statistics in aggregate functions (optional)
typedef unique_ptr<BaseStatistics> (*aggregate_statistics_t)(ClientContext &context, BoundAggregateExpression &expr,
                                                             FunctionData *bind_data,
                                                             vector<unique_ptr<BaseStatistics>> &child_stats,
                                                             NodeStatistics *node_stats);
//! Binds the scalar function and creates the function data
typedef unique_ptr<FunctionData> (*bind_aggregate_function_t)(ClientContext &context, AggregateFunction &function,
                                                              vector<unique_ptr<Expression>> &arguments);
//! The type used for the aggregate destructor method. NOTE: this method is used in destructors and MAY NOT throw.
typedef void (*aggregate_destructor_t)(Vector &state, idx_t count);

//! The type used for updating simple (non-grouped) aggregate functions
typedef void (*aggregate_simple_update_t)(Vector inputs[], FunctionData *bind_data, idx_t input_count, data_ptr_t state,
                                          idx_t count);

//! The type used for updating complex windowed aggregate functions (optional)
typedef std::pair<idx_t, idx_t> FrameBounds;
typedef void (*aggregate_window_t)(Vector inputs[], FunctionData *bind_data, idx_t input_count, data_ptr_t state,
                                   const FrameBounds &frame, const FrameBounds &prev, Vector &result);

class AggregateFunction : public BaseScalarFunction {
public:
	AggregateFunction(string name, vector<LogicalType> arguments, LogicalType return_type, aggregate_size_t state_size,
	                  aggregate_initialize_t initialize, aggregate_update_t update, aggregate_combine_t combine,
	                  aggregate_finalize_t finalize, aggregate_simple_update_t simple_update = nullptr,
	                  bind_aggregate_function_t bind = nullptr, aggregate_destructor_t destructor = nullptr,
	                  aggregate_statistics_t statistics = nullptr, aggregate_window_t window = nullptr)
	    : BaseScalarFunction(name, arguments, return_type, false), state_size(state_size), initialize(initialize),
	      update(update), combine(combine), finalize(finalize), simple_update(simple_update), window(window),
	      bind(bind), destructor(destructor), statistics(statistics) {
	}

	AggregateFunction(vector<LogicalType> arguments, LogicalType return_type, aggregate_size_t state_size,
	                  aggregate_initialize_t initialize, aggregate_update_t update, aggregate_combine_t combine,
	                  aggregate_finalize_t finalize, aggregate_simple_update_t simple_update = nullptr,
	                  bind_aggregate_function_t bind = nullptr, aggregate_destructor_t destructor = nullptr,
	                  aggregate_statistics_t statistics = nullptr, aggregate_window_t window = nullptr)
	    : AggregateFunction(string(), arguments, return_type, state_size, initialize, update, combine, finalize,
	                        simple_update, bind, destructor, statistics, window) {
	}

	//! The hashed aggregate state sizing function
	aggregate_size_t state_size;
	//! The hashed aggregate state initialization function
	aggregate_initialize_t initialize;
	//! The hashed aggregate update state function
	aggregate_update_t update;
	//! The hashed aggregate combine states function
	aggregate_combine_t combine;
	//! The hashed aggregate finalization function
	aggregate_finalize_t finalize;
	//! The simple aggregate update function (may be null)
	aggregate_simple_update_t simple_update;
	//! The windowed aggregate frame update function (may be null)
	aggregate_window_t window;

	//! The bind function (may be null)
	bind_aggregate_function_t bind;
	//! The destructor method (may be null)
	aggregate_destructor_t destructor;

	//! The statistics propagation function (may be null)
	aggregate_statistics_t statistics;

	bool operator==(const AggregateFunction &rhs) const {
		return state_size == rhs.state_size && initialize == rhs.initialize && update == rhs.update &&
		       combine == rhs.combine && finalize == rhs.finalize && window == rhs.window;
	}
	bool operator!=(const AggregateFunction &rhs) const {
		return !(*this == rhs);
	}

	static unique_ptr<BoundAggregateExpression> BindAggregateFunction(ClientContext &context,
	                                                                  AggregateFunction bound_function,
	                                                                  vector<unique_ptr<Expression>> children,
	                                                                  unique_ptr<Expression> filter = nullptr,
	                                                                  bool is_distinct = false);

public:
	template <class STATE, class RESULT_TYPE, class OP>
	static AggregateFunction NullaryAggregate(LogicalType return_type) {
		return AggregateFunction(
		    {}, return_type, AggregateFunction::StateSize<STATE>, AggregateFunction::StateInitialize<STATE, OP>,
		    AggregateFunction::NullaryScatterUpdate<STATE, OP>, AggregateFunction::StateCombine<STATE, OP>,
		    AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>, AggregateFunction::NullaryUpdate<STATE, OP>);
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP>
	static AggregateFunction UnaryAggregate(LogicalType input_type, LogicalType return_type) {
		return AggregateFunction(
		    {input_type}, return_type, AggregateFunction::StateSize<STATE>,
		    AggregateFunction::StateInitialize<STATE, OP>, AggregateFunction::UnaryScatterUpdate<STATE, INPUT_TYPE, OP>,
		    AggregateFunction::StateCombine<STATE, OP>, AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>,
		    AggregateFunction::UnaryUpdate<STATE, INPUT_TYPE, OP>);
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP>
	static AggregateFunction UnaryAggregateDestructor(LogicalType input_type, LogicalType return_type) {
		auto aggregate = UnaryAggregate<STATE, INPUT_TYPE, RESULT_TYPE, OP>(input_type, return_type);
		aggregate.destructor = AggregateFunction::StateDestroy<STATE, OP>;
		return aggregate;
	}

	template <class STATE, class A_TYPE, class B_TYPE, class RESULT_TYPE, class OP>
	static AggregateFunction BinaryAggregate(LogicalType a_type, LogicalType b_type, LogicalType return_type) {
		return AggregateFunction({a_type, b_type}, return_type, AggregateFunction::StateSize<STATE>,
		                         AggregateFunction::StateInitialize<STATE, OP>,
		                         AggregateFunction::BinaryScatterUpdate<STATE, A_TYPE, B_TYPE, OP>,
		                         AggregateFunction::StateCombine<STATE, OP>,
		                         AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>,
		                         AggregateFunction::BinaryUpdate<STATE, A_TYPE, B_TYPE, OP>);
	}

public:
	template <class STATE>
	static idx_t StateSize() {
		return sizeof(STATE);
	}

	template <class STATE, class OP>
	static void StateInitialize(data_ptr_t state) {
		OP::Initialize((STATE *)state);
	}

	template <class STATE, class OP>
	static void NullaryScatterUpdate(Vector inputs[], FunctionData *bind_data, idx_t input_count, Vector &states,
	                                 idx_t count) {
		D_ASSERT(input_count == 0);
		AggregateExecutor::NullaryScatter<STATE, OP>(states, bind_data, count);
	}

	template <class STATE, class OP>
	static void NullaryUpdate(Vector inputs[], FunctionData *bind_data, idx_t input_count, data_ptr_t state,
	                          idx_t count) {
		D_ASSERT(input_count == 0);
		AggregateExecutor::NullaryUpdate<STATE, OP>(state, bind_data, count);
	}

	template <class STATE, class T, class OP>
	static void UnaryScatterUpdate(Vector inputs[], FunctionData *bind_data, idx_t input_count, Vector &states,
	                               idx_t count) {
		D_ASSERT(input_count == 1);
		AggregateExecutor::UnaryScatter<STATE, T, OP>(inputs[0], states, bind_data, count);
	}

	template <class STATE, class INPUT_TYPE, class OP>
	static void UnaryUpdate(Vector inputs[], FunctionData *bind_data, idx_t input_count, data_ptr_t state,
	                        idx_t count) {
		D_ASSERT(input_count == 1);
		AggregateExecutor::UnaryUpdate<STATE, INPUT_TYPE, OP>(inputs[0], bind_data, state, count);
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP>
	static void UnaryWindow(Vector inputs[], FunctionData *bind_data, idx_t input_count, data_ptr_t state,
	                        const FrameBounds &frame, const FrameBounds &prev, Vector &result) {
		D_ASSERT(input_count == 1);
		AggregateExecutor::UnaryWindow<STATE, INPUT_TYPE, RESULT_TYPE, OP>(inputs[0], bind_data, state, frame, prev,
		                                                                   result);
	}

	template <class STATE, class A_TYPE, class B_TYPE, class OP>
	static void BinaryScatterUpdate(Vector inputs[], FunctionData *bind_data, idx_t input_count, Vector &states,
	                                idx_t count) {
		D_ASSERT(input_count == 2);
		AggregateExecutor::BinaryScatter<STATE, A_TYPE, B_TYPE, OP>(bind_data, inputs[0], inputs[1], states, count);
	}

	template <class STATE, class A_TYPE, class B_TYPE, class OP>
	static void BinaryUpdate(Vector inputs[], FunctionData *bind_data, idx_t input_count, data_ptr_t state,
	                         idx_t count) {
		D_ASSERT(input_count == 2);
		AggregateExecutor::BinaryUpdate<STATE, A_TYPE, B_TYPE, OP>(bind_data, inputs[0], inputs[1], state, count);
	}

	template <class STATE, class OP>
	static void StateCombine(Vector &source, Vector &target, idx_t count) {
		AggregateExecutor::Combine<STATE, OP>(source, target, count);
	}

	template <class STATE, class RESULT_TYPE, class OP>
	static void StateFinalize(Vector &states, FunctionData *bind_data, Vector &result, idx_t count) {
		AggregateExecutor::Finalize<STATE, RESULT_TYPE, OP>(states, bind_data, result, count);
	}

	template <class STATE, class OP>
	static void StateDestroy(Vector &states, idx_t count) {
		AggregateExecutor::Destroy<STATE, OP>(states, count);
	}
};

} // namespace duckdb


namespace duckdb {

struct UDFWrapper {
public:
	template <typename TR, typename... Args>
	static scalar_function_t CreateScalarFunction(const string &name, TR (*udf_func)(Args...)) {
		const std::size_t num_template_argc = sizeof...(Args);
		switch (num_template_argc) {
		case 1:
			return CreateUnaryFunction<TR, Args...>(name, udf_func);
		case 2:
			return CreateBinaryFunction<TR, Args...>(name, udf_func);
		case 3:
			return CreateTernaryFunction<TR, Args...>(name, udf_func);
		default:
			throw std::runtime_error("UDF function only supported until ternary!");
		}
	}

	template <typename TR, typename... Args>
	static scalar_function_t CreateScalarFunction(const string &name, vector<LogicalType> args, LogicalType ret_type,
	                                              TR (*udf_func)(Args...)) {
		if (!TypesMatch<TR>(ret_type)) {
			throw std::runtime_error("Return type doesn't match with the first template type.");
		}

		const std::size_t num_template_types = sizeof...(Args);
		if (num_template_types != args.size()) {
			throw std::runtime_error(
			    "The number of templated types should be the same quantity of the LogicalType arguments.");
		}

		switch (num_template_types) {
		case 1:
			return CreateUnaryFunction<TR, Args...>(name, args, ret_type, udf_func);
		case 2:
			return CreateBinaryFunction<TR, Args...>(name, args, ret_type, udf_func);
		case 3:
			return CreateTernaryFunction<TR, Args...>(name, args, ret_type, udf_func);
		default:
			throw std::runtime_error("UDF function only supported until ternary!");
		}
	}

	template <typename TR, typename... Args>
	static void RegisterFunction(const string &name, scalar_function_t udf_function, ClientContext &context,
	                             LogicalType varargs = LogicalType(LogicalTypeId::INVALID)) {
		vector<LogicalType> arguments;
		GetArgumentTypesRecursive<Args...>(arguments);

		LogicalType ret_type = GetArgumentType<TR>();

		RegisterFunction(name, arguments, ret_type, udf_function, context, varargs);
	}

	DUCKDB_API static void RegisterFunction(string name, vector<LogicalType> args, LogicalType ret_type,
	                                        scalar_function_t udf_function, ClientContext &context,
	                                        LogicalType varargs = LogicalType(LogicalTypeId::INVALID));

	//--------------------------------- Aggregate UDFs ------------------------------------//
	template <typename UDF_OP, typename STATE, typename TR, typename TA>
	static AggregateFunction CreateAggregateFunction(const string &name) {
		return CreateUnaryAggregateFunction<UDF_OP, STATE, TR, TA>(name);
	}

	template <typename UDF_OP, typename STATE, typename TR, typename TA, typename TB>
	static AggregateFunction CreateAggregateFunction(const string &name) {
		return CreateBinaryAggregateFunction<UDF_OP, STATE, TR, TA, TB>(name);
	}

	template <typename UDF_OP, typename STATE, typename TR, typename TA>
	static AggregateFunction CreateAggregateFunction(const string &name, LogicalType ret_type, LogicalType input_type) {
		if (!TypesMatch<TR>(ret_type)) {
			throw std::runtime_error("The return argument don't match!");
		}

		if (!TypesMatch<TA>(input_type)) {
			throw std::runtime_error("The input argument don't match!");
		}

		return CreateUnaryAggregateFunction<UDF_OP, STATE, TR, TA>(name, ret_type, input_type);
	}

	template <typename UDF_OP, typename STATE, typename TR, typename TA, typename TB>
	static AggregateFunction CreateAggregateFunction(const string &name, LogicalType ret_type, LogicalType input_typeA,
	                                                 LogicalType input_typeB) {
		if (!TypesMatch<TR>(ret_type)) {
			throw std::runtime_error("The return argument don't match!");
		}

		if (!TypesMatch<TA>(input_typeA)) {
			throw std::runtime_error("The first input argument don't match!");
		}

		if (!TypesMatch<TB>(input_typeB)) {
			throw std::runtime_error("The second input argument don't match!");
		}

		return CreateBinaryAggregateFunction<UDF_OP, STATE, TR, TA, TB>(name, ret_type, input_typeA, input_typeB);
	}

	//! A generic CreateAggregateFunction ---------------------------------------------------------------------------//
	static AggregateFunction CreateAggregateFunction(string name, vector<LogicalType> arguments,
	                                                 LogicalType return_type, aggregate_size_t state_size,
	                                                 aggregate_initialize_t initialize, aggregate_update_t update,
	                                                 aggregate_combine_t combine, aggregate_finalize_t finalize,
	                                                 aggregate_simple_update_t simple_update = nullptr,
	                                                 bind_aggregate_function_t bind = nullptr,
	                                                 aggregate_destructor_t destructor = nullptr) {

		AggregateFunction aggr_function(move(name), move(arguments), move(return_type), state_size, initialize, update,
		                                combine, finalize, simple_update, bind, destructor);
		return aggr_function;
	}

	DUCKDB_API static void RegisterAggrFunction(AggregateFunction aggr_function, ClientContext &context,
	                                            LogicalType varargs = LogicalType(LogicalTypeId::INVALID));

private:
	//-------------------------------- Templated functions --------------------------------//

	template <typename TR, typename TA>
	static scalar_function_t CreateUnaryFunction(const string &name, TR (*udf_func)(TA)) {
		scalar_function_t udf_function = [=](DataChunk &input, ExpressionState &state, Vector &result) -> void {
			UnaryExecutor::Execute<TA, TR>(input.data[0], result, input.size(), udf_func);
		};
		return udf_function;
	}

	template <typename TR, typename TA, typename TB>
	static scalar_function_t CreateBinaryFunction(const string &name, TR (*udf_func)(TA, TB)) {
		scalar_function_t udf_function = [=](DataChunk &input, ExpressionState &state, Vector &result) -> void {
			BinaryExecutor::Execute<TA, TB, TR>(input.data[0], input.data[1], result, input.size(), udf_func);
		};
		return udf_function;
	}

	template <typename TR, typename TA, typename TB, typename TC>
	static scalar_function_t CreateTernaryFunction(const string &name, TR (*udf_func)(TA, TB, TC)) {
		scalar_function_t udf_function = [=](DataChunk &input, ExpressionState &state, Vector &result) -> void {
			TernaryExecutor::Execute<TA, TB, TC, TR>(input.data[0], input.data[1], input.data[2], result, input.size(),
			                                         udf_func);
		};
		return udf_function;
	}

	template <typename TR, typename... Args>
	static scalar_function_t CreateUnaryFunction(const string &name, TR (*udf_func)(Args...)) {
		throw std::runtime_error("Incorrect number of arguments for unary function");
	}

	template <typename TR, typename... Args>
	static scalar_function_t CreateBinaryFunction(const string &name, TR (*udf_func)(Args...)) {
		throw std::runtime_error("Incorrect number of arguments for binary function");
	}

	template <typename TR, typename... Args>
	static scalar_function_t CreateTernaryFunction(const string &name, TR (*udf_func)(Args...)) {
		throw std::runtime_error("Incorrect number of arguments for ternary function");
	}

	template <typename T>
	static LogicalType GetArgumentType() {
		if (std::is_same<T, bool>()) {
			return LogicalType(LogicalTypeId::BOOLEAN);
		} else if (std::is_same<T, int8_t>()) {
			return LogicalType(LogicalTypeId::TINYINT);
		} else if (std::is_same<T, int16_t>()) {
			return LogicalType(LogicalTypeId::SMALLINT);
		} else if (std::is_same<T, int32_t>()) {
			return LogicalType(LogicalTypeId::INTEGER);
		} else if (std::is_same<T, int64_t>()) {
			return LogicalType(LogicalTypeId::BIGINT);
		} else if (std::is_same<T, float>()) {
			return LogicalType(LogicalTypeId::FLOAT);
		} else if (std::is_same<T, double>()) {
			return LogicalType(LogicalTypeId::DOUBLE);
		} else if (std::is_same<T, string_t>()) {
			return LogicalType(LogicalTypeId::VARCHAR);
		} else {
			// unrecognized type
			throw std::runtime_error("Unrecognized type!");
		}
	}

	template <typename TA, typename TB, typename... Args>
	static void GetArgumentTypesRecursive(vector<LogicalType> &arguments) {
		arguments.push_back(GetArgumentType<TA>());
		GetArgumentTypesRecursive<TB, Args...>(arguments);
	}

	template <typename TA>
	static void GetArgumentTypesRecursive(vector<LogicalType> &arguments) {
		arguments.push_back(GetArgumentType<TA>());
	}

private:
	//-------------------------------- Argumented functions --------------------------------//

	template <typename TR, typename... Args>
	static scalar_function_t CreateUnaryFunction(const string &name, vector<LogicalType> args, LogicalType ret_type,
	                                             TR (*udf_func)(Args...)) {
		throw std::runtime_error("Incorrect number of arguments for unary function");
	}

	template <typename TR, typename TA>
	static scalar_function_t CreateUnaryFunction(const string &name, vector<LogicalType> args, LogicalType ret_type,
	                                             TR (*udf_func)(TA)) {
		if (args.size() != 1) {
			throw std::runtime_error("The number of LogicalType arguments (\"args\") should be 1!");
		}
		if (!TypesMatch<TA>(args[0])) {
			throw std::runtime_error("The first arguments don't match!");
		}

		scalar_function_t udf_function = [=](DataChunk &input, ExpressionState &state, Vector &result) -> void {
			UnaryExecutor::Execute<TA, TR>(input.data[0], result, input.size(), udf_func);
		};
		return udf_function;
	}

	template <typename TR, typename... Args>
	static scalar_function_t CreateBinaryFunction(const string &name, vector<LogicalType> args, LogicalType ret_type,
	                                              TR (*udf_func)(Args...)) {
		throw std::runtime_error("Incorrect number of arguments for binary function");
	}

	template <typename TR, typename TA, typename TB>
	static scalar_function_t CreateBinaryFunction(const string &name, vector<LogicalType> args, LogicalType ret_type,
	                                              TR (*udf_func)(TA, TB)) {
		if (args.size() != 2) {
			throw std::runtime_error("The number of LogicalType arguments (\"args\") should be 2!");
		}
		if (!TypesMatch<TA>(args[0])) {
			throw std::runtime_error("The first arguments don't match!");
		}
		if (!TypesMatch<TB>(args[1])) {
			throw std::runtime_error("The second arguments don't match!");
		}

		scalar_function_t udf_function = [=](DataChunk &input, ExpressionState &state, Vector &result) {
			BinaryExecutor::Execute<TA, TB, TR>(input.data[0], input.data[1], result, input.size(), udf_func);
		};
		return udf_function;
	}

	template <typename TR, typename... Args>
	static scalar_function_t CreateTernaryFunction(const string &name, vector<LogicalType> args, LogicalType ret_type,
	                                               TR (*udf_func)(Args...)) {
		throw std::runtime_error("Incorrect number of arguments for ternary function");
	}

	template <typename TR, typename TA, typename TB, typename TC>
	static scalar_function_t CreateTernaryFunction(const string &name, vector<LogicalType> args, LogicalType ret_type,
	                                               TR (*udf_func)(TA, TB, TC)) {
		if (args.size() != 3) {
			throw std::runtime_error("The number of LogicalType arguments (\"args\") should be 3!");
		}
		if (!TypesMatch<TA>(args[0])) {
			throw std::runtime_error("The first arguments don't match!");
		}
		if (!TypesMatch<TB>(args[1])) {
			throw std::runtime_error("The second arguments don't match!");
		}
		if (!TypesMatch<TC>(args[2])) {
			throw std::runtime_error("The second arguments don't match!");
		}

		scalar_function_t udf_function = [=](DataChunk &input, ExpressionState &state, Vector &result) -> void {
			TernaryExecutor::Execute<TA, TB, TC, TR>(input.data[0], input.data[1], input.data[2], result, input.size(),
			                                         udf_func);
		};
		return udf_function;
	}

	template <typename T>
	static bool TypesMatch(LogicalType sql_type) {
		switch (sql_type.id()) {
		case LogicalTypeId::BOOLEAN:
			return std::is_same<T, bool>();
		case LogicalTypeId::TINYINT:
			return std::is_same<T, int8_t>();
		case LogicalTypeId::SMALLINT:
			return std::is_same<T, int16_t>();
		case LogicalTypeId::INTEGER:
			return std::is_same<T, int32_t>();
		case LogicalTypeId::BIGINT:
			return std::is_same<T, int64_t>();
		case LogicalTypeId::DATE:
			return std::is_same<T, date_t>();
		case LogicalTypeId::TIME:
			return std::is_same<T, dtime_t>();
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_MS:
		case LogicalTypeId::TIMESTAMP_NS:
		case LogicalTypeId::TIMESTAMP_SEC:
			return std::is_same<T, timestamp_t>();
		case LogicalTypeId::FLOAT:
			return std::is_same<T, float>();
		case LogicalTypeId::DOUBLE:
			return std::is_same<T, double>();
		case LogicalTypeId::VARCHAR:
		case LogicalTypeId::CHAR:
		case LogicalTypeId::BLOB:
			return std::is_same<T, string_t>();
		default:
			throw std::runtime_error("Type is not supported!");
		}
	}

private:
	//-------------------------------- Aggregate functions --------------------------------//
	template <typename UDF_OP, typename STATE, typename TR, typename TA>
	static AggregateFunction CreateUnaryAggregateFunction(const string &name) {
		LogicalType return_type = GetArgumentType<TR>();
		LogicalType input_type = GetArgumentType<TA>();
		return CreateUnaryAggregateFunction<UDF_OP, STATE, TR, TA>(name, return_type, input_type);
	}

	template <typename UDF_OP, typename STATE, typename TR, typename TA>
	static AggregateFunction CreateUnaryAggregateFunction(const string &name, LogicalType ret_type,
	                                                      LogicalType input_type) {
		AggregateFunction aggr_function =
		    AggregateFunction::UnaryAggregate<STATE, TR, TA, UDF_OP>(input_type, ret_type);
		aggr_function.name = name;
		return aggr_function;
	}

	template <typename UDF_OP, typename STATE, typename TR, typename TA, typename TB>
	static AggregateFunction CreateBinaryAggregateFunction(const string &name) {
		LogicalType return_type = GetArgumentType<TR>();
		LogicalType input_typeA = GetArgumentType<TA>();
		LogicalType input_typeB = GetArgumentType<TB>();
		return CreateBinaryAggregateFunction<UDF_OP, STATE, TR, TA, TB>(name, return_type, input_typeA, input_typeB);
	}

	template <typename UDF_OP, typename STATE, typename TR, typename TA, typename TB>
	static AggregateFunction CreateBinaryAggregateFunction(const string &name, LogicalType ret_type,
	                                                       LogicalType input_typeA, LogicalType input_typeB) {
		AggregateFunction aggr_function =
		    AggregateFunction::BinaryAggregate<STATE, TR, TA, TB, UDF_OP>(input_typeA, input_typeB, ret_type);
		aggr_function.name = name;
		return aggr_function;
	}
}; // end UDFWrapper

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/materialized_query_result.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/chunk_collection.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/order_type.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

enum class OrderType : uint8_t { INVALID = 0, ORDER_DEFAULT = 1, ASCENDING = 2, DESCENDING = 3 };
enum class OrderByNullType : uint8_t { INVALID = 0, ORDER_DEFAULT = 1, NULLS_FIRST = 2, NULLS_LAST = 3 };

} // namespace duckdb




namespace duckdb {

//!  A ChunkCollection represents a set of DataChunks that all have the same
//!  types
/*!
    A ChunkCollection represents a set of DataChunks concatenated together in a
   list. Individual values of the collection can be iterated over using the
   iterator. It is also possible to iterate directly over the chunks for more
   direct access.
*/
class ChunkCollection {
public:
	ChunkCollection() : count(0) {
	}

	//! The amount of columns in the ChunkCollection
	DUCKDB_API vector<LogicalType> &Types() {
		return types;
	}
	const vector<LogicalType> &Types() const {
		return types;
	}

	//! The amount of rows in the ChunkCollection
	DUCKDB_API const idx_t &Count() const {
		return count;
	}

	//! The amount of columns in the ChunkCollection
	DUCKDB_API idx_t ColumnCount() const {
		return types.size();
	}

	//! Append a new DataChunk directly to this ChunkCollection
	DUCKDB_API void Append(DataChunk &new_chunk);

	//! Append another ChunkCollection directly to this ChunkCollection
	DUCKDB_API void Append(ChunkCollection &other);

	//! Merge is like Append but messes up the order and destroys the other collection
	DUCKDB_API void Merge(ChunkCollection &other);

	DUCKDB_API void Verify();

	//! Gets the value of the column at the specified index
	DUCKDB_API Value GetValue(idx_t column, idx_t index);
	//! Sets the value of the column at the specified index
	DUCKDB_API void SetValue(idx_t column, idx_t index, const Value &value);

	DUCKDB_API vector<Value> GetRow(idx_t index);

	DUCKDB_API string ToString() const {
		return chunks.size() == 0 ? "ChunkCollection [ 0 ]"
		                          : "ChunkCollection [ " + std::to_string(count) + " ]: \n" + chunks[0]->ToString();
	}
	DUCKDB_API void Print();

	//! Gets a reference to the chunk at the given index
	DUCKDB_API DataChunk &GetChunkForRow(idx_t row_index) {
		return *chunks[LocateChunk(row_index)];
	}

	//! Gets a reference to the chunk at the given index
	DUCKDB_API DataChunk &GetChunk(idx_t chunk_index) {
		D_ASSERT(chunk_index < chunks.size());
		return *chunks[chunk_index];
	}
	const DataChunk &GetChunk(idx_t chunk_index) const {
		D_ASSERT(chunk_index < chunks.size());
		return *chunks[chunk_index];
	}

	DUCKDB_API const vector<unique_ptr<DataChunk>> &Chunks() {
		return chunks;
	}

	DUCKDB_API idx_t ChunkCount() const {
		return chunks.size();
	}

	DUCKDB_API void Reset() {
		count = 0;
		chunks.clear();
		types.clear();
	}

	DUCKDB_API unique_ptr<DataChunk> Fetch() {
		if (ChunkCount() == 0) {
			return nullptr;
		}

		auto res = move(chunks[0]);
		chunks.erase(chunks.begin() + 0);
		return res;
	}

	DUCKDB_API void Sort(vector<OrderType> &desc, vector<OrderByNullType> &null_order, idx_t result[]);
	//! Reorders the rows in the collection according to the given indices.
	DUCKDB_API void Reorder(idx_t order[]);

	DUCKDB_API void MaterializeSortedChunk(DataChunk &target, idx_t order[], idx_t start_offset);

	//! Returns true if the ChunkCollections are equivalent
	DUCKDB_API bool Equals(ChunkCollection &other);

	//! Locates the chunk that belongs to the specific index
	DUCKDB_API idx_t LocateChunk(idx_t index) {
		idx_t result = index / STANDARD_VECTOR_SIZE;
		D_ASSERT(result < chunks.size());
		return result;
	}

	DUCKDB_API void Heap(vector<OrderType> &desc, vector<OrderByNullType> &null_order, idx_t heap[], idx_t heap_size);
	DUCKDB_API idx_t MaterializeHeapChunk(DataChunk &target, idx_t order[], idx_t start_offset, idx_t heap_size);

private:
	//! The total amount of elements in the collection
	idx_t count;
	//! The set of data chunks in the collection
	vector<unique_ptr<DataChunk>> chunks;
	//! The types of the ChunkCollection
	vector<LogicalType> types;
};
} // namespace duckdb


//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/query_result.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/statement_type.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

//===--------------------------------------------------------------------===//
// Statement Types
//===--------------------------------------------------------------------===//
enum class StatementType : uint8_t {
	INVALID_STATEMENT,      // invalid statement type
	SELECT_STATEMENT,       // select statement type
	INSERT_STATEMENT,       // insert statement type
	UPDATE_STATEMENT,       // update statement type
	CREATE_STATEMENT,       // create statement type
	DELETE_STATEMENT,       // delete statement type
	PREPARE_STATEMENT,      // prepare statement type
	EXECUTE_STATEMENT,      // execute statement type
	ALTER_STATEMENT,        // alter statement type
	TRANSACTION_STATEMENT,  // transaction statement type,
	COPY_STATEMENT,         // copy type
	ANALYZE_STATEMENT,      // analyze type
	VARIABLE_SET_STATEMENT, // variable set statement type
	CREATE_FUNC_STATEMENT,  // create func statement type
	EXPLAIN_STATEMENT,      // explain statement type
	DROP_STATEMENT,         // DROP statement type
	EXPORT_STATEMENT,       // EXPORT statement type
	PRAGMA_STATEMENT,       // PRAGMA statement type
	SHOW_STATEMENT,         // SHOW statement type
	VACUUM_STATEMENT,       // VACUUM statement type
	CALL_STATEMENT,         // CALL statement type
	SET_STATEMENT,          // SET statement type
	LOAD_STATEMENT,         // LOAD statement type
	RELATION_STATEMENT
};

string StatementTypeToString(StatementType type);
bool StatementTypeReturnChanges(StatementType type);

} // namespace duckdb




struct ArrowSchema;

namespace duckdb {

enum class QueryResultType : uint8_t { MATERIALIZED_RESULT, STREAM_RESULT };

//! The QueryResult object holds the result of a query. It can either be a MaterializedQueryResult, in which case the
//! result contains the entire result set, or a StreamQueryResult in which case the Fetch method can be called to
//! incrementally fetch data from the database.
class QueryResult {
public:
	//! Creates an successful empty query result
	DUCKDB_API QueryResult(QueryResultType type, StatementType statement_type);
	//! Creates a successful query result with the specified names and types
	DUCKDB_API QueryResult(QueryResultType type, StatementType statement_type, vector<LogicalType> types,
	                       vector<string> names);
	//! Creates an unsuccessful query result with error condition
	DUCKDB_API QueryResult(QueryResultType type, string error);
	DUCKDB_API virtual ~QueryResult() {
	}

	//! The type of the result (MATERIALIZED or STREAMING)
	QueryResultType type;
	//! The type of the statement that created this result
	StatementType statement_type;
	//! The SQL types of the result
	vector<LogicalType> types;
	//! The names of the result
	vector<string> names;
	//! Whether or not execution was successful
	bool success;
	//! The error string (in case execution was not successful)
	string error;
	//! The next result (if any)
	unique_ptr<QueryResult> next;

public:
	//! Fetches a DataChunk of normalized (flat) vectors from the query result.
	//! Returns nullptr if there are no more results to fetch.
	DUCKDB_API virtual unique_ptr<DataChunk> Fetch();
	//! Fetches a DataChunk from the query result. The vector types
	DUCKDB_API virtual unique_ptr<DataChunk> FetchRaw() = 0;
	// Converts the QueryResult to a string
	DUCKDB_API virtual string ToString() = 0;
	//! Prints the QueryResult to the console
	DUCKDB_API void Print();
	//! Returns true if the two results are identical; false otherwise. Note that this method is destructive; it calls
	//! Fetch() until both results are exhausted. The data in the results will be lost.
	DUCKDB_API bool Equals(QueryResult &other);

	DUCKDB_API idx_t ColumnCount() {
		return types.size();
	}

	DUCKDB_API bool TryFetch(unique_ptr<DataChunk> &result, string &error) {
		try {
			result = Fetch();
			return true;
		} catch (std::exception &ex) {
			error = ex.what();
			return false;
		} catch (...) {
			error = "Unknown error in Fetch";
			return false;
		}
	}

	DUCKDB_API void ToArrowSchema(ArrowSchema *out_array);

private:
	//! The current chunk used by the iterator
	unique_ptr<DataChunk> iterator_chunk;

private:
	class QueryResultIterator;
	class QueryResultRow {
	public:
		explicit QueryResultRow(QueryResultIterator &iterator) : iterator(iterator), row(0) {
		}

		QueryResultIterator &iterator;
		idx_t row;

		template <class T>
		T GetValue(idx_t col_idx) const {
			return iterator.result->iterator_chunk->GetValue(col_idx, iterator.row_idx).GetValue<T>();
		}
	};
	//! The row-based query result iterator. Invoking the
	class QueryResultIterator {
	public:
		explicit QueryResultIterator(QueryResult *result) : current_row(*this), result(result), row_idx(0) {
			if (result) {
				result->iterator_chunk = result->Fetch();
			}
		}

		QueryResultRow current_row;
		QueryResult *result;
		idx_t row_idx;

	public:
		void Next() {
			if (!result->iterator_chunk) {
				return;
			}
			current_row.row++;
			row_idx++;
			if (row_idx >= result->iterator_chunk->size()) {
				result->iterator_chunk = result->Fetch();
				row_idx = 0;
			}
		}

		QueryResultIterator &operator++() {
			Next();
			return *this;
		}
		bool operator!=(const QueryResultIterator &other) const {
			return result->iterator_chunk && result->iterator_chunk->ColumnCount() > 0;
		}
		const QueryResultRow &operator*() const {
			return current_row;
		}
	};

public:
	DUCKDB_API QueryResultIterator begin() {
		return QueryResultIterator(this);
	}
	DUCKDB_API QueryResultIterator end() {
		return QueryResultIterator(nullptr);
	}

protected:
	DUCKDB_API string HeaderToString();

private:
	QueryResult(const QueryResult &) = delete;
};

} // namespace duckdb

namespace duckdb {

class MaterializedQueryResult : public QueryResult {
public:
	//! Creates an empty successful query result
	DUCKDB_API explicit MaterializedQueryResult(StatementType statement_type);
	//! Creates a successful query result with the specified names and types
	DUCKDB_API MaterializedQueryResult(StatementType statement_type, vector<LogicalType> types, vector<string> names);
	//! Creates an unsuccessful query result with error condition
	DUCKDB_API explicit MaterializedQueryResult(string error);

	ChunkCollection collection;

public:
	//! Fetches a DataChunk from the query result.
	//! This will consume the result (i.e. the chunks are taken directly from the ChunkCollection).
	DUCKDB_API unique_ptr<DataChunk> Fetch() override;
	DUCKDB_API unique_ptr<DataChunk> FetchRaw() override;
	//! Converts the QueryResult to a string
	DUCKDB_API string ToString() override;

	//! Gets the (index) value of the (column index) column
	DUCKDB_API Value GetValue(idx_t column, idx_t index);

	template <class T>
	T GetValue(idx_t column, idx_t index) {
		auto value = GetValue(column, index);
		return (T)value.GetValue<int64_t>();
	}
};

} // namespace duckdb
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/prepared_statement.hpp
//
//
//===----------------------------------------------------------------------===//






namespace duckdb {
class ClientContext;
class PreparedStatementData;

//! A prepared statement
class PreparedStatement {
public:
	//! Create a successfully prepared prepared statement object with the given name
	DUCKDB_API PreparedStatement(shared_ptr<ClientContext> context, shared_ptr<PreparedStatementData> data,
	                             string query, idx_t n_param);
	//! Create a prepared statement that was not successfully prepared
	DUCKDB_API explicit PreparedStatement(string error);

	DUCKDB_API ~PreparedStatement();

public:
	//! The client context this prepared statement belongs to
	shared_ptr<ClientContext> context;
	//! The prepared statement data
	shared_ptr<PreparedStatementData> data;
	//! The query that is being prepared
	string query;
	//! Whether or not the statement was successfully prepared
	bool success;
	//! The error message (if success = false)
	string error;
	//! The amount of bound parameters
	idx_t n_param;

public:
	//! Returns the number of columns in the result
	idx_t ColumnCount();
	//! Returns the statement type of the underlying prepared statement object
	StatementType GetStatementType();
	//! Returns the result SQL types of the prepared statement
	const vector<LogicalType> &GetTypes();
	//! Returns the result names of the prepared statement
	const vector<string> &GetNames();

	//! Execute the prepared statement with the given set of arguments
	template <typename... Args>
	unique_ptr<QueryResult> Execute(Args... args) {
		vector<Value> values;
		return ExecuteRecursive(values, args...);
	}

	//! Execute the prepared statement with the given set of values
	DUCKDB_API unique_ptr<QueryResult> Execute(vector<Value> &values, bool allow_stream_result = true);

private:
	unique_ptr<QueryResult> ExecuteRecursive(vector<Value> &values) {
		return Execute(values);
	}

	template <typename T, typename... Args>
	unique_ptr<QueryResult> ExecuteRecursive(vector<Value> &values, T value, Args... args) {
		values.push_back(Value::CreateValue<T>(value));
		return ExecuteRecursive(values, args...);
	}
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation.hpp
//
//
//===----------------------------------------------------------------------===//




//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/join_type.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

//===--------------------------------------------------------------------===//
// Join Types
//===--------------------------------------------------------------------===//
enum class JoinType : uint8_t {
	INVALID = 0, // invalid join type
	LEFT = 1,    // left
	RIGHT = 2,   // right
	INNER = 3,   // inner
	OUTER = 4,   // outer
	SEMI = 5,    // SEMI join returns left side row ONLY if it has a join partner, no duplicates
	ANTI = 6,    // ANTI join returns left side row ONLY if it has NO join partner, no duplicates
	MARK = 7,    // MARK join returns marker indicating whether or not there is a join partner (true), there is no join
	             // partner (false)
	SINGLE = 8   // SINGLE join is like LEFT OUTER JOIN, BUT returns at most one join partner per entry on the LEFT side
	             // (and NULL if no partner is found)
};

//! Convert join type to string
string JoinTypeToString(JoinType type);

//! True if join is left, full or right outer join
bool IsOuterJoin(JoinType type);

//! True if join is left or full outer join
bool IsLeftOuterJoin(JoinType type);

//! True if join is rght or full outer join
bool IsRightOuterJoin(JoinType type);

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/relation_type.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

//===--------------------------------------------------------------------===//
// Catalog Types
//===--------------------------------------------------------------------===//
enum class RelationType : uint8_t {
	INVALID_RELATION,
	TABLE_RELATION,
	PROJECTION_RELATION,
	FILTER_RELATION,
	EXPLAIN_RELATION,
	CROSS_PRODUCT_RELATION,
	JOIN_RELATION,
	AGGREGATE_RELATION,
	SET_OPERATION_RELATION,
	DISTINCT_RELATION,
	LIMIT_RELATION,
	ORDER_RELATION,
	CREATE_VIEW_RELATION,
	CREATE_TABLE_RELATION,
	INSERT_RELATION,
	VALUE_LIST_RELATION,
	DELETE_RELATION,
	UPDATE_RELATION,
	WRITE_CSV_RELATION,
	READ_CSV_RELATION,
	SUBQUERY_RELATION,
	TABLE_FUNCTION_RELATION,
	VIEW_RELATION,
	QUERY_RELATION
};

string RelationTypeToString(RelationType type);

} // namespace duckdb





#include <memory>

namespace duckdb {
struct BoundStatement;

class ClientContext;
class Binder;
class LogicalOperator;
class QueryNode;
class TableRef;

class Relation : public std::enable_shared_from_this<Relation> {
public:
	DUCKDB_API Relation(ClientContext &context, RelationType type) : context(context), type(type) {
	}
	DUCKDB_API virtual ~Relation() {
	}

	ClientContext &context;
	RelationType type;

public:
	DUCKDB_API virtual const vector<ColumnDefinition> &Columns() = 0;
	DUCKDB_API virtual unique_ptr<QueryNode> GetQueryNode() = 0;
	DUCKDB_API virtual BoundStatement Bind(Binder &binder);
	DUCKDB_API virtual string GetAlias();

	DUCKDB_API unique_ptr<QueryResult> Execute();
	DUCKDB_API string ToString();
	DUCKDB_API virtual string ToString(idx_t depth) = 0;

	DUCKDB_API void Print();
	DUCKDB_API void Head(idx_t limit = 10);

	DUCKDB_API shared_ptr<Relation> CreateView(const string &name, bool replace = true, bool temporary = false);
	DUCKDB_API unique_ptr<QueryResult> Query(const string &sql);
	DUCKDB_API unique_ptr<QueryResult> Query(const string &name, const string &sql);

	//! Explain the query plan of this relation
	DUCKDB_API unique_ptr<QueryResult> Explain();

	DUCKDB_API virtual unique_ptr<TableRef> GetTableRef();
	DUCKDB_API virtual bool IsReadOnly() {
		return true;
	}

public:
	// PROJECT
	DUCKDB_API shared_ptr<Relation> Project(const string &select_list);
	DUCKDB_API shared_ptr<Relation> Project(const string &expression, const string &alias);
	DUCKDB_API shared_ptr<Relation> Project(const string &select_list, const vector<string> &aliases);
	DUCKDB_API shared_ptr<Relation> Project(const vector<string> &expressions);
	DUCKDB_API shared_ptr<Relation> Project(const vector<string> &expressions, const vector<string> &aliases);

	// FILTER
	DUCKDB_API shared_ptr<Relation> Filter(const string &expression);
	DUCKDB_API shared_ptr<Relation> Filter(const vector<string> &expressions);

	// LIMIT
	DUCKDB_API shared_ptr<Relation> Limit(int64_t n, int64_t offset = 0);

	// ORDER
	DUCKDB_API shared_ptr<Relation> Order(const string &expression);
	DUCKDB_API shared_ptr<Relation> Order(const vector<string> &expressions);

	// JOIN operation
	DUCKDB_API shared_ptr<Relation> Join(const shared_ptr<Relation> &other, const string &condition,
	                                     JoinType type = JoinType::INNER);

	// SET operations
	DUCKDB_API shared_ptr<Relation> Union(const shared_ptr<Relation> &other);
	DUCKDB_API shared_ptr<Relation> Except(const shared_ptr<Relation> &other);
	DUCKDB_API shared_ptr<Relation> Intersect(const shared_ptr<Relation> &other);

	// DISTINCT operation
	DUCKDB_API shared_ptr<Relation> Distinct();

	// AGGREGATES
	DUCKDB_API shared_ptr<Relation> Aggregate(const string &aggregate_list);
	DUCKDB_API shared_ptr<Relation> Aggregate(const vector<string> &aggregates);
	DUCKDB_API shared_ptr<Relation> Aggregate(const string &aggregate_list, const string &group_list);
	DUCKDB_API shared_ptr<Relation> Aggregate(const vector<string> &aggregates, const vector<string> &groups);

	// ALIAS
	DUCKDB_API shared_ptr<Relation> Alias(const string &alias);

	//! Insert the data from this relation into a table
	DUCKDB_API void Insert(const string &table_name);
	DUCKDB_API void Insert(const string &schema_name, const string &table_name);
	//! Insert a row (i.e.,list of values) into a table
	DUCKDB_API void Insert(const vector<vector<Value>> &values);
	//! Create a table and insert the data from this relation into that table
	DUCKDB_API void Create(const string &table_name);
	DUCKDB_API void Create(const string &schema_name, const string &table_name);

	//! Write a relation to a CSV file
	DUCKDB_API void WriteCSV(const string &csv_file);

	//! Update a table, can only be used on a TableRelation
	DUCKDB_API virtual void Update(const string &update, const string &condition = string());
	//! Delete from a table, can only be used on a TableRelation
	DUCKDB_API virtual void Delete(const string &condition = string());
	//! Create a relation from calling a table in/out function on the input relation
	DUCKDB_API shared_ptr<Relation> TableFunction(const std::string &fname, vector<Value> &values);

public:
	//! Whether or not the relation inherits column bindings from its child or not, only relevant for binding
	DUCKDB_API virtual bool InheritsColumnBindings() {
		return false;
	}
	DUCKDB_API virtual Relation *ChildRelation() {
		return nullptr;
	}

protected:
	DUCKDB_API string RenderWhitespace(idx_t depth);
};

} // namespace duckdb
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/stream_query_result.hpp
//
//
//===----------------------------------------------------------------------===//






namespace duckdb {

class ClientContext;
class MaterializedQueryResult;
class PreparedStatementData;

class StreamQueryResult : public QueryResult {
public:
	//! Create a successful StreamQueryResult. StreamQueryResults should always be successful initially (it makes no
	//! sense to stream an error).
	DUCKDB_API StreamQueryResult(StatementType statement_type, shared_ptr<ClientContext> context,
	                             vector<LogicalType> types, vector<string> names,
	                             shared_ptr<PreparedStatementData> prepared = nullptr);
	DUCKDB_API ~StreamQueryResult() override;

	//! Whether or not the StreamQueryResult is still open
	bool is_open;

public:
	//! Fetches a DataChunk from the query result.
	DUCKDB_API unique_ptr<DataChunk> FetchRaw() override;
	//! Converts the QueryResult to a string
	DUCKDB_API string ToString() override;
	//! Materializes the query result and turns it into a materialized query result
	DUCKDB_API unique_ptr<MaterializedQueryResult> Materialize();

	//! Closes the StreamQueryResult
	DUCKDB_API void Close();

private:
	//! The client context this StreamQueryResult belongs to
	shared_ptr<ClientContext> context;
	//! The prepared statement data this StreamQueryResult was created with (if any)
	shared_ptr<PreparedStatementData> prepared;
};

} // namespace duckdb
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/table_description.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

struct TableDescription {
	//! The schema of the table
	string schema;
	//! The table name of the table
	string table;
	//! The columns of the table
	vector<ColumnDefinition> columns;
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/sql_statement.hpp
//
//
//===----------------------------------------------------------------------===//






//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/printer.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

//! Printer is a static class that allows printing to logs or stdout/stderr
class Printer {
public:
	//! Print the object to stderr
	static void Print(const string &str);
	//! Prints Progress
	static void PrintProgress(int percentage, const char *pbstr, int pbwidth);
	//! Prints an empty line when progress bar is done
	static void FinishProgressBarPrint(const char *pbstr, int pbwidth);
};
} // namespace duckdb


namespace duckdb {
//! SQLStatement is the base class of any type of SQL statement.
class SQLStatement {
public:
	explicit SQLStatement(StatementType type) : type(type) {};
	virtual ~SQLStatement() {
	}

	//! The statement type
	StatementType type;
	//! The statement location within the query string
	idx_t stmt_location;
	//! The statement length within the query string
	idx_t stmt_length;
	//! The number of prepared statement parameters (if any)
	idx_t n_param;
	//! The query text that corresponds to this SQL statement
	string query;

public:
	//! Create a copy of this SelectStatement
	virtual unique_ptr<SQLStatement> Copy() const = 0;
};
} // namespace duckdb


namespace duckdb {

class ClientContext;
class DatabaseInstance;
class DuckDB;
class LogicalOperator;

typedef void (*warning_callback)(std::string);

//! A connection to a database. This represents a (client) connection that can
//! be used to query the database.
class Connection {
public:
	DUCKDB_API explicit Connection(DuckDB &database);
	DUCKDB_API explicit Connection(DatabaseInstance &database);

	shared_ptr<ClientContext> context;
	warning_callback warning_cb;

public:
	//! Returns query profiling information for the current query
	DUCKDB_API string GetProfilingInformation(ProfilerPrintFormat format = ProfilerPrintFormat::QUERY_TREE);

	//! Interrupt execution of the current query
	DUCKDB_API void Interrupt();

	//! Enable query profiling
	DUCKDB_API void EnableProfiling();
	//! Disable query profiling
	DUCKDB_API void DisableProfiling();

	DUCKDB_API void SetWarningCallback(warning_callback);

	//! Enable aggressive verification/testing of queries, should only be used in testing
	DUCKDB_API void EnableQueryVerification();
	DUCKDB_API void DisableQueryVerification();
	//! Force parallel execution, even for smaller tables. Should only be used in testing.
	DUCKDB_API void ForceParallelism();

	//! Issues a query to the database and returns a QueryResult. This result can be either a StreamQueryResult or a
	//! MaterializedQueryResult. The result can be stepped through with calls to Fetch(). Note that there can only be
	//! one active StreamQueryResult per Connection object. Calling SendQuery() will invalidate any previously existing
	//! StreamQueryResult.
	DUCKDB_API unique_ptr<QueryResult> SendQuery(const string &query);
	//! Issues a query to the database and materializes the result (if necessary). Always returns a
	//! MaterializedQueryResult.
	DUCKDB_API unique_ptr<MaterializedQueryResult> Query(const string &query);
	//! Issues a query to the database and materializes the result (if necessary). Always returns a
	//! MaterializedQueryResult.
	DUCKDB_API unique_ptr<MaterializedQueryResult> Query(unique_ptr<SQLStatement> statement);
	// prepared statements
	template <typename... Args>
	unique_ptr<QueryResult> Query(const string &query, Args... args) {
		vector<Value> values;
		return QueryParamsRecursive(query, values, args...);
	}

	//! Prepare the specified query, returning a prepared statement object
	DUCKDB_API unique_ptr<PreparedStatement> Prepare(const string &query);
	//! Prepare the specified statement, returning a prepared statement object
	DUCKDB_API unique_ptr<PreparedStatement> Prepare(unique_ptr<SQLStatement> statement);

	//! Get the table info of a specific table (in the default schema), or nullptr if it cannot be found
	DUCKDB_API unique_ptr<TableDescription> TableInfo(const string &table_name);
	//! Get the table info of a specific table, or nullptr if it cannot be found
	DUCKDB_API unique_ptr<TableDescription> TableInfo(const string &schema_name, const string &table_name);

	//! Extract a set of SQL statements from a specific query
	DUCKDB_API vector<unique_ptr<SQLStatement>> ExtractStatements(const string &query);
	//! Extract the logical plan that corresponds to a query
	DUCKDB_API unique_ptr<LogicalOperator> ExtractPlan(const string &query);

	//! Appends a DataChunk to the specified table
	DUCKDB_API void Append(TableDescription &description, DataChunk &chunk);

	//! Returns a relation that produces a table from this connection
	DUCKDB_API shared_ptr<Relation> Table(const string &tname);
	DUCKDB_API shared_ptr<Relation> Table(const string &schema_name, const string &table_name);
	//! Returns a relation that produces a view from this connection
	DUCKDB_API shared_ptr<Relation> View(const string &tname);
	DUCKDB_API shared_ptr<Relation> View(const string &schema_name, const string &table_name);
	//! Returns a relation that calls a specified table function
	DUCKDB_API shared_ptr<Relation> TableFunction(const string &tname);
	DUCKDB_API shared_ptr<Relation> TableFunction(const string &tname, const vector<Value> &values);
	//! Returns a relation that produces values
	DUCKDB_API shared_ptr<Relation> Values(const vector<vector<Value>> &values);
	DUCKDB_API shared_ptr<Relation> Values(const vector<vector<Value>> &values, const vector<string> &column_names,
	                                       const string &alias = "values");
	DUCKDB_API shared_ptr<Relation> Values(const string &values);
	DUCKDB_API shared_ptr<Relation> Values(const string &values, const vector<string> &column_names,
	                                       const string &alias = "values");
	//! Reads CSV file
	DUCKDB_API shared_ptr<Relation> ReadCSV(const string &csv_file);
	DUCKDB_API shared_ptr<Relation> ReadCSV(const string &csv_file, const vector<string> &columns);
	//! Returns a relation from a query
	DUCKDB_API shared_ptr<Relation> RelationFromQuery(string query, string alias = "queryrelation");

	DUCKDB_API void BeginTransaction();
	DUCKDB_API void Commit();
	DUCKDB_API void Rollback();
	DUCKDB_API void SetAutoCommit(bool auto_commit);
	DUCKDB_API bool IsAutoCommit();

	template <typename TR, typename... Args>
	void CreateScalarFunction(const string &name, TR (*udf_func)(Args...)) {
		scalar_function_t function = UDFWrapper::CreateScalarFunction<TR, Args...>(name, udf_func);
		UDFWrapper::RegisterFunction<TR, Args...>(name, function, *context);
	}

	template <typename TR, typename... Args>
	void CreateScalarFunction(const string &name, vector<LogicalType> args, LogicalType ret_type,
	                          TR (*udf_func)(Args...)) {
		scalar_function_t function =
		    UDFWrapper::CreateScalarFunction<TR, Args...>(name, args, move(ret_type), udf_func);
		UDFWrapper::RegisterFunction(name, args, ret_type, function, *context);
	}

	template <typename TR, typename... Args>
	void CreateVectorizedFunction(const string &name, scalar_function_t udf_func,
	                              LogicalType varargs = LogicalType::INVALID) {
		UDFWrapper::RegisterFunction<TR, Args...>(name, udf_func, *context, move(varargs));
	}

	DUCKDB_API void CreateVectorizedFunction(const string &name, vector<LogicalType> args, LogicalType ret_type,
	                                         scalar_function_t udf_func, LogicalType varargs = LogicalType::INVALID) {
		UDFWrapper::RegisterFunction(name, move(args), move(ret_type), udf_func, *context, move(varargs));
	}

	//------------------------------------- Aggreate Functions ----------------------------------------//
	template <typename UDF_OP, typename STATE, typename TR, typename TA>
	void CreateAggregateFunction(const string &name) {
		AggregateFunction function = UDFWrapper::CreateAggregateFunction<UDF_OP, STATE, TR, TA>(name);
		UDFWrapper::RegisterAggrFunction(function, *context);
	}

	template <typename UDF_OP, typename STATE, typename TR, typename TA, typename TB>
	void CreateAggregateFunction(const string &name) {
		AggregateFunction function = UDFWrapper::CreateAggregateFunction<UDF_OP, STATE, TR, TA, TB>(name);
		UDFWrapper::RegisterAggrFunction(function, *context);
	}

	template <typename UDF_OP, typename STATE, typename TR, typename TA>
	void CreateAggregateFunction(const string &name, LogicalType ret_type, LogicalType input_typeA) {
		AggregateFunction function =
		    UDFWrapper::CreateAggregateFunction<UDF_OP, STATE, TR, TA>(name, ret_type, input_typeA);
		UDFWrapper::RegisterAggrFunction(function, *context);
	}

	template <typename UDF_OP, typename STATE, typename TR, typename TA, typename TB>
	void CreateAggregateFunction(const string &name, LogicalType ret_type, LogicalType input_typeA,
	                             LogicalType input_typeB) {
		AggregateFunction function =
		    UDFWrapper::CreateAggregateFunction<UDF_OP, STATE, TR, TA, TB>(name, ret_type, input_typeA, input_typeB);
		UDFWrapper::RegisterAggrFunction(function, *context);
	}

	DUCKDB_API void CreateAggregateFunction(const string &name, vector<LogicalType> arguments, LogicalType return_type,
	                                        aggregate_size_t state_size, aggregate_initialize_t initialize,
	                                        aggregate_update_t update, aggregate_combine_t combine,
	                                        aggregate_finalize_t finalize,
	                                        aggregate_simple_update_t simple_update = nullptr,
	                                        bind_aggregate_function_t bind = nullptr,
	                                        aggregate_destructor_t destructor = nullptr) {
		AggregateFunction function =
		    UDFWrapper::CreateAggregateFunction(name, arguments, return_type, state_size, initialize, update, combine,
		                                        finalize, simple_update, bind, destructor);
		UDFWrapper::RegisterAggrFunction(function, *context);
	}

private:
	unique_ptr<QueryResult> QueryParamsRecursive(const string &query, vector<Value> &values);

	template <typename T, typename... Args>
	unique_ptr<QueryResult> QueryParamsRecursive(const string &query, vector<Value> &values, T value, Args... args) {
		values.push_back(Value::CreateValue<T>(value));
		return QueryParamsRecursive(query, values, args...);
	}
};

} // namespace duckdb
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/database.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/mutex.hpp
//
//
//===----------------------------------------------------------------------===//



#include <mutex>

namespace duckdb {
using std::lock_guard;
using std::mutex;
using std::unique_lock;
} // namespace duckdb


//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/config.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/allocator.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {
class Allocator;
class ClientContext;
class DatabaseInstance;

struct PrivateAllocatorData {
	virtual ~PrivateAllocatorData() {
	}
};

typedef data_ptr_t (*allocate_function_ptr_t)(PrivateAllocatorData *private_data, idx_t size);
typedef void (*free_function_ptr_t)(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size);

class AllocatedData {
public:
	AllocatedData(Allocator &allocator, data_ptr_t pointer, idx_t allocated_size);
	~AllocatedData();

	data_ptr_t get() {
		return pointer;
	}
	const_data_ptr_t get() const {
		return pointer;
	}
	void Reset();

private:
	Allocator &allocator;
	data_ptr_t pointer;
	idx_t allocated_size;
};

class Allocator {
public:
	Allocator();
	Allocator(allocate_function_ptr_t allocate_function_p, free_function_ptr_t free_function_p,
	          unique_ptr<PrivateAllocatorData> private_data);

	data_ptr_t AllocateData(idx_t size);
	void FreeData(data_ptr_t pointer, idx_t size);

	unique_ptr<AllocatedData> Allocate(idx_t size) {
		return make_unique<AllocatedData>(*this, AllocateData(size), size);
	}

	static data_ptr_t DefaultAllocate(PrivateAllocatorData *private_data, idx_t size) {
		return new data_t[size];
	}
	static void DefaultFree(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size) {
		delete[] pointer;
	}
	static Allocator &Get(ClientContext &context);
	static Allocator &Get(DatabaseInstance &db);

	PrivateAllocatorData *GetPrivateData() {
		return private_data.get();
	}

private:
	allocate_function_ptr_t allocate_function;
	free_function_ptr_t free_function;

	unique_ptr<PrivateAllocatorData> private_data;
};

} // namespace duckdb







//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/replacement_scan.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

class TableFunctionRef;

typedef unique_ptr<TableFunctionRef> (*replacement_scan_t)(const string &table_name, void *data);

//! Replacement table scans are automatically attempted when a table name cannot be found in the schema
//! This allows you to do e.g. SELECT * FROM 'filename.csv', and automatically convert this into a CSV scan
struct ReplacementScan {
	explicit ReplacementScan(replacement_scan_t function, void *data = nullptr) : function(function), data(data) {
	}

	replacement_scan_t function;
	void *data;
};

} // namespace duckdb


namespace duckdb {
class ClientContext;
class TableFunctionRef;

enum class AccessMode : uint8_t { UNDEFINED = 0, AUTOMATIC = 1, READ_ONLY = 2, READ_WRITE = 3 };
enum class CheckpointAbort : uint8_t { NO_ABORT = 0, DEBUG_ABORT_BEFORE_TRUNCATE = 1, DEBUG_ABORT_BEFORE_HEADER = 2 };

enum class ConfigurationOptionType : uint32_t {
	INVALID = 0,
	ACCESS_MODE,
	DEFAULT_ORDER_TYPE,
	DEFAULT_NULL_ORDER,
	ENABLE_EXTERNAL_ACCESS,
	ENABLE_OBJECT_CACHE,
	MAXIMUM_MEMORY,
	THREADS
};

struct ConfigurationOption {
	ConfigurationOptionType type;
	const char *name;
	const char *description;
	LogicalTypeId parameter_type;
};

// this is optional and only used in tests at the moment
struct DBConfig {
	friend class DatabaseInstance;
	friend class StorageManager;

public:
	DUCKDB_API ~DBConfig();

	//! Access mode of the database (AUTOMATIC, READ_ONLY or READ_WRITE)
	AccessMode access_mode = AccessMode::AUTOMATIC;
	//! The allocator used by the system
	Allocator allocator;
	// Checkpoint when WAL reaches this size (default: 16MB)
	idx_t checkpoint_wal_size = 1 << 24;
	//! Whether or not to use Direct IO, bypassing operating system buffers
	bool use_direct_io = false;
	//! The FileSystem to use, can be overwritten to allow for injecting custom file systems for testing purposes (e.g.
	//! RamFS or something similar)
	unique_ptr<FileSystem> file_system;
	//! The maximum memory used by the database system (in bytes). Default: 80% of System available memory
	idx_t maximum_memory = (idx_t)-1;
	//! The maximum amount of CPU threads used by the database system. Default: all available.
	idx_t maximum_threads = (idx_t)-1;
	//! Whether or not to create and use a temporary directory to store intermediates that do not fit in memory
	bool use_temporary_directory = true;
	//! Directory to store temporary structures that do not fit in memory
	string temporary_directory;
	//! The collation type of the database
	string collation = string();
	//! The order type used when none is specified (default: ASC)
	OrderType default_order_type = OrderType::ASCENDING;
	//! Null ordering used when none is specified (default: NULLS FIRST)
	OrderByNullType default_null_order = OrderByNullType::NULLS_FIRST;
	//! enable COPY and related commands
	bool enable_external_access = true;
	//! Whether or not object cache is used
	bool object_cache_enable = false;
	//! Database configuration variables as controlled by SET
	unordered_map<std::string, Value> set_variables;
	//! Force checkpoint when CHECKPOINT is called or on shutdown, even if no changes have been made
	bool force_checkpoint = false;
	//! Run a checkpoint on successful shutdown and delete the WAL, to leave only a single database file behind
	bool checkpoint_on_shutdown = true;
	//! Debug flag that decides when a checkpoing should be aborted. Only used for testing purposes.
	CheckpointAbort checkpoint_abort = CheckpointAbort::NO_ABORT;
	//! Replacement table scans are automatically attempted when a table name cannot be found in the schema
	vector<ReplacementScan> replacement_scans;

public:
	DUCKDB_API static DBConfig &GetConfig(ClientContext &context);
	DUCKDB_API static DBConfig &GetConfig(DatabaseInstance &db);
	DUCKDB_API static vector<ConfigurationOption> GetOptions();

	//! Fetch an option by name. Returns a pointer to the option, or nullptr if none exists.
	DUCKDB_API static ConfigurationOption *GetOptionByName(const string &name);

	DUCKDB_API void SetOption(const ConfigurationOption &option, const Value &value);

	DUCKDB_API static idx_t ParseMemoryLimit(const string &arg);
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/extension.hpp
//
//
//===----------------------------------------------------------------------===//






namespace duckdb {
class DuckDB;

//! The Extension class is the base class used to define extensions
class Extension {
public:
	DUCKDB_API virtual void Load(DuckDB &db) = 0;
	DUCKDB_API virtual ~Extension() = default;
};

} // namespace duckdb

namespace duckdb {
class StorageManager;
class Catalog;
class TransactionManager;
class ConnectionManager;
class FileSystem;
class TaskScheduler;
class ObjectCache;

class DatabaseInstance : public std::enable_shared_from_this<DatabaseInstance> {
	friend class DuckDB;

public:
	DUCKDB_API DatabaseInstance();
	DUCKDB_API ~DatabaseInstance();

	DBConfig config;

public:
	StorageManager &GetStorageManager();
	Catalog &GetCatalog();
	FileSystem &GetFileSystem();
	TransactionManager &GetTransactionManager();
	TaskScheduler &GetScheduler();
	ObjectCache &GetObjectCache();
	ConnectionManager &GetConnectionManager();

	idx_t NumberOfThreads();

	static DatabaseInstance &GetDatabase(ClientContext &context);

private:
	void Initialize(const char *path, DBConfig *config);

	void Configure(DBConfig &config);

private:
	unique_ptr<StorageManager> storage;
	unique_ptr<Catalog> catalog;
	unique_ptr<TransactionManager> transaction_manager;
	unique_ptr<TaskScheduler> scheduler;
	unique_ptr<ObjectCache> object_cache;
	unique_ptr<ConnectionManager> connection_manager;
};

//! The database object. This object holds the catalog and all the
//! database-specific meta information.
class DuckDB {
public:
	DUCKDB_API explicit DuckDB(const char *path = nullptr, DBConfig *config = nullptr);
	DUCKDB_API explicit DuckDB(const string &path, DBConfig *config = nullptr);
	DUCKDB_API ~DuckDB();

	//! Reference to the actual database instance
	shared_ptr<DatabaseInstance> instance;

public:
	template <class T>
	void LoadExtension() {
		T extension;
		extension.Load(*this);
	}

	DUCKDB_API FileSystem &GetFileSystem();

	DUCKDB_API idx_t NumberOfThreads();
	DUCKDB_API static const char *SourceID();
	DUCKDB_API static const char *LibraryVersion();
};

} // namespace duckdb




#ifdef DUCKDB_BUILD_LOADABLE_EXTENSION
#ifdef _WIN32
#include <windows.h>
#include <delayimp.h>

extern "C" {
/*
This is interesting: Windows would normally require a duckdb.dll being on the DLL search path when we load an extension
using LoadLibrary(). However, there is likely no such dll, because DuckDB was statically linked, or is running as part
of an R or Python module with a completely different name (that we don't know) or something of the sorts. Amazingly,
Windows supports lazy-loading DLLs by linking them with /DELAYLOAD. Then a callback will be triggerd whenever we access
symbols in the extension. Since DuckDB is already running in the host process (hopefully), we can use
GetModuleHandle(NULL) to return the current process so the symbols are looked for there. See here for another
explanation of this crazy process:

* https://docs.microsoft.com/en-us/cpp/build/reference/linker-support-for-delay-loaded-dlls?view=msvc-160
* https://docs.microsoft.com/en-us/cpp/build/reference/understanding-the-helper-function?view=msvc-160
*/
FARPROC WINAPI duckdb_dllimport_delay_hook(unsigned dliNotify, PDelayLoadInfo pdli) {
	switch (dliNotify) {
	case dliNotePreLoadLibrary:
		if (strcmp(pdli->szDll, "duckdb.dll") != 0) {
			return NULL;
		}
		return (FARPROC)GetModuleHandle(NULL);
	default:
		return NULL;
	}

	return NULL;
}

ExternC const PfnDliHook __pfnDliNotifyHook2 = duckdb_dllimport_delay_hook;
ExternC const PfnDliHook __pfnDliFailureHook2 = duckdb_dllimport_delay_hook;
}
#endif
#endif
//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// duckdb.h
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//



#ifdef _WIN32
#ifdef DUCKDB_BUILD_LIBRARY
#define DUCKDB_API __declspec(dllexport)
#else
#define DUCKDB_API __declspec(dllimport)
#endif
#else
#define DUCKDB_API
#endif

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef uint64_t idx_t;

typedef enum DUCKDB_TYPE {
	DUCKDB_TYPE_INVALID = 0,
	// bool
	DUCKDB_TYPE_BOOLEAN,
	// int8_t
	DUCKDB_TYPE_TINYINT,
	// int16_t
	DUCKDB_TYPE_SMALLINT,
	// int32_t
	DUCKDB_TYPE_INTEGER,
	// int64_t
	DUCKDB_TYPE_BIGINT,
	// float
	DUCKDB_TYPE_FLOAT,
	// double
	DUCKDB_TYPE_DOUBLE,
	// duckdb_timestamp (us)
	DUCKDB_TYPE_TIMESTAMP,
	// duckdb_timestamp (s)
	DUCKDB_TYPE_TIMESTAMP_S,
	// duckdb_timestamp (ns)
	DUCKDB_TYPE_TIMESTAMP_NS,
	// duckdb_timestamp (ms)
	DUCKDB_TYPE_TIMESTAMP_MS,
	// duckdb_date
	DUCKDB_TYPE_DATE,
	// duckdb_time
	DUCKDB_TYPE_TIME,
	// duckdb_interval
	DUCKDB_TYPE_INTERVAL,
	// duckdb_hugeint
	DUCKDB_TYPE_HUGEINT,
	// const char*
	DUCKDB_TYPE_VARCHAR,
	// duckdb_blob
	DUCKDB_TYPE_BLOB
} duckdb_type;

typedef struct {
	int32_t year;
	int8_t month;
	int8_t day;
} duckdb_date;

typedef struct {
	int8_t hour;
	int8_t min;
	int8_t sec;
	int16_t micros;
} duckdb_time;

typedef struct {
	duckdb_date date;
	duckdb_time time;
} duckdb_timestamp;

typedef struct {
	int32_t months;
	int32_t days;
	int64_t micros;
} duckdb_interval;

typedef struct {
	uint64_t lower;
	int64_t upper;
} duckdb_hugeint;

typedef struct {
	void *data;
	idx_t size;
} duckdb_blob;

typedef struct {
	void *data;
	bool *nullmask;
	duckdb_type type;
	char *name;
} duckdb_column;

typedef struct {
	idx_t column_count;
	idx_t row_count;
	idx_t rows_changed;
	duckdb_column *columns;
	char *error_message;
} duckdb_result;

// typedef struct {
// 	void *data;
// 	bool *nullmask;
// } duckdb_column_data;

// typedef struct {
// 	int column_count;
// 	int count;
// 	duckdb_column_data *columns;
// } duckdb_chunk;

typedef void *duckdb_database;
typedef void *duckdb_connection;
typedef void *duckdb_prepared_statement;
typedef void *duckdb_appender;

typedef enum { DuckDBSuccess = 0, DuckDBError = 1 } duckdb_state;

//! Opens a database file at the given path (nullptr for in-memory). Returns DuckDBSuccess on success, or DuckDBError on
//! failure. [OUT: database]
DUCKDB_API duckdb_state duckdb_open(const char *path, duckdb_database *out_database);
//! Closes the database.
DUCKDB_API void duckdb_close(duckdb_database *database);

//! Creates a connection to the specified database. [OUT: connection]
DUCKDB_API duckdb_state duckdb_connect(duckdb_database database, duckdb_connection *out_connection);
//! Closes the specified connection handle
DUCKDB_API void duckdb_disconnect(duckdb_connection *connection);

//! Executes the specified SQL query in the specified connection handle. [OUT: result descriptor]
DUCKDB_API duckdb_state duckdb_query(duckdb_connection connection, const char *query, duckdb_result *out_result);
//! Destroys the specified result
DUCKDB_API void duckdb_destroy_result(duckdb_result *result);

//! Returns the column name of the specified column. The result does not need to be freed;
//! the column names will automatically be destroyed when the result is destroyed.
DUCKDB_API const char *duckdb_column_name(duckdb_result *result, idx_t col);

// SAFE fetch functions
// These functions will perform conversions if necessary. On failure (e.g. if conversion cannot be performed) a special
// value is returned.

//! Converts the specified value to a bool. Returns false on failure or NULL.
DUCKDB_API bool duckdb_value_boolean(duckdb_result *result, idx_t col, idx_t row);
//! Converts the specified value to an int8_t. Returns 0 on failure or NULL.
DUCKDB_API int8_t duckdb_value_int8(duckdb_result *result, idx_t col, idx_t row);
//! Converts the specified value to an int16_t. Returns 0 on failure or NULL.
DUCKDB_API int16_t duckdb_value_int16(duckdb_result *result, idx_t col, idx_t row);
//! Converts the specified value to an int64_t. Returns 0 on failure or NULL.
DUCKDB_API int32_t duckdb_value_int32(duckdb_result *result, idx_t col, idx_t row);
//! Converts the specified value to an int64_t. Returns 0 on failure or NULL.
DUCKDB_API int64_t duckdb_value_int64(duckdb_result *result, idx_t col, idx_t row);
//! Converts the specified value to an uint8_t. Returns 0 on failure or NULL.
DUCKDB_API uint8_t duckdb_value_uint8(duckdb_result *result, idx_t col, idx_t row);
//! Converts the specified value to an uint16_t. Returns 0 on failure or NULL.
DUCKDB_API uint16_t duckdb_value_uint16(duckdb_result *result, idx_t col, idx_t row);
//! Converts the specified value to an uint64_t. Returns 0 on failure or NULL.
DUCKDB_API uint32_t duckdb_value_uint32(duckdb_result *result, idx_t col, idx_t row);
//! Converts the specified value to an uint64_t. Returns 0 on failure or NULL.
DUCKDB_API uint64_t duckdb_value_uint64(duckdb_result *result, idx_t col, idx_t row);
//! Converts the specified value to a float. Returns 0.0 on failure or NULL.
DUCKDB_API float duckdb_value_float(duckdb_result *result, idx_t col, idx_t row);
//! Converts the specified value to a double. Returns 0.0 on failure or NULL.
DUCKDB_API double duckdb_value_double(duckdb_result *result, idx_t col, idx_t row);
//! Converts the specified value to a string. Returns nullptr on failure or NULL. The result must be freed with
//! duckdb_free.
DUCKDB_API char *duckdb_value_varchar(duckdb_result *result, idx_t col, idx_t row);
//! Fetches a blob from a result set column. Returns a blob with blob.data set to nullptr on failure or NULL. The
//! resulting "blob.data" must be freed with duckdb_free.
DUCKDB_API duckdb_blob duckdb_value_blob(duckdb_result *result, idx_t col, idx_t row);

//! Allocate [size] amounts of memory using the duckdb internal malloc function. Any memory allocated in this manner
//! should be freed using duckdb_free
DUCKDB_API void *duckdb_malloc(size_t size);
//! Free a value returned from duckdb_malloc, duckdb_value_varchar or duckdb_value_blob
DUCKDB_API void duckdb_free(void *ptr);

// Prepared Statements

//! prepares the specified SQL query in the specified connection handle. [OUT: prepared statement descriptor]
DUCKDB_API duckdb_state duckdb_prepare(duckdb_connection connection, const char *query,
                                       duckdb_prepared_statement *out_prepared_statement);

DUCKDB_API duckdb_state duckdb_nparams(duckdb_prepared_statement prepared_statement, idx_t *nparams_out);

//! binds parameters to prepared statement
DUCKDB_API duckdb_state duckdb_bind_boolean(duckdb_prepared_statement prepared_statement, idx_t param_idx, bool val);
DUCKDB_API duckdb_state duckdb_bind_int8(duckdb_prepared_statement prepared_statement, idx_t param_idx, int8_t val);
DUCKDB_API duckdb_state duckdb_bind_int16(duckdb_prepared_statement prepared_statement, idx_t param_idx, int16_t val);
DUCKDB_API duckdb_state duckdb_bind_int32(duckdb_prepared_statement prepared_statement, idx_t param_idx, int32_t val);
DUCKDB_API duckdb_state duckdb_bind_int64(duckdb_prepared_statement prepared_statement, idx_t param_idx, int64_t val);
DUCKDB_API duckdb_state duckdb_bind_uint8(duckdb_prepared_statement prepared_statement, idx_t param_idx, int8_t val);
DUCKDB_API duckdb_state duckdb_bind_uint16(duckdb_prepared_statement prepared_statement, idx_t param_idx, int16_t val);
DUCKDB_API duckdb_state duckdb_bind_uint32(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint32_t val);
DUCKDB_API duckdb_state duckdb_bind_uint64(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint64_t val);
DUCKDB_API duckdb_state duckdb_bind_float(duckdb_prepared_statement prepared_statement, idx_t param_idx, float val);
DUCKDB_API duckdb_state duckdb_bind_double(duckdb_prepared_statement prepared_statement, idx_t param_idx, double val);
DUCKDB_API duckdb_state duckdb_bind_varchar(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                            const char *val);
DUCKDB_API duckdb_state duckdb_bind_varchar_length(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                                   const char *val, idx_t length);
DUCKDB_API duckdb_state duckdb_bind_blob(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                         const void *data, idx_t length);
DUCKDB_API duckdb_state duckdb_bind_null(duckdb_prepared_statement prepared_statement, idx_t param_idx);

//! Executes the prepared statements with currently bound parameters
DUCKDB_API duckdb_state duckdb_execute_prepared(duckdb_prepared_statement prepared_statement,
                                                duckdb_result *out_result);

//! Destroys the specified prepared statement descriptor
DUCKDB_API void duckdb_destroy_prepare(duckdb_prepared_statement *prepared_statement);

DUCKDB_API duckdb_state duckdb_appender_create(duckdb_connection connection, const char *schema, const char *table,
                                               duckdb_appender *out_appender);

DUCKDB_API duckdb_state duckdb_appender_begin_row(duckdb_appender appender);
DUCKDB_API duckdb_state duckdb_appender_end_row(duckdb_appender appender);

DUCKDB_API duckdb_state duckdb_append_bool(duckdb_appender appender, bool value);

DUCKDB_API duckdb_state duckdb_append_int8(duckdb_appender appender, int8_t value);
DUCKDB_API duckdb_state duckdb_append_int16(duckdb_appender appender, int16_t value);
DUCKDB_API duckdb_state duckdb_append_int32(duckdb_appender appender, int32_t value);
DUCKDB_API duckdb_state duckdb_append_int64(duckdb_appender appender, int64_t value);

DUCKDB_API duckdb_state duckdb_append_uint8(duckdb_appender appender, uint8_t value);
DUCKDB_API duckdb_state duckdb_append_uint16(duckdb_appender appender, uint16_t value);
DUCKDB_API duckdb_state duckdb_append_uint32(duckdb_appender appender, uint32_t value);
DUCKDB_API duckdb_state duckdb_append_uint64(duckdb_appender appender, uint64_t value);

DUCKDB_API duckdb_state duckdb_append_float(duckdb_appender appender, float value);
DUCKDB_API duckdb_state duckdb_append_double(duckdb_appender appender, double value);

DUCKDB_API duckdb_state duckdb_append_varchar(duckdb_appender appender, const char *val);
DUCKDB_API duckdb_state duckdb_append_varchar_length(duckdb_appender appender, const char *val, idx_t length);
DUCKDB_API duckdb_state duckdb_append_blob(duckdb_appender appender, const void *data, idx_t length);
DUCKDB_API duckdb_state duckdb_append_null(duckdb_appender appender);

DUCKDB_API duckdb_state duckdb_appender_flush(duckdb_appender appender);
DUCKDB_API duckdb_state duckdb_appender_close(duckdb_appender appender);

DUCKDB_API duckdb_state duckdb_appender_destroy(duckdb_appender *appender);

#ifdef __cplusplus
}
#endif
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/date.hpp
//
//
//===----------------------------------------------------------------------===//







namespace duckdb {

//! The Date class is a static class that holds helper functions for the Date
//! type.
class Date {
public:
	static const string_t MONTH_NAMES[12];
	static const string_t MONTH_NAMES_ABBREVIATED[12];
	static const string_t DAY_NAMES[7];
	static const string_t DAY_NAMES_ABBREVIATED[7];
	static const int32_t NORMAL_DAYS[13];
	static const int32_t CUMULATIVE_DAYS[13];
	static const int32_t LEAP_DAYS[13];
	static const int32_t CUMULATIVE_LEAP_DAYS[13];
	static const int32_t CUMULATIVE_YEAR_DAYS[401];
	static const int8_t MONTH_PER_DAY_OF_YEAR[365];
	static const int8_t LEAP_MONTH_PER_DAY_OF_YEAR[366];

	constexpr static const int32_t MIN_YEAR = -290307;
	constexpr static const int32_t MAX_YEAR = 294247;
	constexpr static const int32_t EPOCH_YEAR = 1970;

	constexpr static const int32_t YEAR_INTERVAL = 400;
	constexpr static const int32_t DAYS_PER_YEAR_INTERVAL = 146097;

public:
	//! Convert a string in the format "YYYY-MM-DD" to a date object
	static date_t FromString(const string &str, bool strict = false);
	//! Convert a string in the format "YYYY-MM-DD" to a date object
	static date_t FromCString(const char *str, idx_t len, bool strict = false);
	//! Convert a date object to a string in the format "YYYY-MM-DD"
	static string ToString(date_t date);
	//! Try to convert text in a buffer to a date; returns true if parsing was successful
	static bool TryConvertDate(const char *buf, idx_t len, idx_t &pos, date_t &result, bool strict = false);

	//! Create a string "YYYY-MM-DD" from a specified (year, month, day)
	//! combination
	static string Format(int32_t year, int32_t month, int32_t day);

	//! Extract the year, month and day from a given date object
	static void Convert(date_t date, int32_t &out_year, int32_t &out_month, int32_t &out_day);
	//! Create a Date object from a specified (year, month, day) combination
	static date_t FromDate(int32_t year, int32_t month, int32_t day);

	//! Returns true if (year) is a leap year, and false otherwise
	static bool IsLeapYear(int32_t year);

	//! Returns true if the specified (year, month, day) combination is a valid
	//! date
	static bool IsValid(int32_t year, int32_t month, int32_t day);

	//! Extract the epoch from the date (seconds since 1970-01-01)
	static int64_t Epoch(date_t date);
	//! Extract the epoch from the date (nanoseconds since 1970-01-01)
	static int64_t EpochNanoseconds(date_t date);
	//! Convert the epoch (seconds since 1970-01-01) to a date_t
	static date_t EpochToDate(int64_t epoch);

	//! Extract the number of days since epoch (days since 1970-01-01)
	static int32_t EpochDays(date_t date);
	//! Convert the epoch number of days to a date_t
	static date_t EpochDaysToDate(int32_t epoch);

	//! Extract year of a date entry
	static int32_t ExtractYear(date_t date);
	//! Extract year of a date entry, but optimized to first try the last year found
	static int32_t ExtractYear(date_t date, int32_t *last_year);
	static int32_t ExtractYear(timestamp_t ts, int32_t *last_year);
	//! Extract month of a date entry
	static int32_t ExtractMonth(date_t date);
	//! Extract day of a date entry
	static int32_t ExtractDay(date_t date);
	//! Extract the day of the week (1-7)
	static int32_t ExtractISODayOfTheWeek(date_t date);
	//! Extract the day of the year
	static int32_t ExtractDayOfTheYear(date_t date);
	//! Extract the ISO week number
	//! ISO weeks start on Monday and the first week of a year
	//! contains January 4 of that year.
	//! In the ISO week-numbering system, it is possible for early-January dates
	//! to be part of the 52nd or 53rd week of the previous year.
	static int32_t ExtractISOWeekNumber(date_t date);
	//! Extract the week number as Python handles it.
	//! Either Monday or Sunday is the first day of the week,
	//! and any date before the first Monday/Sunday returns week 0
	//! This is a bit more consistent because week numbers in a year are always incrementing
	static int32_t ExtractWeekNumberRegular(date_t date, bool monday_first = true);
	//! Returns the date of the monday of the current week.
	static date_t GetMondayOfCurrentWeek(date_t date);

	//! Helper function to parse two digits from a string (e.g. "30" -> 30, "03" -> 3, "3" -> 3)
	static bool ParseDoubleDigit(const char *buf, idx_t len, idx_t &pos, int32_t &result);

private:
	static void ExtractYearOffset(int32_t &n, int32_t &year, int32_t &year_offset);
};
} // namespace duckdb
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/arrow.hpp
//
//
//===----------------------------------------------------------------------===//

#ifndef ARROW_FLAG_DICTIONARY_ORDERED

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE           2
#define ARROW_FLAG_MAP_KEYS_SORTED    4

struct ArrowSchema {
	// Array type description
	const char *format;
	const char *name;
	const char *metadata;
	int64_t flags;
	int64_t n_children;
	struct ArrowSchema **children;
	struct ArrowSchema *dictionary;

	// Release callback
	void (*release)(struct ArrowSchema *);
	// Opaque producer-specific data
	void *private_data;
};

struct ArrowArray {
	// Array data description
	int64_t length;
	int64_t null_count;
	int64_t offset;
	int64_t n_buffers;
	int64_t n_children;
	const void **buffers;
	struct ArrowArray **children;
	struct ArrowArray *dictionary;

	// Release callback
	void (*release)(struct ArrowArray *);
	// Opaque producer-specific data
	void *private_data;
};

// EXPERIMENTAL
struct ArrowArrayStream {
	// Callback to get the stream type
	// (will be the same for all arrays in the stream).
	// Return value: 0 if successful, an `errno`-compatible error code otherwise.
	int (*get_schema)(struct ArrowArrayStream *, struct ArrowSchema *out);
	// Callback to get the next array
	// (if no error and the array is released, the stream has ended)
	// Return value: 0 if successful, an `errno`-compatible error code otherwise.
	int (*get_next)(struct ArrowArrayStream *, struct ArrowArray *out);

	// Callback to get optional detailed error information.
	// This must only be called if the last stream operation failed
	// with a non-0 return code.  The returned pointer is only valid until
	// the next operation on this stream (including release).
	// If unavailable, NULL is returned.
	const char *(*get_last_error)(struct ArrowArrayStream *);

	// Release callback: release the stream's own resources.
	// Note that arrays returned by `get_next` must be individually released.
	void (*release)(struct ArrowArrayStream *);
	// Opaque producer-specific data
	void *private_data;
};

#ifdef __cplusplus
}
#endif

#endif
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/blob.hpp
//
//
//===----------------------------------------------------------------------===//






namespace duckdb {

//! The Blob class is a static class that holds helper functions for the Blob type.
class Blob {
public:
	// map of integer -> hex value
	static constexpr const char *HEX_TABLE = "0123456789ABCDEF";
	// reverse map of byte -> integer value, or -1 for invalid hex values
	static const int HEX_MAP[256];
	//! map of index -> base64 character
	static constexpr const char *BASE64_MAP = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
	//! padding character used in base64 encoding
	static constexpr const char BASE64_PADDING = '=';

public:
	//! Returns the string size of a blob -> string conversion
	static idx_t GetStringSize(string_t blob);
	//! Converts a blob to a string, writing the output to the designated output string.
	//! The string needs to have space for at least GetStringSize(blob) bytes.
	static void ToString(string_t blob, char *output);
	//! Convert a blob object to a string
	static string ToString(string_t blob);

	//! Returns the blob size of a string -> blob conversion
	static idx_t GetBlobSize(string_t str);
	//! Convert a string to a blob. This function should ONLY be called after calling GetBlobSize, since it does NOT
	//! perform data validation.
	static void ToBlob(string_t str, data_ptr_t output);
	//! Convert a string object to a blob
	static string ToBlob(string_t str);

	// base 64 conversion functions
	//! Returns the string size of a blob -> base64 conversion
	static idx_t ToBase64Size(string_t blob);
	//! Converts a blob to a base64 string, output should have space for at least ToBase64Size(blob) bytes
	static void ToBase64(string_t blob, char *output);

	//! Returns the string size of a base64 string -> blob conversion
	static idx_t FromBase64Size(string_t str);
	//! Converts a base64 string to a blob, output should have space for at least FromBase64Size(blob) bytes
	static void FromBase64(string_t str, data_ptr_t output, idx_t output_size);
};
} // namespace duckdb
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/decimal.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

//! The Decimal class is a static class that holds helper functions for the Decimal type
class Decimal {
public:
	static constexpr uint8_t MAX_WIDTH_INT16 = 4;
	static constexpr uint8_t MAX_WIDTH_INT32 = 9;
	static constexpr uint8_t MAX_WIDTH_INT64 = 18;
	static constexpr uint8_t MAX_WIDTH_INT128 = 38;
	static constexpr uint8_t MAX_WIDTH_DECIMAL = MAX_WIDTH_INT128;

public:
	static string ToString(int16_t value, uint8_t scale);
	static string ToString(int32_t value, uint8_t scale);
	static string ToString(int64_t value, uint8_t scale);
	static string ToString(hugeint_t value, uint8_t scale);
};
} // namespace duckdb
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/timestamp.hpp
//
//
//===----------------------------------------------------------------------===//






namespace duckdb {

struct timestamp_struct {
	int32_t year;
	int8_t month;
	int8_t day;
	int8_t hour;
	int8_t min;
	int8_t sec;
	int16_t msec;
};
//! The Timestamp class is a static class that holds helper functions for the Timestamp
//! type.
class Timestamp {
public:
	//! Convert a string in the format "YYYY-MM-DD hh:mm:ss" to a timestamp object
	static timestamp_t FromString(const string &str);
	static timestamp_t FromCString(const char *str, idx_t len);
	//! Convert a date object to a string in the format "YYYY-MM-DD hh:mm:ss"
	static string ToString(timestamp_t timestamp);

	static date_t GetDate(timestamp_t timestamp);

	static dtime_t GetTime(timestamp_t timestamp);
	//! Create a Timestamp object from a specified (date, time) combination
	static timestamp_t FromDatetime(date_t date, dtime_t time);
	//! Extract the date and time from a given timestamp object
	static void Convert(timestamp_t date, date_t &out_date, dtime_t &out_time);
	//! Returns current timestamp
	static timestamp_t GetCurrentTimestamp();

	//! Convert the epoch (in sec) to a timestamp
	static timestamp_t FromEpochSeconds(int64_t ms);
	//! Convert the epoch (in ms) to a timestamp
	static timestamp_t FromEpochMs(int64_t ms);
	//! Convert the epoch (in microseconds) to a timestamp
	static timestamp_t FromEpochMicroSeconds(int64_t micros);
	//! Convert the epoch (in nanoseconds) to a timestamp
	static timestamp_t FromEpochNanoSeconds(int64_t micros);

	//! Convert the epoch (in seconds) to a timestamp
	static int64_t GetEpochSeconds(timestamp_t timestamp);
	//! Convert the epoch (in ms) to a timestamp
	static int64_t GetEpochMs(timestamp_t timestamp);
	//! Convert a timestamp to epoch (in microseconds)
	static int64_t GetEpochMicroSeconds(timestamp_t timestamp);
	//! Convert a timestamp to epoch (in nanoseconds)
	static int64_t GetEpochNanoSeconds(timestamp_t timestamp);

	static bool TryParseUTCOffset(const char *str, idx_t &pos, idx_t len, int &hour_offset, int &minute_offset);
};
} // namespace duckdb
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/time.hpp
//
//
//===----------------------------------------------------------------------===//






namespace duckdb {

//! The Time class is a static class that holds helper functions for the Time
//! type.
class Time {
public:
	//! Convert a string in the format "hh:mm:ss" to a time object
	static dtime_t FromString(const string &str, bool strict = false);
	static dtime_t FromCString(const char *buf, idx_t len, bool strict = false);
	static bool TryConvertTime(const char *buf, idx_t len, idx_t &pos, dtime_t &result, bool strict = false);

	//! Convert a time object to a string in the format "hh:mm:ss"
	static string ToString(dtime_t time);

	static string Format(int32_t hour, int32_t minute, int32_t second, int32_t microseconds = 0);

	static dtime_t FromTime(int32_t hour, int32_t minute, int32_t second, int32_t microseconds = 0);

	static bool IsValidTime(int32_t hour, int32_t minute, int32_t second, int32_t microseconds = 0);
	//! Extract the time from a given timestamp object
	static void Convert(dtime_t time, int32_t &out_hour, int32_t &out_min, int32_t &out_sec, int32_t &out_micros);
};

} // namespace duckdb
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer/buffered_serializer.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

#define SERIALIZER_DEFAULT_SIZE 1024

struct BinaryData {
	unique_ptr<data_t[]> data;
	idx_t size;
};

class BufferedSerializer : public Serializer {
public:
	//! Serializes to a buffer allocated by the serializer, will expand when
	//! writing past the initial threshold
	explicit BufferedSerializer(idx_t maximum_size = SERIALIZER_DEFAULT_SIZE);
	//! Serializes to a provided (owned) data pointer
	BufferedSerializer(unique_ptr<data_t[]> data, idx_t size);
	BufferedSerializer(data_ptr_t data, idx_t size);

	idx_t maximum_size;
	data_ptr_t data;

	BinaryData blob;

public:
	void WriteData(const_data_ptr_t buffer, uint64_t write_size) override;

	//! Retrieves the data after the writing has been completed
	BinaryData GetData() {
		return std::move(blob);
	}

	void Reset() {
		blob.size = 0;
	}
};

} // namespace duckdb
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/appender.hpp
//
//
//===----------------------------------------------------------------------===//







namespace duckdb {

class ClientContext;
class DuckDB;
class TableCatalogEntry;
class Connection;

//! The Appender class can be used to append elements to a table.
class Appender {
	//! A reference to a database connection that created this appender
	shared_ptr<ClientContext> context;
	//! The table description (including column names)
	unique_ptr<TableDescription> description;
	//! Internal chunk used for appends
	DataChunk chunk;
	//! The current column to append to
	idx_t column = 0;

public:
	DUCKDB_API Appender(Connection &con, const string &schema_name, const string &table_name);
	DUCKDB_API Appender(Connection &con, const string &table_name);
	DUCKDB_API ~Appender();

	//! Begins a new row append, after calling this the other AppendX() functions
	//! should be called the correct amount of times. After that,
	//! EndRow() should be called.
	DUCKDB_API void BeginRow();
	//! Finishes appending the current row.
	DUCKDB_API void EndRow();

	// Append functions
	template <class T>
	void Append(T value) {
		throw Exception("Undefined type for Appender::Append!");
	}

	DUCKDB_API void Append(const char *value, uint32_t length);

	// prepared statements
	template <typename... Args>
	void AppendRow(Args... args) {
		BeginRow();
		AppendRowRecursive(args...);
	}

	//! Commit the changes made by the appender.
	DUCKDB_API void Flush();
	//! Flush the changes made by the appender and close it. The appender cannot be used after this point
	DUCKDB_API void Close();

	//! Obtain a reference to the internal vector that is used to append to the table
	DUCKDB_API DataChunk &GetAppendChunk() {
		return chunk;
	}

	DUCKDB_API idx_t CurrentColumn() {
		return column;
	}

private:
	template <class T>
	void AppendValueInternal(T value);
	template <class SRC, class DST>
	void AppendValueInternal(Vector &vector, SRC input);

	void AppendRowRecursive() {
		EndRow();
	}

	template <typename T, typename... Args>
	void AppendRowRecursive(T value, Args... args) {
		Append<T>(value);
		AppendRowRecursive(args...);
	}

	void AppendValue(const Value &value);
};

template <>
void DUCKDB_API Appender::Append(bool value);
template <>
void DUCKDB_API Appender::Append(int8_t value);
template <>
void DUCKDB_API Appender::Append(int16_t value);
template <>
void DUCKDB_API Appender::Append(int32_t value);
template <>
void DUCKDB_API Appender::Append(int64_t value);
template <>
void DUCKDB_API Appender::Append(uint8_t value);
template <>
void DUCKDB_API Appender::Append(uint16_t value);
template <>
void DUCKDB_API Appender::Append(uint32_t value);
template <>
void DUCKDB_API Appender::Append(uint64_t value);
template <>
void DUCKDB_API Appender::Append(float value);
template <>
void DUCKDB_API Appender::Append(double value);
template <>
void DUCKDB_API Appender::Append(date_t value);
template <>
void DUCKDB_API Appender::Append(dtime_t value);
template <>
void DUCKDB_API Appender::Append(timestamp_t value);
template <>
void DUCKDB_API Appender::Append(const char *value);
template <>
void DUCKDB_API Appender::Append(Value value);
template <>
void DUCKDB_API Appender::Append(std::nullptr_t value);

} // namespace duckdb
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/client_context.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/schema_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//




//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/catalog_type.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

//===--------------------------------------------------------------------===//
// Catalog Types
//===--------------------------------------------------------------------===//
enum class CatalogType : uint8_t {
	INVALID = 0,
	TABLE_ENTRY = 1,
	SCHEMA_ENTRY = 2,
	VIEW_ENTRY = 3,
	INDEX_ENTRY = 4,
	PREPARED_STATEMENT = 5,
	SEQUENCE_ENTRY = 6,
	COLLATION_ENTRY = 7,

	// functions
	TABLE_FUNCTION_ENTRY = 25,
	SCALAR_FUNCTION_ENTRY = 26,
	AGGREGATE_FUNCTION_ENTRY = 27,
	PRAGMA_FUNCTION_ENTRY = 28,
	COPY_FUNCTION_ENTRY = 29,
	MACRO_ENTRY = 30,

	// version info
	UPDATED_ENTRY = 50,
	DELETED_ENTRY = 51,
};

string CatalogTypeToString(CatalogType type);

} // namespace duckdb


//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/atomic.hpp
//
//
//===----------------------------------------------------------------------===//



#include <atomic>

namespace duckdb {
using std::atomic;
}


#include <memory>

namespace duckdb {
struct AlterInfo;
class Catalog;
class CatalogSet;
class ClientContext;

//! Abstract base class of an entry in the catalog
class CatalogEntry {
public:
	CatalogEntry(CatalogType type, Catalog *catalog, string name);
	virtual ~CatalogEntry();

	virtual unique_ptr<CatalogEntry> AlterEntry(ClientContext &context, AlterInfo *info) {
		throw CatalogException("Unsupported alter type for catalog entry!");
	}

	virtual unique_ptr<CatalogEntry> Copy(ClientContext &context) {
		throw CatalogException("Unsupported copy type for catalog entry!");
	}
	//! Sets the CatalogEntry as the new root entry (i.e. the newest entry) - this is called on a rollback to an
	//! AlterEntry
	virtual void SetAsRoot() {
	}
	//! Convert the catalog entry to a SQL string that can be used to re-construct the catalog entry
	virtual string ToSQL() {
		throw CatalogException("Unsupported catalog type for ToSQL()");
	}

	//! The oid of the entry
	idx_t oid;
	//! The type of this catalog entry
	CatalogType type;
	//! Reference to the catalog this entry belongs to
	Catalog *catalog;
	//! Reference to the catalog set this entry is stored in
	CatalogSet *set;
	//! The name of the entry
	string name;
	//! Whether or not the object is deleted
	bool deleted;
	//! Whether or not the object is temporary and should not be added to the WAL
	bool temporary;
	//! Whether or not the entry is an internal entry (cannot be deleted, not dumped, etc)
	bool internal;
	//! Timestamp at which the catalog entry was created
	atomic<transaction_t> timestamp;
	//! Child entry
	unique_ptr<CatalogEntry> child;
	//! Parent entry (the node that owns this node)
	CatalogEntry *parent;
};
} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_set.hpp
//
//
//===----------------------------------------------------------------------===//




//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/default/default_generator.hpp
//
//
//===----------------------------------------------------------------------===//






namespace duckdb {
class ClientContext;

class DefaultGenerator {
public:
	explicit DefaultGenerator(Catalog &catalog) : catalog(catalog), created_all_entries(false) {
	}
	virtual ~DefaultGenerator() {
	}

	Catalog &catalog;
	atomic<bool> created_all_entries;

public:
	//! Creates a default entry with the specified name, or returns nullptr if no such entry can be generated
	virtual unique_ptr<CatalogEntry> CreateDefaultEntry(ClientContext &context, const string &entry_name) = 0;
	//! Get a list of all default entries in the generator
	virtual vector<string> GetDefaultEntries() = 0;
};

} // namespace duckdb






#include <functional>
#include <memory>

namespace duckdb {
struct AlterInfo;

class ClientContext;

typedef unordered_map<CatalogSet *, unique_lock<mutex>> set_lock_map_t;

struct MappingValue {
	explicit MappingValue(idx_t index_) : index(index_), timestamp(0), deleted(false), parent(nullptr) {
	}

	idx_t index;
	transaction_t timestamp;
	bool deleted;
	unique_ptr<MappingValue> child;
	MappingValue *parent;
};

//! The Catalog Set stores (key, value) map of a set of CatalogEntries
class CatalogSet {
	friend class DependencyManager;

public:
	explicit CatalogSet(Catalog &catalog, unique_ptr<DefaultGenerator> defaults = nullptr);

	//! Create an entry in the catalog set. Returns whether or not it was
	//! successful.
	bool CreateEntry(ClientContext &context, const string &name, unique_ptr<CatalogEntry> value,
	                 unordered_set<CatalogEntry *> &dependencies);

	bool AlterEntry(ClientContext &context, const string &name, AlterInfo *alter_info);

	bool DropEntry(ClientContext &context, const string &name, bool cascade);

	void CleanupEntry(CatalogEntry *catalog_entry);

	//! Returns the entry with the specified name
	CatalogEntry *GetEntry(ClientContext &context, const string &name);

	//! Gets the entry that is most similar to the given name (i.e. smallest levenshtein distance), or empty string if
	//! none is found
	string SimilarEntry(ClientContext &context, const string &name);

	//! Rollback <entry> to be the currently valid entry for a certain catalog
	//! entry
	void Undo(CatalogEntry *entry);

	//! Scan the catalog set, invoking the callback method for every committed entry
	void Scan(const std::function<void(CatalogEntry *)> &callback);
	//! Scan the catalog set, invoking the callback method for every entry
	void Scan(ClientContext &context, const std::function<void(CatalogEntry *)> &callback);

	template <class T>
	vector<T *> GetEntries(ClientContext &context) {
		vector<T *> result;
		Scan(context, [&](CatalogEntry *entry) { result.push_back((T *)entry); });
		return result;
	}

	static bool HasConflict(ClientContext &context, transaction_t timestamp);
	static bool UseTimestamp(ClientContext &context, transaction_t timestamp);

	idx_t GetEntryIndex(CatalogEntry *entry);
	CatalogEntry *GetEntryFromIndex(idx_t index);
	void UpdateTimestamp(CatalogEntry *entry, transaction_t timestamp);

	//! Returns the root entry with the specified name regardless of transaction (or nullptr if there are none)
	CatalogEntry *GetRootEntry(const string &name);

private:
	//! Given a root entry, gets the entry valid for this transaction
	CatalogEntry *GetEntryForTransaction(ClientContext &context, CatalogEntry *current);
	CatalogEntry *GetCommittedEntry(CatalogEntry *current);
	bool GetEntryInternal(ClientContext &context, const string &name, idx_t &entry_index, CatalogEntry *&entry);
	bool GetEntryInternal(ClientContext &context, idx_t entry_index, CatalogEntry *&entry);
	//! Drops an entry from the catalog set; must hold the catalog_lock to safely call this
	void DropEntryInternal(ClientContext &context, idx_t entry_index, CatalogEntry &entry, bool cascade,
	                       set_lock_map_t &lock_set);
	CatalogEntry *CreateEntryInternal(ClientContext &context, unique_ptr<CatalogEntry> entry);
	MappingValue *GetMapping(ClientContext &context, const string &name, bool allow_lowercase_alias,
	                         bool get_latest = false);
	void PutMapping(ClientContext &context, const string &name, idx_t entry_index);
	void DeleteMapping(ClientContext &context, const string &name);

private:
	Catalog &catalog;
	//! The catalog lock is used to make changes to the data
	mutex catalog_lock;
	//! Mapping of string to catalog entry
	unordered_map<string, unique_ptr<MappingValue>> mapping;
	//! The set of catalog entries
	unordered_map<idx_t, unique_ptr<CatalogEntry>> entries;
	//! The current catalog entry index
	idx_t current_entry = 0;
	//! The generator used to generate default internal entries
	unique_ptr<DefaultGenerator> defaults;
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/query_error_context.hpp
//
//
//===----------------------------------------------------------------------===//







namespace duckdb {
class SQLStatement;

class QueryErrorContext {
public:
	explicit QueryErrorContext(SQLStatement *statement_ = nullptr, idx_t query_location_ = INVALID_INDEX)
	    : statement(statement_), query_location(query_location_) {
	}

	//! The query statement
	SQLStatement *statement;
	//! The location in which the error should be thrown
	idx_t query_location;

public:
	static string Format(const string &query, const string &error_message, int error_location);

	string FormatErrorRecursive(const string &msg, vector<ExceptionFormatValue> &values);
	template <class T, typename... Args>
	string FormatErrorRecursive(const string &msg, vector<ExceptionFormatValue> &values, T param, Args... params) {
		values.push_back(ExceptionFormatValue::CreateFormatValue<T>(param));
		return FormatErrorRecursive(msg, values, params...);
	}

	template <typename... Args>
	string FormatError(const string &msg, Args... params) {
		vector<ExceptionFormatValue> values;
		return FormatErrorRecursive(msg, values, params...);
	}
};

} // namespace duckdb


namespace duckdb {
class ClientContext;

class StandardEntry;
class TableCatalogEntry;
class TableFunctionCatalogEntry;
class SequenceCatalogEntry;
class Serializer;
class Deserializer;

enum class OnCreateConflict : uint8_t;

struct AlterTableInfo;
struct CreateIndexInfo;
struct CreateFunctionInfo;
struct CreateCollationInfo;
struct CreateViewInfo;
struct BoundCreateTableInfo;
struct CreatePragmaFunctionInfo;
struct CreateSequenceInfo;
struct CreateSchemaInfo;
struct CreateTableFunctionInfo;
struct CreateCopyFunctionInfo;

struct DropInfo;

//! A schema in the catalog
class SchemaCatalogEntry : public CatalogEntry {
	friend class Catalog;

public:
	SchemaCatalogEntry(Catalog *catalog, string name, bool is_internal);

private:
	//! The catalog set holding the tables
	CatalogSet tables;
	//! The catalog set holding the indexes
	CatalogSet indexes;
	//! The catalog set holding the table functions
	CatalogSet table_functions;
	//! The catalog set holding the copy functions
	CatalogSet copy_functions;
	//! The catalog set holding the pragma functions
	CatalogSet pragma_functions;
	//! The catalog set holding the scalar and aggregate functions
	CatalogSet functions;
	//! The catalog set holding the sequences
	CatalogSet sequences;
	//! The catalog set holding the collations
	CatalogSet collations;

public:
	//! Gets a catalog entry from the given catalog set matching the given name
	CatalogEntry *GetEntry(ClientContext &context, CatalogType type, const string &name, bool if_exists,
	                       QueryErrorContext error_context = QueryErrorContext());

	//! Scan the specified catalog set, invoking the callback method for every entry
	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry *)> &callback);
	//! Scan the specified catalog set, invoking the callback method for every committed entry
	void Scan(CatalogType type, const std::function<void(CatalogEntry *)> &callback);

	//! Serialize the meta information of the SchemaCatalogEntry a serializer
	virtual void Serialize(Serializer &serializer);
	//! Deserializes to a CreateSchemaInfo
	static unique_ptr<CreateSchemaInfo> Deserialize(Deserializer &source);

	string ToSQL() override;

	//! Creates an index with the given name in the schema
	CatalogEntry *CreateIndex(ClientContext &context, CreateIndexInfo *info, TableCatalogEntry *table);

private:
	//! Create a scalar or aggregate function within the given schema
	CatalogEntry *CreateFunction(ClientContext &context, CreateFunctionInfo *info);
	//! Creates a table with the given name in the schema
	CatalogEntry *CreateTable(ClientContext &context, BoundCreateTableInfo *info);
	//! Creates a view with the given name in the schema
	CatalogEntry *CreateView(ClientContext &context, CreateViewInfo *info);
	//! Creates a sequence with the given name in the schema
	CatalogEntry *CreateSequence(ClientContext &context, CreateSequenceInfo *info);
	//! Create a table function within the given schema
	CatalogEntry *CreateTableFunction(ClientContext &context, CreateTableFunctionInfo *info);
	//! Create a copy function within the given schema
	CatalogEntry *CreateCopyFunction(ClientContext &context, CreateCopyFunctionInfo *info);
	//! Create a pragma function within the given schema
	CatalogEntry *CreatePragmaFunction(ClientContext &context, CreatePragmaFunctionInfo *info);
	//! Create a collation within the given schema
	CatalogEntry *CreateCollation(ClientContext &context, CreateCollationInfo *info);

	//! Drops an entry from the schema
	void DropEntry(ClientContext &context, DropInfo *info);

	//! Alters a catalog entry
	void Alter(ClientContext &context, AlterInfo *info);

	//! Add a catalog entry to this schema
	CatalogEntry *AddEntry(ClientContext &context, unique_ptr<StandardEntry> entry, OnCreateConflict on_conflict);
	//! Add a catalog entry to this schema
	CatalogEntry *AddEntry(ClientContext &context, unique_ptr<StandardEntry> entry, OnCreateConflict on_conflict,
	                       unordered_set<CatalogEntry *> dependencies);

	//! Get the catalog set for the specified type
	CatalogSet &GetCatalogSet(CatalogType type);
};
} // namespace duckdb


//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/deque.hpp
//
//
//===----------------------------------------------------------------------===//



#include <deque>

namespace duckdb {
using std::deque;
}
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/output_type.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

enum class ExplainOutputType : uint8_t { ALL = 0, OPTIMIZED_ONLY = 1, PHYSICAL_ONLY = 2 };

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/pair.hpp
//
//
//===----------------------------------------------------------------------===//



#include <utility>

namespace duckdb {
using std::make_pair;
using std::pair;
} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/progress_bar.hpp
//
//
//===----------------------------------------------------------------------===//



#ifndef DUCKDB_NO_THREADS
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/thread.hpp
//
//
//===----------------------------------------------------------------------===//



#include <thread>

namespace duckdb {
using std::thread;
}

#include <future>
#endif


//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/executor.hpp
//
//
//===----------------------------------------------------------------------===//





//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/pipeline.hpp
//
//
//===----------------------------------------------------------------------===//




//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/physical_sink.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/physical_operator.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog.hpp
//
//
//===----------------------------------------------------------------------===//







#include <functional>


namespace duckdb {
struct CreateSchemaInfo;
struct DropInfo;
struct BoundCreateTableInfo;
struct AlterTableInfo;
struct CreateTableFunctionInfo;
struct CreateCopyFunctionInfo;
struct CreatePragmaFunctionInfo;
struct CreateFunctionInfo;
struct CreateViewInfo;
struct CreateSequenceInfo;
struct CreateCollationInfo;

class ClientContext;
class Transaction;

class AggregateFunctionCatalogEntry;
class CollateCatalogEntry;
class SchemaCatalogEntry;
class TableCatalogEntry;
class ViewCatalogEntry;
class SequenceCatalogEntry;
class TableFunctionCatalogEntry;
class CopyFunctionCatalogEntry;
class PragmaFunctionCatalogEntry;
class CatalogSet;
class DatabaseInstance;
class DependencyManager;

//! The Catalog object represents the catalog of the database.
class Catalog {
public:
	explicit Catalog(DatabaseInstance &db);
	~Catalog();

	//! Reference to the database
	DatabaseInstance &db;
	//! The catalog set holding the schemas
	unique_ptr<CatalogSet> schemas;
	//! The DependencyManager manages dependencies between different catalog objects
	unique_ptr<DependencyManager> dependency_manager;
	//! Write lock for the catalog
	mutex write_lock;

public:
	//! Get the ClientContext from the Catalog
	static Catalog &GetCatalog(ClientContext &context);
	static Catalog &GetCatalog(DatabaseInstance &db);

	DependencyManager &GetDependencyManager() {
		return *dependency_manager;
	}

	//! Returns the current version of the catalog (incremented whenever anything changes, not stored between restarts)
	idx_t GetCatalogVersion();
	//! Trigger a modification in the catalog, increasing the catalog version and returning the previous version
	idx_t ModifyCatalog();

	//! Creates a schema in the catalog.
	CatalogEntry *CreateSchema(ClientContext &context, CreateSchemaInfo *info);
	//! Creates a table in the catalog.
	CatalogEntry *CreateTable(ClientContext &context, BoundCreateTableInfo *info);
	//! Create a table function in the catalog
	CatalogEntry *CreateTableFunction(ClientContext &context, CreateTableFunctionInfo *info);
	//! Create a copy function in the catalog
	CatalogEntry *CreateCopyFunction(ClientContext &context, CreateCopyFunctionInfo *info);
	//! Create a pragma function in the catalog
	CatalogEntry *CreatePragmaFunction(ClientContext &context, CreatePragmaFunctionInfo *info);
	//! Create a scalar or aggregate function in the catalog
	CatalogEntry *CreateFunction(ClientContext &context, CreateFunctionInfo *info);
	//! Creates a table in the catalog.
	CatalogEntry *CreateView(ClientContext &context, CreateViewInfo *info);
	//! Creates a table in the catalog.
	CatalogEntry *CreateSequence(ClientContext &context, CreateSequenceInfo *info);
	//! Creates a collation in the catalog
	CatalogEntry *CreateCollation(ClientContext &context, CreateCollationInfo *info);

	//! Creates a table in the catalog.
	CatalogEntry *CreateTable(ClientContext &context, SchemaCatalogEntry *schema, BoundCreateTableInfo *info);
	//! Create a table function in the catalog
	CatalogEntry *CreateTableFunction(ClientContext &context, SchemaCatalogEntry *schema,
	                                  CreateTableFunctionInfo *info);
	//! Create a copy function in the catalog
	CatalogEntry *CreateCopyFunction(ClientContext &context, SchemaCatalogEntry *schema, CreateCopyFunctionInfo *info);
	//! Create a pragma function in the catalog
	CatalogEntry *CreatePragmaFunction(ClientContext &context, SchemaCatalogEntry *schema,
	                                   CreatePragmaFunctionInfo *info);
	//! Create a scalar or aggregate function in the catalog
	CatalogEntry *CreateFunction(ClientContext &context, SchemaCatalogEntry *schema, CreateFunctionInfo *info);
	//! Creates a table in the catalog.
	CatalogEntry *CreateView(ClientContext &context, SchemaCatalogEntry *schema, CreateViewInfo *info);
	//! Creates a table in the catalog.
	CatalogEntry *CreateSequence(ClientContext &context, SchemaCatalogEntry *schema, CreateSequenceInfo *info);
	//! Creates a collation in the catalog
	CatalogEntry *CreateCollation(ClientContext &context, SchemaCatalogEntry *schema, CreateCollationInfo *info);

	//! Drops an entry from the catalog
	void DropEntry(ClientContext &context, DropInfo *info);

	//! Returns the schema object with the specified name, or throws an exception if it does not exist
	SchemaCatalogEntry *GetSchema(ClientContext &context, const string &name = DEFAULT_SCHEMA,
	                              QueryErrorContext error_context = QueryErrorContext());
	//! Scans all the schemas in the system one-by-one, invoking the callback for each entry
	void ScanSchemas(ClientContext &context, std::function<void(CatalogEntry *)> callback);
	//! Gets the "schema.name" entry of the specified type, if if_exists=true returns nullptr if entry does not exist,
	//! otherwise an exception is thrown
	CatalogEntry *GetEntry(ClientContext &context, CatalogType type, string schema, const string &name,
	                       bool if_exists = false, QueryErrorContext error_context = QueryErrorContext());

	template <class T>
	T *GetEntry(ClientContext &context, string schema_name, const string &name, bool if_exists = false,
	            QueryErrorContext error_context = QueryErrorContext());

	//! Alter an existing entry in the catalog.
	void Alter(ClientContext &context, AlterInfo *info);

private:
	//! The catalog version, incremented whenever anything changes in the catalog
	atomic<idx_t> catalog_version;

private:
	void DropSchema(ClientContext &context, DropInfo *info);
};

template <>
TableCatalogEntry *Catalog::GetEntry(ClientContext &context, string schema_name, const string &name, bool if_exists,
                                     QueryErrorContext error_context);
template <>
ViewCatalogEntry *Catalog::GetEntry(ClientContext &context, string schema_name, const string &name, bool if_exists,
                                    QueryErrorContext error_context);
template <>
SequenceCatalogEntry *Catalog::GetEntry(ClientContext &context, string schema_name, const string &name, bool if_exists,
                                        QueryErrorContext error_context);
template <>
TableFunctionCatalogEntry *Catalog::GetEntry(ClientContext &context, string schema_name, const string &name,
                                             bool if_exists, QueryErrorContext error_context);
template <>
CopyFunctionCatalogEntry *Catalog::GetEntry(ClientContext &context, string schema_name, const string &name,
                                            bool if_exists, QueryErrorContext error_context);
template <>
PragmaFunctionCatalogEntry *Catalog::GetEntry(ClientContext &context, string schema_name, const string &name,
                                              bool if_exists, QueryErrorContext error_context);
template <>
AggregateFunctionCatalogEntry *Catalog::GetEntry(ClientContext &context, string schema_name, const string &name,
                                                 bool if_exists, QueryErrorContext error_context);
template <>
CollateCatalogEntry *Catalog::GetEntry(ClientContext &context, string schema_name, const string &name, bool if_exists,
                                       QueryErrorContext error_context);

} // namespace duckdb


//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/physical_operator_type.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

//===--------------------------------------------------------------------===//
// Physical Operator Types
//===--------------------------------------------------------------------===//
enum class PhysicalOperatorType : uint8_t {
	INVALID,
	LEAF,
	ORDER_BY,
	LIMIT,
	TOP_N,
	AGGREGATE,
	WINDOW,
	UNNEST,
	SIMPLE_AGGREGATE,
	HASH_GROUP_BY,
	PERFECT_HASH_GROUP_BY,
	SORT_GROUP_BY,
	FILTER,
	PROJECTION,
	COPY_TO_FILE,
	RESERVOIR_SAMPLE,
	STREAMING_SAMPLE,
	// -----------------------------
	// Scans
	// -----------------------------
	TABLE_SCAN,
	DUMMY_SCAN,
	CHUNK_SCAN,
	RECURSIVE_CTE_SCAN,
	DELIM_SCAN,
	EXTERNAL_FILE_SCAN,
	QUERY_DERIVED_SCAN,
	EXPRESSION_SCAN,
	// -----------------------------
	// Joins
	// -----------------------------
	BLOCKWISE_NL_JOIN,
	NESTED_LOOP_JOIN,
	HASH_JOIN,
	CROSS_PRODUCT,
	PIECEWISE_MERGE_JOIN,
	DELIM_JOIN,
	INDEX_JOIN,
	// -----------------------------
	// SetOps
	// -----------------------------
	UNION,
	RECURSIVE_CTE,

	// -----------------------------
	// Updates
	// -----------------------------
	INSERT,
	INSERT_SELECT,
	DELETE_OPERATOR,
	UPDATE,
	EXPORT_EXTERNAL_FILE,

	// -----------------------------
	// Schema
	// -----------------------------
	CREATE_TABLE,
	CREATE_TABLE_AS,
	CREATE_INDEX,
	ALTER,
	CREATE_SEQUENCE,
	CREATE_VIEW,
	CREATE_SCHEMA,
	CREATE_MACRO,
	DROP,
	PRAGMA,
	TRANSACTION,

	// -----------------------------
	// Helpers
	// -----------------------------
	EXPLAIN,
	EMPTY_RESULT,
	EXECUTE,
	PREPARE,
	VACUUM,
	EXPORT,
	SET,
	LOAD,
	INOUT_FUNCTION
};

string PhysicalOperatorToString(PhysicalOperatorType type);

} // namespace duckdb



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_context.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {
class ClientContext;
class ThreadContext;
class TaskContext;

class ExecutionContext {
public:
	ExecutionContext(ClientContext &client_p, ThreadContext &thread_p, TaskContext &task_p)
	    : client(client_p), thread(thread_p), task(task_p) {
	}

	//! The client-global context; caution needs to be taken when used in parallel situations
	ClientContext &client;
	//! The thread-local context for this execution
	ThreadContext &thread;
	//! The task context for this execution
	TaskContext &task;
};

} // namespace duckdb


#include <functional>
#include <utility>

namespace duckdb {
class ExpressionExecutor;
class PhysicalOperator;

//! The current state/context of the operator. The PhysicalOperatorState is
//! updated using the GetChunk function, and allows the caller to repeatedly
//! call the GetChunk function and get new batches of data everytime until the
//! data source is exhausted.
class PhysicalOperatorState {
public:
	PhysicalOperatorState(PhysicalOperator &op, PhysicalOperator *child);
	virtual ~PhysicalOperatorState() = default;

	//! Flag indicating whether or not the operator is finished [note: not all
	//! operators use this flag]
	bool finished;
	//! DataChunk that stores data from the child of this operator
	DataChunk child_chunk;
	//! State of the child of this operator
	unique_ptr<PhysicalOperatorState> child_state;
};

//! PhysicalOperator is the base class of the physical operators present in the
//! execution plan
/*!
    The execution model is a pull-based execution model. GetChunk is called on
   the root node, which causes the root node to be executed, and presumably call
   GetChunk again on its child nodes. Every node in the operator chain has a
   state that is updated as GetChunk is called: PhysicalOperatorState (different
   operators subclass this state and add different properties).
*/
class PhysicalOperator {
public:
	PhysicalOperator(PhysicalOperatorType type, vector<LogicalType> types, idx_t estimated_cardinality)
	    : type(type), types(std::move(types)), estimated_cardinality(estimated_cardinality) {
	}
	virtual ~PhysicalOperator() {
	}

	//! The physical operator type
	PhysicalOperatorType type;
	//! The set of children of the operator
	vector<unique_ptr<PhysicalOperator>> children;
	//! The types returned by this physical operator
	vector<LogicalType> types;
	//! The extimated cardinality of this physical operator
	idx_t estimated_cardinality;

public:
	virtual string GetName() const;
	virtual string ParamsToString() const {
		return "";
	}
	virtual string ToString() const;
	void Print();

	//! Return a vector of the types that will be returned by this operator
	vector<LogicalType> &GetTypes() {
		return types;
	}
	//! Initialize a given chunk to the types that will be returned by this
	//! operator, this will prepare chunk for a call to GetChunk. This method
	//! only has to be called once for any amount of calls to GetChunk.
	virtual void InitializeChunk(DataChunk &chunk) {
		auto &types = GetTypes();
		chunk.Initialize(types);
	}
	virtual void InitializeChunkEmpty(DataChunk &chunk) {
		auto &types = GetTypes();
		chunk.InitializeEmpty(types);
	}
	//! Retrieves a chunk from this operator and stores it in the chunk
	//! variable.
	virtual void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) const = 0;

	void GetChunk(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) const;

	//! Create a new empty instance of the operator state
	virtual unique_ptr<PhysicalOperatorState> GetOperatorState() {
		return make_unique<PhysicalOperatorState>(*this, children.size() == 0 ? nullptr : children[0].get());
	}

	virtual void FinalizeOperatorState(PhysicalOperatorState &state, ExecutionContext &context) {
		if (!children.empty() && state.child_state) {
			children[0]->FinalizeOperatorState(*state.child_state, context);
		}
	}

	virtual bool IsSink() const {
		return false;
	}
};

} // namespace duckdb


namespace duckdb {

class Pipeline;

class GlobalOperatorState {
public:
	virtual ~GlobalOperatorState() {
	}
};

class LocalSinkState {
public:
	virtual ~LocalSinkState() {
	}
};

class PhysicalSink : public PhysicalOperator {
public:
	PhysicalSink(PhysicalOperatorType type, vector<LogicalType> types, idx_t estimated_cardinality)
	    : PhysicalOperator(type, move(types), estimated_cardinality) {
	}

	unique_ptr<GlobalOperatorState> sink_state;

public:
	//! The sink method is called constantly with new input, as long as new input is available. Note that this method
	//! CAN be called in parallel, proper locking is needed when accessing data inside the GlobalOperatorState.
	virtual void Sink(ExecutionContext &context, GlobalOperatorState &gstate, LocalSinkState &lstate,
	                  DataChunk &input) const = 0;
	// The combine is called when a single thread has completed execution of its part of the pipeline, it is the final
	// time that a specific LocalSinkState is accessible. This method can be called in parallel while other Sink() or
	// Combine() calls are active on the same GlobalOperatorState.
	virtual void Combine(ExecutionContext &context, GlobalOperatorState &gstate, LocalSinkState &lstate) {
	}
	//! The finalize is called when ALL threads are finished execution. It is called only once per pipeline, and is
	//! entirely single threaded.
	virtual bool Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> gstate) {
		this->sink_state = move(gstate);
		return true;
	}

	virtual unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) {
		return make_unique<LocalSinkState>();
	}
	virtual unique_ptr<GlobalOperatorState> GetGlobalState(ClientContext &context) {
		return make_unique<GlobalOperatorState>();
	}

	bool IsSink() const override {
		return true;
	}

	void Schedule(ClientContext &context);
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table_function.hpp
//
//
//===----------------------------------------------------------------------===//







#include <functional>

namespace duckdb {
class BaseStatistics;
class LogicalGet;
struct ParallelState;
class TableFilterSet;

struct FunctionOperatorData {
	virtual ~FunctionOperatorData() {
	}
};

struct TableFilterCollection {
	TableFilterSet *table_filters;
	explicit TableFilterCollection(TableFilterSet *table_filters) : table_filters(table_filters) {
	}
};

typedef unique_ptr<FunctionData> (*table_function_bind_t)(ClientContext &context, vector<Value> &inputs,
                                                          unordered_map<string, Value> &named_parameters,
                                                          vector<LogicalType> &input_table_types,
                                                          vector<string> &input_table_names,
                                                          vector<LogicalType> &return_types, vector<string> &names);
typedef unique_ptr<FunctionOperatorData> (*table_function_init_t)(ClientContext &context, const FunctionData *bind_data,
                                                                  const vector<column_t> &column_ids,
                                                                  TableFilterCollection *filters);
typedef unique_ptr<BaseStatistics> (*table_statistics_t)(ClientContext &context, const FunctionData *bind_data,
                                                         column_t column_index);
typedef void (*table_function_t)(ClientContext &context, const FunctionData *bind_data,
                                 FunctionOperatorData *operator_state, DataChunk *input, DataChunk &output);

typedef void (*table_function_parallel_t)(ClientContext &context, const FunctionData *bind_data,
                                          FunctionOperatorData *operator_state, DataChunk *input, DataChunk &output,
                                          ParallelState *parallel_state);

typedef void (*table_function_cleanup_t)(ClientContext &context, const FunctionData *bind_data,
                                         FunctionOperatorData *operator_state);
typedef idx_t (*table_function_max_threads_t)(ClientContext &context, const FunctionData *bind_data);
typedef unique_ptr<ParallelState> (*table_function_init_parallel_state_t)(ClientContext &context,
                                                                          const FunctionData *bind_data);
typedef unique_ptr<FunctionOperatorData> (*table_function_init_parallel_t)(ClientContext &context,
                                                                           const FunctionData *bind_data,
                                                                           ParallelState *state,
                                                                           const vector<column_t> &column_ids,
                                                                           TableFilterCollection *filters);
typedef bool (*table_function_parallel_state_next_t)(ClientContext &context, const FunctionData *bind_data,
                                                     FunctionOperatorData *state, ParallelState *parallel_state);
typedef int (*table_function_progress_t)(ClientContext &context, const FunctionData *bind_data);
typedef void (*table_function_dependency_t)(unordered_set<CatalogEntry *> &dependencies, const FunctionData *bind_data);
typedef unique_ptr<NodeStatistics> (*table_function_cardinality_t)(ClientContext &context,
                                                                   const FunctionData *bind_data);
typedef void (*table_function_pushdown_complex_filter_t)(ClientContext &context, LogicalGet &get,
                                                         FunctionData *bind_data,
                                                         vector<unique_ptr<Expression>> &filters);
typedef string (*table_function_to_string_t)(const FunctionData *bind_data);

class TableFunction : public SimpleNamedParameterFunction {
public:
	TableFunction(string name, vector<LogicalType> arguments, table_function_t function,
	              table_function_bind_t bind = nullptr, table_function_init_t init = nullptr,
	              table_statistics_t statistics = nullptr, table_function_cleanup_t cleanup = nullptr,
	              table_function_dependency_t dependency = nullptr, table_function_cardinality_t cardinality = nullptr,
	              table_function_pushdown_complex_filter_t pushdown_complex_filter = nullptr,
	              table_function_to_string_t to_string = nullptr, table_function_max_threads_t max_threads = nullptr,
	              table_function_init_parallel_state_t init_parallel_state = nullptr,
	              table_function_parallel_t parallel_function = nullptr,
	              table_function_init_parallel_t parallel_init = nullptr,
	              table_function_parallel_state_next_t parallel_state_next = nullptr, bool projection_pushdown = false,
	              bool filter_pushdown = false, table_function_progress_t query_progress = nullptr)
	    : SimpleNamedParameterFunction(std::move(name), move(arguments)), bind(bind), init(init), function(function),
	      statistics(statistics), cleanup(cleanup), dependency(dependency), cardinality(cardinality),
	      pushdown_complex_filter(pushdown_complex_filter), to_string(to_string), max_threads(max_threads),
	      init_parallel_state(init_parallel_state), parallel_function(parallel_function), parallel_init(parallel_init),
	      parallel_state_next(parallel_state_next), table_scan_progress(query_progress),
	      projection_pushdown(projection_pushdown), filter_pushdown(filter_pushdown) {
	}
	TableFunction(const vector<LogicalType> &arguments, table_function_t function, table_function_bind_t bind = nullptr,
	              table_function_init_t init = nullptr, table_statistics_t statistics = nullptr,
	              table_function_cleanup_t cleanup = nullptr, table_function_dependency_t dependency = nullptr,
	              table_function_cardinality_t cardinality = nullptr,
	              table_function_pushdown_complex_filter_t pushdown_complex_filter = nullptr,
	              table_function_to_string_t to_string = nullptr, table_function_max_threads_t max_threads = nullptr,
	              table_function_init_parallel_state_t init_parallel_state = nullptr,
	              table_function_parallel_t parallel_function = nullptr,
	              table_function_init_parallel_t parallel_init = nullptr,
	              table_function_parallel_state_next_t parallel_state_next = nullptr, bool projection_pushdown = false,
	              bool filter_pushdown = false, table_function_progress_t query_progress = nullptr)
	    : TableFunction(string(), arguments, function, bind, init, statistics, cleanup, dependency, cardinality,
	                    pushdown_complex_filter, to_string, max_threads, init_parallel_state, parallel_function,
	                    parallel_init, parallel_state_next, projection_pushdown, filter_pushdown, query_progress) {
	}
	TableFunction() : SimpleNamedParameterFunction("", {}) {
	}

	//! Bind function
	//! This function is used for determining the return type of a table producing function and returning bind data
	//! The returned FunctionData object should be constant and should not be changed during execution.
	table_function_bind_t bind;
	//! (Optional) init function
	//! Initialize the operator state of the function. The operator state is used to keep track of the progress in the
	//! table function.
	table_function_init_t init;
	//! The main function
	table_function_t function;
	//! (Optional) statistics function
	//! Returns the statistics of a specified column
	table_statistics_t statistics;
	//! (Optional) cleanup function
	//! The final cleanup function, called after all data is exhausted from the main function
	table_function_cleanup_t cleanup;
	//! (Optional) dependency function
	//! Sets up which catalog entries this table function depend on
	table_function_dependency_t dependency;
	//! (Optional) cardinality function
	//! Returns the expected cardinality of this scan
	table_function_cardinality_t cardinality;
	//! (Optional) pushdown a set of arbitrary filter expressions, rather than only simple comparisons with a constant
	//! Any functions remaining in the expression list will be pushed as a regular filter after the scan
	table_function_pushdown_complex_filter_t pushdown_complex_filter;
	//! (Optional) function for rendering the operator to a string in profiling output
	table_function_to_string_t to_string;
	//! (Optional) function that returns the maximum amount of threads that can work on this task
	table_function_max_threads_t max_threads;
	//! (Optional) initialize the parallel scan state, called once in total.
	table_function_init_parallel_state_t init_parallel_state;
	//! (Optional) Parallel version of the main function
	table_function_parallel_t parallel_function;
	//! (Optional) initialize the parallel scan given the parallel state. Called once per task. Return nullptr if there
	//! is nothing left to scan.
	table_function_init_parallel_t parallel_init;
	//! (Optional) return the next chunk to process in the parallel scan, or return nullptr if there is none
	table_function_parallel_state_next_t parallel_state_next;
	//! (Optional) return how much of the table we have scanned up to this point (% of the data)
	table_function_progress_t table_scan_progress;
	//! Whether or not the table function supports projection pushdown. If not supported a projection will be added
	//! that filters out unused columns.
	bool projection_pushdown;
	//! Whether or not the table function supports filter pushdown. If not supported a filter will be added
	//! that applies the table filter directly.
	bool filter_pushdown;

	string ToString() override {
		return SimpleNamedParameterFunction::ToString();
	}
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/parallel_state.hpp
//
//
//===----------------------------------------------------------------------===//



namespace duckdb {

struct ParallelState {
	virtual ~ParallelState() {
	}
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/task_scheduler.hpp
//
//
//===----------------------------------------------------------------------===//






//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/task.hpp
//
//
//===----------------------------------------------------------------------===//



namespace duckdb {

class Task {
public:
	virtual ~Task() {
	}

	//! Execute the task
	virtual void Execute() = 0;
};

} // namespace duckdb



namespace duckdb {

struct ConcurrentQueue;
struct QueueProducerToken;
class ClientContext;
class TaskScheduler;

struct SchedulerThread;

struct ProducerToken {
	ProducerToken(TaskScheduler &scheduler, unique_ptr<QueueProducerToken> token);
	~ProducerToken();

	TaskScheduler &scheduler;
	unique_ptr<QueueProducerToken> token;
	mutex producer_lock;
};

//! The TaskScheduler is responsible for managing tasks and threads
class TaskScheduler {
	// timeout for semaphore wait, default 50ms
	constexpr static int64_t TASK_TIMEOUT_USECS = 50000;

public:
	TaskScheduler();
	~TaskScheduler();

	static TaskScheduler &GetScheduler(ClientContext &context);

	unique_ptr<ProducerToken> CreateProducer();
	//! Schedule a task to be executed by the task scheduler
	void ScheduleTask(ProducerToken &producer, unique_ptr<Task> task);
	//! Fetches a task from a specific producer, returns true if successful or false if no tasks were available
	bool GetTaskFromProducer(ProducerToken &token, unique_ptr<Task> &task);
	//! Run tasks forever until "marker" is set to false, "marker" must remain valid until the thread is joined
	void ExecuteForever(atomic<bool> *marker);

	//! Sets the amount of active threads executing tasks for the system; n-1 background threads will be launched.
	//! The main thread will also be used for execution
	void SetThreads(int32_t n);
	//! Returns the number of threads
	int32_t NumberOfThreads();

private:
	void SetThreadsInternal(int32_t n);

	//! The task queue
	unique_ptr<ConcurrentQueue> queue;
	//! The active background threads of the task scheduler
	vector<unique_ptr<SchedulerThread>> threads;
	//! Markers used by the various threads, if the markers are set to "false" the thread execution is stopped
	vector<unique_ptr<atomic<bool>>> markers;
};

} // namespace duckdb



namespace duckdb {
class Executor;
class TaskContext;

//! The Pipeline class represents an execution pipeline
class Pipeline : public std::enable_shared_from_this<Pipeline> {
	friend class Executor;

public:
	Pipeline(Executor &execution_context, ProducerToken &token);

	Executor &executor;
	ProducerToken &token;

public:
	//! Execute a task within the pipeline on a single thread
	void Execute(TaskContext &task);

	void AddDependency(shared_ptr<Pipeline> &pipeline);
	void CompleteDependency();
	bool HasDependencies() {
		return !dependencies.empty();
	}

	void Reset(ClientContext &context);
	void Schedule();

	//! Finish a single task of this pipeline
	void FinishTask();
	//! Finish executing this pipeline
	void Finish();

	string ToString() const;
	void Print() const;

	void SetRecursiveCTE(PhysicalOperator *op) {
		this->recursive_cte = op;
	}
	PhysicalOperator *GetRecursiveCTE() {
		return recursive_cte;
	}
	void ClearParents();

	void IncrementTasks(idx_t amount) {
		this->total_tasks += amount;
	}

	bool IsFinished() {
		return finished;
	}
	//! Returns query progress
	bool GetProgress(int &current_percentage);

public:
	//! The current threads working on the pipeline
	atomic<idx_t> finished_tasks;
	//! The maximum amount of threads that can work on the pipeline
	atomic<idx_t> total_tasks;

private:
	//! The child from which to pull chunks
	PhysicalOperator *child;
	//! The global sink state
	unique_ptr<GlobalOperatorState> sink_state;
	//! The sink (i.e. destination) for data; this is e.g. a hash table to-be-built
	PhysicalSink *sink;
	//! The parent pipelines (i.e. pipelines that are dependent on this pipeline to finish)
	unordered_map<Pipeline *, weak_ptr<Pipeline>> parents;
	//! The dependencies of this pipeline
	unordered_map<Pipeline *, weak_ptr<Pipeline>> dependencies;
	//! The amount of completed dependencies (the pipeline can only be started after the dependencies have finished
	//! executing)
	atomic<idx_t> finished_dependencies;

	//! The parallel operator (if any)
	PhysicalOperator *parallel_node;
	//! The parallel state (if any)
	unique_ptr<ParallelState> parallel_state;

	//! Whether or not the pipeline is finished executing
	bool finished;
	//! The recursive CTE node that this pipeline belongs to, and may be executed multiple times
	PhysicalOperator *recursive_cte;

private:
	bool GetProgress(ClientContext &context, PhysicalOperator *op, int &current_percentage);
	void ScheduleSequentialTask();
	bool LaunchScanTasks(PhysicalOperator *op, idx_t max_threads, unique_ptr<ParallelState> parallel_state);
	bool ScheduleOperator(PhysicalOperator *op);
};

} // namespace duckdb



#include <queue>

namespace duckdb {
class ClientContext;
class DataChunk;
class PhysicalOperator;
class PhysicalOperatorState;
class ThreadContext;
class Task;

struct ProducerToken;

class Executor {
	friend class Pipeline;
	friend class PipelineTask;

public:
	explicit Executor(ClientContext &context);
	~Executor();

	ClientContext &context;

public:
	void Initialize(PhysicalOperator *physical_plan);
	void BuildPipelines(PhysicalOperator *op, Pipeline *parent);

	void Reset();

	vector<LogicalType> GetTypes();

	unique_ptr<DataChunk> FetchChunk();

	//! Push a new error
	void PushError(const string &exception);
	bool GetError(string &exception);

	//! Flush a thread context into the client context
	void Flush(ThreadContext &context);

	//! Returns the progress of the pipelines
	bool GetPipelinesProgress(int &current_progress);

private:
	PhysicalOperator *physical_plan;
	unique_ptr<PhysicalOperatorState> physical_state;

	mutex executor_lock;
	//! The pipelines of the current query
	vector<shared_ptr<Pipeline>> pipelines;
	//! The producer of this query
	unique_ptr<ProducerToken> producer;
	//! Exceptions that occurred during the execution of the current query
	vector<string> exceptions;

	//! The amount of completed pipelines of the query
	atomic<idx_t> completed_pipelines;
	//! The total amount of pipelines in the query
	idx_t total_pipelines;

	unordered_map<PhysicalOperator *, Pipeline *> delim_join_dependencies;
	PhysicalOperator *recursive_cte;
};
} // namespace duckdb



namespace duckdb {
class ProgressBar {
public:
	explicit ProgressBar(Executor *executor, idx_t show_progress_after, idx_t time_update_bar = 100)
	    : executor(executor), show_progress_after(show_progress_after), time_update_bar(time_update_bar),
	      current_percentage(-1), stop(false) {
	}
	~ProgressBar();

	//! Starts the thread
	void Start();
	//! Stops the thread
	void Stop();
	//! Gets current percentage
	int GetCurrentPercentage();

	void Initialize(idx_t show_progress_after) {
		this->show_progress_after = show_progress_after;
	}

private:
	const string PROGRESS_BAR_STRING = "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||";
	static constexpr const idx_t PROGRESS_BAR_WIDTH = 60;
	Executor *executor = nullptr;
#ifndef DUCKDB_NO_THREADS
	thread progress_bar_thread;
	std::condition_variable c;
	mutex m;
#endif
	idx_t show_progress_after;
	idx_t time_update_bar;
	atomic<int> current_percentage;
	atomic<bool> stop;
	//! In case our progress bar tries to use a scan operator that is not implemented we don't print anything
	bool supported = true;
	//! Starts the Progress Bar Thread that prints the progress bar
	void ProgressBarThread();

#ifndef DUCKDB_NO_THREADS
	template <class DURATION>
	bool WaitFor(DURATION duration) {
		unique_lock<mutex> l(m);
		return !c.wait_for(l, duration, [this]() { return stop.load(); });
	}
#endif
};
} // namespace duckdb






//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/transaction_context.hpp
//
//
//===----------------------------------------------------------------------===//






namespace duckdb {

class ClientContext;
class Transaction;
class TransactionManager;

//! The transaction context keeps track of all the information relating to the
//! current transaction
class TransactionContext {
public:
	TransactionContext(TransactionManager &transaction_manager, ClientContext &context)
	    : transaction_manager(transaction_manager), context(context), auto_commit(true), current_transaction(nullptr) {
	}
	~TransactionContext();

	Transaction &ActiveTransaction() {
		D_ASSERT(current_transaction);
		return *current_transaction;
	}

	bool HasActiveTransaction() {
		return !!current_transaction;
	}

	void RecordQuery(string query);
	void BeginTransaction();
	void Commit();
	void Rollback();
	void ClearTransaction();

	void SetAutoCommit(bool value);
	bool IsAutoCommit() {
		return auto_commit;
	}

private:
	TransactionManager &transaction_manager;
	ClientContext &context;
	bool auto_commit;

	Transaction *current_transaction;

	TransactionContext(const TransactionContext &) = delete;
};

} // namespace duckdb

#include <random>


namespace duckdb {
class Appender;
class Catalog;
class DatabaseInstance;
class LogicalOperator;
class PreparedStatementData;
class Relation;
class BufferedFileWriter;
class QueryProfiler;
class QueryProfilerHistory;
class ClientContextLock;
struct CreateScalarFunctionInfo;
class ScalarFunctionCatalogEntry;

//! The ClientContext holds information relevant to the current client session
//! during execution
class ClientContext : public std::enable_shared_from_this<ClientContext> {
	friend class TransactionManager;

public:
	DUCKDB_API explicit ClientContext(shared_ptr<DatabaseInstance> db);
	DUCKDB_API ~ClientContext();
	//! Query profiler
	unique_ptr<QueryProfiler> profiler;
	//! QueryProfiler History
	unique_ptr<QueryProfilerHistory> query_profiler_history;
	//! The database that this client is connected to
	shared_ptr<DatabaseInstance> db;
	//! Data for the currently running transaction
	TransactionContext transaction;
	//! Whether or not the query is interrupted
	atomic<bool> interrupted;
	//! The current query being executed by the client context
	string query;

	//! The query executor
	Executor executor;

	//! The Progress Bar
	unique_ptr<ProgressBar> progress_bar;
	//! If the progress bar is enabled or not.
	bool enable_progress_bar = false;
	//! If the print of the progress bar is enabled
	bool print_progress_bar = true;
	//! The wait time before showing the progress bar
	int wait_time = 2000;

	unique_ptr<SchemaCatalogEntry> temporary_objects;
	unordered_map<string, shared_ptr<PreparedStatementData>> prepared_statements;

	// Whether or not aggressive query verification is enabled
	bool query_verification_enabled = false;
	//! Enable the running of optimizers
	bool enable_optimizer = true;
	//! Force parallelism of small tables, used for testing
	bool force_parallelism = false;
	//! Force index join independent of table cardinality, used for testing
	bool force_index_join = false;
	//! Maximum bits allowed for using a perfect hash table (i.e. the perfect HT can hold up to 2^perfect_ht_threshold
	//! elements)
	idx_t perfect_ht_threshold = 12;
	//! The writer used to log queries (if logging is enabled)
	unique_ptr<BufferedFileWriter> log_query_writer;
	//! The explain output type used when none is specified (default: PHYSICAL_ONLY)
	ExplainOutputType explain_output_type = ExplainOutputType::PHYSICAL_ONLY;
	//! The random generator used by random(). Its seed value can be set by setseed().
	std::mt19937 random_engine;

	//! The schema search path, in order by which entries are searched if no schema entry is provided
	vector<string> catalog_search_path = {TEMP_SCHEMA, DEFAULT_SCHEMA, "pg_catalog"};

public:
	DUCKDB_API Transaction &ActiveTransaction() {
		return transaction.ActiveTransaction();
	}

	//! Interrupt execution of a query
	DUCKDB_API void Interrupt();
	//! Enable query profiling
	DUCKDB_API void EnableProfiling();
	//! Disable query profiling
	DUCKDB_API void DisableProfiling();

	//! Issue a query, returning a QueryResult. The QueryResult can be either a StreamQueryResult or a
	//! MaterializedQueryResult. The StreamQueryResult will only be returned in the case of a successful SELECT
	//! statement.
	DUCKDB_API unique_ptr<QueryResult> Query(const string &query, bool allow_stream_result);
	DUCKDB_API unique_ptr<QueryResult> Query(unique_ptr<SQLStatement> statement, bool allow_stream_result);
	//! Fetch a query from the current result set (if any)
	DUCKDB_API unique_ptr<DataChunk> Fetch();
	//! Cleanup the result set (if any).
	DUCKDB_API void Cleanup();
	//! Destroy the client context
	DUCKDB_API void Destroy();

	//! Get the table info of a specific table, or nullptr if it cannot be found
	DUCKDB_API unique_ptr<TableDescription> TableInfo(const string &schema_name, const string &table_name);
	//! Appends a DataChunk to the specified table. Returns whether or not the append was successful.
	DUCKDB_API void Append(TableDescription &description, DataChunk &chunk);
	//! Try to bind a relation in the current client context; either throws an exception or fills the result_columns
	//! list with the set of returned columns
	DUCKDB_API void TryBindRelation(Relation &relation, vector<ColumnDefinition> &result_columns);

	//! Execute a relation
	DUCKDB_API unique_ptr<QueryResult> Execute(const shared_ptr<Relation> &relation);

	//! Prepare a query
	DUCKDB_API unique_ptr<PreparedStatement> Prepare(const string &query);
	//! Directly prepare a SQL statement
	DUCKDB_API unique_ptr<PreparedStatement> Prepare(unique_ptr<SQLStatement> statement);

	//! Execute a prepared statement with the given name and set of parameters
	//! It is possible that the prepared statement will be re-bound. This will generally happen if the catalog is
	//! modified in between the prepared statement being bound and the prepared statement being run.
	DUCKDB_API unique_ptr<QueryResult> Execute(const string &query, shared_ptr<PreparedStatementData> &prepared,
	                                           vector<Value> &values, bool allow_stream_result = true);

	//! Gets current percentage of the query's progress, returns 0 in case the progress bar is disabled.
	int GetProgress();

	//! Register function in the temporary schema
	DUCKDB_API void RegisterFunction(CreateFunctionInfo *info);

	//! Parse statements from a query
	DUCKDB_API vector<unique_ptr<SQLStatement>> ParseStatements(const string &query);
	//! Extract the logical plan of a query
	DUCKDB_API unique_ptr<LogicalOperator> ExtractPlan(const string &query);
	void HandlePragmaStatements(vector<unique_ptr<SQLStatement>> &statements);

	//! Runs a function with a valid transaction context, potentially starting a transaction if the context is in auto
	//! commit mode.
	DUCKDB_API void RunFunctionInTransaction(const std::function<void(void)> &fun,
	                                         bool requires_valid_transaction = true);
	//! Same as RunFunctionInTransaction, but does not obtain a lock on the client context or check for validation
	DUCKDB_API void RunFunctionInTransactionInternal(ClientContextLock &lock, const std::function<void(void)> &fun,
	                                                 bool requires_valid_transaction = true);

private:
	//! Parse statements from a query
	vector<unique_ptr<SQLStatement>> ParseStatementsInternal(ClientContextLock &lock, const string &query);
	//! Perform aggressive query verification of a SELECT statement. Only called when query_verification_enabled is
	//! true.
	string VerifyQuery(ClientContextLock &lock, const string &query, unique_ptr<SQLStatement> statement);

	void InitialCleanup(ClientContextLock &lock);
	//! Internal clean up, does not lock. Caller must hold the context_lock.
	void CleanupInternal(ClientContextLock &lock);
	string FinalizeQuery(ClientContextLock &lock, bool success);
	//! Internal fetch, does not lock. Caller must hold the context_lock.
	unique_ptr<DataChunk> FetchInternal(ClientContextLock &lock);
	//! Internally execute a set of SQL statement. Caller must hold the context_lock.
	unique_ptr<QueryResult> RunStatements(ClientContextLock &lock, const string &query,
	                                      vector<unique_ptr<SQLStatement>> &statements, bool allow_stream_result);
	//! Internally prepare and execute a prepared SQL statement. Caller must hold the context_lock.
	unique_ptr<QueryResult> RunStatement(ClientContextLock &lock, const string &query,
	                                     unique_ptr<SQLStatement> statement, bool allow_stream_result);
	unique_ptr<QueryResult> RunStatementOrPreparedStatement(ClientContextLock &lock, const string &query,
	                                                        unique_ptr<SQLStatement> statement,
	                                                        shared_ptr<PreparedStatementData> &prepared,
	                                                        vector<Value> *values, bool allow_stream_result);

	//! Internally prepare a SQL statement. Caller must hold the context_lock.
	shared_ptr<PreparedStatementData> CreatePreparedStatement(ClientContextLock &lock, const string &query,
	                                                          unique_ptr<SQLStatement> statement);
	//! Internally execute a prepared SQL statement. Caller must hold the context_lock.
	unique_ptr<QueryResult> ExecutePreparedStatement(ClientContextLock &lock, const string &query,
	                                                 shared_ptr<PreparedStatementData> statement,
	                                                 vector<Value> bound_values, bool allow_stream_result);
	//! Call CreatePreparedStatement() and ExecutePreparedStatement() without any bound values
	unique_ptr<QueryResult> RunStatementInternal(ClientContextLock &lock, const string &query,
	                                             unique_ptr<SQLStatement> statement, bool allow_stream_result);
	unique_ptr<PreparedStatement> PrepareInternal(ClientContextLock &lock, unique_ptr<SQLStatement> statement);
	void LogQueryInternal(ClientContextLock &lock, const string &query);

	unique_ptr<ClientContextLock> LockContext();

	bool UpdateFunctionInfoFromEntry(ScalarFunctionCatalogEntry *existing_function, CreateScalarFunctionInfo *new_info);

private:
	//! The currently opened StreamQueryResult (if any)
	StreamQueryResult *open_result = nullptr;
	//! Lock on using the ClientContext in parallel
	mutex context_lock;
};

} // namespace duckdb
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_table_function_info.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_function_info.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_info.hpp
//
//
//===----------------------------------------------------------------------===//



//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/parse_info.hpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

struct ParseInfo {
	virtual ~ParseInfo() {
	}
};

} // namespace duckdb



namespace duckdb {

enum class OnCreateConflict : uint8_t {
	// Standard: throw error
	ERROR_ON_CONFLICT,
	// CREATE IF NOT EXISTS, silently do nothing on conflict
	IGNORE_ON_CONFLICT,
	// CREATE OR REPLACE
	REPLACE_ON_CONFLICT
};

struct CreateInfo : public ParseInfo {
	explicit CreateInfo(CatalogType type, string schema = DEFAULT_SCHEMA)
	    : type(type), schema(schema), on_conflict(OnCreateConflict::ERROR_ON_CONFLICT), temporary(false),
	      internal(false) {
	}
	~CreateInfo() override {
	}

	//! The to-be-created catalog type
	CatalogType type;
	//! The schema name of the entry
	string schema;
	//! What to do on create conflict
	OnCreateConflict on_conflict;
	//! Whether or not the entry is temporary
	bool temporary;
	//! Whether or not the entry is an internal entry
	bool internal;
	//! The SQL string of the CREATE statement
	string sql;

public:
	virtual unique_ptr<CreateInfo> Copy() const = 0;
	void CopyProperties(CreateInfo &other) const {
		other.type = type;
		other.schema = schema;
		other.on_conflict = on_conflict;
		other.temporary = temporary;
		other.internal = internal;
		other.sql = sql;
	}
};

} // namespace duckdb



namespace duckdb {

struct CreateFunctionInfo : public CreateInfo {
	explicit CreateFunctionInfo(CatalogType type) : CreateInfo(type) {
		D_ASSERT(type == CatalogType::SCALAR_FUNCTION_ENTRY || type == CatalogType::AGGREGATE_FUNCTION_ENTRY ||
		         type == CatalogType::TABLE_FUNCTION_ENTRY || type == CatalogType::PRAGMA_FUNCTION_ENTRY ||
		         type == CatalogType::MACRO_ENTRY);
	}

	//! Function name
	string name;
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/function_set.hpp
//
//
//===----------------------------------------------------------------------===//







namespace duckdb {

template <class T>
class FunctionSet {
public:
	explicit FunctionSet(string name) : name(name) {
	}

	//! The name of the function set
	string name;
	//! The set of functions
	vector<T> functions;

public:
	void AddFunction(T function) {
		function.name = name;
		functions.push_back(function);
	}
};

class ScalarFunctionSet : public FunctionSet<ScalarFunction> {
public:
	explicit ScalarFunctionSet(string name) : FunctionSet(move(name)) {
	}
};

class AggregateFunctionSet : public FunctionSet<AggregateFunction> {
public:
	explicit AggregateFunctionSet(string name) : FunctionSet(move(name)) {
	}
};

class TableFunctionSet : public FunctionSet<TableFunction> {
public:
	explicit TableFunctionSet(string name) : FunctionSet(move(name)) {
	}
};

} // namespace duckdb


namespace duckdb {

struct CreateTableFunctionInfo : public CreateFunctionInfo {
	explicit CreateTableFunctionInfo(TableFunctionSet set)
	    : CreateFunctionInfo(CatalogType::TABLE_FUNCTION_ENTRY), functions(move(set.functions)) {
		this->name = set.name;
	}
	explicit CreateTableFunctionInfo(TableFunction function) : CreateFunctionInfo(CatalogType::TABLE_FUNCTION_ENTRY) {
		this->name = function.name;
		functions.push_back(move(function));
	}

	//! The table functions
	vector<TableFunction> functions;

public:
	unique_ptr<CreateInfo> Copy() const override {
		TableFunctionSet set(name);
		set.functions = functions;
		auto result = make_unique<CreateTableFunctionInfo>(move(set));
		CopyProperties(*result);
		return move(result);
	}
};

} // namespace duckdb
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_copy_function_info.hpp
//
//
//===----------------------------------------------------------------------===//




//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/copy_function.hpp
//
//
//===----------------------------------------------------------------------===//





//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/copy_info.hpp
//
//
//===----------------------------------------------------------------------===//








namespace duckdb {

struct CopyInfo : public ParseInfo {
	CopyInfo() : schema(DEFAULT_SCHEMA) {
	}

	//! The schema name to copy to/from
	string schema;
	//! The table name to copy to/from
	string table;
	//! List of columns to copy to/from
	vector<string> select_list;
	//! The file path to copy to/from
	string file_path;
	//! Whether or not this is a copy to file (false) or copy from a file (true)
	bool is_from;
	//! The file format of the external file
	string format;
	//! Set of (key, value) options
	unordered_map<string, vector<Value>> options;

public:
	unique_ptr<CopyInfo> Copy() const {
		auto result = make_unique<CopyInfo>();
		result->schema = schema;
		result->table = table;
		result->select_list = select_list;
		result->file_path = file_path;
		result->is_from = is_from;
		result->format = format;
		result->options = options;
		return result;
	}
};

} // namespace duckdb


namespace duckdb {
class ExecutionContext;

struct LocalFunctionData {
	virtual ~LocalFunctionData() {
	}
};

struct GlobalFunctionData {
	virtual ~GlobalFunctionData() {
	}
};

typedef unique_ptr<FunctionData> (*copy_to_bind_t)(ClientContext &context, CopyInfo &info, vector<string> &names,
                                                   vector<LogicalType> &sql_types);
typedef unique_ptr<LocalFunctionData> (*copy_to_initialize_local_t)(ClientContext &context, FunctionData &bind_data);
typedef unique_ptr<GlobalFunctionData> (*copy_to_initialize_global_t)(ClientContext &context, FunctionData &bind_data);
typedef void (*copy_to_sink_t)(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                               LocalFunctionData &lstate, DataChunk &input);
typedef void (*copy_to_combine_t)(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                                  LocalFunctionData &lstate);
typedef void (*copy_to_finalize_t)(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate);

typedef unique_ptr<FunctionData> (*copy_from_bind_t)(ClientContext &context, CopyInfo &info,
                                                     vector<string> &expected_names,
                                                     vector<LogicalType> &expected_types);

class CopyFunction : public Function {
public:
	explicit CopyFunction(string name)
	    : Function(name), copy_to_bind(nullptr), copy_to_initialize_local(nullptr), copy_to_initialize_global(nullptr),
	      copy_to_sink(nullptr), copy_to_combine(nullptr), copy_to_finalize(nullptr), copy_from_bind(nullptr) {
	}

	copy_to_bind_t copy_to_bind;
	copy_to_initialize_local_t copy_to_initialize_local;
	copy_to_initialize_global_t copy_to_initialize_global;
	copy_to_sink_t copy_to_sink;
	copy_to_combine_t copy_to_combine;
	copy_to_finalize_t copy_to_finalize;

	copy_from_bind_t copy_from_bind;
	TableFunction copy_from_function;

	string extension;
};

} // namespace duckdb


namespace duckdb {

struct CreateCopyFunctionInfo : public CreateInfo {
	explicit CreateCopyFunctionInfo(CopyFunction function)
	    : CreateInfo(CatalogType::COPY_FUNCTION_ENTRY), function(function) {
		this->name = function.name;
	}

	//! Function name
	string name;
	//! The table function
	CopyFunction function;

public:
	unique_ptr<CreateInfo> Copy() const override {
		auto result = make_unique<CreateCopyFunctionInfo>(function);
		CopyProperties(*result);
		return move(result);
	}
};

} // namespace duckdb
