// Zero-copy Arrow registration shim.
//
// Registers DuckDB's native `arrow_scan` table function with a Rust-driven, replayable,
// projection-honoring factory. All Arrow logic lives in Rust; this file is a trampoline
// (translate the C++ ArrowStreamParameters that Rust cannot read into plain C) plus the
// TableFunction/CreateView registration call.
//
// Ownership: `factory_ptr` is a Rust Box<ArrowFactory>; its first two fields are the Rust
// callback pointers (matched by RustCallbacks below, #[repr(C)] on the Rust side). DuckDB
// stores `factory_ptr` in the view's bind data and calls ShimProduce per scan and
// ShimGetSchema at bind time. The view references the factory by pointer; the Rust side keeps
// the box alive until the view is gone (see ArrowView).

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <unordered_map>
#include <vector>

#include "duckdb.h"                                   // duckdb_connection
#include "duckdb.hpp"                                 // duckdb::Connection, Value, Relation
#include "duckdb/function/table/arrow.hpp"           // ArrowStreamParameters, stream factory typedefs
#include "duckdb/common/arrow/arrow_wrapper.hpp"     // ArrowArrayStreamWrapper
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

using duckdb::ArrowArrayStreamWrapper;
using duckdb::ArrowStreamParameters;

// Must match the first two fields of the Rust #[repr(C)] ArrowFactory.
typedef void (*RustProduceFn)(void *factory, const char *const *names, size_t n,
                              const uint8_t *filter_buf, size_t filter_len, ArrowArrayStream *out);
typedef void (*RustGetSchemaFn)(void *factory, ArrowSchema *out);
struct RustCallbacks {
    RustProduceFn produce;
    RustGetSchemaFn get_schema;
};

// ──────────────────────────────────────────────────────────────────────────────
// Filter wire-format helpers
// ──────────────────────────────────────────────────────────────────────────────
//
// The wire format is a little-endian byte buffer that encodes DuckDB's TableFilterSet
// into a tree of FilterNode structures. The Rust filter module decodes this buffer and
// evaluates it against the projected RecordBatch before returning the Arrow stream.
//
// Node format:  tag:u8 + payload
//   tag 0  (COMPARE):    path + op:u8 + scalar
//   tag 1  (IS_NULL):    path
//   tag 2  (IS_NOT_NULL): path
//   tag 3  (AND):        n:u32  + n*node
//   tag 4  (OR):         n:u32  + n*node
//   tag 5  (IN):         path + n:u32 + n*scalar
//   tag 255 (UNHANDLED): raw_filter_type:u32
//
// path:  depth:u32 + depth*u32  (depth >= 1; index[0] = projected root column,
//        index[1..] = struct child indices)
//
// Scalar format:  stype:u8 + payload
//   stype 0 (i64) 1 (u64) 2 (f64) 3 (bool) 4 (utf8 len:u32+bytes) 5 (null)
//   stype 6 (date32 i32:days) 7 (time unit:u8 + i64:µs) 8 (timestamp unit:u8 + i64)
//   stype 9 (decimal128 i128:16LE-bytes + scale:u8)  stype 10 (blob len:u32 + bytes)
//   unit byte: 0=s 1=ms 2=µs 3=ns
//   stype 255 (unsupported)
//
// op byte (for tag 0):  0=EQ 1=NE 2=LT 3=GT 4=LE 5=GE  (matches ExpressionType 25-30)

static void push_u8(std::vector<uint8_t> &buf, uint8_t v) { buf.push_back(v); }

static void push_u32(std::vector<uint8_t> &buf, uint32_t v) {
    buf.push_back((uint8_t)(v & 0xff));
    buf.push_back((uint8_t)((v >> 8) & 0xff));
    buf.push_back((uint8_t)((v >> 16) & 0xff));
    buf.push_back((uint8_t)((v >> 24) & 0xff));
}

static void push_i32(std::vector<uint8_t> &buf, int32_t v) {
    uint32_t u = (uint32_t)v;
    for (int i = 0; i < 4; i++) { buf.push_back((uint8_t)(u & 0xff)); u >>= 8; }
}

static void push_i64(std::vector<uint8_t> &buf, int64_t v) {
    uint64_t u = (uint64_t)v;
    for (int i = 0; i < 8; i++) { buf.push_back((uint8_t)(u & 0xff)); u >>= 8; }
}

static void push_u64(std::vector<uint8_t> &buf, uint64_t v) {
    for (int i = 0; i < 8; i++) { buf.push_back((uint8_t)(v & 0xff)); v >>= 8; }
}

static void push_i128(std::vector<uint8_t> &buf, duckdb::hugeint_t h) {
    // Little-endian i128: low 64 bits then high 64 bits (two's complement).
    push_u64(buf, h.lower);
    push_i64(buf, h.upper);
}

static void push_f64(std::vector<uint8_t> &buf, double v) {
    uint64_t u;
    memcpy(&u, &v, 8);
    for (int i = 0; i < 8; i++) { buf.push_back((uint8_t)(u & 0xff)); u >>= 8; }
}

static void push_column_path(std::vector<uint8_t> &buf, const std::vector<duckdb::idx_t> &path) {
    push_u32(buf, (uint32_t)path.size());
    for (auto idx : path) { push_u32(buf, (uint32_t)idx); }
}

static void emit_scalar(const duckdb::Value &v, std::vector<uint8_t> &buf) {
    if (v.IsNull()) { push_u8(buf, 5); return; }
    auto type_id = v.type().id();
    switch (type_id) {
        case duckdb::LogicalTypeId::BIGINT:
        case duckdb::LogicalTypeId::INTEGER:
        case duckdb::LogicalTypeId::SMALLINT:
        case duckdb::LogicalTypeId::TINYINT:
            push_u8(buf, 0); push_i64(buf, v.GetValue<int64_t>()); break;
        case duckdb::LogicalTypeId::UBIGINT:
        case duckdb::LogicalTypeId::UINTEGER:
        case duckdb::LogicalTypeId::USMALLINT:
        case duckdb::LogicalTypeId::UTINYINT:
            push_u8(buf, 1); push_u64(buf, v.GetValue<uint64_t>()); break;
        case duckdb::LogicalTypeId::DOUBLE:
        case duckdb::LogicalTypeId::FLOAT:
            push_u8(buf, 2); push_f64(buf, v.GetValue<double>()); break;
        case duckdb::LogicalTypeId::BOOLEAN:
            push_u8(buf, 3); push_u8(buf, v.GetValue<bool>() ? 1 : 0); break;
        case duckdb::LogicalTypeId::VARCHAR: {
            push_u8(buf, 4);
            std::string s = v.ToString();
            push_u32(buf, (uint32_t)s.size());
            for (char c : s) { buf.push_back((uint8_t)c); }
            break;
        }
        case duckdb::LogicalTypeId::DATE:
            push_u8(buf, 6);
            push_i32(buf, duckdb::DateValue::Get(v).days);
            break;
        case duckdb::LogicalTypeId::TIME:
            push_u8(buf, 7);
            push_u8(buf, 2); // unit = µs
            push_i64(buf, duckdb::TimeValue::Get(v).micros);
            break;
        case duckdb::LogicalTypeId::TIMESTAMP:
        case duckdb::LogicalTypeId::TIMESTAMP_TZ:
            push_u8(buf, 8);
            push_u8(buf, 2); // µs
            push_i64(buf, duckdb::TimestampValue::Get(v).value);
            break;
        case duckdb::LogicalTypeId::TIMESTAMP_MS:
            push_u8(buf, 8);
            push_u8(buf, 1); // ms
            push_i64(buf, duckdb::TimestampMSValue::Get(v).value);
            break;
        case duckdb::LogicalTypeId::TIMESTAMP_NS:
            push_u8(buf, 8);
            push_u8(buf, 3); // ns
            push_i64(buf, duckdb::TimestampNSValue::Get(v).value);
            break;
        case duckdb::LogicalTypeId::TIMESTAMP_SEC:
            push_u8(buf, 8);
            push_u8(buf, 0); // s
            push_i64(buf, duckdb::TimestampSValue::Get(v).value);
            break;
        case duckdb::LogicalTypeId::BLOB: {
            push_u8(buf, 10);
            const std::string &s = duckdb::StringValue::Get(v);
            push_u32(buf, (uint32_t)s.size());
            for (char c : s) { buf.push_back((uint8_t)c); }
            break;
        }
        case duckdb::LogicalTypeId::DECIMAL: {
            // Only INT128-physical decimals are pushed by CanPushdown; the arrow column is Decimal128.
            uint8_t width = duckdb::DecimalType::GetWidth(v.type());
            uint8_t scale = duckdb::DecimalType::GetScale(v.type());
            if (width <= 18) { push_u8(buf, 255); break; } // not INT128-physical → unsupported (fail-loud)
            push_u8(buf, 9);
            push_i128(buf, v.GetValueUnsafe<duckdb::hugeint_t>());
            push_u8(buf, scale);
            break;
        }
        default:
            push_u8(buf, 255); break;
    }
}

// Returns true if this filter_type is handled (will not emit tag 255).
static bool is_handled_filter_type(duckdb::TableFilterType t) {
    using duckdb::TableFilterType;
    switch (t) {
        case TableFilterType::CONSTANT_COMPARISON:
        case TableFilterType::IS_NULL:
        case TableFilterType::IS_NOT_NULL:
        case TableFilterType::CONJUNCTION_AND:
        case TableFilterType::CONJUNCTION_OR:
        case TableFilterType::IN_FILTER:
        case TableFilterType::STRUCT_EXTRACT:
            return true;
        default:
            return false;
    }
}

static void emit_filter(const duckdb::TableFilter &filter, const std::vector<duckdb::idx_t> &path, std::vector<uint8_t> &buf) {
    using duckdb::TableFilterType;
    if (filter.filter_type == TableFilterType::CONSTANT_COMPARISON) {
        auto &c = filter.Cast<duckdb::ConstantFilter>();
        push_u8(buf, 0); // tag COMPARE
        push_column_path(buf, path);
        // Map ExpressionType to op byte: EQ=25->0, NE=26->1, LT=27->2, GT=28->3, LE=29->4, GE=30->5
        uint8_t op = 255;
        switch ((int)c.comparison_type) {
            case 25: op = 0; break; // COMPARE_EQUAL
            case 26: op = 1; break; // COMPARE_NOTEQUAL
            case 27: op = 2; break; // COMPARE_LESSTHAN
            case 28: op = 3; break; // COMPARE_GREATERTHAN
            case 29: op = 4; break; // COMPARE_LESSTHANOREQUALTO
            case 30: op = 5; break; // COMPARE_GREATERTHANOREQUALTO
        }
        push_u8(buf, op);
        emit_scalar(c.constant, buf);
    } else if (filter.filter_type == TableFilterType::IS_NULL) {
        push_u8(buf, 1); // tag IS_NULL
        push_column_path(buf, path);
    } else if (filter.filter_type == TableFilterType::IS_NOT_NULL) {
        push_u8(buf, 2); // tag IS_NOT_NULL
        push_column_path(buf, path);
    } else if (filter.filter_type == TableFilterType::CONJUNCTION_AND) {
        // Recursively emit children (all share the same path)
        auto &conj = filter.Cast<duckdb::ConjunctionAndFilter>();
        push_u8(buf, 3); // tag AND
        push_u32(buf, (uint32_t)conj.child_filters.size());
        for (auto &child : conj.child_filters) {
            emit_filter(*child, path, buf);
        }
    } else if (filter.filter_type == TableFilterType::CONJUNCTION_OR) {
        // Recursively emit children (all share the same path)
        auto &conj = filter.Cast<duckdb::ConjunctionOrFilter>();
        push_u8(buf, 4); // tag OR
        push_u32(buf, (uint32_t)conj.child_filters.size());
        for (auto &child : conj.child_filters) {
            emit_filter(*child, path, buf);
        }
    } else if (filter.filter_type == TableFilterType::IN_FILTER) {
        auto &inf = filter.Cast<duckdb::InFilter>();
        push_u8(buf, 5); // tag IN
        push_column_path(buf, path);
        push_u32(buf, (uint32_t)inf.values.size());
        for (auto &v : inf.values) {
            emit_scalar(v, buf);
        }
    } else if (filter.filter_type == TableFilterType::OPTIONAL_FILTER) {
        // OPTIONAL_FILTER is not required for correctness; the join above re-checks.
        // If the child is a handled kind, emit it; otherwise skip safely (empty AND = keep all rows).
        auto &opt = filter.Cast<duckdb::OptionalFilter>();
        if (opt.child_filter && is_handled_filter_type(opt.child_filter->filter_type)) {
            emit_filter(*opt.child_filter, path, buf);
        } else {
            push_u8(buf, 3); // tag AND
            push_u32(buf, 0); // 0 children = no-op (keep all rows)
        }
    } else if (filter.filter_type == TableFilterType::STRUCT_EXTRACT) {
        // Descend into a struct child: append child_idx to the path and recurse on the child filter.
        auto &sf = filter.Cast<duckdb::StructFilter>();
        std::vector<duckdb::idx_t> child_path = path;
        child_path.push_back(sf.child_idx);
        emit_filter(*sf.child_filter, child_path, buf);
    } else if (filter.filter_type == TableFilterType::DYNAMIC_FILTER ||
               filter.filter_type == TableFilterType::BLOOM_FILTER) {
        // Join-reduction filters: not required for correctness; the join above re-checks.
        // Skip safely: emit empty AND(0) = keep all rows.
        push_u8(buf, 3); // tag AND
        push_u32(buf, 0); // 0 children = no-op
    } else {
        // Truly unhandled: tag 255 + raw filter_type as u32.
        // The Rust evaluator will return FilterError, routing to error_stream (clean failure).
        push_u8(buf, 255);
        push_u32(buf, (uint32_t)filter.filter_type);
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Shim callbacks
// ──────────────────────────────────────────────────────────────────────────────

// Produce a fresh stream for one scan, honoring projection and filters. Called by DuckDB possibly
// many times (self-joins, repeated scans) — replayability comes from Rust building a new stream
// from the held batches each call.
static duckdb::unique_ptr<ArrowArrayStreamWrapper> ShimProduce(uintptr_t factory_ptr,
                                                               ArrowStreamParameters &params) {
    auto *cbs = reinterpret_cast<RustCallbacks *>(factory_ptr);
    std::vector<const char *> names;
    names.reserve(params.projected_columns.columns.size());
    for (auto &col : params.projected_columns.columns) {
        names.push_back(col.c_str());
    }

    // Build filter buffer: root AND node, one child per filter entry.
    //
    // The TableFilterSet is keyed by the SCAN / PROJECTED output-column position, NOT a
    // table-column index — verified against DuckDB's physical_table_scan (the key indexes
    // `column_ids`) and arrow_scan's own `filter_to_col` (also keyed by scan position). So the
    // key indexes the produced projected batch directly; an earlier reverse-map via
    // `filter_to_col` was WRONG and silently mis-filtered multi-column projections.
    std::vector<uint8_t> filter_buf;
    if (params.filters && !params.filters->filters.empty()) {
        auto &filters = params.filters->filters;
        push_u8(filter_buf, 3); // tag AND
        push_u32(filter_buf, (uint32_t)filters.size());
        for (auto &kv : filters) {
            duckdb::idx_t projected_col = kv.first; // already the projected batch position
            const duckdb::TableFilter &tf = *kv.second;
            emit_filter(tf, std::vector<duckdb::idx_t>{projected_col}, filter_buf);
        }
    } else {
        // No filters: emit empty AND (keep all rows)
        push_u8(filter_buf, 3); // tag AND
        push_u32(filter_buf, 0); // 0 children
    }

    auto wrapper = duckdb::make_uniq<ArrowArrayStreamWrapper>();
    // Rust fills wrapper->arrow_array_stream in place; the wrapper owns + releases it.
    cbs->produce(reinterpret_cast<void *>(factory_ptr), names.data(), names.size(),
                 filter_buf.data(), filter_buf.size(), &wrapper->arrow_array_stream);
    return wrapper;
}

// DuckDB passes the same registered factory pointer here, typed as ArrowArrayStream* per the
// typedef; reinterpret it back to the factory.
static void ShimGetSchema(ArrowArrayStream *factory_ptr, ArrowSchema &schema) {
    auto *cbs = reinterpret_cast<RustCallbacks *>(factory_ptr);
    cbs->get_schema(reinterpret_cast<void *>(factory_ptr), &schema);
}

extern "C" int ddb_rs_arrow_register(duckdb_connection conn, const char *name, void *factory_ptr) {
    try {
        auto *connection = reinterpret_cast<duckdb::Connection *>(conn);
        duckdb::vector<duckdb::Value> values;
        values.push_back(duckdb::Value::POINTER(reinterpret_cast<uintptr_t>(factory_ptr)));
        values.push_back(duckdb::Value::POINTER(reinterpret_cast<uintptr_t>(&ShimProduce)));
        values.push_back(duckdb::Value::POINTER(reinterpret_cast<uintptr_t>(&ShimGetSchema)));
        connection->TableFunction("arrow_scan", values)->CreateView(name, /*replace*/ true, /*temporary*/ true);
        return 0;
    } catch (...) {
        return 1;
    }
}
