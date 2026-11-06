//! The `UNION` logical type.
//!
//! Rust enums of single-field variants map to `UNION` through `#[derive(AsDuckDBUnion)]`, which
//! generates typed values, borrowed vector rows, and vector writes. [`Union`] is the untyped fallback.

use crate::{
    Result,
    enums::LogicalTypeID,
    vector::{HasChildren, VectorElement, VectorWriter},
};

/// Accepts any `UNION` vector without checking its members.
///
/// Rows only report NULLs. Derive `AsDuckDBUnion` for typed members, or use
/// [`Vector::get_value_slow`](crate::vector::Vector::get_value_slow) and
/// [`Vector::write_value_slow`](crate::vector::Vector::write_value_slow) when the schema is only known at runtime.
///
/// ```
/// use duckdb_neo::{AsDuckDBUnion, Result, vector::Vector};
///
/// #[derive(AsDuckDBUnion)]
/// enum Input {
///     #[duckdb(rename = "num")]
///     Num(i16),
///     #[duckdb(rename = "str")]
///     Str(Option<String>),
/// }
///
/// fn describe(vector: &Vector<'_, Input>) -> Result<String> {
///     Ok(match vector.reader()?.get(0)? {
///         Some(InputRef::Num(num)) => format!("{num:?}"),
///         Some(InputRef::Str(text)) => format!("{text:?}"),
///         None => "NULL".to_owned(),
///     })
/// }
///
/// fn write(vector: &mut Vector<'_, Input>) -> Result<()> {
///     vector.writer()?.set(0, Some(Input::Str(Some("duck".to_owned()))))
/// }
/// ```
#[derive(Debug)]
pub struct Union;

// SAFETY: there is no physical row data, and `get` reads nothing.
unsafe impl VectorElement for Union {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::Union;
    type Ref<'a>
        = ()
    where
        Self: 'a;
    type Internal<'a> = ();

    unsafe fn get<'a, U: VectorElement>(
        _reader: &crate::vector::VectorReader<'a, U>,
        _physical: usize,
        _logical: usize,
    ) -> Self::Ref<'a>
    where
        Self: 'a,
    {
    }
}

// SAFETY: the tag and every member have one row per parent row.
unsafe impl HasChildren for Union {
    fn child_len(writer: &VectorWriter<'_, Self>) -> Result<usize> {
        Ok(writer.len())
    }
}

/// Runtime support for the union derive in `duckdb-loadable-macros`; not a stable API.
pub mod derive_support {
    use crate::{
        Error, Parameters, Result, connection::FFILink, data_chunk::VectorCollection, logical_type::LogicalType,
        logical_type::LogicalTypeID, value::Value, vector::VectorElement,
    };

    /// Build a `union_type` value whose member `tag` holds `member`.
    ///
    /// Casting to a single-member union first keeps the tag even when several members share a type.
    pub fn union_value<C: FFILink + ?Sized>(
        link: &C,
        union_type: &LogicalType,
        tag: usize,
        member: &Value,
    ) -> Result<Value> {
        let (name, member_type) = union_type.get_param(tag)?;
        let single = link.logical_type_create("UNION", Parameters::named(&[(name, &member_type)]))?;
        link.value_cast(&link.value_cast(member, &single)?, union_type)
    }

    /// Check that `value` is a `UNION` with members `names`, and return the active one's index and value.
    ///
    /// Members are matched by name, preferring an exact over a case-insensitive match.
    pub fn union_member(value: &Value, names: &[&str]) -> Result<(usize, Value)> {
        let logical_type = value.logical_type()?;
        if logical_type.id()? != LogicalTypeID::Union {
            return Err(Error::invalid_input(format!(
                "Expected UNION value, found {}",
                logical_type.to_text()?
            )));
        }
        let members = logical_type.get_params()?;
        if members.len() != names.len() {
            return Err(Error::invalid_input(format!(
                "UNION has {} members, expected {}",
                members.len(),
                names.len()
            )));
        }
        let tag = value
            .get_child(0)?
            .get::<u8>()?
            .ok_or_else(|| Error::invalid_input("UNION tag is NULL"))? as usize;
        let mut active = None;
        for (index, name) in names.iter().enumerate() {
            let member = members
                .iter()
                .position(|(member, _)| member == name)
                .or_else(|| members.iter().position(|(member, _)| member.eq_ignore_ascii_case(name)))
                .ok_or_else(|| Error::invalid_input(format!("UNION member '{name}' is missing")))?;
            if member == tag {
                active = Some(index);
            }
        }
        let active = active.ok_or_else(|| Error::invalid_input(format!("UNION tag {tag} is out of range")))?;
        Ok((active, value.get_child(1)?))
    }

    /// Reject a SQL NULL for a member whose Rust type is not optional.
    pub fn required_member<T>(value: Option<T>, name: &str) -> Result<T> {
        value.ok_or_else(|| Error::invalid_input(format!("UNION member '{name}' is NULL")))
    }

    /// Check that a vector type is a `UNION` with `count` members, and a `UTINYINT` tag child.
    pub fn validate_union_layout(
        other: &LogicalType,
        children: Option<&VectorCollection>,
        count: usize,
    ) -> Result<bool> {
        if other.id()? != LogicalTypeID::Union || other.param_count()? != count {
            return Ok(false);
        }
        let Some(children) = children.filter(|children| children.col_count() == count + 1) else {
            return Ok(false);
        };
        let tag = children.get_untyped_vector_at(0)?;
        u8::validate(tag.logical_type(), tag.children())
    }

    /// Check the name (case-insensitively) and child vector of the member at `index`.
    pub fn validate_union_member<T: VectorElement>(
        other: &LogicalType,
        children: Option<&VectorCollection>,
        index: usize,
        name: &str,
    ) -> Result<bool> {
        let Some(children) = children.filter(|children| index + 1 < children.col_count()) else {
            return Ok(false);
        };
        if !other.get_param(index)?.0.eq_ignore_ascii_case(name) {
            return Ok(false);
        }
        let child = children.get_untyped_vector_at(index + 1)?;
        T::validate(child.logical_type(), child.children())
    }

    /// Report a valid row whose tag names no member, which DuckDB never produces.
    #[cold]
    pub fn invalid_union_tag(tag: Option<u8>, count: usize) -> ! {
        panic!("UNION row has tag {tag:?}, but the union has {count} members")
    }
}

#[cfg(test)]
mod tests {
    use duckdb_loadable_macros::AsDuckDBUnion;

    use super::*;
    use crate::{
        DuckDBType, Parameters, ToValue,
        data_chunk::DataChunk,
        environment::{Environment, StorageLocation},
        vector::Unknown,
    };

    #[derive(Debug, PartialEq, AsDuckDBUnion)]
    enum NumOrText {
        #[duckdb(rename = "num")]
        Num(i32),
        #[duckdb(rename = "str")]
        Text(String),
    }

    #[derive(Debug, AsDuckDBUnion)]
    #[allow(dead_code)]
    enum Swapped {
        #[duckdb(rename = "num")]
        Num(String),
        #[duckdb(rename = "str")]
        Text(i32),
    }

    #[test]
    fn union_marker_reads_rows_and_slow_values() -> Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
        let chunk = conn
            .query(
                "SELECT unnest([union_value(str := 'duck'), NULL])::UNION(num INTEGER, str VARCHAR)",
                Parameters::None,
            )?
            .next_chunk()?
            .unwrap();
        let vector = chunk.get_vector_at::<Union>(0)?;
        assert!(vector.reader()?.get(0)?.is_some());
        assert!(vector.reader()?.get(1)?.is_none());
        assert_eq!(
            vector.get_value_slow(0)?.get::<NumOrText>()?,
            Some(NumOrText::Text("duck".to_owned()))
        );
        assert!(
            conn.query("SELECT {'num': 1}", Parameters::None)?
                .next_chunk()?
                .unwrap()
                .get_vector_at::<Union>(0)
                .is_err()
        );

        let mut chunk = DataChunk::create(&conn, &[NumOrText::logical_type(&conn)?], true)?;
        {
            let mut vector = chunk.get_vector_at_mut::<Union>(0)?;
            vector.set_size(2)?;
            vector.write_value_slow(0, NumOrText::Num(7).value(&conn)?)?;
            vector.set_null_slow(1)?;
        }
        let vector = chunk.get_vector_at::<NumOrText>(0)?;
        let reader = vector.reader()?;
        assert_eq!(reader.get(0)?, Some(NumOrTextRef::Num(Some(&7))));
        assert!(reader.get(1)?.is_none());
        Ok(())
    }

    #[test]
    fn union_type_mismatch_preserves_row() -> Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
        let value = conn
            .query(
                "SELECT union_value(str := 'duck')::UNION(num INTEGER, str VARCHAR)",
                Parameters::None,
            )?
            .next_chunk()?
            .unwrap()
            .get_vector_at::<Union>(0)?
            .get_value_slow(0)?;
        let mut chunk = DataChunk::create(&conn, &[value.logical_type()?], true)?;
        {
            let mut vector = chunk.get_vector_at_mut::<Union>(0)?;
            vector.set_size(2)?;
            vector.write_value_slow(0, value)?;
            vector.set_null_slow(1)?;
        }
        assert!(chunk.get_vector_at_mut::<Swapped>(0).is_err());
        assert!(chunk.get_vector_at_mut::<Unknown>(0)?.cast::<Swapped>().is_err());
        let vector = chunk.get_vector_at::<NumOrText>(0)?;
        let reader = vector.reader()?;
        assert_eq!(reader.get(0)?, Some(NumOrTextRef::Text(Some("duck"))));
        assert!(reader.get(1)?.is_none());
        Ok(())
    }
}
