// Licensed under Apache License, Version 2.0.

//! Native CQL collection functions.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.functions.CollectionFcts`

use super::registry::{CqlFunction, FunctionRegistry};
use crate::functions::decimal_math::DecimalValue;
use cassandra_types::comparator::compare_bytes;
use cassandra_types::{CqlType, CqlValue};
use num_bigint::BigInt;
use num_traits::Zero;
use std::cmp::Ordering;
use std::sync::Arc;

pub fn register_all(registry: &FunctionRegistry) {
    for frozen in [false, true] {
        for key_type in collection_map_native_types() {
            for value_type in collection_map_native_types() {
                let arg_type = CqlType::Map(
                    Box::new(key_type.clone()),
                    Box::new(value_type.clone()),
                    frozen,
                );
                registry.register(Arc::new(MapProjectionFunction {
                    projection: MapProjection::Keys,
                    arg_type: arg_type.clone(),
                    return_type: CqlType::Set(Box::new(key_type.clone()), false),
                }));
                registry.register(Arc::new(MapProjectionFunction {
                    projection: MapProjection::Values,
                    arg_type,
                    return_type: CqlType::List(Box::new(value_type.clone()), false),
                }));
            }
        }
    }
    for arg_type in collection_count_arg_types() {
        registry.register(Arc::new(CollectionCountFunction { arg_type }));
    }
    for element_type in collection_extremum_element_types() {
        for frozen in [false, true] {
            for kind in [CollectionKind::List, CollectionKind::Set] {
                let arg_type = kind.cql_type(element_type.clone(), frozen);
                registry.register(Arc::new(CollectionExtremumFunction {
                    extremum: Extremum::Min,
                    arg_type: arg_type.clone(),
                    element_type: element_type.clone(),
                }));
                registry.register(Arc::new(CollectionExtremumFunction {
                    extremum: Extremum::Max,
                    arg_type,
                    element_type: element_type.clone(),
                }));
            }
        }
    }
    for element_type in collection_numeric_element_types() {
        for frozen in [false, true] {
            for kind in [CollectionKind::List, CollectionKind::Set] {
                let arg_type = kind.cql_type(element_type.clone(), frozen);
                registry.register(Arc::new(CollectionNumericFunction {
                    aggregation: NumericAggregation::Sum,
                    arg_type: arg_type.clone(),
                    element_type: element_type.clone(),
                }));
                registry.register(Arc::new(CollectionNumericFunction {
                    aggregation: NumericAggregation::Avg,
                    arg_type,
                    element_type: element_type.clone(),
                }));
            }
        }
    }
}

pub(crate) fn resolve_dynamic(name: &str, arg_types: &[CqlType]) -> Option<Arc<dyn CqlFunction>> {
    if arg_types.len() != 1 {
        return None;
    }

    match name.to_ascii_lowercase().as_str() {
        "map_keys" => {
            let CqlType::Map(key_type, value_type, frozen) = &arg_types[0] else {
                return None;
            };
            Some(Arc::new(MapProjectionFunction {
                projection: MapProjection::Keys,
                arg_type: CqlType::Map(key_type.clone(), value_type.clone(), *frozen),
                return_type: CqlType::Set(Box::new((**key_type).clone()), false),
            }))
        }
        "map_values" => {
            let CqlType::Map(key_type, value_type, frozen) = &arg_types[0] else {
                return None;
            };
            Some(Arc::new(MapProjectionFunction {
                projection: MapProjection::Values,
                arg_type: CqlType::Map(key_type.clone(), value_type.clone(), *frozen),
                return_type: CqlType::List(Box::new((**value_type).clone()), false),
            }))
        }
        "collection_min" | "collection_max" => {
            let element_type = collection_element_type(&arg_types[0])?;
            let extremum = if name.eq_ignore_ascii_case("collection_min") {
                Extremum::Min
            } else {
                Extremum::Max
            };
            Some(Arc::new(CollectionExtremumFunction {
                extremum,
                arg_type: arg_types[0].clone(),
                element_type,
            }))
        }
        _ => None,
    }
}

struct CollectionCountFunction {
    arg_type: CqlType,
}

struct MapProjectionFunction {
    projection: MapProjection,
    arg_type: CqlType,
    return_type: CqlType,
}

#[derive(Debug, Clone, Copy)]
enum MapProjection {
    Keys,
    Values,
}

impl MapProjection {
    fn name(self) -> &'static str {
        match self {
            MapProjection::Keys => "map_keys",
            MapProjection::Values => "map_values",
        }
    }
}

struct CollectionExtremumFunction {
    extremum: Extremum,
    arg_type: CqlType,
    element_type: CqlType,
}

struct CollectionNumericFunction {
    aggregation: NumericAggregation,
    arg_type: CqlType,
    element_type: CqlType,
}

#[derive(Debug, Clone, Copy)]
enum CollectionKind {
    List,
    Set,
}

impl CollectionKind {
    fn cql_type(self, element_type: CqlType, frozen: bool) -> CqlType {
        match self {
            CollectionKind::List => CqlType::List(Box::new(element_type), frozen),
            CollectionKind::Set => CqlType::Set(Box::new(element_type), frozen),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Extremum {
    Min,
    Max,
}

impl Extremum {
    fn name(self) -> &'static str {
        match self {
            Extremum::Min => "collection_min",
            Extremum::Max => "collection_max",
        }
    }

    fn should_replace(self, ordering: Ordering) -> bool {
        match self {
            Extremum::Min => ordering == Ordering::Less,
            Extremum::Max => ordering == Ordering::Greater,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum NumericAggregation {
    Sum,
    Avg,
}

impl NumericAggregation {
    fn name(self) -> &'static str {
        match self {
            NumericAggregation::Sum => "collection_sum",
            NumericAggregation::Avg => "collection_avg",
        }
    }
}

impl CqlFunction for CollectionCountFunction {
    fn name(&self) -> &str {
        "collection_count"
    }

    fn arg_types(&self) -> Vec<CqlType> {
        vec![self.arg_type.clone()]
    }

    fn return_type(&self) -> CqlType {
        CqlType::Int
    }

    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        let Some(bytes) = args.first().and_then(|arg| *arg) else {
            return Ok(None);
        };
        if bytes.len() < 4 {
            return Err(format!(
                "invalid collection value: expected at least 4 bytes, got {}",
                bytes.len()
            ));
        }
        let count = i32::from_be_bytes(bytes[0..4].try_into().expect("slice has 4 bytes"));
        if count < 0 {
            return Err(format!("invalid collection value: negative count {count}"));
        }
        Ok(Some(count.to_be_bytes().to_vec()))
    }
}

impl CqlFunction for MapProjectionFunction {
    fn name(&self) -> &str {
        self.projection.name()
    }

    fn arg_types(&self) -> Vec<CqlType> {
        vec![self.arg_type.clone()]
    }

    fn return_type(&self) -> CqlType {
        self.return_type.clone()
    }

    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        let Some(bytes) = args.first().and_then(|arg| *arg) else {
            return Ok(None);
        };
        let map = match CqlValue::deserialize_value(&self.arg_type, bytes)
            .map_err(|error| error.to_string())?
        {
            CqlValue::Map(entries) => entries,
            _ => unreachable!("deserialize_value(Map) returns Map"),
        };

        let result = match self.projection {
            MapProjection::Keys => {
                CqlValue::Set(map.into_iter().map(|(key, _)| key).collect()).serialize_value()
            }
            MapProjection::Values => {
                CqlValue::List(map.into_iter().map(|(_, value)| value).collect()).serialize_value()
            }
        };
        Ok(Some(result))
    }
}

impl CqlFunction for CollectionExtremumFunction {
    fn name(&self) -> &str {
        self.extremum.name()
    }

    fn arg_types(&self) -> Vec<CqlType> {
        vec![self.arg_type.clone()]
    }

    fn return_type(&self) -> CqlType {
        self.element_type.clone()
    }

    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        let Some(bytes) = args.first().and_then(|arg| *arg) else {
            return Ok(None);
        };
        let elements = read_collection_elements(bytes)?;
        let mut best: Option<&[u8]> = None;
        for element in elements {
            if best.is_none_or(|current| {
                self.extremum
                    .should_replace(compare_bytes(&self.element_type, element, current))
            }) {
                best = Some(element);
            }
        }
        Ok(best.map(Vec::from))
    }
}

impl CqlFunction for CollectionNumericFunction {
    fn name(&self) -> &str {
        self.aggregation.name()
    }

    fn arg_types(&self) -> Vec<CqlType> {
        vec![self.arg_type.clone()]
    }

    fn return_type(&self) -> CqlType {
        self.element_type.clone()
    }

    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        let Some(bytes) = args.first().and_then(|arg| *arg) else {
            return Ok(None);
        };
        let elements = read_collection_elements(bytes)?;
        eval_numeric_collection(self.aggregation, &self.element_type, &elements).map(Some)
    }
}

fn collection_count_arg_types() -> Vec<CqlType> {
    let any = Box::new(CqlType::Empty);
    vec![
        CqlType::List(any.clone(), false),
        CqlType::Set(any.clone(), false),
        CqlType::Map(any.clone(), any, false),
    ]
}

fn collection_element_type(cql_type: &CqlType) -> Option<CqlType> {
    match cql_type {
        CqlType::List(element_type, _) | CqlType::Set(element_type, _) => {
            Some((**element_type).clone())
        }
        _ => None,
    }
}

fn collection_map_native_types() -> Vec<CqlType> {
    vec![
        CqlType::Ascii,
        CqlType::Bigint,
        CqlType::Blob,
        CqlType::Boolean,
        CqlType::Date,
        CqlType::Decimal,
        CqlType::Double,
        CqlType::Float,
        CqlType::Inet,
        CqlType::Int,
        CqlType::Smallint,
        CqlType::Timestamp,
        CqlType::Time,
        CqlType::Timeuuid,
        CqlType::Tinyint,
        CqlType::Uuid,
        CqlType::Varchar,
        CqlType::Varint,
        CqlType::Duration,
    ]
}

fn collection_numeric_element_types() -> Vec<CqlType> {
    vec![
        CqlType::Tinyint,
        CqlType::Smallint,
        CqlType::Int,
        CqlType::Bigint,
        CqlType::Varint,
        CqlType::Decimal,
        CqlType::Float,
        CqlType::Double,
    ]
}

fn collection_extremum_element_types() -> Vec<CqlType> {
    vec![
        CqlType::Ascii,
        CqlType::Bigint,
        CqlType::Blob,
        CqlType::Boolean,
        CqlType::Date,
        CqlType::Decimal,
        CqlType::Double,
        CqlType::Float,
        CqlType::Inet,
        CqlType::Int,
        CqlType::Smallint,
        CqlType::Timestamp,
        CqlType::Time,
        CqlType::Timeuuid,
        CqlType::Tinyint,
        CqlType::Uuid,
        CqlType::Varchar,
        CqlType::Varint,
        CqlType::Duration,
    ]
}

fn read_collection_elements(bytes: &[u8]) -> Result<Vec<&[u8]>, String> {
    if bytes.len() < 4 {
        return Err(format!(
            "invalid collection value: expected at least 4 bytes, got {}",
            bytes.len()
        ));
    }
    let count = i32::from_be_bytes(bytes[0..4].try_into().expect("slice has 4 bytes"));
    if count < 0 {
        return Err(format!("invalid collection value: negative count {count}"));
    }

    let mut elements = Vec::with_capacity(count as usize);
    let mut pos = 4usize;
    for _ in 0..count {
        if pos + 4 > bytes.len() {
            return Err("invalid collection value: truncated element length".to_string());
        }
        let len = i32::from_be_bytes(bytes[pos..pos + 4].try_into().expect("slice has 4 bytes"));
        pos += 4;
        if len < 0 {
            return Err(format!(
                "invalid collection value: negative element length {len}"
            ));
        }
        let len = len as usize;
        let end = pos
            .checked_add(len)
            .ok_or_else(|| "invalid collection value: element length overflow".to_string())?;
        if end > bytes.len() {
            return Err("invalid collection value: truncated element bytes".to_string());
        }
        elements.push(&bytes[pos..end]);
        pos = end;
    }
    if pos != bytes.len() {
        return Err("invalid collection value: trailing bytes".to_string());
    }
    Ok(elements)
}

fn eval_numeric_collection(
    aggregation: NumericAggregation,
    element_type: &CqlType,
    elements: &[&[u8]],
) -> Result<Vec<u8>, String> {
    match (aggregation, element_type) {
        (NumericAggregation::Sum, CqlType::Tinyint) => {
            let mut sum = 0i8;
            for element in elements {
                sum = sum.wrapping_add(read_i8(element)?);
            }
            Ok(vec![sum as u8])
        }
        (NumericAggregation::Avg, CqlType::Tinyint) => {
            let avg = integer_average(elements, |element| Ok(read_i8(element)? as i128))?;
            Ok(vec![(avg as i8) as u8])
        }
        (NumericAggregation::Sum, CqlType::Smallint) => {
            let mut sum = 0i16;
            for element in elements {
                sum = sum.wrapping_add(read_i16(element)?);
            }
            Ok(sum.to_be_bytes().to_vec())
        }
        (NumericAggregation::Avg, CqlType::Smallint) => {
            let avg = integer_average(elements, |element| Ok(read_i16(element)? as i128))?;
            Ok((avg as i16).to_be_bytes().to_vec())
        }
        (NumericAggregation::Sum, CqlType::Int) => {
            let mut sum = 0i32;
            for element in elements {
                sum = sum.wrapping_add(read_i32(element)?);
            }
            Ok(sum.to_be_bytes().to_vec())
        }
        (NumericAggregation::Avg, CqlType::Int) => {
            let avg = integer_average(elements, |element| Ok(read_i32(element)? as i128))?;
            Ok((avg as i32).to_be_bytes().to_vec())
        }
        (NumericAggregation::Sum, CqlType::Bigint) => {
            let mut sum = 0i64;
            for element in elements {
                sum = sum.wrapping_add(read_i64(element)?);
            }
            Ok(sum.to_be_bytes().to_vec())
        }
        (NumericAggregation::Avg, CqlType::Bigint) => {
            let avg = integer_average(elements, |element| Ok(read_i64(element)? as i128))?;
            Ok((avg as i64).to_be_bytes().to_vec())
        }
        (NumericAggregation::Sum, CqlType::Varint) => {
            Ok(sum_varints(elements)?.to_signed_bytes_be())
        }
        (NumericAggregation::Avg, CqlType::Varint) => {
            if elements.is_empty() {
                return Ok(BigInt::zero().to_signed_bytes_be());
            }
            Ok((sum_varints(elements)? / BigInt::from(elements.len())).to_signed_bytes_be())
        }
        (NumericAggregation::Sum, CqlType::Decimal) => Ok(sum_decimals(elements)?.to_bytes()),
        (NumericAggregation::Avg, CqlType::Decimal) => Ok(avg_decimals(elements)?.to_bytes()),
        (NumericAggregation::Sum, CqlType::Float) => Ok((kahan_sum(elements, read_f32_as_f64)?
            as f32)
            .to_be_bytes()
            .to_vec()),
        (NumericAggregation::Avg, CqlType::Float) => {
            let avg = float_average(elements, read_f32_as_f64)? as f32;
            Ok(avg.to_be_bytes().to_vec())
        }
        (NumericAggregation::Sum, CqlType::Double) => {
            Ok(kahan_sum(elements, read_f64)?.to_be_bytes().to_vec())
        }
        (NumericAggregation::Avg, CqlType::Double) => {
            Ok(float_average(elements, read_f64)?.to_be_bytes().to_vec())
        }
        _ => Err(format!(
            "unsupported collection numeric element type {}",
            element_type.cql_name()
        )),
    }
}

fn integer_average<F>(elements: &[&[u8]], mut read: F) -> Result<i128, String>
where
    F: FnMut(&[u8]) -> Result<i128, String>,
{
    if elements.is_empty() {
        return Ok(0);
    }
    let mut sum = 0i128;
    for element in elements {
        sum += read(element)?;
    }
    Ok(sum / elements.len() as i128)
}

fn sum_varints(elements: &[&[u8]]) -> Result<BigInt, String> {
    let mut sum = BigInt::zero();
    for element in elements {
        sum += BigInt::from_signed_bytes_be(element);
    }
    Ok(sum)
}

fn sum_decimals(elements: &[&[u8]]) -> Result<DecimalValue, String> {
    let mut sum = DecimalValue::from_i64(0);
    for element in elements {
        sum = sum.add(&DecimalValue::from_bytes(element)?);
    }
    Ok(sum)
}

fn avg_decimals(elements: &[&[u8]]) -> Result<DecimalValue, String> {
    let mut avg = DecimalValue::from_i64(0);
    for (index, element) in elements.iter().enumerate() {
        let count = i64::try_from(index + 1)
            .map_err(|_| "decimal average element count overflow".to_string())?;
        let number = DecimalValue::from_bytes(element)?;
        let delta = number.subtract(&avg);
        avg = avg.add(&delta.divide_i64_half_even_same_scale(count)?);
    }
    Ok(avg)
}

fn kahan_sum<F>(elements: &[&[u8]], mut read: F) -> Result<f64, String>
where
    F: FnMut(&[u8]) -> Result<f64, String>,
{
    let mut sum = 0f64;
    let mut compensation = 0f64;
    let mut simple_sum = 0f64;
    for element in elements {
        let value = read(element)?;
        simple_sum += value;
        let tmp = value - compensation;
        let rounded = sum + tmp;
        compensation = (rounded - sum) - tmp;
        sum = rounded;
    }
    let result = sum + compensation;
    if result.is_nan() && simple_sum.is_infinite() {
        Ok(simple_sum)
    } else {
        Ok(result)
    }
}

fn float_average<F>(elements: &[&[u8]], read: F) -> Result<f64, String>
where
    F: FnMut(&[u8]) -> Result<f64, String>,
{
    if elements.is_empty() {
        return Ok(0f64);
    }
    Ok(kahan_sum(elements, read)? / elements.len() as f64)
}

fn read_i8(bytes: &[u8]) -> Result<i8, String> {
    if bytes.len() != 1 {
        return Err(format!(
            "invalid tinyint bytes: expected 1, got {}",
            bytes.len()
        ));
    }
    Ok(bytes[0] as i8)
}

fn read_i16(bytes: &[u8]) -> Result<i16, String> {
    let array: [u8; 2] = bytes
        .try_into()
        .map_err(|_| format!("invalid smallint bytes: expected 2, got {}", bytes.len()))?;
    Ok(i16::from_be_bytes(array))
}

fn read_i32(bytes: &[u8]) -> Result<i32, String> {
    let array: [u8; 4] = bytes
        .try_into()
        .map_err(|_| format!("invalid int bytes: expected 4, got {}", bytes.len()))?;
    Ok(i32::from_be_bytes(array))
}

fn read_i64(bytes: &[u8]) -> Result<i64, String> {
    let array: [u8; 8] = bytes
        .try_into()
        .map_err(|_| format!("invalid bigint bytes: expected 8, got {}", bytes.len()))?;
    Ok(i64::from_be_bytes(array))
}

fn read_f32_as_f64(bytes: &[u8]) -> Result<f64, String> {
    let array: [u8; 4] = bytes
        .try_into()
        .map_err(|_| format!("invalid float bytes: expected 4, got {}", bytes.len()))?;
    Ok(f32::from_be_bytes(array) as f64)
}

fn read_f64(bytes: &[u8]) -> Result<f64, String> {
    let array: [u8; 8] = bytes
        .try_into()
        .map_err(|_| format!("invalid double bytes: expected 8, got {}", bytes.len()))?;
    Ok(f64::from_be_bytes(array))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::functions::FunctionRegistry;

    #[test]
    fn map_keys_and_values_resolve_for_native_map_types() {
        let registry = FunctionRegistry::with_builtins();
        let map_type = CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Int), false);

        let keys = registry.resolve("map_keys", &[map_type.clone()]).unwrap();
        assert_eq!(
            keys.return_type(),
            CqlType::Set(Box::new(CqlType::Varchar), false)
        );

        let values = registry.resolve("map_values", &[map_type]).unwrap();
        assert_eq!(
            values.return_type(),
            CqlType::List(Box::new(CqlType::Int), false)
        );
    }

    #[test]
    fn map_keys_and_values_project_serialized_maps() {
        let registry = FunctionRegistry::with_builtins();
        let map_type = CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Int), false);
        let map = CqlValue::Map(vec![
            (CqlValue::Varchar("a".to_string()), CqlValue::Int(1)),
            (CqlValue::Varchar("b".to_string()), CqlValue::Int(2)),
        ])
        .serialize_value();

        let keys = registry.resolve("map_keys", &[map_type.clone()]).unwrap();
        let key_bytes = keys.execute(&[Some(&map)]).unwrap().unwrap();
        assert_eq!(
            CqlValue::deserialize_value(&keys.return_type(), &key_bytes).unwrap(),
            CqlValue::Set(vec![
                CqlValue::Varchar("a".to_string()),
                CqlValue::Varchar("b".to_string())
            ])
        );

        let values = registry.resolve("map_values", &[map_type]).unwrap();
        let value_bytes = values.execute(&[Some(&map)]).unwrap().unwrap();
        assert_eq!(
            CqlValue::deserialize_value(&values.return_type(), &value_bytes).unwrap(),
            CqlValue::List(vec![CqlValue::Int(1), CqlValue::Int(2)])
        );
    }

    #[test]
    fn map_keys_and_values_resolve_for_complex_map_types() {
        let registry = FunctionRegistry::with_builtins();
        let key_type = CqlType::Tuple(vec![CqlType::Int, CqlType::Varchar]);
        let value_type = CqlType::List(Box::new(CqlType::Varchar), true);
        let map_type = CqlType::Map(
            Box::new(key_type.clone()),
            Box::new(value_type.clone()),
            true,
        );
        let map = CqlValue::Map(vec![(
            CqlValue::Tuple(vec![
                Some(CqlValue::Int(7)),
                Some(CqlValue::Varchar("seven".to_string())),
            ]),
            CqlValue::List(vec![
                CqlValue::Varchar("a".to_string()),
                CqlValue::Varchar("b".to_string()),
            ]),
        )])
        .serialize_value();

        let keys = registry.resolve("map_keys", &[map_type.clone()]).unwrap();
        assert_eq!(keys.return_type(), CqlType::Set(Box::new(key_type), false));
        let key_bytes = keys.execute(&[Some(&map)]).unwrap().unwrap();
        assert_eq!(
            CqlValue::deserialize_value(&keys.return_type(), &key_bytes).unwrap(),
            CqlValue::Set(vec![CqlValue::Tuple(vec![
                Some(CqlValue::Int(7)),
                Some(CqlValue::Varchar("seven".to_string()))
            ])])
        );

        let values = registry.resolve("map_values", &[map_type]).unwrap();
        assert_eq!(
            values.return_type(),
            CqlType::List(Box::new(value_type), false)
        );
        let value_bytes = values.execute(&[Some(&map)]).unwrap().unwrap();
        assert_eq!(
            CqlValue::deserialize_value(&values.return_type(), &value_bytes).unwrap(),
            CqlValue::List(vec![CqlValue::List(vec![
                CqlValue::Varchar("a".to_string()),
                CqlValue::Varchar("b".to_string())
            ])])
        );
    }

    #[test]
    fn map_keys_preserves_null_input() {
        let registry = FunctionRegistry::with_builtins();
        let map_type = CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Int), true);
        let keys = registry.resolve("map_keys", &[map_type]).unwrap();

        assert_eq!(keys.execute(&[None]).unwrap(), None);
    }

    #[test]
    fn collection_count_resolves_for_list_set_and_map() {
        let registry = FunctionRegistry::with_builtins();
        assert_eq!(
            registry
                .resolve(
                    "collection_count",
                    &[CqlType::List(Box::new(CqlType::Int), false)]
                )
                .unwrap()
                .return_type(),
            CqlType::Int
        );
        assert!(
            registry
                .resolve(
                    "collection_count",
                    &[CqlType::Set(Box::new(CqlType::Varchar), false)]
                )
                .is_some()
        );
        assert!(
            registry
                .resolve(
                    "collection_count",
                    &[CqlType::Map(
                        Box::new(CqlType::Varchar),
                        Box::new(CqlType::Bigint),
                        false
                    )]
                )
                .is_some()
        );
    }

    #[test]
    fn collection_count_counts_serialized_collections() {
        let registry = FunctionRegistry::with_builtins();
        let function = registry
            .resolve(
                "collection_count",
                &[CqlType::List(Box::new(CqlType::Int), false)],
            )
            .unwrap();
        let list = CqlValue::List(vec![CqlValue::Int(1), CqlValue::Int(2), CqlValue::Int(3)])
            .serialize_value();

        assert_eq!(
            function.execute(&[Some(&list)]).unwrap(),
            Some(3i32.to_be_bytes().to_vec())
        );

        let empty_set = CqlValue::Set(vec![]).serialize_value();
        assert_eq!(
            function.execute(&[Some(&empty_set)]).unwrap(),
            Some(0i32.to_be_bytes().to_vec())
        );
    }

    #[test]
    fn collection_count_preserves_null_and_rejects_invalid_wire_values() {
        let registry = FunctionRegistry::with_builtins();
        let function = registry
            .resolve(
                "collection_count",
                &[CqlType::Map(
                    Box::new(CqlType::Int),
                    Box::new(CqlType::Varchar),
                    false,
                )],
            )
            .unwrap();

        assert_eq!(function.execute(&[None]).unwrap(), None);
        assert_eq!(
            function.execute(&[Some(&[0, 1, 2])]),
            Err("invalid collection value: expected at least 4 bytes, got 3".to_string())
        );
        assert_eq!(
            function.execute(&[Some(&(-1i32).to_be_bytes())]),
            Err("invalid collection value: negative count -1".to_string())
        );
    }

    #[test]
    fn collection_min_and_max_resolve_for_native_list_and_set_elements() {
        let registry = FunctionRegistry::with_builtins();
        let list_int = CqlType::List(Box::new(CqlType::Int), false);
        let min = registry.resolve("collection_min", &[list_int]).unwrap();
        assert_eq!(min.return_type(), CqlType::Int);

        let frozen_set_text = CqlType::Set(Box::new(CqlType::Varchar), true);
        let max = registry
            .resolve("collection_max", &[frozen_set_text])
            .unwrap();
        assert_eq!(max.return_type(), CqlType::Varchar);
    }

    #[test]
    fn collection_min_and_max_use_cql_comparison() {
        let registry = FunctionRegistry::with_builtins();
        let list_int = CqlType::List(Box::new(CqlType::Int), false);
        let min = registry
            .resolve("collection_min", &[list_int.clone()])
            .unwrap();
        let max = registry.resolve("collection_max", &[list_int]).unwrap();
        let list = CqlValue::List(vec![CqlValue::Int(10), CqlValue::Int(-2), CqlValue::Int(7)])
            .serialize_value();

        assert_eq!(
            min.execute(&[Some(&list)]).unwrap(),
            Some((-2i32).to_be_bytes().to_vec())
        );
        assert_eq!(
            max.execute(&[Some(&list)]).unwrap(),
            Some(10i32.to_be_bytes().to_vec())
        );
    }

    #[test]
    fn collection_min_and_max_resolve_for_complex_list_and_set_elements() {
        let registry = FunctionRegistry::with_builtins();
        let tuple_type = CqlType::Tuple(vec![CqlType::Int, CqlType::Varchar]);
        let list_type = CqlType::List(Box::new(tuple_type.clone()), true);
        let list = CqlValue::List(vec![
            CqlValue::Tuple(vec![
                Some(CqlValue::Int(3)),
                Some(CqlValue::Varchar("three".to_string())),
            ]),
            CqlValue::Tuple(vec![
                Some(CqlValue::Int(1)),
                Some(CqlValue::Varchar("one".to_string())),
            ]),
        ])
        .serialize_value();

        let min = registry
            .resolve("collection_min", &[list_type.clone()])
            .unwrap();
        assert_eq!(min.return_type(), tuple_type);
        assert_eq!(
            CqlValue::deserialize_value(
                &min.return_type(),
                &min.execute(&[Some(&list)]).unwrap().unwrap()
            )
            .unwrap(),
            CqlValue::Tuple(vec![
                Some(CqlValue::Int(1)),
                Some(CqlValue::Varchar("one".to_string()))
            ])
        );

        let nested_type = CqlType::List(Box::new(CqlType::Int), true);
        let set_type = CqlType::Set(Box::new(nested_type.clone()), true);
        let set = CqlValue::Set(vec![
            CqlValue::List(vec![CqlValue::Int(1), CqlValue::Int(2)]),
            CqlValue::List(vec![CqlValue::Int(1), CqlValue::Int(5)]),
        ])
        .serialize_value();
        let max = registry.resolve("collection_max", &[set_type]).unwrap();
        assert_eq!(max.return_type(), nested_type);
        assert_eq!(
            CqlValue::deserialize_value(
                &max.return_type(),
                &max.execute(&[Some(&set)]).unwrap().unwrap()
            )
            .unwrap(),
            CqlValue::List(vec![CqlValue::Int(1), CqlValue::Int(5)])
        );
    }

    #[test]
    fn collection_min_returns_null_for_empty_collections() {
        let registry = FunctionRegistry::with_builtins();
        let function = registry
            .resolve(
                "collection_min",
                &[CqlType::Set(Box::new(CqlType::Varchar), false)],
            )
            .unwrap();
        let empty_set = CqlValue::Set(vec![]).serialize_value();

        assert_eq!(function.execute(&[Some(&empty_set)]).unwrap(), None);
        assert_eq!(function.execute(&[None]).unwrap(), None);
    }

    #[test]
    fn collection_sum_and_avg_resolve_for_fixed_numeric_collections() {
        let registry = FunctionRegistry::with_builtins();
        let list_double = CqlType::List(Box::new(CqlType::Double), false);
        let sum = registry.resolve("collection_sum", &[list_double]).unwrap();
        assert_eq!(sum.return_type(), CqlType::Double);

        let frozen_set_bigint = CqlType::Set(Box::new(CqlType::Bigint), true);
        let avg = registry
            .resolve("collection_avg", &[frozen_set_bigint])
            .unwrap();
        assert_eq!(avg.return_type(), CqlType::Bigint);
    }

    #[test]
    fn collection_sum_uses_java_style_integer_wrapping() {
        let registry = FunctionRegistry::with_builtins();
        let function = registry
            .resolve(
                "collection_sum",
                &[CqlType::List(Box::new(CqlType::Tinyint), false)],
            )
            .unwrap();
        let list =
            CqlValue::List(vec![CqlValue::Tinyint(120), CqlValue::Tinyint(10)]).serialize_value();

        assert_eq!(function.execute(&[Some(&list)]).unwrap(), Some(vec![130]));
    }

    #[test]
    fn collection_avg_truncates_integer_results_and_empty_is_zero() {
        let registry = FunctionRegistry::with_builtins();
        let function = registry
            .resolve(
                "collection_avg",
                &[CqlType::List(Box::new(CqlType::Int), false)],
            )
            .unwrap();
        let list = CqlValue::List(vec![CqlValue::Int(5), CqlValue::Int(2)]).serialize_value();
        let empty = CqlValue::List(vec![]).serialize_value();

        assert_eq!(
            function.execute(&[Some(&list)]).unwrap(),
            Some(3i32.to_be_bytes().to_vec())
        );
        assert_eq!(
            function.execute(&[Some(&empty)]).unwrap(),
            Some(0i32.to_be_bytes().to_vec())
        );
    }

    #[test]
    fn collection_sum_and_avg_support_floats() {
        let registry = FunctionRegistry::with_builtins();
        let list_float = CqlType::List(Box::new(CqlType::Float), false);
        let sum = registry
            .resolve("collection_sum", &[list_float.clone()])
            .unwrap();
        let avg = registry.resolve("collection_avg", &[list_float]).unwrap();
        let list = CqlValue::List(vec![
            CqlValue::Float(1.25),
            CqlValue::Float(2.25),
            CqlValue::Float(3.5),
        ])
        .serialize_value();

        assert_eq!(
            sum.execute(&[Some(&list)]).unwrap(),
            Some(7.0f32.to_be_bytes().to_vec())
        );
        assert_eq!(
            avg.execute(&[Some(&list)]).unwrap(),
            Some((7.0f32 / 3.0).to_be_bytes().to_vec())
        );
    }

    #[test]
    fn collection_sum_and_avg_support_arbitrary_precision_varints() {
        let registry = FunctionRegistry::with_builtins();
        let list_varint = CqlType::List(Box::new(CqlType::Varint), false);
        let sum = registry
            .resolve("collection_sum", &[list_varint.clone()])
            .unwrap();
        let avg = registry.resolve("collection_avg", &[list_varint]).unwrap();
        let huge = BigInt::parse_bytes(b"123456789012345678901234567890", 10)
            .unwrap()
            .to_signed_bytes_be();
        let list = CqlValue::List(vec![
            CqlValue::Varint(huge),
            CqlValue::Varint(BigInt::from(10).to_signed_bytes_be()),
        ])
        .serialize_value();

        assert_eq!(sum.return_type(), CqlType::Varint);
        let result = sum.execute(&[Some(&list)]).unwrap().unwrap();
        assert_eq!(
            BigInt::from_signed_bytes_be(&result).to_string(),
            "123456789012345678901234567900"
        );

        let result = avg.execute(&[Some(&list)]).unwrap().unwrap();
        assert_eq!(
            BigInt::from_signed_bytes_be(&result).to_string(),
            "61728394506172839450617283950"
        );
    }

    #[test]
    fn collection_sum_and_avg_support_decimals() {
        let registry = FunctionRegistry::with_builtins();
        let list_decimal = CqlType::List(Box::new(CqlType::Decimal), false);
        let sum = registry
            .resolve("collection_sum", &[list_decimal.clone()])
            .unwrap();
        let avg = registry.resolve("collection_avg", &[list_decimal]).unwrap();
        let list = CqlValue::List(vec![
            CqlValue::Decimal {
                scale: 1,
                unscaled: BigInt::from(10).to_signed_bytes_be(),
            },
            CqlValue::Decimal {
                scale: 1,
                unscaled: BigInt::from(20).to_signed_bytes_be(),
            },
        ])
        .serialize_value();

        assert_eq!(sum.return_type(), CqlType::Decimal);
        let result = sum.execute(&[Some(&list)]).unwrap().unwrap();
        assert_eq!(
            cassandra_types::bigint::decimal_to_string(&result).unwrap(),
            "3.0"
        );

        let result = avg.execute(&[Some(&list)]).unwrap().unwrap();
        assert_eq!(
            cassandra_types::bigint::decimal_to_string(&result).unwrap(),
            "1.5"
        );
    }
}
