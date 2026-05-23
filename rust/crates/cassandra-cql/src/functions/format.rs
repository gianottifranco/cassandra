// Licensed under Apache License, Version 2.0.

//! Native formatting functions for time and byte quantities.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.functions.FormatFcts`

use super::registry::{CqlFunction, FunctionRegistry};
use cassandra_types::bigint;
use cassandra_types::{CqlType, CqlValue};
use std::sync::Arc;

const FORMAT_VALUE_TYPES: &[CqlType] = &[
    CqlType::Int,
    CqlType::Tinyint,
    CqlType::Smallint,
    CqlType::Bigint,
    CqlType::Varint,
    CqlType::Ascii,
    CqlType::Varchar,
];

pub fn register_all(registry: &FunctionRegistry) {
    for function in [FormatFunctionKind::Time, FormatFunctionKind::Bytes] {
        for value_type in FORMAT_VALUE_TYPES {
            registry.register(Arc::new(FormatFunction {
                function,
                arg_types: vec![value_type.clone()],
            }));
            registry.register(Arc::new(FormatFunction {
                function,
                arg_types: vec![value_type.clone(), CqlType::Ascii],
            }));
            registry.register(Arc::new(FormatFunction {
                function,
                arg_types: vec![value_type.clone(), CqlType::Ascii, CqlType::Ascii],
            }));
        }
    }
}

struct FormatFunction {
    function: FormatFunctionKind,
    arg_types: Vec<CqlType>,
}

#[derive(Debug, Clone, Copy)]
enum FormatFunctionKind {
    Time,
    Bytes,
}

impl FormatFunctionKind {
    fn name(self) -> &'static str {
        match self {
            FormatFunctionKind::Time => "format_time",
            FormatFunctionKind::Bytes => "format_bytes",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimeUnit {
    Days,
    Hours,
    Minutes,
    Seconds,
    Millis,
    Micros,
    Nanos,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ByteUnit {
    Bytes,
    KiB,
    MiB,
    GiB,
}

impl CqlFunction for FormatFunction {
    fn name(&self) -> &str {
        self.function.name()
    }

    fn arg_types(&self) -> Vec<CqlType> {
        self.arg_types.clone()
    }

    fn return_type(&self) -> CqlType {
        CqlType::Varchar
    }

    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        let Some(value_bytes) = args.first().and_then(|arg| *arg) else {
            return Ok(None);
        };
        if args.iter().skip(1).any(|arg| arg.is_none()) {
            return Err("none of the arguments may be null".to_string());
        }

        let value = read_format_value(&self.arg_types[0], value_bytes)?;
        if value < 0 {
            return Err("value must be non-negative".to_string());
        }

        let formatted = match self.function {
            FormatFunctionKind::Time => format_time_value(value, &self.arg_types, args)?,
            FormatFunctionKind::Bytes => format_bytes_value(value, &self.arg_types, args)?,
        };
        Ok(Some(CqlValue::Varchar(formatted).serialize_value()))
    }
}

fn read_format_value(cql_type: &CqlType, bytes: &[u8]) -> Result<i64, String> {
    match CqlValue::deserialize_value(cql_type, bytes).map_err(|error| error.to_string())? {
        CqlValue::Int(value) => Ok(value as i64),
        CqlValue::Tinyint(value) => Ok(value as i64),
        CqlValue::Smallint(value) => Ok(value as i64),
        CqlValue::Bigint(value) => Ok(value),
        CqlValue::Ascii(value) | CqlValue::Varchar(value) => value
            .parse::<i64>()
            .map_err(|_| format!("unable to convert string '{value}' to a value of type long")),
        CqlValue::Varint(value) => {
            let text = bigint::varint_to_string(&value);
            text.parse::<i64>()
                .map_err(|_| format!("unable to convert varint '{text}' to a value of type long"))
        }
        _ => Err(format!(
            "unsupported format value type {}",
            cql_type.cql_name()
        )),
    }
}

fn format_time_value(
    value: i64,
    arg_types: &[CqlType],
    args: &[Option<&[u8]>],
) -> Result<String, String> {
    match args.len() {
        1 => {
            let (converted, symbol) = auto_time_unit(value);
            Ok(format!("{} {symbol}", format_decimal(converted)))
        }
        2 => {
            let target = parse_time_unit(read_ascii_arg(&arg_types[1], args[1].unwrap())?)?;
            Ok(format!(
                "{} {}",
                format_decimal(convert_time(value, TimeUnit::Millis, target)),
                read_ascii_arg(&arg_types[1], args[1].unwrap())?
            ))
        }
        3 => {
            let source = parse_time_unit(read_ascii_arg(&arg_types[1], args[1].unwrap())?)?;
            let target_symbol = read_ascii_arg(&arg_types[2], args[2].unwrap())?;
            let target = parse_time_unit(target_symbol)?;
            Ok(format!(
                "{} {target_symbol}",
                format_decimal(convert_time(value, source, target))
            ))
        }
        _ => Err("invalid number of arguments".to_string()),
    }
}

fn format_bytes_value(
    value: i64,
    arg_types: &[CqlType],
    args: &[Option<&[u8]>],
) -> Result<String, String> {
    let (source, target) = match args.len() {
        1 => (ByteUnit::Bytes, auto_byte_unit(value)),
        2 => (
            ByteUnit::Bytes,
            parse_byte_unit(read_ascii_arg(&arg_types[1], args[1].unwrap())?)?,
        ),
        3 => (
            parse_byte_unit(read_ascii_arg(&arg_types[1], args[1].unwrap())?)?,
            parse_byte_unit(read_ascii_arg(&arg_types[2], args[2].unwrap())?)?,
        ),
        _ => return Err("invalid number of arguments".to_string()),
    };
    Ok(format!(
        "{} {}",
        format_decimal(convert_bytes(value, source, target)),
        target.symbol()
    ))
}

fn read_ascii_arg<'a>(cql_type: &CqlType, bytes: &'a [u8]) -> Result<&'a str, String> {
    match cql_type {
        CqlType::Ascii | CqlType::Varchar => std::str::from_utf8(bytes).map_err(|e| e.to_string()),
        _ => Err(format!(
            "expected unit argument, got {}",
            cql_type.cql_name()
        )),
    }
}

fn format_decimal(value: f64) -> String {
    if !value.is_finite() {
        return value.to_string();
    }
    let rounded = (value * 100.0).round() / 100.0;
    let mut text = format!("{rounded:.2}");
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    text
}

fn auto_time_unit(value: i64) -> (f64, &'static str) {
    for (factor, symbol) in [
        (86_400_000i64, "d"),
        (3_600_000i64, "h"),
        (60_000i64, "m"),
        (1_000i64, "s"),
    ] {
        if value >= factor {
            return (value as f64 / factor as f64, symbol);
        }
    }
    (value as f64, "ms")
}

fn parse_time_unit(symbol: &str) -> Result<TimeUnit, String> {
    match symbol.to_ascii_lowercase().as_str() {
        "d" => Ok(TimeUnit::Days),
        "h" => Ok(TimeUnit::Hours),
        "m" => Ok(TimeUnit::Minutes),
        "s" => Ok(TimeUnit::Seconds),
        "ms" => Ok(TimeUnit::Millis),
        "us" | "µs" => Ok(TimeUnit::Micros),
        "ns" => Ok(TimeUnit::Nanos),
        _ => Err(format!(
            "Unsupported time unit: {symbol}. Supported units are: ns, us, ms, s, m, h, d"
        )),
    }
}

fn convert_time(value: i64, source: TimeUnit, target: TimeUnit) -> f64 {
    let converted = value as f64 * source.nanos() / target.nanos();
    if converted.is_infinite() {
        f64::MAX
    } else {
        converted
    }
}

impl TimeUnit {
    fn nanos(self) -> f64 {
        match self {
            TimeUnit::Days => 86_400_000_000_000.0,
            TimeUnit::Hours => 3_600_000_000_000.0,
            TimeUnit::Minutes => 60_000_000_000.0,
            TimeUnit::Seconds => 1_000_000_000.0,
            TimeUnit::Millis => 1_000_000.0,
            TimeUnit::Micros => 1_000.0,
            TimeUnit::Nanos => 1.0,
        }
    }
}

fn auto_byte_unit(value: i64) -> ByteUnit {
    if value > 1024 * 1024 * 1024 {
        ByteUnit::GiB
    } else if value > 1024 * 1024 {
        ByteUnit::MiB
    } else if value > 1024 {
        ByteUnit::KiB
    } else {
        ByteUnit::Bytes
    }
}

fn parse_byte_unit(symbol: &str) -> Result<ByteUnit, String> {
    match symbol.to_ascii_lowercase().as_str() {
        "b" => Ok(ByteUnit::Bytes),
        "kib" => Ok(ByteUnit::KiB),
        "mib" => Ok(ByteUnit::MiB),
        "gib" => Ok(ByteUnit::GiB),
        _ => Err(format!(
            "Unsupported data storage unit: {symbol}. Supported units are: B, KiB, MiB, GiB"
        )),
    }
}

fn convert_bytes(value: i64, source: ByteUnit, target: ByteUnit) -> f64 {
    let bytes = (value as f64 * source.bytes()).min(i64::MAX as f64);
    match target {
        ByteUnit::Bytes => bytes,
        ByteUnit::KiB => bytes / 1024.0,
        ByteUnit::MiB => bytes / (1024.0 * 1024.0),
        ByteUnit::GiB => bytes / (1024.0 * 1024.0 * 1024.0),
    }
}

impl ByteUnit {
    fn bytes(self) -> f64 {
        match self {
            ByteUnit::Bytes => 1.0,
            ByteUnit::KiB => 1024.0,
            ByteUnit::MiB => 1024.0 * 1024.0,
            ByteUnit::GiB => 1024.0 * 1024.0 * 1024.0,
        }
    }

    fn symbol(self) -> &'static str {
        match self {
            ByteUnit::Bytes => "B",
            ByteUnit::KiB => "KiB",
            ByteUnit::MiB => "MiB",
            ByteUnit::GiB => "GiB",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::functions::FunctionRegistry;

    #[test]
    fn format_time_auto_selects_largest_matching_unit() {
        let registry = FunctionRegistry::with_builtins();
        let function = registry.resolve("format_time", &[CqlType::Int]).unwrap();

        assert_eq!(
            decode(function.execute(&[Some(&20_250i32.to_be_bytes())]).unwrap()),
            Some("20.25 s".to_string())
        );
    }

    #[test]
    fn format_time_converts_explicit_units() {
        let registry = FunctionRegistry::with_builtins();
        let function = registry
            .resolve(
                "format_time",
                &[CqlType::Bigint, CqlType::Ascii, CqlType::Ascii],
            )
            .unwrap();

        assert_eq!(
            decode(
                function
                    .execute(&[
                        Some(&2i64.to_be_bytes()),
                        Some("h".as_bytes()),
                        Some("m".as_bytes())
                    ])
                    .unwrap()
            ),
            Some("120 m".to_string())
        );
    }

    #[test]
    fn format_bytes_auto_selects_binary_unit() {
        let registry = FunctionRegistry::with_builtins();
        let function = registry.resolve("format_bytes", &[CqlType::Int]).unwrap();

        assert_eq!(
            decode(
                function
                    .execute(&[Some(&(100 * 1024 + 150i32).to_be_bytes())])
                    .unwrap()
            ),
            Some("100.15 KiB".to_string())
        );
    }

    #[test]
    fn format_bytes_converts_string_values_and_units() {
        let registry = FunctionRegistry::with_builtins();
        let function = registry
            .resolve(
                "format_bytes",
                &[CqlType::Varchar, CqlType::Ascii, CqlType::Ascii],
            )
            .unwrap();

        assert_eq!(
            decode(
                function
                    .execute(&[
                        Some("2".as_bytes()),
                        Some("GiB".as_bytes()),
                        Some("MiB".as_bytes())
                    ])
                    .unwrap()
            ),
            Some("2048 MiB".to_string())
        );
    }

    #[test]
    fn format_functions_match_java_null_and_negative_rules() {
        let registry = FunctionRegistry::with_builtins();
        let function = registry
            .resolve("format_time", &[CqlType::Int, CqlType::Ascii])
            .unwrap();

        assert_eq!(
            function.execute(&[None, Some("s".as_bytes())]).unwrap(),
            None
        );
        assert_eq!(
            function.execute(&[Some(&1i32.to_be_bytes()), None]),
            Err("none of the arguments may be null".to_string())
        );
        assert_eq!(
            function.execute(&[Some(&(-1i32).to_be_bytes()), Some("s".as_bytes())]),
            Err("value must be non-negative".to_string())
        );
    }

    #[test]
    fn format_functions_reject_bad_units() {
        let registry = FunctionRegistry::with_builtins();
        let function = registry
            .resolve("format_bytes", &[CqlType::Int, CqlType::Ascii])
            .unwrap();

        assert_eq!(
            function.execute(&[Some(&1i32.to_be_bytes()), Some("MB".as_bytes())]),
            Err(
                "Unsupported data storage unit: MB. Supported units are: B, KiB, MiB, GiB"
                    .to_string()
            )
        );
    }

    fn decode(bytes: Option<Vec<u8>>) -> Option<String> {
        bytes.map(
            |bytes| match CqlValue::deserialize_value(&CqlType::Varchar, &bytes).unwrap() {
                CqlValue::Varchar(value) => value,
                _ => unreachable!("deserialize_value(Varchar) returns Varchar"),
            },
        )
    }
}
