// Licensed under Apache License, Version 2.0.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use cassandra_types::CqlType;
use cassandra_types::comparator::compare_bytes;
use cassandra_types::composite::{Component, EOC_NONE, serialize_composite};
use cassandra_types::dynamic_composite::{
    DynamicComponent, deserialize_dynamic_composite, serialize_dynamic_composite,
};
use cassandra_types::type_parser::parse_type;

#[test]
fn legacy_composite_and_dynamic_composite_round_trip_and_compare() {
    let composite_type = parse_type("CompositeType(UTF8Type,Int32Type)").unwrap();
    assert_eq!(
        composite_type,
        CqlType::Composite(vec![CqlType::Varchar, CqlType::Int])
    );

    let composite_a = serialize_composite(&[
        Component {
            data: b"row".to_vec(),
            eoc: EOC_NONE,
        },
        Component {
            data: 7i32.to_be_bytes().to_vec(),
            eoc: EOC_NONE,
        },
    ]);
    let composite_b = serialize_composite(&[
        Component {
            data: b"row".to_vec(),
            eoc: EOC_NONE,
        },
        Component {
            data: 9i32.to_be_bytes().to_vec(),
            eoc: EOC_NONE,
        },
    ]);
    assert_eq!(
        compare_bytes(&composite_type, &composite_a, &composite_b),
        Ordering::Less
    );

    let dynamic_type = parse_type("DynamicCompositeType(b=>BytesType,i=>IntegerType)").unwrap();
    let CqlType::DynamicComposite(aliases) = dynamic_type.clone() else {
        panic!("expected dynamic composite");
    };
    assert_eq!(
        aliases,
        BTreeMap::from([(b'b', CqlType::Blob), (b'i', CqlType::Varint)])
    );

    let dynamic_a = serialize_dynamic_composite(
        &[
            DynamicComponent {
                comparator: CqlType::Blob,
                value: b"row".to_vec(),
                eoc: EOC_NONE,
            },
            DynamicComponent {
                comparator: CqlType::Varint,
                value: vec![7],
                eoc: EOC_NONE,
            },
        ],
        &aliases,
    )
    .unwrap();
    let dynamic_b = serialize_dynamic_composite(
        &[
            DynamicComponent {
                comparator: CqlType::Blob,
                value: b"row".to_vec(),
                eoc: EOC_NONE,
            },
            DynamicComponent {
                comparator: CqlType::Varint,
                value: vec![9],
                eoc: EOC_NONE,
            },
        ],
        &aliases,
    )
    .unwrap();

    assert_eq!(
        deserialize_dynamic_composite(&dynamic_a, &aliases).unwrap()[1].value,
        vec![7]
    );
    assert_eq!(
        compare_bytes(&dynamic_type, &dynamic_a, &dynamic_b),
        Ordering::Less
    );
}
