//! Source-bound plans emitted by the planner's JSON_TABLE fixture exporter.
use super::StatelessPhysicalExecutor;
use arrow::array::{ArrayRef, StringArray};
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::common::collect;
use serde_json::{Value, json};

fn rows(batches: &[RecordBatch]) -> Vec<Value> {
    let mut writer: arrow::json::Writer<_, arrow::json::writer::LineDelimited> =
        arrow::json::WriterBuilder::new()
            .with_explicit_nulls(true)
            .build(Vec::new());
    writer
        .write_batches(&batches.iter().collect::<Vec<_>>())
        .unwrap();
    writer.finish().unwrap();
    String::from_utf8(writer.into_inner())
        .unwrap()
        .lines()
        .map(|row| serde_json::from_str(row).unwrap())
        .collect()
}

fn sorted(mut rows: Vec<Value>) -> Vec<Value> {
    rows.sort_by_key(Value::to_string);
    rows
}

#[tokio::test]
#[ignore = "requires current-source JSON_TABLE planner fixture export"]
async fn json_table_planner_worker_runtime_batches_1_and_8() {
    let directory =
        std::path::PathBuf::from(std::env::var("STREAMR_JSON_TABLE_FIXTURE_DIR").unwrap());
    for (fixture, expected) in [
        (
            "tags",
            vec![
                json!({"tag":"vip","enabled":true}),
                json!({"tag":"old","enabled":false}),
                json!({"tag":"gone","enabled":null}),
            ],
        ),
        (
            "accounts",
            vec![
                json!({"account":"discord","details":"{\"external_id\":\"d2\"}","removed":false}),
                json!({"account":"steam","details":"null","removed":true}),
            ],
        ),
        (
            "correlated",
            vec![
                json!({"canonical_id":"p1","tag":"vip","enabled":true}),
                json!({"canonical_id":"p2","tag":"beta","enabled":false}),
            ],
        ),
    ] {
        let proto = std::fs::read(directory.join(format!("{fixture}.pb"))).unwrap();
        let input = arrow::ipc::reader::StreamReader::try_new(
            std::fs::File::open(directory.join(format!("{fixture}.arrow"))).unwrap(),
            None,
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        let input = arrow::compute::concat_batches(&input[0].schema(), &input).unwrap();
        // Repeat the finite VALUES fixture to fill batches of exactly eight.
        assert!(8usize.is_multiple_of(input.num_rows()));
        let repeats = 8 / input.num_rows();
        let expanded =
            arrow::compute::concat_batches(&input.schema(), &vec![input.clone(); repeats]).unwrap();
        let repeated_expected: Vec<Value> = (0..repeats).flat_map(|_| expected.clone()).collect();
        for batch_size in [1, 8] {
            let mut worker =
                StatelessPhysicalExecutor::new(&proto, &arroyo_planner::physical::new_registry())
                    .unwrap();
            let mut actual = Vec::new();
            for offset in (0..expanded.num_rows()).step_by(batch_size) {
                let output = collect(
                    worker
                        .process_batch(expanded.slice(offset, batch_size))
                        .await,
                )
                .await
                .unwrap();
                actual.extend(rows(&output));
            }
            assert_eq!(
                sorted(actual),
                sorted(repeated_expected.clone()),
                "{fixture} batch {batch_size}"
            );
            let empty = collect(
                worker
                    .process_batch(RecordBatch::new_empty(expanded.schema()))
                    .await,
            )
            .await
            .unwrap();
            assert!(
                empty.iter().all(|batch| batch.num_rows() == 0),
                "empty source {fixture}"
            );
            println!(
                "JSON_TABLE_WORKER fixture={fixture} source_batch={batch_size} input_rows=8 output_rows={}",
                repeated_expected.len()
            );
        }
    }
}

#[tokio::test]
#[ignore = "requires current-source JSON_TABLE planner fixture export"]
async fn json_table_worker_errors_and_cancellation_release_input() {
    let directory =
        std::path::PathBuf::from(std::env::var("STREAMR_JSON_TABLE_FIXTURE_DIR").unwrap());
    let proto = std::fs::read(directory.join("correlated.pb")).unwrap();
    let input = arrow::ipc::reader::StreamReader::try_new(
        std::fs::File::open(directory.join("correlated.arrow")).unwrap(),
        None,
    )
    .unwrap()
    .next()
    .unwrap()
    .unwrap();
    let payload_index = input
        .schema()
        .index_of("column2")
        .or_else(|_| input.schema().index_of("payload"))
        .unwrap();
    let mut worker =
        StatelessPhysicalExecutor::new(&proto, &arroyo_planner::physical::new_registry()).unwrap();
    let make = |payload: &str| {
        let mut columns: Vec<ArrayRef> = input.slice(0, 1).columns().to_vec();
        columns[payload_index] = std::sync::Arc::new(StringArray::from(vec![payload]));
        RecordBatch::try_new(input.schema(), columns).unwrap()
    };
    for payload in [
        "{}",
        "null",
        "malformed",
        r#"{"traits":{}}"#,
        r#"{"traits":{"tags":null}}"#,
    ] {
        let output = collect(worker.process_batch(make(payload)).await)
            .await
            .unwrap();
        assert!(
            output.iter().all(|batch| batch.num_rows() == 0),
            "{payload}"
        );
    }
    let oversized = json!({"traits":{"tags": (0..1025).map(|key| (key.to_string(), Value::Bool(true))).collect::<serde_json::Map<_, _>>()}}).to_string();
    let error = collect(worker.process_batch(make(&oversized)).await)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        datafusion::common::DataFusionError::ResourcesExhausted(_)
    ));
    let output = collect(worker.process_batch(input.slice(0, 1)).await)
        .await
        .unwrap();
    assert_eq!(
        rows(&output),
        vec![json!({"canonical_id":"p1","tag":"vip","enabled":true})]
    );
    let stream = worker.process_batch(input.slice(0, 1)).await;
    drop(stream);
    let output = collect(worker.process_batch(input.slice(1, 1)).await)
        .await
        .unwrap();
    assert_eq!(
        rows(&output),
        vec![json!({"canonical_id":"p2","tag":"beta","enabled":false})]
    );
}

#[tokio::test]
#[ignore = "requires current-source JSON_TABLE planner fixture export"]
async fn json_table_worker_nested_json_defaults_presence_and_alias_collision() {
    let directory =
        std::path::PathBuf::from(std::env::var("STREAMR_JSON_TABLE_FIXTURE_DIR").unwrap());
    for (fixture, expected) in [
        (
            "tags_standalone",
            vec![
                json!({"tag":"vip","enabled":true}),
                json!({"tag":"old","enabled":false}),
                json!({"tag":"gone","enabled":null}),
            ],
        ),
        (
            "accounts_standalone",
            vec![
                json!({"account":"discord","details":"{\"external_id\":\"d2\"}","removed":false}),
                json!({"account":"steam","details":"null","removed":true}),
            ],
        ),
    ] {
        let proto = std::fs::read(directory.join(format!("{fixture}.pb"))).unwrap();
        let mut worker =
            StatelessPhysicalExecutor::new(&proto, &arroyo_planner::physical::new_registry())
                .unwrap();
        let result = collect(
            worker
                .process_batch(RecordBatch::new_empty(std::sync::Arc::new(
                    arrow::datatypes::Schema::empty(),
                )))
                .await,
        )
        .await
        .unwrap();
        assert_eq!(
            sorted(rows(&result)),
            sorted(expected),
            "exact standalone SQL {fixture}"
        );
    }
    for (fixture, expected) in [
        (
            "collision",
            vec![
                json!({"__unnested":"p1","tag":"vip","enabled":true}),
                json!({"__unnested":"p2","tag":"beta","enabled":false}),
            ],
        ),
        (
            "presence",
            vec![
                json!({"case_name":"missing","present":false,"is_null":false}),
                json!({"case_name":"null","present":true,"is_null":true}),
                json!({"case_name":"empty","present":true,"is_null":false}),
                json!({"case_name":"malformed","present":false,"is_null":false}),
                json!({"case_name":"sql_null","present":null,"is_null":null}),
                json!({"case_name":"scalar","present":true,"is_null":false}),
                json!({"case_name":"false","present":true,"is_null":false}),
                json!({"case_name":"parent_null","present":false,"is_null":false}),
            ],
        ),
    ] {
        let proto = std::fs::read(directory.join(format!("{fixture}.pb"))).unwrap();
        let input = arrow::ipc::reader::StreamReader::try_new(
            std::fs::File::open(directory.join(format!("{fixture}.arrow"))).unwrap(),
            None,
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        let input = arrow::compute::concat_batches(&input[0].schema(), &input).unwrap();
        for batch_size in [1, 8] {
            let mut worker =
                StatelessPhysicalExecutor::new(&proto, &arroyo_planner::physical::new_registry())
                    .unwrap();
            let mut actual = Vec::new();
            for offset in (0..input.num_rows()).step_by(batch_size) {
                actual.extend(rows(
                    &collect(
                        worker
                            .process_batch(
                                input.slice(offset, batch_size.min(input.num_rows() - offset)),
                            )
                            .await,
                    )
                    .await
                    .unwrap(),
                ));
            }
            assert_eq!(
                sorted(actual),
                sorted(expected.clone()),
                "{fixture} batch {batch_size}"
            );
        }
    }
    let proto = std::fs::read(directory.join("accounts.pb")).unwrap();
    let input = arrow::ipc::reader::StreamReader::try_new(
        std::fs::File::open(directory.join("accounts.arrow")).unwrap(),
        None,
    )
    .unwrap()
    .next()
    .unwrap()
    .unwrap();
    let payload = r#"{"object":{"nested":[1,null,{"text":"a\"b\nλ"}]},"array":[true,false],"string":"a\"b\nλ","nil":null}"#;
    let mut columns: Vec<ArrayRef> = input.columns().to_vec();
    columns[0] = std::sync::Arc::new(StringArray::from(vec![payload]));
    let input = RecordBatch::try_new(input.schema(), columns).unwrap();
    let mut worker =
        StatelessPhysicalExecutor::new(&proto, &arroyo_planner::physical::new_registry()).unwrap();
    let output = collect(worker.process_batch(input).await).await.unwrap();
    let mut actual = rows(&output);
    for item in &mut actual {
        item["details"] = serde_json::from_str(item["details"].as_str().unwrap()).unwrap();
    }
    assert_eq!(
        sorted(actual),
        sorted(vec![
            json!({"account":"object","details":{"nested":[1,null,{"text":"a\"b\nλ"}]},"removed":false}),
            json!({"account":"array","details":[true,false],"removed":false}),
            json!({"account":"string","details":"a\"b\nλ","removed":false}),
            json!({"account":"nil","details":null,"removed":true})
        ])
    );
}

#[tokio::test]
#[ignore = "requires current-source JSON_TABLE planner fixture export"]
async fn json_table_worker_scalar_defaults_and_per_event_byte_bound() {
    let directory =
        std::path::PathBuf::from(std::env::var("STREAMR_JSON_TABLE_FIXTURE_DIR").unwrap());
    let proto = std::fs::read(directory.join("tags.pb")).unwrap();
    let input = arrow::ipc::reader::StreamReader::try_new(
        std::fs::File::open(directory.join("tags.arrow")).unwrap(),
        None,
    )
    .unwrap()
    .next()
    .unwrap()
    .unwrap();
    let mut worker =
        StatelessPhysicalExecutor::new(&proto, &arroyo_planner::physical::new_registry()).unwrap();
    let documents = [
        Some(
            r#"{"traits":{"tags":{"number":1,"object":{},"array":[],"false":false,"null":null}}}"#,
        ),
        None,
    ];
    for document in documents {
        let mut columns: Vec<ArrayRef> = input.columns().to_vec();
        columns[0] = std::sync::Arc::new(StringArray::from(vec![document]));
        let output = collect(
            worker
                .process_batch(RecordBatch::try_new(input.schema(), columns).unwrap())
                .await,
        )
        .await
        .unwrap();
        let expected = if document.is_some() {
            vec![
                json!({"tag":"number","enabled":null}),
                json!({"tag":"object","enabled":null}),
                json!({"tag":"array","enabled":null}),
                json!({"tag":"false","enabled":false}),
                json!({"tag":"null","enabled":null}),
            ]
        } else {
            vec![]
        };
        assert_eq!(sorted(rows(&output)), sorted(expected));
    }
    let large = json!({"traits":{"tags":{"large":"a".repeat(arroyo_planner::sql_json::path::MAX_OUTPUT_BYTES / 8)}}}).to_string();
    for batch_size in [1, 8] {
        let mut count = 0;
        for _ in (0..8).step_by(batch_size) {
            let mut columns: Vec<ArrayRef> = input
                .columns()
                .iter()
                .map(|column| arrow::compute::concat(&vec![column.as_ref(); batch_size]).unwrap())
                .collect();
            columns[0] = std::sync::Arc::new(StringArray::from(vec![large.as_str(); batch_size]));
            let output = collect(
                worker
                    .process_batch(RecordBatch::try_new(input.schema(), columns).unwrap())
                    .await,
            )
            .await
            .unwrap();
            assert!(
                rows(&output)
                    .iter()
                    .all(|row| row == &json!({"tag":"large","enabled":null}))
            );
            count += output.iter().map(RecordBatch::num_rows).sum::<usize>();
        }
        assert_eq!(count, 8);
    }
}

#[test]
#[ignore = "requires current-source JSON_TABLE planner fixture export"]
fn json_table_worker_rejects_invalid_physical_metadata_before_execution() {
    use datafusion_proto::protobuf::physical_expr_node::ExprType;
    use datafusion_proto::protobuf::physical_plan_node::PhysicalPlanType;
    use datafusion_proto::protobuf::{PhysicalExprNode, PhysicalPlanNode, PhysicalScalarUdfNode};
    use prost::Message;
    fn alter_expression(
        expr: &mut PhysicalExprNode,
        name: &str,
        mutation: &mut dyn FnMut(&mut PhysicalScalarUdfNode),
    ) -> usize {
        let Some(ExprType::ScalarUdf(function)) = &mut expr.expr_type else {
            return 0;
        };
        if function.name == name {
            mutation(function);
            return 1;
        }
        function
            .args
            .iter_mut()
            .map(|arg| alter_expression(arg, name, mutation))
            .sum()
    }
    fn alter_functions(
        plan: &mut PhysicalPlanNode,
        name: &str,
        mutation: &mut dyn FnMut(&mut PhysicalScalarUdfNode),
    ) -> usize {
        match plan.physical_plan_type.as_mut().unwrap() {
            PhysicalPlanType::Projection(projection) => {
                let mut found = projection
                    .expr
                    .iter_mut()
                    .map(|expr| alter_expression(expr, name, mutation))
                    .sum::<usize>();
                found += alter_functions(projection.input.as_mut().unwrap(), name, mutation);
                found
            }
            PhysicalPlanType::Unnest(unnest) => {
                alter_functions(unnest.input.as_mut().unwrap(), name, mutation)
            }
            PhysicalPlanType::Extension(extension) => extension
                .inputs
                .iter_mut()
                .map(|input| alter_functions(input, name, mutation))
                .sum(),
            _ => 0,
        }
    }
    fn alter_unnest(
        plan: &mut PhysicalPlanNode,
        mutation: &mut dyn FnMut(&mut datafusion_proto::protobuf::UnnestExecNode),
    ) -> usize {
        match plan.physical_plan_type.as_mut().unwrap() {
            PhysicalPlanType::Projection(projection) => {
                alter_unnest(projection.input.as_mut().unwrap(), mutation)
            }
            PhysicalPlanType::Unnest(unnest) => {
                mutation(unnest);
                1
            }
            PhysicalPlanType::Extension(extension) => extension
                .inputs
                .iter_mut()
                .map(|input| alter_unnest(input, mutation))
                .sum(),
            _ => 0,
        }
    }
    let literal = |value: datafusion::common::ScalarValue| PhysicalExprNode {
        expr_type: Some(ExprType::Literal((&value).try_into().unwrap())),
    };
    let directory =
        std::path::PathBuf::from(std::env::var("STREAMR_JSON_TABLE_FIXTURE_DIR").unwrap());
    for (fixture, names) in [
        (
            "correlated",
            vec!["json_table_rows", "json_value", "json_value_boolean"],
        ),
        ("accounts", vec!["json_query", "json_exists"]),
    ] {
        let bytes = std::fs::read(directory.join(format!("{fixture}.pb"))).unwrap();
        let original = PhysicalPlanNode::decode(bytes.as_slice()).unwrap();
        assert!(
            StatelessPhysicalExecutor::new(&bytes, &arroyo_planner::physical::new_registry())
                .is_ok()
        );
        for name in names {
            for kind in [
                "missing_args",
                "extra_args",
                "null_path",
                "invalid_path",
                "nonliteral_path",
                "wrong_document",
                "return_type",
                "nullable",
            ] {
                let mut proto = original.clone();
                let count = alter_functions(&mut proto, name, &mut |function| match kind {
                    "missing_args" => function.args.clear(),
                    "extra_args" => {
                        function
                            .args
                            .push(literal(datafusion::common::ScalarValue::Utf8(Some(
                                "$".into(),
                            ))))
                    }
                    "null_path" => {
                        function.args[1] = literal(datafusion::common::ScalarValue::Utf8(None))
                    }
                    "invalid_path" => {
                        function.args[1] = literal(datafusion::common::ScalarValue::Utf8(Some(
                            "strict $.x".into(),
                        )))
                    }
                    "nonliteral_path" => function.args[1] = function.args[0].clone(),
                    "wrong_document" => {
                        function.args[0] =
                            literal(datafusion::common::ScalarValue::Boolean(Some(true)))
                    }
                    "return_type" => {
                        function.return_type = Some(
                            (&if matches!(name, "json_value_boolean" | "json_exists") {
                                arrow::datatypes::DataType::Utf8
                            } else {
                                arrow::datatypes::DataType::Boolean
                            })
                                .try_into()
                                .unwrap(),
                        )
                    }
                    "nullable" => function.nullable = !function.nullable,
                    _ => unreachable!(),
                });
                assert!(count > 0, "fixture must retain {name}");
                assert!(
                    StatelessPhysicalExecutor::new(
                        &proto.encode_to_vec(),
                        &arroyo_planner::physical::new_registry()
                    )
                    .is_err(),
                    "decode accepted {name}/{kind}"
                );
            }
        }
    }
    let bytes = std::fs::read(directory.join("correlated.pb")).unwrap();
    let original = PhysicalPlanNode::decode(bytes.as_slice()).unwrap();
    for kind in [
        "input_index",
        "multiple_lists",
        "deep_list",
        "struct_column",
        "options",
        "carried_name",
        "carried_type",
        "carried_nullable",
        "element_nullable",
        "metadata",
    ] {
        let mut proto = original.clone();
        assert_eq!(
            alter_unnest(&mut proto, &mut |unnest| {
                if kind == "input_index" {
                    unnest.list_type_columns[0].index_in_input_schema = u32::MAX;
                    return;
                }
                match kind {
                    "multiple_lists" => {
                        unnest.list_type_columns.push(unnest.list_type_columns[0]);
                        return;
                    }
                    "deep_list" => {
                        unnest.list_type_columns[0].depth = 2;
                        return;
                    }
                    "struct_column" => {
                        unnest.struct_type_columns.push(0);
                        return;
                    }
                    "options" => {
                        unnest.options.as_mut().unwrap().preserve_nulls = false;
                        return;
                    }
                    _ => {}
                }
                let schema: arrow::datatypes::Schema =
                    unnest.schema.as_ref().unwrap().try_into().unwrap();
                let mut fields = schema.fields().to_vec();
                let index = if kind == "element_nullable" {
                    unnest.list_type_columns[0].index_in_input_schema as usize
                } else {
                    0
                };
                let field = fields[index].as_ref().clone();
                fields[index] = std::sync::Arc::new(match kind {
                    "carried_name" => field.with_name("invalid_carried"),
                    "carried_type" => field.with_data_type(arrow::datatypes::DataType::Boolean),
                    "carried_nullable" | "element_nullable" => {
                        field.clone().with_nullable(!field.is_nullable())
                    }
                    "metadata" => field.with_metadata(std::collections::HashMap::from([(
                        "invalid".into(),
                        "metadata".into(),
                    )])),
                    _ => unreachable!(),
                });
                unnest.schema = Some(
                    (&arrow::datatypes::Schema::new_with_metadata(
                        fields,
                        schema.metadata().clone(),
                    ))
                        .try_into()
                        .unwrap(),
                );
            }),
            1
        );
        assert!(
            StatelessPhysicalExecutor::new(
                &proto.encode_to_vec(),
                &arroyo_planner::physical::new_registry()
            )
            .is_err(),
            "decode accepted invalid Unnest {kind}"
        );
    }
}
