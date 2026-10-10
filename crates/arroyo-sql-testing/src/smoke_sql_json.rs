//! Complete SELECT SQL planned natively, physically decoded and run by worker.
use super::*;

#[test_log(tokio::test)]
#[ignore = "requires current-source planner-emitted complete SELECT fixture"]
async fn sql_json_complete_projection_worker() {
    use arrow::datatypes::DataType;
    configure_test_worker();
    let directory = PathBuf::from(env::var("STREAMR_SQL_JSON_FIXTURE_DIR").unwrap());
    let proto = std::fs::read(directory.join("select.pb")).unwrap();
    let input = arrow::ipc::reader::StreamReader::try_new(
        std::fs::File::open(directory.join("values.arrow")).unwrap(),
        None,
    )
    .unwrap()
    .collect::<std::result::Result<Vec<_>, _>>()
    .unwrap();
    assert_eq!(input.len(), 1);
    assert_eq!(input[0].num_rows(), 1);
    let mut worker = arroyo_worker::arrow::StatelessPhysicalExecutor::new(
        &proto,
        &arroyo_planner::physical::new_registry(),
    )
    .unwrap();
    let batches =
        datafusion::physical_plan::common::collect(worker.process_batch(input[0].clone()).await)
            .await
            .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    let schema = batches[0].schema();
    assert_eq!(schema.fields().len(), 8);
    for (name, kind) in [
        ("canonical_id", DataType::Utf8),
        ("score", DataType::Float64),
        ("wishlisted", DataType::Boolean),
        ("name_present", DataType::Boolean),
        ("name", DataType::Utf8),
        ("email", DataType::Utf8),
        ("accounts", DataType::Utf8),
        ("coherent_payload", DataType::Utf8),
    ] {
        assert_eq!(schema.field_with_name(name).unwrap().data_type(), &kind);
    }
    let mut writer: arrow::json::Writer<_, arrow::json::writer::LineDelimited> =
        arrow::json::WriterBuilder::new()
            .with_explicit_nulls(true)
            .build(Vec::new());
    writer.write_batches(&[&batches[0]]).unwrap();
    writer.finish().unwrap();
    let text = String::from_utf8(writer.into_inner()).unwrap();
    std::fs::write(directory.join("select-output.jsonl"), &text).unwrap();
    let rows: Vec<Value> = text
        .lines()
        .map(|row| serde_json::from_str(row).unwrap())
        .collect();
    assert_eq!(rows.len(), 1);
    let mut row = rows.into_iter().next().unwrap();
    // Assert SQL NULL in both the typed result and serialized row.
    use arrow::array::Array;
    assert!(
        batches[0]
            .column(schema.index_of("name").unwrap())
            .is_null(0)
    );
    assert_eq!(row.get("name"), Some(&Value::Null));
    for field in ["accounts", "coherent_payload"] {
        row[field] = serde_json::from_str(row[field].as_str().unwrap()).unwrap();
    }
    assert_eq!(
        row,
        serde_json::json!({
            "canonical_id":"p1", "name_present":true, "name":null,
            "score":0.0, "wishlisted":false, "email":"a@example.test",
            "accounts":{"discord":{"external_id":"d1"}},
            "coherent_payload":{"source":"google","medium":"","accounts":{"discord":{"external_id":"d1"}}}
        })
    );
    println!("SQL_JSON_COMPLETE_SELECT_WORKER rows=1 schema={schema:?}");
}
