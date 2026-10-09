//! Dedicated-process SQL checkpoint fault qualification. Run each ignored test
//! with STREAMR_TEST_BACKEND=rocksdb, selecting controller or leader through
//! STREAMR_TEST_CHECKPOINT_MODE. The single-file sink truncates to the selected
//! checkpoint offset before source replay; this is its delivery contract only.
use super::*;
use arroyo_types::CheckpointFilePathLayout;

struct FaultFixture {
    root: PathBuf,
    job_id: Arc<String>,
    program: LogicalProgram,
    output: PathBuf,
    disk_operator: String,
    disk_node: u32,
    disk_table: String,
    advances: [i32; 2],
    native_oracles: [PathBuf; 4],
    cdc_keys: Vec<String>,
    ordered_native: bool,
}

async fn fixture(label: &str) -> FaultFixture {
    assert_eq!(env::var("STREAMR_TEST_BACKEND").as_deref(), Ok("rocksdb"));
    assert_ne!(env::var("STREAMR_TEST_CHECKPOINT_STOP").as_deref(), Ok("1"));
    configure_test_worker();
    let job_id = Arc::new(format!(
        "{label}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let root = env::temp_dir().join(job_id.as_str());
    tokio::fs::create_dir_all(root.join("checkpoints"))
        .await
        .unwrap();
    config::update(|c| c.checkpoint_url = format!("file://{}", root.join("checkpoints").display()));
    let output = root.join("output.json");
    native_fixture(root, job_id, output).await
}

// Dedicated-process caller fixture. Oracles are independently declared JSONL,
// not generated from captured output. Epoch-specific finals allow the native
// updating operator to coalesce the replay suffix differently after epoch1/2.
async fn native_fixture(root: PathBuf, job_id: Arc<String>, output: PathBuf) -> FaultFixture {
    let manifest_path =
        PathBuf::from(env::var("STREAMR_FAULT_MANIFEST").expect("native fault manifest required"));
    assert!(manifest_path.is_absolute());
    let manifest: Value =
        serde_json::from_str(&read_to_string(&manifest_path).await.unwrap()).unwrap();
    let base = manifest_path.parent().unwrap();
    let kind = manifest["kind"].as_str().expect("kind required");
    assert_eq!(
        config::config().pipeline.source_batch_size,
        1,
        "native fault oracles require per-event watermarks"
    );
    let (owner, mut disk_table) = match kind {
        "aggregate" => {
            assert_eq!(
                env::var("STREAMR_TEST_NATIVE_AGGREGATES").as_deref(),
                Ok("1")
            );
            // Existing configuration, only this dedicated fixture selects it.
            // A startup tick is still possible and must be declared in complete
            // expected-output alternatives; this does not suppress that tick.
            let seconds = manifest["aggregate_flush_seconds"].as_u64().unwrap();
            assert!(Duration::from_secs(seconds) > test_runtime_timeout());
            config::update(|c| {
                c.pipeline.update_aggregate_flush_interval = Duration::from_secs(seconds).into()
            });
            (
                OperatorName::UpdatingAggregate,
                "native-aggregate-v1".to_owned(),
            )
        }
        "session" => {
            assert_eq!(env::var("STREAMR_TEST_NATIVE_WINDOWS").as_deref(), Ok("1"));
            (OperatorName::SessionWindowAggregate, "n".to_owned())
        }
        "state_table" => {
            assert_eq!(env::var("STREAMR_TEST_TYPED_SQL").as_deref(), Ok("1"));
            (OperatorName::FusedStateTable, String::new())
        }
        _ => panic!("unsupported native fault fixture kind"),
    };
    let asset = |field: &str| base.join(manifest[field].as_str().expect("fixture asset required"));
    let input = root.join("input.jsonl");
    tokio::fs::copy(asset("input"), &input).await.unwrap();
    let input_rows = read_to_string(&input).await.unwrap().lines().count();
    let first = usize::try_from(manifest["checkpoint_input_rows_1"].as_u64().unwrap()).unwrap();
    let second = usize::try_from(manifest["checkpoint_input_rows_2"].as_u64().unwrap()).unwrap();
    assert!(
        0 < first && first < second && second < input_rows,
        "both checkpoints must be proper source prefixes"
    );
    let mut oracles = Vec::new();
    for field in [
        "expected_checkpoint_1",
        "expected_checkpoint_2",
        "expected_restore_1",
        "expected_restore_2",
    ] {
        let source = asset(field);
        if kind == "state_table" {
            assert_eq!(
                source.extension().and_then(|s| s.to_str()),
                Some("jsonl"),
                "ordered state-table fault oracles require one exact JSONL stream"
            );
        }
        let target = root.join(format!(
            "{field}.{}",
            source.extension().unwrap().to_str().unwrap()
        ));
        tokio::fs::copy(source, &target).await.unwrap();
        // Validate independent complete object rows before starting workers.
        expected_alternatives(&target).await;
        oracles.push(target);
    }
    let query = read_to_string(asset("query")).await.unwrap();
    assert!(query.contains("{{INPUT}}") && query.contains("{{OUTPUT}}"));
    let query = query
        .replace("{{INPUT}}", &input.to_str().unwrap().replace('\'', "''"))
        .replace("{{OUTPUT}}", &output.to_str().unwrap().replace('\'', "''"));
    tokio::fs::write(root.join("query.sql"), &query)
        .await
        .unwrap();
    let program = get_graph(query, &get_udfs()).await.unwrap();
    let mut owners = Vec::new();
    let mut sources = 0;
    for node in program.graph.node_weights() {
        assert_eq!(node.parallelism, 1);
        for (op, _) in node.operator_chain.iter() {
            if op.operator_name == owner {
                if kind == "state_table" {
                    disk_table = state_table_fault_transport(
                        &op.operator_config,
                        manifest["state_table_name"]
                            .as_str()
                            .expect("state_table_name required"),
                    );
                }
                owners.push((node.node_id, op.operator_id.clone()));
            }
            if op.operator_name == OperatorName::ConnectorSource {
                sources += 1;
                let source: arroyo_rpc::grpc::api::ConnectorOp =
                    prost::Message::decode(op.operator_config.as_slice()).unwrap();
                assert_eq!(source.connector, "single_file");
                let config: arroyo_rpc::OperatorConfig =
                    serde_json::from_str(&source.config).unwrap();
                assert!(
                    config
                        .table
                        .get("wait_for_control")
                        .is_none_or(|v| v.is_null() || v.as_bool() == Some(true))
                );
                assert_eq!(config.table["path"].as_str(), input.to_str());
            }
        }
    }
    assert_eq!(sources, 1);
    assert_eq!(
        owners.len(),
        1,
        "one selected native checkpoint owner required"
    );
    let (disk_node, disk_operator) = owners.pop().unwrap();
    let oracles: [PathBuf; 4] = oracles.try_into().unwrap();
    let cdc_keys: Vec<String> = if kind == "aggregate" {
        let keys = manifest["cdc_key_columns"].as_array().unwrap();
        assert!(!keys.is_empty());
        keys.iter()
            .map(|v| v.as_str().unwrap().to_owned())
            .collect()
    } else {
        vec![]
    };
    FaultFixture {
        root,
        job_id,
        program,
        output,
        disk_node,
        disk_operator,
        disk_table,
        // The source admits its first row automatically; barriers serialize
        // admitted rows. A nonstopping checkpoint also admits one next row;
        // second-prefix NoOps exclude that checkpoint credit.
        advances: [
            i32::try_from(first - 1).unwrap(),
            i32::try_from(second - first - 1).unwrap(),
        ],
        native_oracles: oracles,
        cdc_keys,
        ordered_native: kind == "state_table",
    }
}

// Select an existing typed table from the actual planned owner. The manifest
// names the caller's table; its checkpoint transport is derived by the same
// codec as FusedStateTable::tables(), never guessed from a SQL name.
fn state_table_fault_transport(config: &[u8], name: &str) -> String {
    let owner: arroyo_rpc::grpc::api::FusedStateTableOperator =
        prost::Message::decode(config).unwrap();
    let matches: Vec<_> = owner
        .tables
        .iter()
        .filter(|table| table.name == name)
        .collect();
    assert_eq!(
        matches.len(),
        1,
        "one declared fault table required in owner"
    );
    arroyo_state_protocol::typed_checkpoint::transport_table_name(
        matches[0].table_identity.as_bytes(),
    )
    .unwrap()
}

fn ordered_fault_tokens(rows: &[Value]) -> Vec<String> {
    assert!(rows.len() <= 4096, "small fault oracle row limit");
    rows.iter()
        .map(|row| {
            assert!(row.is_object(), "exact output rows must be objects");
            // Object field insertion order is not a SQL value difference,
            // even when serde_json preserve_order is feature-unified. Preserve
            // row and nested-array order while canonicalizing every object.
            let mut canonical = row.clone();
            canonical.sort_all_objects();
            serde_json::to_string(&canonical).unwrap()
        })
        .collect()
}

fn exact_tokens(rows: &[Value]) -> Vec<String> {
    assert!(rows.len() <= 4096, "small fault oracle row limit");
    let mut result: Vec<_> = rows
        .iter()
        .map(|row| {
            assert!(row.is_object(), "exact output rows must be objects");
            serde_json::to_string(row).unwrap()
        })
        .collect();
    result.sort();
    result
}

async fn output_rows(path: &Path) -> Vec<Value> {
    read_to_string(path)
        .await
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

async fn expected_alternatives(path: &Path) -> Vec<Vec<String>> {
    if path.extension().and_then(|e| e.to_str()) == Some("json") {
        let value: Value = serde_json::from_str(&read_to_string(path).await.unwrap()).unwrap();
        let alternatives = value.as_array().unwrap();
        assert!(!alternatives.is_empty() && alternatives.len() <= 64);
        alternatives
            .iter()
            .map(|v| exact_tokens(v.as_array().unwrap()))
            .collect()
    } else {
        vec![exact_tokens(&output_rows(path).await)]
    }
}

async fn check_native_exact(fixture: &FaultFixture, expected: &Path) {
    check_native_rows(fixture, expected, output_rows(&fixture.output).await).await;
}

async fn check_native_rows(fixture: &FaultFixture, expected: &Path, rows: Vec<Value>) {
    if fixture.ordered_native {
        assert_eq!(
            ordered_fault_tokens(&rows),
            ordered_fault_tokens(&output_rows(expected).await),
            "ordered state-table fault output differs from independent typed oracle"
        );
        return;
    }
    if !fixture.cdc_keys.is_empty() {
        let mut previous = HashMap::<String, Value>::new();
        for envelope in &rows {
            let before = &envelope["before"];
            let after = &envelope["after"];
            let row = if after.is_null() { before } else { after };
            assert!(row.is_object());
            let key = Value::Array(
                fixture
                    .cdc_keys
                    .iter()
                    .map(|name| row.get(name).expect("CDC key absent").clone())
                    .collect(),
            )
            .to_string();
            match envelope["op"].as_str().unwrap() {
                "c" => {
                    assert!(before.is_null() && !after.is_null());
                    assert!(previous.insert(key, after.clone()).is_none());
                }
                "u" => {
                    assert!(!after.is_null());
                    assert_eq!(previous.get(&key), Some(before));
                    assert_ne!(before, after);
                    previous.insert(key, after.clone());
                }
                "d" => {
                    assert!(after.is_null());
                    assert_eq!(previous.remove(&key).as_ref(), Some(before));
                }
                _ => panic!("unexpected CDC action"),
            }
        }
    }
    assert!(
        expected_alternatives(expected)
            .await
            .contains(&exact_tokens(&rows)),
        "output differs from every independently declared complete typed oracle"
    );
}

// Read the committed connector counters through the existing restore reader.
// Live output may already include the checkpoint-authorized next source row.
async fn committed_file_value<V: arroyo_types::Data + Copy>(
    fixture: &FaultFixture,
    epoch: usize,
    owner: OperatorName,
    key: &str,
) -> V {
    let selected = if leader_mode() {
        let storage = arroyo_state::get_storage_provider(&StorageProviderFor::Worker)
            .await
            .unwrap();
        let reference = paths(fixture).checkpoint_manifest(
            arroyo_state_protocol::types::Generation(0),
            Epoch(epoch as u64),
        );
        let manifest = arroyo_state_protocol::store::read_protobuf(storage.as_ref(), &reference)
            .await
            .unwrap()
            .unwrap();
        arroyo_rpc::MetadataOrManifest::Manifest(manifest)
    } else {
        arroyo_rpc::MetadataOrManifest::Metadata(
            StateBackend::load_checkpoint_metadata(
                &StorageProviderFor::Worker,
                &fixture.job_id,
                epoch as u32,
            )
            .await
            .unwrap(),
        )
    };
    let mut owners = Vec::new();
    for node in fixture.program.graph.node_weights() {
        for (op, _) in node.operator_chain.iter() {
            if op.operator_name == owner {
                owners.push((node.node_id, op.operator_id.clone()));
            }
        }
    }
    assert_eq!(owners.len(), 1);
    let (node, operator_id) = owners.pop().unwrap();
    let info = Arc::new(arroyo_types::TaskInfo {
        job_id: (*fixture.job_id).clone(),
        operator_idx: node,
        operator_name: format!("{owner:?}"),
        operator_id,
        task_index: 0,
        parallelism: 1,
        key_range: 0..=u64::MAX,
        checkpoint_file_path_layout: layout(),
    });
    let (tx, _rx) = channel(16);
    let configs = arroyo_state::global_table_config(
        "f",
        if owner == OperatorName::ConnectorSink {
            "file_sink"
        } else {
            "file_source"
        },
    );
    let (mut tables, _) =
        arroyo_state::tables::table_manager::TableManager::load(info, configs, tx, Some(&selected))
            .await
            .unwrap();
    tables
        .get_global_keyed_state::<String, V>("f")
        .await
        .unwrap()
        .get(&key.to_owned())
        .copied()
        .expect("committed file counter missing")
}

async fn check_native_prefix(fixture: &FaultFixture, epoch: usize) {
    let oracles = &fixture.native_oracles;
    let expected_input = fixture.advances[0] as usize
        + 1
        + if epoch == 2 {
            fixture.advances[1] as usize + 1
        } else {
            0
        };
    let rows: usize = committed_file_value(
        fixture,
        epoch,
        OperatorName::ConnectorSource,
        fixture.root.join("input.jsonl").to_str().unwrap(),
    )
    .await;
    assert_eq!(rows, expected_input, "wrong committed source prefix");
    let offset: u64 = committed_file_value(
        fixture,
        epoch,
        OperatorName::ConnectorSink,
        fixture.output.to_str().unwrap(),
    )
    .await;
    let bytes = tokio::fs::read(&fixture.output).await.unwrap();
    let offset = usize::try_from(offset).unwrap();
    assert!(offset <= bytes.len());
    let prefix = &bytes[..offset];
    assert!(prefix.is_empty() || prefix.last() == Some(&b'\n'));
    let prefix_path = fixture.root.join(format!("committed-epoch{epoch}.jsonl"));
    tokio::fs::write(&prefix_path, prefix).await.unwrap();
    check_native_rows(
        fixture,
        &oracles[epoch - 1],
        output_rows(&prefix_path).await,
    )
    .await;
    println!("NATIVE_FAULT_COMMITTED epoch={epoch} input_rows={rows} sink_offset={offset}");
}

async fn checkpoint_directory(fixture: &FaultFixture, logical_path: &str) -> PathBuf {
    let storage = arroyo_state::get_storage_provider(&StorageProviderFor::Worker)
        .await
        .unwrap();
    // Infer the pinned object_store::path::Path from the existing API. From<&str>
    // encodes reserved operator-name bytes; a raw filesystem join is incorrect.
    let key = logical_path.into();
    let encoded = storage.qualify_path(&key).to_string();
    fixture.root.join("checkpoints").join(encoded)
}

fn paths(fixture: &FaultFixture) -> arroyo_state_protocol::ProtocolPaths {
    arroyo_state_protocol::ProtocolPaths::new(
        arroyo_types::PipelineId::new("pipe-test"),
        arroyo_types::JobId(fixture.job_id.clone()),
    )
}

fn layout() -> CheckpointFilePathLayout {
    if leader_mode() {
        CheckpointFilePathLayout::Protocol {
            pipeline_id: arroyo_types::PipelineId::new("pipe-test"),
            generation: 0,
        }
    } else {
        CheckpointFilePathLayout::Legacy
    }
}

async fn abort_and_wait(
    engine: &RunningEngine,
    graph: &LogicalGraph,
    rx: &mut Receiver<ControlResp>,
) {
    let expected: HashSet<_> = graph.node_weights().map(|node| (node.node_id, 0)).collect();
    engine.abort_workers();
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut stopped = HashSet::new();
        while !expected.is_subset(&stopped) {
            match rx.recv().await {
                Some(ControlResp::TaskFailed {
                    task_id,
                    subtask_idx,
                    ..
                })
                | Some(ControlResp::TaskFinished {
                    task_id,
                    subtask_idx,
                    ..
                }) => {
                    stopped.insert((task_id, subtask_idx));
                }
                Some(_) => {}
                None => panic!("workers stopped before reporting every aborted stage"),
            }
        }
    })
    .await
    .expect("aborted worker stages did not terminate");
}

async fn restore_and_check(fixture: &FaultFixture, epoch: u64) {
    let (tx, mut rx) = channel(128);
    let program = local_program(
        &fixture.job_id,
        &fixture.program.graph,
        &get_udfs(),
        Some(epoch),
        tx,
    )
    .await;
    finish_from_checkpoint(&fixture.job_id, program, &mut rx).await;
    check_native_exact(fixture, &fixture.native_oracles[epoch as usize + 1]).await;
}

#[test_log(tokio::test)]
#[ignore = "dedicated process: caller native fixture, rocksdb and native config switch"]
async fn milestone3_native_upload_failure_restores_selected_checkpoint() {
    upload_failure().await;
}

async fn upload_failure() {
    let fixture = fixture("upload-failure").await;
    let (tx, mut rx) = channel(128);
    let program = local_program(
        &fixture.job_id,
        &fixture.program.graph,
        &get_udfs(),
        None,
        tx,
    )
    .await;
    let running = Engine::for_local(program, "pipe-test".into(), (*fixture.job_id).clone())
        .await
        .unwrap()
        .start()
        .await;
    advance(&running, fixture.advances[0]).await;
    checkpoint(
        &mut SmokeTestContext {
            job_id: fixture.job_id.clone(),
            engine: &running,
            control_rx: &mut rx,
            program: Arc::new(fixture.program.clone()),
        },
        1,
    )
    .await;
    check_native_prefix(&fixture, 1).await;
    let committed_output_bytes = committed_file_value::<u64>(
        &fixture,
        1,
        OperatorName::ConnectorSink,
        fixture.output.to_str().unwrap(),
    )
    .await;
    advance(&running, fixture.advances[1]).await;
    let blocked_path = layout().table_checkpoint_path(
        &fixture.job_id,
        &fixture.disk_operator,
        &fixture.disk_table,
        0,
        2,
        false,
    );
    let blocker = checkpoint_directory(&fixture, &blocked_path).await;
    tokio::fs::create_dir_all(blocker.parent().unwrap())
        .await
        .unwrap();
    tokio::fs::write(&blocker, b"injected upload failure: not a directory")
        .await
        .unwrap();
    let barrier = CheckpointBarrier {
        epoch: 2,
        min_epoch: 0,
        timestamp: SystemTime::now(),
        then_stop: false,
    };
    for source in running.source_controls() {
        source
            .send(ControlMessage::Checkpoint(barrier))
            .await
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            match rx
                .recv()
                .await
                .expect("workers stopped before injected fault")
            {
                ControlResp::CheckpointCompleted(c)
                    if c.operator_id == fixture.disk_operator && c.checkpoint_epoch == 2 =>
                {
                    panic!("disk operator reported completion despite failed upload")
                }
                ControlResp::TaskFailed { task_id, error, .. } => {
                    assert_eq!(
                        task_id, fixture.disk_node,
                        "unexpected worker failed: {error:?}"
                    );
                    assert_eq!(
                        error.operator_id.as_deref(),
                        Some(fixture.disk_operator.as_str()),
                        "failure did not come from the disk table exporter"
                    );

                    assert!(
                        error.message.contains(blocker.to_str().unwrap()),
                        "failure omitted injected upload path"
                    );
                    assert!(
                        error.message.contains("NotADirectory")
                            || error.message.contains("Not a directory"),
                        "failure was not the injected filesystem refusal"
                    );

                    println!(
                        "EXPECTED_UPLOAD_FAILURE mode={} error={error:?}",
                        if leader_mode() {
                            "leader"
                        } else {
                            "controller"
                        }
                    );
                    break;
                }
                _ => {}
            }
        }
    })
    .await
    .expect("file upload failure did not reach worker TaskFailed");
    tokio::time::timeout(Duration::from_secs(30), async {
        while tokio::fs::metadata(&fixture.output).await.unwrap().len() <= committed_output_bytes {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("newer uncommitted SQL rows never reached the sink");
    let storage = arroyo_state::get_storage_provider(&StorageProviderFor::Worker)
        .await
        .unwrap();
    if leader_mode() {
        assert!(
            !storage
                .exists(
                    paths(&fixture)
                        .checkpoint_manifest(arroyo_state_protocol::types::Generation(0), Epoch(2))
                        .to_string()
                )
                .await
                .unwrap()
        );
    } else {
        assert!(
            !storage
                .exists(format!(
                    "{}/checkpoints/checkpoint-0000002/metadata",
                    fixture.job_id
                ))
                .await
                .unwrap()
        );
    }
    abort_and_wait(&running, &fixture.program.graph, &mut rx).await;
    drop(running);
    tokio::fs::remove_file(blocker).await.unwrap();
    restore_and_check(&fixture, 1).await;
    println!(
        "UPLOAD_FAILURE_RECOVERY selected_epoch=1 output_records={} mode={}",
        output_rows(&fixture.output).await.len(),
        if leader_mode() {
            "leader"
        } else {
            "controller"
        }
    );

    println!("NATIVE_FAULT_ARTIFACT_ROOT {}", fixture.root.display());
}

#[test_log(tokio::test)]
#[ignore = "dedicated process: caller native fixture, rocksdb and native config switch"]
async fn milestone3_native_retained_checkpoint_survives_cleanup() {
    retained_recovery().await;
}

async fn retained_recovery() {
    let fixture = fixture("retained-recovery").await;
    let (tx, mut rx) = channel(128);
    let program = local_program(
        &fixture.job_id,
        &fixture.program.graph,
        &get_udfs(),
        None,
        tx,
    )
    .await;
    let running = Engine::for_local(program, "pipe-test".into(), (*fixture.job_id).clone())
        .await
        .unwrap()
        .start()
        .await;
    for epoch in [1, 2] {
        advance(&running, fixture.advances[epoch as usize - 1]).await;
        checkpoint(
            &mut SmokeTestContext {
                job_id: fixture.job_id.clone(),
                engine: &running,
                control_rx: &mut rx,
                program: Arc::new(fixture.program.clone()),
            },
            epoch,
        )
        .await;
        check_native_prefix(&fixture, epoch as usize).await;
    }
    let first_disk_file_dir = layout().table_checkpoint_path(
        &fixture.job_id,
        &fixture.disk_operator,
        &fixture.disk_table,
        0,
        1,
        false,
    );
    let retained_disk_file_dir = layout().table_checkpoint_path(
        &fixture.job_id,
        &fixture.disk_operator,
        &fixture.disk_table,
        0,
        2,
        false,
    );
    let first_dir = checkpoint_directory(&fixture, &first_disk_file_dir).await;
    let retained_dir = checkpoint_directory(&fixture, &retained_disk_file_dir).await;
    assert!(
        tokio::fs::read_dir(&first_dir)
            .await
            .unwrap()
            .next_entry()
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        tokio::fs::read_dir(&retained_dir)
            .await
            .unwrap()
            .next_entry()
            .await
            .unwrap()
            .is_some()
    );
    let storage = arroyo_state::get_storage_provider(&StorageProviderFor::Worker)
        .await
        .unwrap();
    if leader_mode() {
        let protocol_paths = paths(&fixture);
        let head = protocol_paths
            .checkpoint_manifest(arroyo_state_protocol::types::Generation(0), Epoch(2));
        for _ in 0..2 {
            arroyo_state_protocol::gc::cleanup_leader_checkpoints(
                storage.as_ref(),
                &protocol_paths,
                head.clone(),
                Epoch(2),
            )
            .await
            .unwrap();
        }
        assert!(
            !storage
                .exists(
                    protocol_paths
                        .checkpoint_manifest(arroyo_state_protocol::types::Generation(0), Epoch(1))
                        .to_string()
                )
                .await
                .unwrap()
        );
    } else {
        for _ in 0..2 {
            for operator in tasks_per_operator(&fixture.program.graph).keys() {
                ParquetBackend::cleanup_operator(
                    &StorageProviderFor::Worker,
                    (*fixture.job_id).clone(),
                    operator.clone(),
                    1,
                    2,
                )
                .await
                .unwrap();
            }
        }
    }
    assert!(match tokio::fs::read_dir(&first_dir).await {
        Ok(mut files) => files.next_entry().await.unwrap().is_none(),
        Err(error) => error.kind() == std::io::ErrorKind::NotFound,
    });
    assert!(
        tokio::fs::read_dir(&retained_dir)
            .await
            .unwrap()
            .next_entry()
            .await
            .unwrap()
            .is_some()
    );
    abort_and_wait(&running, &fixture.program.graph, &mut rx).await;
    drop(running);
    restore_and_check(&fixture, 2).await;
    println!(
        "RETAINED_CLEANUP_RECOVERY selected_epoch=2 removed_epoch=1 retries=2 output_records={} mode={}",
        output_rows(&fixture.output).await.len(),
        if leader_mode() {
            "leader"
        } else {
            "controller"
        }
    );

    println!("NATIVE_FAULT_ARTIFACT_ROOT {}", fixture.root.display());
}

#[test]
fn native_fault_table_selection_uses_declared_planned_identity() {
    use arroyo_rpc::grpc::api::{FusedStateTableOperator, StateTableDefinition};
    let owner = FusedStateTableOperator {
        tables: vec![
            StateTableDefinition {
                name: "caller_table".into(),
                table_identity: "opaque-a".into(),
                ..Default::default()
            },
            StateTableDefinition {
                name: "other_table".into(),
                table_identity: "opaque-b".into(),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    assert_eq!(
        state_table_fault_transport(&prost::Message::encode_to_vec(&owner), "caller_table"),
        arroyo_state_protocol::typed_checkpoint::transport_table_name(b"opaque-a").unwrap()
    );
    assert_ne!(
        state_table_fault_transport(&prost::Message::encode_to_vec(&owner), "caller_table"),
        state_table_fault_transport(&prost::Message::encode_to_vec(&owner), "other_table")
    );
}

#[test]
#[should_panic(expected = "one declared fault table required in owner")]
fn native_fault_table_selection_rejects_absent_caller_table() {
    let owner = arroyo_rpc::grpc::api::FusedStateTableOperator::default();
    state_table_fault_transport(&prost::Message::encode_to_vec(&owner), "missing");
}

#[test]
fn native_state_table_fault_oracle_preserves_order_types_nulls_and_full_values() {
    let rows = vec![
        serde_json::json!({"event_id": 1, "action": "insert", "old": null, "new": "opaque-value", "lookup": true}),
        serde_json::json!({"event_id": 2, "action": "none", "old": "opaque-value", "new": "opaque-value", "lookup": true}),
    ];
    assert_eq!(ordered_fault_tokens(&rows).len(), 2);
    let mut changed = rows.clone();
    changed.reverse();
    assert_ne!(ordered_fault_tokens(&rows), ordered_fault_tokens(&changed));
    let mut changed = rows.clone();
    changed[0]["lookup"] = serde_json::json!(1);
    assert_ne!(ordered_fault_tokens(&rows), ordered_fault_tokens(&changed));
    let mut changed = rows.clone();
    changed[0]["old"] = serde_json::json!("");
    assert_ne!(ordered_fault_tokens(&rows), ordered_fault_tokens(&changed));
    let mut changed = rows.clone();
    changed[1]["new"] = serde_json::json!("opaque-value-corrupted");
    assert_ne!(ordered_fault_tokens(&rows), ordered_fault_tokens(&changed));
    let mut changed = rows.clone();
    changed[1].as_object_mut().unwrap().remove("lookup");
    assert_ne!(ordered_fault_tokens(&rows), ordered_fault_tokens(&changed));
}

#[test]
fn native_state_table_fault_oracle_ignores_object_order_but_keeps_nested_array_order() {
    let original: Value = serde_json::from_str(
        r#"{"event_id":1,"nested":{"alpha":true,"beta":null},"events":[{"key":"a","value":1},{"key":"b","value":2}]}"#,
    ).unwrap();
    let reordered: Value = serde_json::from_str(
        r#"{"events":[{"value":1,"key":"a"},{"value":2,"key":"b"}],"nested":{"beta":null,"alpha":true},"event_id":1}"#,
    ).unwrap();
    assert_eq!(
        ordered_fault_tokens(std::slice::from_ref(&original)),
        ordered_fault_tokens(&[reordered])
    );
    let mut reversed = original.clone();
    reversed["events"].as_array_mut().unwrap().reverse();
    assert_ne!(
        ordered_fault_tokens(&[original]),
        ordered_fault_tokens(&[reversed])
    );
}
