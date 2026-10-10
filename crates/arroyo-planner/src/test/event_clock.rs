//! Event clock context and physical serialization regressions (STR-61).
use crate::physical::{ArroyoPhysicalExtensionCodec, DecodingContext};
use crate::{ArroyoSchemaProvider, SqlConfig, parse_and_get_program};
use arroyo_datastream::logical::OperatorName;
use arroyo_rpc::grpc::api::ValuePlanOperator;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion_proto::{physical_plan::AsExecutionPlan, protobuf::PhysicalPlanNode};
use prost::Message;
use test_log::test;

const SOURCE: &str = "CREATE TABLE events (
    id BIGINT PRIMARY KEY, event_time TIMESTAMP NOT NULL,
    completeness_time TIMESTAMP NOT NULL,
    WATERMARK FOR event_time AS completeness_time - INTERVAL '5' SECOND
) WITH (connector='single_file', path='/tmp/event-clock.jsonl', format='json', type='source');";

async fn compile(sql: &str) -> crate::CompiledSql {
    parse_and_get_program(
        sql,
        ArroyoSchemaProvider::new(),
        SqlConfig {
            default_parallelism: 1,
        },
    )
    .await
    .unwrap_or_else(|error| panic!("{error}\n{sql}"))
}

async fn rejects(sql: &str, diagnostic: &str) {
    let error = parse_and_get_program(sql, ArroyoSchemaProvider::new(), SqlConfig::default())
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains(diagnostic),
        "expected {diagnostic}, got {error}"
    );
}

#[test(tokio::test)]
async fn event_clock_projection_view_filter_and_physical_round_trip() {
    assert!(
        ArroyoSchemaProvider::new()
            .functions
            .contains_key("current_timestamp")
    );
    let sql = format!(
        "{SOURCE}
        CREATE VIEW renamed AS SELECT id, event_time AS renamed_time FROM events;
        CREATE VIEW hidden AS SELECT id FROM renamed;
        SELECT id, WATERMARK_TIMESTAMP() AS reference, WATERMARK_DATE() AS reference_date,
            CURRENT_TIMESTAMP AS processing_timestamp, CURRENT_DATE AS processing_date
        FROM hidden WHERE WATERMARK_DATE() >= DATE '1969-12-31'"
    );
    let compiled = compile(&sql).await;
    let provider = ArroyoSchemaProvider::new();
    let runtime = RuntimeEnvBuilder::new().build().unwrap();
    let codec = ArroyoPhysicalExtensionCodec {
        context: DecodingContext::Planning,
    };
    let mut decoded = 0;
    for node in compiled.program.graph.node_weights() {
        for (operator, _) in node.operator_chain.iter() {
            if operator.operator_name != OperatorName::ArrowValue {
                continue;
            }
            let config = ValuePlanOperator::decode(operator.operator_config.as_slice()).unwrap();
            let proto = PhysicalPlanNode::decode(config.physical_plan.as_slice()).unwrap();
            let physical = proto
                .try_into_physical_plan(&provider, &runtime, &codec)
                .expect("context binding must survive worker physical reconstruction");
            let display = format!(
                "{}",
                datafusion::physical_plan::displayable(physical.as_ref()).indent(true)
            );
            assert!(
                !display.contains("watermark_timestamp("),
                "unbound clock: {display}"
            );
            assert!(
                !display.contains("watermark_date("),
                "unbound clock: {display}"
            );
            decoded += 1;
        }
    }
    assert!(decoded > 0);
}

#[test(tokio::test)]
async fn event_clock_cdc_allows_independent_for_and_as_columns() {
    let source = SOURCE.replace("format='json'", "format='debezium_json'");
    compile(&format!(
        "{source} SELECT id, WATERMARK_TIMESTAMP(), WATERMARK_DATE() FROM events"
    ))
    .await;
    let source = source.replace(
        "WATERMARK FOR event_time",
        "WATERMARK FOR completeness_time",
    );
    compile(&format!(
        "{source} SELECT id, WATERMARK_TIMESTAMP(), WATERMARK_DATE() FROM events"
    ))
    .await;
}

#[test(tokio::test)]
async fn event_clock_lookup_keeps_trigger_designation() {
    compile(&format!(
        "{SOURCE}
        CREATE STATE TABLE retained (id BIGINT PRIMARY KEY, _timestamp TIMESTAMP) PARTITION BY id;
        SELECT events.id, target._timestamp AS retained_time,
            WATERMARK_TIMESTAMP() AS event_reference, WATERMARK_DATE() AS date_reference
        FROM events LEFT JOIN retained AS target ON events.id=target.id
        WHERE events.id > 0 AND WATERMARK_DATE() >= DATE '1969-12-31'"
    ))
    .await;
}

#[test(tokio::test)]
async fn event_clock_missing_and_ambiguous_designation_are_errors() {
    rejects(
        "SELECT WATERMARK_DATE()",
        "requires a source with WATERMARK FOR",
    )
    .await;
    let no_designation = "CREATE TABLE events WITH (connector='impulse', event_rate='1');
        SELECT WATERMARK_TIMESTAMP() FROM events";
    rejects(no_designation, "requires a source with WATERMARK FOR").await;
    let second = SOURCE.replace("events", "other_events");
    rejects(&format!("{SOURCE}{second}
        SELECT WATERMARK_TIMESTAMP() FROM events INNER JOIN other_events ON events.id=other_events.id"),
        "ambiguous WATERMARK FOR sources").await;
}

#[test(tokio::test)]
async fn event_clock_aggregate_dependency_is_not_lowered_to_contribution_time() {
    rejects(&format!("{SOURCE} SELECT COUNT(*) FILTER (WHERE WATERMARK_DATE() > DATE '2026-10-10') FROM events"),
        "retained contribution time is a distinct input").await;
    rejects(
        &format!(
            "{SOURCE} CREATE VIEW counts AS SELECT COUNT(*) AS n FROM events;
        SELECT n, WATERMARK_DATE() FROM counts"
        ),
        "aggregate output time is not an event clock",
    )
    .await;
}

#[test(tokio::test)]
async fn event_clock_rejects_a_view_that_overwrites_trigger_time() {
    rejects(
        &format!(
            "{SOURCE} CREATE VIEW replaced AS
        SELECT id, completeness_time AS _timestamp FROM events;
        SELECT WATERMARK_TIMESTAMP() FROM replaced"
        ),
        "triggering timestamp was replaced by a projection",
    )
    .await;
}

#[test(tokio::test)]
async fn event_clock_view_alias_retains_aggregate_dependency() {
    rejects(
        &format!(
            "{SOURCE}
        CREATE VIEW clock_rows AS SELECT id, WATERMARK_DATE() AS d FROM events;
        CREATE VIEW renamed_clock AS SELECT id, d AS reference_date FROM clock_rows;
        SELECT COUNT(*) FILTER (WHERE reference_date = DATE '2026-10-10') FROM renamed_clock"
        ),
        "retained contribution time is a distinct input",
    )
    .await;
    compile(&format!(
        "{SOURCE}
        CREATE VIEW contribution_rows AS SELECT id, CAST(event_time AS DATE) AS d FROM events;
        SELECT COUNT(*) FILTER (WHERE d = DATE '2026-10-10') FROM contribution_rows"
    ))
    .await;
}

#[test(tokio::test)]
async fn calendar_filter_serializes_horizons_static_gates_and_raw_clock() {
    use arroyo_rpc::grpc::api::UpdatingAggregateOperator;
    let horizons = [1, 7, 14, 30, 90];
    let expressions = horizons.iter().map(|days| {
        let temporal = if *days == 1 {
            "CAST(event_time AS DATE) = WATERMARK_DATE()".to_string()
        } else {
            format!("CAST(event_time AS DATE) BETWEEN WATERMARK_DATE() - INTERVAL '{}' DAY AND WATERMARK_DATE()", days - 1)
        };
        format!("COUNT(*) FILTER (WHERE id > 0 AND ({temporal})) AS count_{days}")
    }).collect::<Vec<_>>().join(",");
    let compiled = compile(&format!(
        "{SOURCE} SET updating_ttl = NULL;
        SELECT id, {expressions}, COUNT(*) AS lifetime FROM events GROUP BY id"
    ))
    .await;
    let configs = compiled
        .program
        .graph
        .node_weights()
        .flat_map(|node| node.operator_chain.iter())
        .filter(|(operator, _)| operator.operator_name == OperatorName::UpdatingAggregate)
        .map(|(operator, _)| {
            UpdatingAggregateOperator::decode(operator.operator_config.as_slice()).unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(configs.len(), 1);
    let config = &configs[0];
    assert_eq!(config.calendar_aggregates.len(), horizons.len());
    for (calendar, days) in config.calendar_aggregates.iter().zip(horizons) {
        assert_eq!(calendar.horizon_days, days);
        assert!(calendar.static_filter.is_some());
        assert!(!calendar.argument.is_empty());
        assert!(!calendar.contribution_date.is_empty());
        assert!(!calendar.reference_date.is_empty());
        // AS completeness_time is intentionally a different column from FOR.
        assert!(!calendar.context_id.contains("completeness_time"));
        assert!(
            calendar.context_id.contains("event_time")
                || calendar.context_id.contains("_timestamp")
        );
    }
    let reconstructed =
        UpdatingAggregateOperator::decode(config.encode_to_vec().as_slice()).unwrap();
    assert_eq!(
        reconstructed.calendar_aggregates,
        config.calendar_aggregates
    );
}

#[test(tokio::test)]
async fn calendar_filter_rejects_unsupported_clock_predicates() {
    for predicate in [
        "CAST(event_time AS DATE) > WATERMARK_DATE()",
        "CAST(event_time AS DATE) BETWEEN WATERMARK_DATE() - INTERVAL '6' HOUR AND WATERMARK_DATE()",
        "CAST(event_time AS DATE) BETWEEN WATERMARK_DATE() - INTERVAL '1' MONTH AND WATERMARK_DATE()",
        "CAST(event_time AS DATE) = WATERMARK_DATE() OR id > 0",
        "CAST(event_time AS DATE) BETWEEN WATERMARK_DATE() - INTERVAL '-1' DAY AND WATERMARK_DATE()",
    ] {
        rejects(
            &format!(
                "{SOURCE} SELECT id, COUNT(*) FILTER (WHERE {predicate}) FROM events GROUP BY id"
            ),
            "unsupported clock-dependent FILTER",
        )
        .await;
    }
}

#[test(tokio::test)]
async fn calendar_filter_ordinary_static_filter_remains_ordinary() {
    use arroyo_rpc::grpc::api::UpdatingAggregateOperator;
    let compiled = compile(&format!(
        "{SOURCE} SELECT id, COUNT(*) FILTER (WHERE id > 0) FROM events GROUP BY id"
    ))
    .await;
    for (operator, _) in compiled
        .program
        .graph
        .node_weights()
        .flat_map(|node| node.operator_chain.iter())
    {
        if operator.operator_name == OperatorName::UpdatingAggregate {
            let config =
                UpdatingAggregateOperator::decode(operator.operator_config.as_slice()).unwrap();
            assert!(config.calendar_aggregates.is_empty());
        }
    }
}

#[test(tokio::test)]
async fn calendar_filter_serializes_twenty_five_independent_outputs() {
    use arroyo_rpc::grpc::api::UpdatingAggregateOperator;
    let mut expressions = Vec::new();
    for gate in 0..5 {
        for days in [1, 7, 14, 30, 90] {
            let predicate = if days == 1 {
                "CAST(completeness_time AS DATE) = WATERMARK_DATE()".to_string()
            } else {
                format!(
                    "CAST(completeness_time AS DATE) BETWEEN WATERMARK_DATE() - INTERVAL '{}' DAY AND WATERMARK_DATE()",
                    days - 1
                )
            };
            let argument = if gate % 2 == 0 {
                "COUNT(id)"
            } else {
                "SUM(id)"
            };
            expressions.push(format!(
                "{argument} FILTER (WHERE id > {gate} AND ({predicate})) AS value_{gate}_{days}"
            ));
        }
    }
    let compiled = compile(&format!(
        "{SOURCE} SET updating_ttl = NULL;
        SELECT {}, COUNT(*) AS lifetime FROM events",
        expressions.join(",")
    ))
    .await;
    let config = compiled
        .program
        .graph
        .node_weights()
        .flat_map(|node| node.operator_chain.iter())
        .find(|(operator, _)| operator.operator_name == OperatorName::UpdatingAggregate)
        .map(|(operator, _)| {
            UpdatingAggregateOperator::decode(operator.operator_config.as_slice()).unwrap()
        })
        .unwrap();
    assert_eq!(config.calendar_aggregates.len(), 25);
    assert!(
        config
            .calendar_aggregates
            .iter()
            .all(|calendar| calendar.static_filter.is_some()
                && calendar.contribution_date != calendar.reference_date)
    );
    // The independent contribution column must never become the FOR clock.
    assert!(
        config
            .calendar_aggregates
            .iter()
            .all(|calendar| !calendar.context_id.contains("completeness_time"))
    );
}

#[test(tokio::test)]
async fn calendar_filter_cdc_alias_projection_coalesce_and_ordinal_mapping() {
    use arroyo_rpc::grpc::api::UpdatingAggregateOperator;
    use datafusion_proto::protobuf::{
        PhysicalExprNode, physical_expr_node::ExprType, physical_plan_node::PhysicalPlanType,
    };
    let source = SOURCE.replace("format='json'", "format='debezium_json'");
    let compiled = compile(&format!("{source}
        SET updating_ttl = NULL;
        CREATE VIEW projected AS SELECT id, CAST(completeness_time AS DATE) AS contribution_day FROM events;
        SELECT e.id, COUNT(e.id) AS lifetime,
          COUNT(*) FILTER (WHERE e.id > 0 AND e.contribution_day = WATERMARK_DATE()) AS recent_count,
          COALESCE(SUM(e.id) FILTER (WHERE e.id > 1 AND e.contribution_day BETWEEN WATERMARK_DATE() - INTERVAL '6' DAY AND WATERMARK_DATE()), 0) AS recent_sum
        FROM projected AS e GROUP BY e.id")).await;
    let config = compiled
        .program
        .graph
        .node_weights()
        .flat_map(|node| node.operator_chain.iter())
        .find(|(operator, _)| operator.operator_name == OperatorName::UpdatingAggregate)
        .map(|(operator, _)| {
            UpdatingAggregateOperator::decode(operator.operator_config.as_slice()).unwrap()
        })
        .unwrap();
    let proto = PhysicalPlanNode::decode(config.aggregate_exec.as_slice()).unwrap();
    let Some(PhysicalPlanType::Aggregate(aggregate)) = proto.physical_plan_type else {
        panic!("expected aggregate physical plan")
    };
    assert_eq!(config.calendar_aggregates.len(), 2);
    // A lifetime expression precedes the calendar expressions; hidden MAX follows
    // them. Descriptor ordinals must identify their actual aggregate inputs.
    assert_eq!(
        config
            .calendar_aggregates
            .iter()
            .map(|calendar| calendar.aggregate_index)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    let input_schema: arroyo_rpc::df::ArroyoSchema =
        config.input_schema.clone().unwrap().try_into().unwrap();
    for calendar in &config.calendar_aggregates {
        let index = calendar.aggregate_index as usize;
        let Some(ExprType::AggregateExpr(expression)) = &aggregate.aggr_expr[index].expr_type
        else {
            panic!("expected aggregate expression")
        };
        assert_eq!(
            PhysicalExprNode::decode(calendar.argument.as_slice()).unwrap(),
            expression.expr[0]
        );
        assert_eq!(
            calendar
                .static_filter
                .as_ref()
                .map(|bytes| PhysicalExprNode::decode(bytes.as_slice()).unwrap()),
            aggregate.filter_expr[index].expr
        );
        let date = PhysicalExprNode::decode(calendar.contribution_date.as_slice()).unwrap();
        let Some(ExprType::Column(column)) = date.expr_type else {
            panic!("projected contribution DATE must resolve to its wire input column")
        };
        assert_eq!(
            column.index as usize,
            input_schema.schema.index_of("contribution_day").unwrap()
        );
        let reference = PhysicalExprNode::decode(calendar.reference_date.as_slice()).unwrap();
        let Some(ExprType::Cast(cast)) = reference.expr_type else {
            panic!("reference DATE must cast the retained triggering timestamp")
        };
        let Some(ExprType::Column(column)) = cast.expr.as_ref().unwrap().expr_type.as_ref() else {
            panic!("reference DATE must preserve the triggering input column")
        };
        assert_eq!(
            column.index as usize,
            input_schema
                .schema
                .index_of(arroyo_rpc::TIMESTAMP_FIELD)
                .unwrap()
        );
        assert!(!calendar.context_id.contains("contribution_day"));
    }
    assert!(aggregate.filter_expr[0].expr.is_none());
}

#[test(tokio::test)]
async fn calendar_filter_serializes_coerced_numeric_aggregate_arguments() {
    use arroyo_rpc::grpc::api::UpdatingAggregateOperator;
    use datafusion_proto::protobuf::{
        physical_expr_node::ExprType, physical_plan_node::PhysicalPlanType,
    };
    for data_type in ["SMALLINT", "DECIMAL(12, 2)"] {
        let source = SOURCE.replace(
            "id BIGINT PRIMARY KEY,",
            "id BIGINT PRIMARY KEY, amount BIGINT,",
        );
        let compiled = compile(&format!("{source} SET updating_ttl = NULL;
            SELECT id, SUM(CAST(amount AS {data_type})) FILTER (WHERE CAST(event_time AS DATE) = WATERMARK_DATE()) AS recent
            FROM events GROUP BY id")).await;
        let config = compiled
            .program
            .graph
            .node_weights()
            .flat_map(|node| node.operator_chain.iter())
            .find(|(operator, _)| operator.operator_name == OperatorName::UpdatingAggregate)
            .map(|(operator, _)| {
                UpdatingAggregateOperator::decode(operator.operator_config.as_slice()).unwrap()
            })
            .unwrap();
        let proto = PhysicalPlanNode::decode(config.aggregate_exec.as_slice()).unwrap();
        let Some(PhysicalPlanType::Aggregate(aggregate)) = proto.physical_plan_type else {
            panic!("expected physical aggregate")
        };
        let descriptor = &config.calendar_aggregates[0];
        let Some(ExprType::AggregateExpr(expression)) =
            &aggregate.aggr_expr[descriptor.aggregate_index as usize].expr_type
        else {
            panic!("expected aggregate expression")
        };
        assert_eq!(
            descriptor.argument,
            expression.expr[0].encode_to_vec(),
            "coerced {data_type} argument must determine family identity"
        );
    }
}
