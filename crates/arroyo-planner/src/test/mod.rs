mod plan_tests;

use arrow_schema::DataType;
use arroyo_connectors::{
    EmptyConfig,
    nexmark::{NexmarkConnector, NexmarkTable},
};
use arroyo_operator::connector::Connector;
use arroyo_udf_host::parse::NullableType;
use test_log::test;

use crate::{ArroyoSchemaProvider, SqlConfig, parse_and_get_program};

fn get_test_schema_provider() -> ArroyoSchemaProvider {
    let mut schema_provider = ArroyoSchemaProvider::new();

    let nexmark = (NexmarkConnector {})
        .from_config(
            Some(1),
            "nexmark",
            EmptyConfig {},
            NexmarkTable {
                event_rate: 10.0,
                runtime: Some(10.0 * 1_000_000.0),
            },
            None,
        )
        .unwrap();

    schema_provider.add_connector_table(nexmark);

    schema_provider
}

#[test(tokio::test)]
async fn test_udf() {
    let mut schema_provider = get_test_schema_provider();

    schema_provider
        .add_rust_udf("#[udf] fn my_sqr(x: i64) -> i64 { x * x }", "")
        .unwrap();

    schema_provider
        .add_rust_udf(
            "#[udf] fn my_sqr_opt(x: i64) -> Option<i64> { Some(x * x) }",
            "",
        )
        .unwrap();

    let def = schema_provider.udf_defs.get("my_sqr").unwrap();
    assert_eq!(def.ret, NullableType::not_null(DataType::Int64));

    let def = schema_provider.udf_defs.get("my_sqr_opt").unwrap();
    assert_eq!(def.ret, NullableType::null(DataType::Int64));

    let sql = "SELECT my_sqr(bid.auction), my_sqr_opt(bid.auction) FROM nexmark";
    parse_and_get_program(sql, schema_provider, SqlConfig::default())
        .await
        .unwrap();
}

#[test(tokio::test)]
async fn stateful_processor_runtime_fixture_plans() {
    use arroyo_datastream::logical::OperatorName;
    use arroyo_rpc::grpc::api::StatefulProcessorOperator;
    use prost::Message;
    use std::collections::HashSet;

    for query in [
        include_str!(
            "../../../arroyo-sql-testing/src/test/queries/stateful_processor_qualified_filter.sql"
        ),
        include_str!(
            "../../../arroyo-sql-testing/src/test/queries/stateful_processor_computed_cte.sql"
        ),
        include_str!(
            "../../../arroyo-sql-testing/src/test/queries/stateful_processor_sequential_ctes.sql"
        ),
        include_str!(
            "../../../arroyo-sql-testing/src/test/queries/stateful_processor_operations.sql"
        ),
    ] {
        let compiled =
            parse_and_get_program(query, ArroyoSchemaProvider::new(), SqlConfig::default())
                .await
                .unwrap();
        let mut result_fields = HashSet::new();
        let mut stateful_nodes = 0;
        for node in compiled.program.graph.node_weights() {
            for (operator, _) in node.operator_chain.iter() {
                if operator.operator_name != OperatorName::StatefulProcessor {
                    continue;
                }
                stateful_nodes += 1;
                assert_eq!(node.parallelism, 1);
                let config =
                    StatefulProcessorOperator::decode(operator.operator_config.as_slice()).unwrap();
                for operation in config.operations {
                    assert!(result_fields.insert(operation.output_field));
                }
            }
        }
        assert_eq!(
            stateful_nodes, 1,
            "linear CTE state stages must have one ordered checkpoint owner"
        );
    }
}

#[test(tokio::test)]
async fn stateful_lazy_expressions_have_precise_errors() {
    for (expression, message) in [
        (
            "coalesce(state_get('m', CAST(bid.auction AS TEXT)), 'fallback')",
            "COALESCE",
        ),
        (
            "CASE WHEN state_delete('m', CAST(bid.auction AS TEXT)) THEN 'yes' ELSE 'no' END",
            "CASE conditions",
        ),
        (
            "CASE WHEN random() > 0.5 THEN state_put('m', CAST(bid.auction AS TEXT), 'selected') ELSE CAST(NULL AS TEXT) END",
            "volatile CASE",
        ),
    ] {
        let sql = format!("SELECT {expression} FROM nexmark");
        let error = parse_and_get_program(&sql, get_test_schema_provider(), SqlConfig::default())
            .await
            .unwrap_err();
        assert!(error.to_string().contains(message), "{error}");
    }
}

#[test(tokio::test)]
async fn state_map_names_cannot_be_checkpoint_paths() {
    for name in [
        "",
        "../unsafe",
        "nested/map",
        "nested\\map",
        ".",
        "..",
        "非ASCII",
    ] {
        let sql = format!("SELECT state_get('{name}', CAST(bid.auction AS TEXT)) FROM nexmark");
        let error = parse_and_get_program(&sql, get_test_schema_provider(), SqlConfig::default())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("state map name"), "{error}");
    }
}
