use crate::ArroyoSchemaProvider;
use datafusion::common::ScalarValue;
use datafusion::execution::{SessionStateBuilder, TaskContext};
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use prost::Message;
use std::sync::Arc;

async fn actual(sql: &str) -> datafusion::common::Result<Vec<ScalarValue>> {
    let statement = crate::parse_sql(sql)?.remove(0);
    let logical = crate::tables::produce_optimized_plan(&statement, &ArroyoSchemaProvider::new())?;
    let state = SessionStateBuilder::new()
        .with_config(datafusion::prelude::SessionConfig::new().with_target_partitions(1))
        .with_default_features()
        .with_physical_optimizer_rules(vec![])
        .build();
    let physical = DefaultPhysicalPlanner::default()
        .create_physical_plan(&logical, &state)
        .await?;
    let codec = crate::physical::ArroyoPhysicalExtensionCodec::default();
    let bytes = PhysicalPlanNode::try_from_physical_plan(physical, &codec)?.encode_to_vec();
    let context = Arc::new(TaskContext::default());
    let physical = PhysicalPlanNode::decode(bytes.as_slice())
        .unwrap()
        .try_into_physical_plan(
            &crate::physical::new_registry(),
            context.runtime_env().as_ref(),
            &codec,
        )?;
    let physical = crate::local_physical::wrap_local_execution(physical)?;
    let batches = datafusion::physical_plan::collect(physical, context).await?;
    let mut result = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            result.push(ScalarValue::try_from_array(batch.column(0), row)?);
        }
    }
    Ok(result)
}

#[tokio::test]
async fn finite_scalar_max_null_empty_cardinality_and_case() {
    assert_eq!(actual("SELECT (SELECT MAX(v) FROM (VALUES (prior.a), (prior.b)) AS local(v)) FROM (VALUES (3, 1), (NULL, NULL)) AS prior(a,b)").await.unwrap(),
        vec![ScalarValue::Int64(Some(3)), ScalarValue::Int64(None)]);
    assert_eq!(
        actual("SELECT (SELECT v FROM (VALUES (1)) AS t(v) WHERE false)")
            .await
            .unwrap(),
        vec![ScalarValue::Int64(None)]
    );
    assert!(
        actual("SELECT (SELECT v FROM (VALUES (1),(2)) AS t(v))")
            .await
            .unwrap_err()
            .to_string()
            .contains("more than one row")
    );
    assert_eq!(
        actual("SELECT CASE WHEN false THEN (SELECT v FROM (VALUES (1),(2)) AS t(v)) ELSE 7 END")
            .await
            .unwrap(),
        vec![ScalarValue::Int64(Some(7))]
    );
}

#[tokio::test]
async fn finite_array_difference_and_admission_exact_sql() {
    let sql = "SELECT ARRAY(SELECT member FROM UNNEST(previous.items || incoming.add_items) AS additions(member) EXCEPT SELECT member FROM UNNEST(incoming.remove_items) AS removals(member) ORDER BY member) AS items FROM (VALUES (ARRAY['a','b'])) AS previous(items) CROSS JOIN (VALUES (ARRAY['b','c'], ARRAY['b'])) AS incoming(add_items, remove_items)";
    let result = actual(sql).await.unwrap();
    assert_eq!(
        result[0],
        ScalarValue::List(ScalarValue::new_list(
            &[
                ScalarValue::Utf8(Some("a".into())),
                ScalarValue::Utf8(Some("c".into()))
            ],
            &arrow_schema::DataType::Utf8,
            true
        ))
    );
    let sql = "SELECT CASE WHEN incoming.member IS NOT NULL AND (COALESCE(CARDINALITY(previous.items),0) < 1000 OR EXISTS (SELECT 1 FROM UNNEST(previous.items) AS existing(member) WHERE existing.member = incoming.member)) THEN ARRAY(SELECT member FROM UNNEST(previous.items) AS existing(member) UNION VALUES (incoming.member) ORDER BY member) ELSE previous.items END AS items FROM (VALUES (ARRAY['b','a'])) AS previous(items) CROSS JOIN (VALUES ('c')) AS incoming(member)";
    let result = actual(sql).await.unwrap();
    assert_eq!(
        result[0],
        ScalarValue::List(ScalarValue::new_list(
            &[
                ScalarValue::Utf8(Some("a".into())),
                ScalarValue::Utf8(Some("b".into())),
                ScalarValue::Utf8(Some("c".into()))
            ],
            &arrow_schema::DataType::Utf8,
            true
        ))
    );
}

#[tokio::test]
async fn unsupported_grandparent_correlation_fails_clearly() {
    let error = actual("SELECT (SELECT (SELECT outside.v)) FROM (VALUES (4)) AS outside(v)")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("outside"));
    assert!(format!("{error:?}").contains("not found"));
}

#[tokio::test]
async fn nested_aggregate_and_sort_subqueries() {
    assert_eq!(
        actual("SELECT (SELECT MAX((SELECT t.v)) FROM (VALUES (4),(2)) AS t(v))")
            .await
            .unwrap(),
        vec![ScalarValue::Int64(Some(4))]
    );
    assert_eq!(
        actual("SELECT ARRAY(SELECT t.v FROM (VALUES (4),(2)) AS t(v) ORDER BY (SELECT t.v))")
            .await
            .unwrap(),
        vec![ScalarValue::List(ScalarValue::new_list(
            &[ScalarValue::Int64(Some(2)), ScalarValue::Int64(Some(4))],
            &arrow_schema::DataType::Int64,
            true
        ))]
    );
}

#[tokio::test]
async fn first_admitted_membership_cap_and_blank_member() {
    const SQL: &str = "SELECT CASE WHEN incoming.member IS NOT NULL AND (COALESCE(CARDINALITY(previous.items),0) < 1000 OR EXISTS (SELECT 1 FROM UNNEST(previous.items) AS existing(member) WHERE existing.member=incoming.member)) THEN ARRAY(SELECT member FROM UNNEST(previous.items) AS existing(member) UNION VALUES(incoming.member) ORDER BY member) ELSE previous.items END FROM (VALUES(ARRAY[''])) previous(items) CROSS JOIN (VALUES('x')) incoming(member)";
    let statement = crate::parse_sql(SQL).unwrap().remove(0);
    let logical =
        crate::tables::produce_optimized_plan(&statement, &ArroyoSchemaProvider::new()).unwrap();
    let state = SessionStateBuilder::new()
        .with_config(datafusion::prelude::SessionConfig::new().with_target_partitions(1))
        .with_default_features()
        .with_physical_optimizer_rules(vec![])
        .build();
    let physical = DefaultPhysicalPlanner::default()
        .create_physical_plan(&logical, &state)
        .await
        .unwrap();
    let codec = crate::physical::ArroyoPhysicalExtensionCodec::default();
    let node = PhysicalPlanNode::try_from_physical_plan(physical, &codec).unwrap();
    let context = TaskContext::default();
    let physical = node
        .try_into_physical_plan(
            &crate::physical::new_registry(),
            context.runtime_env().as_ref(),
            &codec,
        )
        .unwrap();
    let projection = physical
        .as_any()
        .downcast_ref::<datafusion::physical_plan::projection::ProjectionExec>()
        .unwrap();
    let expression = projection.expr()[0].0.clone();
    let schema = projection.input().schema();
    assert_eq!(schema.fields().len(), 2);
    let mut members = vec![ScalarValue::Utf8(Some(String::new()))];
    let evaluate = |members: &[ScalarValue], member: &str| {
        arrow_array::RecordBatch::try_new(
            schema.clone(),
            vec![
                ScalarValue::List(ScalarValue::new_list(
                    members,
                    &arrow_schema::DataType::Utf8,
                    true,
                ))
                .to_array()
                .unwrap(),
                ScalarValue::Utf8(Some(member.into())).to_array().unwrap(),
            ],
        )
        .unwrap()
    };
    for number in (1..1000).rev() {
        let incoming = format!("m{number:04}");
        let batch = evaluate(&members, &incoming);
        let resolved = crate::local_physical::resolve_expression(expression.clone(), &batch)
            .await
            .unwrap();
        let value = resolved.evaluate(&batch).unwrap().into_array(1).unwrap();
        let ScalarValue::List(value) = ScalarValue::try_from_array(&value, 0).unwrap() else {
            panic!()
        };
        members = (0..value.value(0).len())
            .map(|index| ScalarValue::try_from_array(&value.value(0), index).unwrap())
            .collect();
        assert_eq!(members.len(), 1001 - number as usize);
    }
    assert_eq!(members.len(), 1000);
    assert!(members.contains(&ScalarValue::Utf8(Some(String::new()))));
    for incoming in ["!unseen", "m0001"] {
        let batch = evaluate(&members, incoming);
        let resolved = crate::local_physical::resolve_expression(expression.clone(), &batch)
            .await
            .unwrap();
        assert_eq!(
            ScalarValue::try_from_array(
                &resolved.evaluate(&batch).unwrap().into_array(1).unwrap(),
                0
            )
            .unwrap(),
            ScalarValue::List(ScalarValue::new_list(
                &members,
                &arrow_schema::DataType::Utf8,
                true
            ))
        );
    }
}

#[tokio::test]
async fn finite_full_join_cte_null_elements_and_array_width() {
    assert_eq!(actual("SELECT (WITH lhs AS (SELECT v FROM (VALUES(1),(2)) t(v)) SELECT COUNT(*) FROM lhs FULL OUTER JOIN (VALUES(2),(3)) rhs(v) ON lhs.v=rhs.v)").await.unwrap(),vec![ScalarValue::Int64(Some(3))]);
    assert!(actual("SELECT ARRAY(SELECT 1,2)").await.is_err());
    assert_eq!(actual("SELECT ARRAY(SELECT v FROM UNNEST(ARRAY[1,NULL,1]) t(v) EXCEPT SELECT v FROM (VALUES(1)) x(v))").await.unwrap(),vec![ScalarValue::List(ScalarValue::new_list(&[ScalarValue::Int64(None)],&arrow_schema::DataType::Int64,true))]);
    assert_eq!(
        actual("SELECT ARRAY(SELECT v FROM (VALUES(1)) t(v) WHERE false)")
            .await
            .unwrap(),
        vec![ScalarValue::List(ScalarValue::new_list(
            &[],
            &arrow_schema::DataType::Int64,
            true
        ))]
    );
}

#[tokio::test]
async fn finite_json_table_composition_and_temporal_max() {
    assert_eq!(actual(r#"SELECT ARRAY(SELECT patch.tag FROM JSON_TABLE(source.doc,'$.keyvalue()' COLUMNS(tag VARCHAR PATH '$.key',enabled BOOLEAN PATH '$.value')) patch WHERE patch.enabled=true ORDER BY patch.tag) FROM (VALUES('{"a":true,"b":false,"c":null}')) source(doc)"#).await.unwrap(),vec![ScalarValue::List(ScalarValue::new_list(&[ScalarValue::Utf8(Some("a".into()))],&arrow_schema::DataType::Utf8,true))]);
    let values = actual("SELECT (SELECT MAX(exits.t) FROM (VALUES(previous.a),(previous.b),(previous.c)) exits(t)) FROM (VALUES(TIMESTAMP '2026-10-01 00:00:00',CAST(NULL AS TIMESTAMP),TIMESTAMP '2026-10-03 00:00:00')) previous(a,b,c)").await.unwrap();
    assert_eq!(
        values,
        actual("SELECT TIMESTAMP '2026-10-03 00:00:00'")
            .await
            .unwrap()
    );
    assert!(actual("SELECT (SELECT random())").await.is_err());
    assert!(actual("SELECT (SELECT missing.value)").await.is_err());
}

#[tokio::test]
async fn nested_join_predicate_fails_during_planning() {
    let error = actual("SELECT (SELECT COUNT(*) FROM (VALUES (1)) AS l(v) FULL JOIN (VALUES (1)) AS r(v) ON l.v = r.v AND EXISTS (SELECT 1 WHERE l.v = 1))").await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("finite local subqueries in JOIN ON predicates are unsupported"),
        "{error}"
    );
}
