//! MetaLake's rows as its connector emits them, through the planner's own unrolling with no primary key: each insert
//! a row, each delete a retraction, and a row's id the same whichever change carries it.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Int64Type, Schema, TimeUnit};
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::{TIMESTAMP_FIELD, UPDATING_META_FIELD};
use datafusion::catalog::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;

use crate::physical::DebeziumUnrollingExec;

/// Rows as MetaLake's core gives them: the table's columns, and what happened to each.
fn core_rows(keys: &[i64], changes: &[&str], seqs: &[i64]) -> RecordBatch {
    let times: Vec<i64> = seqs.iter().map(|s| 1_700_000_000_000_000 + s).collect();
    let columns: Vec<(&str, ArrayRef)> = vec![
        ("k", Arc::new(Int64Array::from(keys.to_vec()))),
        (
            "_change_type",
            Arc::new(StringArray::from(changes.to_vec())),
        ),
        ("_commit_seq", Arc::new(Int64Array::from(seqs.to_vec()))),
        (
            "_commit_timestamp",
            Arc::new(TimestampMicrosecondArray::from(times).with_timezone("UTC")),
        ),
    ];
    RecordBatch::try_from_iter(columns).unwrap()
}

#[tokio::test]
async fn test_metalake_rows_unroll() {
    let row = DataType::Struct(vec![Field::new("k", DataType::Int64, true)].into());
    let schema = Arc::new(Schema::new(vec![
        Field::new("before", row.clone(), true),
        Field::new("after", row, true),
        Field::new("op", DataType::Utf8, false),
        Field::new(
            TIMESTAMP_FIELD,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]));
    let out = ArroyoSchema::new_unkeyed(schema.clone(), 3);

    // 1 inserted, 2 deleted, then 2 inserted again
    let rows = core_rows(&[1, 2, 2], &["insert", "delete", "insert"], &[5, 6, 7]);
    let enveloped = metalake_arroyo::envelope::envelope(&rows, &out).unwrap();
    let input = DataSourceExec::new(Arc::new(
        MemorySourceConfig::try_new(&[vec![enveloped]], schema, None).unwrap(),
    ));
    let unrolled = DebeziumUnrollingExec::try_new(Arc::new(input), vec![]).unwrap();
    let batches = collect(Arc::new(unrolled), SessionContext::new().task_ctx())
        .await
        .unwrap();
    let batch = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();

    let keys = batch
        .column_by_name("k")
        .unwrap()
        .as_primitive::<Int64Type>();
    let meta = batch
        .column_by_name(UPDATING_META_FIELD)
        .unwrap()
        .as_struct();
    let retract = meta.column_by_name("is_retract").unwrap().as_boolean();
    let ids = meta.column_by_name("id").unwrap().as_fixed_size_binary();
    assert_eq!(keys.values().to_vec(), [1, 2, 2]);
    assert_eq!(
        (0..3).map(|i| retract.value(i)).collect::<Vec<_>>(),
        [false, true, false]
    );
    assert_ne!(ids.value(0), ids.value(1), "rows differ, ids differ");
    assert_eq!(
        ids.value(1),
        ids.value(2),
        "a row deleted and inserted again is the same row"
    );

    let times = batch
        .column_by_name(TIMESTAMP_FIELD)
        .unwrap()
        .as_primitive::<arrow::datatypes::TimestampNanosecondType>();
    assert_eq!(times.value(0), (1_700_000_000_000_000 + 5) * 1_000);
    assert!(!batch.column_by_name("k").unwrap().is_null(1));
}

/// A query that names MetaLake tables with no `WITH`, planned once `resolve` has found them in a catalog, as the API
/// does before it plans. It needs a Postgres at `DATABASE_URL`, as MetaLake's own tests do: without one there is no
/// catalog to find them in, and the test has nothing to check.
#[tokio::test]
async fn test_metalake_names_resolved() {
    use std::time::{SystemTime, UNIX_EPOCH};

    use metalake::{Catalog, CatalogConfig, NewTable};
    use metalake_arroyo::resolve::{Environment, resolve};

    use crate::{ArroyoSchemaProvider, SqlConfig, parse_and_get_program};

    let Ok(url) = std::env::var("DATABASE_URL") else {
        return;
    };
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let schema = format!("arroyo_names_{nanos}");
    let warehouse = format!("file://{}", std::env::temp_dir().join(&schema).display());
    let config = CatalogConfig::new(url.clone(), &schema).warehouse(&warehouse);
    let catalog = Catalog::init(config, None).await.unwrap();
    let main = catalog.branch("main").await.unwrap();
    main.transaction()
        .create_namespace(&["shop"])
        .commit()
        .await
        .unwrap();
    for (name, columns) in [("orders", vec!["k"]), ("totals", vec!["k", "orders"])] {
        let fields: Vec<Field> = columns
            .iter()
            .map(|c| Field::new(*c, DataType::Int64, true))
            .collect();
        let (tx, _) = main
            .transaction()
            .create_table(NewTable {
                namespace: &["shop"],
                name,
                schema: &Schema::new(fields),
                location: None,
                cluster_key: vec![],
                partitioning: vec![],
                properties: vec![],
            })
            .unwrap();
        tx.commit().await.unwrap();
    }
    catalog.close();

    let env = Environment {
        postgres: url,
        schema,
        storage: vec![],
    };
    let query = "INSERT INTO shop.totals SELECT k, count(*) AS orders FROM shop.orders GROUP BY k";
    let mut provider = ArroyoSchemaProvider::new();
    let tables = resolve(query, |name| provider.get_table(name).is_some(), &env)
        .await
        .unwrap();
    assert_eq!(tables.len(), 2);
    for table in tables {
        provider.add_connector_table(table);
    }
    parse_and_get_program(query, provider, SqlConfig::default())
        .await
        .unwrap();
}
