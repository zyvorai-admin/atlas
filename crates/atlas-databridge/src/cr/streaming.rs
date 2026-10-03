// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0
//! Strimzi + Debezium CR builders for CDC: a `KafkaConnect` cluster (with the Debezium + JDBC-sink
//! plugins and a Kubernetes Secret config-provider) plus two `KafkaConnector`s — a Debezium source
//! (reads the cloud DB's WAL/binlog into Kafka) and a JDBC sink (applies the topics to the edge DB).
//!
//! NOTE: these specs are structurally correct but UNVERIFIED against a live Strimzi/Kafka + real DB
//! stack (see docs/DATABRIDGE.md). Secret values use Strimzi's config-provider syntax
//! `${secrets:<ns>/<secret>:<key>}` so credentials never appear in the CR.

use serde_json::{json, Value};

use crate::connector::SourceKind;

pub const GROUP: &str = "kafka.strimzi.io";
// Current Strimzi serves the Kafka/KafkaConnect/KafkaConnector kinds at v1 (v1beta2 was removed).
pub const VERSION: &str = "v1";
pub const CONNECT_KIND: &str = "KafkaConnect";
pub const CONNECTOR_KIND: &str = "KafkaConnector";

/// Names derived from a plan's short id, stable across reconciles.
pub fn connect_name(short: &str) -> String {
    format!("dbz-connect-{short}")
}
pub fn source_connector_name(short: &str) -> String {
    format!("dbz-src-{short}")
}
pub fn sink_connector_name(short: &str) -> String {
    format!("jdbc-sink-{short}")
}
pub fn topic_prefix(short: &str) -> String {
    format!("db{short}")
}

/// FNV-1a hash of a plan's short id, used to derive a MySQL/MariaDB `database.server.id` that's
/// unique per plan (short ids are all the same length, so hashing the length is not unique).
fn server_id_hash(short: &str) -> i64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    for b in short.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(PRIME);
    }
    (hash % 1_000_000_000) as i64
}

/// A `KafkaConnect` cluster spec (Strimzi v1). `groupId` + the three storage topics are top-level
/// required fields; the Secret config-provider lets connectors reference `${secrets:…}`.
///
/// `image` must be a Kafka Connect image that BUNDLES the Debezium (postgres/mysql) + a JDBC-sink
/// plugin (e.g. built with Strimzi's `spec.build` + a registry, or a prebuilt image). The default
/// Strimzi Connect image has no connector plugins, so leaving `image` empty means connectors won't
/// instantiate — set `ATLAS_DATABRIDGE_CONNECT_IMAGE` at deploy time.
pub fn connect_spec(bootstrap_servers: &str, replicas: i64, image: Option<&str>) -> Value {
    let mut spec = json!({
        "replicas": replicas.max(1),
        "bootstrapServers": bootstrap_servers,
        "groupId": "zyvor-databridge-connect",
        "configStorageTopic": "zyvor-connect-configs",
        "offsetStorageTopic": "zyvor-connect-offsets",
        "statusStorageTopic": "zyvor-connect-status",
        // Strimzi's default (initialDelaySeconds=60, periodSeconds=10, failureThreshold=3 -> ~90s
        // to first kill) is too tight for a Connect image bundling Debezium + JDBC-sink plugins: JVM
        // boot + Kafka group-rebalance routinely takes 90-120s+ even on an idle host, and CI/lab hosts
        // under load push that further — Strimzi then repeatedly kills the pod mid-startup before it
        // ever reports healthy, so it can never actually come up.
        "livenessProbe": { "initialDelaySeconds": 180, "timeoutSeconds": 10, "periodSeconds": 15, "failureThreshold": 6 },
        "readinessProbe": { "initialDelaySeconds": 180, "timeoutSeconds": 10, "periodSeconds": 15, "failureThreshold": 6 },
        "config": {
            "config.providers": "secrets",
            "config.providers.secrets.class": "io.strimzi.kafka.KubernetesSecretConfigProvider",
            // Internal topics default to RF 3; a single-broker lab Kafka needs 1 or Connect can't
            // create them (/health 500 -> crashloop).
            "config.storage.replication.factor": 1,
            "offset.storage.replication.factor": 1,
            "status.storage.replication.factor": 1
        }
    });
    if let Some(img) = image.filter(|s| !s.is_empty()) {
        spec["image"] = json!(img);
    }
    spec
}

/// Debezium source connector config for the given engine. `secret_ns`/`secret` reference the source
/// credentials Secret (keys `username`/`password`).
///
/// Snapshot mode is derived from the engine: homogeneous sources (Postgres/MySQL/MariaDB) were
/// already seeded by the dump→restore full-load, so Debezium streams from `never`. Heterogeneous
/// sources (Oracle/SQL Server → Postgres) have no dump full-load — Debezium's `initial` snapshot
/// seeds the edge (the JDBC sink runs with `auto.create`), then it streams changes.
#[allow(clippy::too_many_arguments)]
pub fn debezium_source_spec(
    kind: SourceKind,
    short: &str,
    host: &str,
    port: i64,
    database: &str,
    secret_ns: &str,
    secret: &str,
) -> Value {
    // MongoDB is not a JDBC/SQL source — it uses a single connection string, not host/port/db/user
    // fields — so build it separately.
    if kind.is_document() {
        return mongo_debezium_source_spec(short, host, port, database, secret_ns, secret);
    }

    let user = format!("${{secrets:{secret_ns}/{secret}:username}}");
    let pass = format!("${{secrets:{secret_ns}/{secret}:password}}");
    let prefix = topic_prefix(short);
    let bootstrap = "zyvor-kafka-kafka-bootstrap:9092";
    let snapshot_mode = if kind.homogeneous() {
        "never"
    } else {
        "initial"
    };
    let mut config = json!({
        "connector.class": kind.debezium_class(),
        "tasks.max": 1,
        "database.hostname": host,
        "database.port": port,
        "database.user": user,
        "database.password": pass,
        "database.dbname": database,
        "topic.prefix": prefix,
        // Encode DECIMAL/NUMERIC as a double, not a VariableScaleDecimal STRUCT the JDBC sink can't
        // bind. Snapshot mode depends on the engine (see doc comment).
        "decimal.handling.mode": "double",
        "snapshot.mode": snapshot_mode,
        // Debezium's default temporal encoding (adaptive_time_microseconds) emits DATE/TIME/TIMESTAMP
        // columns as ISO-8601 strings (e.g. "2026-08-25T14:59:41Z") in the Kafka message — the Aiven
        // JDBC sink binds a column's value with the JDBC driver's native setTimestamp/setDate, which
        // rejects that string format outright (MysqlDataTruncation: "Incorrect datetime value"),
        // failing every row with a temporal column. `connect` mode instead emits Kafka Connect's
        // logical Date/Time/Timestamp types, which the JDBC sink binds correctly.
        "time.precision.mode": "connect",
    });
    match kind {
        SourceKind::Postgres => {
            config["plugin.name"] = json!("pgoutput");
            config["slot.name"] = json!(format!("dbz_{short}"));
            config["publication.autocreate.mode"] = json!("filtered");
        }
        SourceKind::Mysql | SourceKind::Mariadb => {
            // MySqlConnector / MariaDbConnector share the config keys. Must be unique per plan —
            // `short` is fixed-length, so hashing it (not `.len()`, which is constant) is required
            // or every plan collides on the same binlog replication client id.
            config["database.server.id"] = json!(184_000_000 + (server_id_hash(short) % 1_000_000));
            config["schema.history.internal.kafka.bootstrap.servers"] = json!(bootstrap);
            config["schema.history.internal.kafka.topic"] = json!(format!("dbz-history-{short}"));
        }
        SourceKind::Sqlserver => {
            // SQL Server captures per-database via `database.names`; it also keeps a schema history.
            config["database.names"] = json!(database);
            config["database.encrypt"] = json!(false);
            config["schema.history.internal.kafka.bootstrap.servers"] = json!(bootstrap);
            config["schema.history.internal.kafka.topic"] = json!(format!("dbz-history-{short}"));
        }
        SourceKind::Oracle => {
            // LogMiner adapter; `database` is treated as the PDB name for a multitenant Oracle.
            config["database.connection.adapter"] = json!("logminer");
            config["database.pdb.name"] = json!(database);
            config["schema.history.internal.kafka.bootstrap.servers"] = json!(bootstrap);
            config["schema.history.internal.kafka.topic"] = json!(format!("dbz-history-{short}"));
        }
        // Handled by the is_document() early return above.
        SourceKind::Mongodb => unreachable!("mongodb built by mongo_debezium_source_spec"),
    }
    json!({ "class": config["connector.class"], "tasksMax": 1, "config": config })
}

/// Debezium **MongoDB** source connector config. Mongo takes a single `mongodb.connection.string`
/// (not host/port/user fields), reading the source's change streams / oplog. `snapshot.mode=no_data`
/// because `mongodump` seeds the edge in full-load (Mongo Debezium 3.x rejects `never`). Requires
/// the source to be a replica set.
fn mongo_debezium_source_spec(
    short: &str,
    host: &str,
    port: i64,
    database: &str,
    secret_ns: &str,
    secret: &str,
) -> Value {
    let user = format!("${{secrets:{secret_ns}/{secret}:username}}");
    let pass = format!("${{secrets:{secret_ns}/{secret}:password}}");
    let prefix = topic_prefix(short);
    // authSource=admin, explicit for consistency with the full-load/validate Jobs (the admin user
    // lives in `admin`). The Java driver defaults path-less URIs to admin, but pinning it removes any
    // ambiguity across Mongo tooling — see the loader `authSource` fix.
    let conn = format!("mongodb://{user}:{pass}@{host}:{port}/?replicaSet=rs0&authSource=admin");
    let config = json!({
        "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
        "tasks.max": 1,
        "mongodb.connection.string": conn,
        "topic.prefix": prefix,
        "database.include.list": database,
        "capture.mode": "change_streams_update_full",
        // Debezium Mongo 3.x: `never` is not valid — use `no_data` (skip snapshot; full-load already
        // seeded the edge via mongodump). Relational connectors still accept `never`.
        "snapshot.mode": "no_data",
    });
    json!({ "class": config["connector.class"], "tasksMax": 1, "config": config })
}

/// **MongoDB** sink connector config: the MongoDB Kafka Connect sink applying the Debezium Mongo CDC
/// topics to the edge Percona Server for MongoDB. `edge_host` is the edge replica-set service; the
/// user/pass come from the PSMDB users Secret via Strimzi's config-provider. Topics `<prefix>.<db>.
/// <collection>` are routed (RegexRouter) to `<collection>` so the sink writes into `edge_db`.
#[allow(clippy::too_many_arguments)]
pub fn mongo_sink_spec(
    short: &str,
    edge_host: &str,
    edge_db: &str,
    secret_ns: &str,
    edge_secret: &str,
    user_key: &str,
    pass_key: &str,
) -> Value {
    let prefix = topic_prefix(short);
    let user = format!("${{secrets:{secret_ns}/{edge_secret}:{user_key}}}");
    let pass = format!("${{secrets:{secret_ns}/{edge_secret}:{pass_key}}}");
    // authSource=admin, explicit — see mongo_debezium_source_spec / the loader authSource fix.
    let uri = format!("mongodb://{user}:{pass}@{edge_host}/?replicaSet=rs0&authSource=admin");
    // RegexRouter rewrites `<prefix>.<db>.<collection>` → `<collection>` before the sink looks up
    // per-topic config. MongoSinkConfig matches the *post-SMT* topic against `topics.regex`, so the
    // pattern must accept both the original Debezium topic (consumer subscribe) and the bare
    // collection name (config lookup after route). See MongoSinkConfig.getMongoSinkTopicConfig.
    let topics_regex = format!("({prefix}[.][^.]+[.].*|[^.]+)");
    let config = json!({
        "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
        "tasks.max": 1,
        "topics.regex": topics_regex,
        "connection.uri": uri,
        "database": edge_db,
        // Interpret Debezium MongoDB change events and apply the corresponding insert/update/delete.
        "change.data.capture.handler": "com.mongodb.kafka.connect.sink.cdc.debezium.mongodb.MongoDbHandler",
        "consumer.override.auto.offset.reset": "earliest",
        "consumer.override.metadata.max.age.ms": "10000",
        // Route `<prefix>.<db>.<collection>` -> `<collection>`; DefaultNamespaceMapper then uses
        // `database` + topic(=collection) as the write namespace.
        "transforms": "route",
        "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.route.regex": format!("{prefix}[.][^.]+[.](.*)"),
        "transforms.route.replacement": "$1"
    });
    json!({ "class": "com.mongodb.kafka.connect.MongoSinkConnector", "tasksMax": 1, "config": config })
}

/// JDBC sink connector config applying the source topics to the edge DB. `edge_secret` is the
/// operator app Secret (CNPG `uri`/Percona `root`); we build a JDBC URL to the edge service.
#[allow(clippy::too_many_arguments)]
pub fn jdbc_sink_spec(
    short: &str,
    jdbc_url: &str,
    secret_ns: &str,
    edge_secret: &str,
    edge_user: &str,
    edge_pass_key: &str,
    pk_fields: &str,
    auto_create: bool,
) -> Value {
    let prefix = topic_prefix(short);
    let config = json!({
        "connector.class": "io.aiven.connect.jdbc.JdbcSinkConnector",
        "tasks.max": 1,
        "topics.regex": format!("{prefix}[.][^.]+[.].*"),
        "connection.url": jdbc_url,
        "connection.user": edge_user,
        "connection.password": format!("${{secrets:{secret_ns}/{edge_secret}:{edge_pass_key}}}"),
        "insert.mode": "upsert",
        // A single Aiven sink connector takes one pk.fields for all its tables (the record-key PK
        // column name(s)); default `id`, override with ATLAS_DATABRIDGE_SINK_PK_FIELDS.
        "pk.mode": "record_key",
        "pk.fields": pk_fields,
        // Homogeneous migrations pre-create the edge tables in full-load, so auto.create is off.
        // Heterogeneous (Oracle/SQL Server → Postgres) has no dump full-load — the sink creates the
        // edge tables from the Debezium snapshot records.
        "auto.create": auto_create,
        "auto.evolve": true,
        "consumer.override.auto.offset.reset": "earliest",
        // topics.regex discovers new Debezium topics on a metadata refresh; shorten it from the 5min
        // default so a table's topic (created on first change) is picked up promptly (no restart).
        "consumer.override.metadata.max.age.ms": "10000",
        // Flatten the Debezium envelope to the row image, and route `<prefix>.<schema>.<table>` topics
        // to the bare `<table>` name so the sink writes to the matching edge table.
        "transforms": "unwrap,route",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.route.regex": format!("{prefix}[.][^.]+[.](.*)"),
        "transforms.route.replacement": "$1"
    });
    json!({ "class": "io.aiven.connect.jdbc.JdbcSinkConnector", "tasksMax": 1, "config": config })
}

/// Debezium JDBC sink config for **MySQL-family edges** (Percona XtraDB). Unlike the Aiven sink it
/// consumes the Debezium envelope as-is, so Debezium's own logical types bind correctly — notably
/// `io.debezium.time.ZonedTimestamp`, which Debezium always uses for MySQL/MariaDB `TIMESTAMP`
/// columns regardless of `time.precision.mode` and which the Aiven sink rejects
/// (`Incorrect datetime value`). Primary keys come from the record key (every key column, so
/// composite keys work without `pk.fields`), deletes are applied, and the edge tables (pre-created
/// by the homogeneous full-load) gain new columns via `schema.evolution=basic`.
pub fn debezium_jdbc_sink_spec(
    short: &str,
    jdbc_url: &str,
    secret_ns: &str,
    edge_secret: &str,
    edge_user: &str,
    edge_pass_key: &str,
) -> Value {
    let prefix = topic_prefix(short);
    let config = json!({
        "connector.class": DEBEZIUM_JDBC_SINK,
        "tasks.max": 1,
        "topics.regex": format!("{prefix}[.][^.]+[.].*"),
        "connection.url": jdbc_url,
        "connection.username": edge_user,
        "connection.password": format!("${{secrets:{secret_ns}/{edge_secret}:{edge_pass_key}}}"),
        "insert.mode": "upsert",
        "primary.key.mode": "record_key",
        "delete.enabled": true,
        "schema.evolution": "basic",
        "consumer.override.auto.offset.reset": "earliest",
        "consumer.override.metadata.max.age.ms": "10000",
        // `<prefix>.<db>.<table>` -> `<table>`; the sink then writes to the topic-named table.
        "collection.name.format": "${topic}",
        "transforms": "route",
        "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.route.regex": format!("{prefix}[.][^.]+[.](.*)"),
        "transforms.route.replacement": "$1"
    });
    json!({ "class": DEBEZIUM_JDBC_SINK, "tasksMax": 1, "config": config })
}

const DEBEZIUM_JDBC_SINK: &str = "io.debezium.connector.jdbc.JdbcSinkConnector";

/// Whether a Strimzi `KafkaConnector` reports its connector `state == RUNNING`.
pub fn connector_running(status: &Value) -> bool {
    status
        .get("connectorStatus")
        .and_then(|c| c.get("connector"))
        .and_then(|c| c.get("state"))
        .and_then(|s| s.as_str())
        .map(|s| s.eq_ignore_ascii_case("RUNNING"))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_stable() {
        assert_eq!(connect_name("abc123"), "dbz-connect-abc123");
        assert_eq!(source_connector_name("abc123"), "dbz-src-abc123");
        assert_eq!(topic_prefix("abc123"), "dbabc123");
    }

    #[test]
    fn pg_source_uses_pgoutput_and_secret_provider() {
        let s = debezium_source_spec(
            SourceKind::Postgres,
            "abc123",
            "prod.rds.aws",
            5432,
            "appdb",
            "zyvor-databridge",
            "src-creds",
        );
        assert_eq!(
            s["config"]["connector.class"],
            "io.debezium.connector.postgresql.PostgresConnector"
        );
        assert_eq!(s["config"]["plugin.name"], "pgoutput");
        assert_eq!(s["config"]["topic.prefix"], "dbabc123");
        assert_eq!(s["config"]["snapshot.mode"], "never"); // homogeneous — full-load seeded it
        assert_eq!(
            s["config"]["database.password"],
            "${secrets:zyvor-databridge/src-creds:password}"
        );
    }

    #[test]
    fn mysql_source_sets_server_id_and_history() {
        let s = debezium_source_spec(
            SourceKind::Mysql,
            "abc123",
            "prod.rds.aws",
            3306,
            "appdb",
            "zyvor-databridge",
            "src-creds",
        );
        assert_eq!(
            s["config"]["connector.class"],
            "io.debezium.connector.mysql.MySqlConnector"
        );
        assert!(s["config"]["database.server.id"].as_i64().unwrap() > 0);
        assert!(s["config"]["schema.history.internal.kafka.topic"]
            .as_str()
            .unwrap()
            .contains("abc123"));
    }

    #[test]
    fn mariadb_uses_its_own_connector_class() {
        let s = debezium_source_spec(
            SourceKind::Mariadb,
            "abc123",
            "h",
            3306,
            "appdb",
            "ns",
            "creds",
        );
        assert_eq!(
            s["config"]["connector.class"],
            "io.debezium.connector.mariadb.MariaDbConnector"
        );
        assert!(s["config"]["database.server.id"].as_i64().unwrap() > 0);
    }

    #[test]
    fn heterogeneous_sources_snapshot_initial() {
        let ora = debezium_source_spec(
            SourceKind::Oracle,
            "abc123",
            "h",
            1521,
            "ORCLPDB",
            "ns",
            "creds",
        );
        assert_eq!(
            ora["config"]["connector.class"],
            "io.debezium.connector.oracle.OracleConnector"
        );
        assert_eq!(ora["config"]["snapshot.mode"], "initial");
        assert_eq!(ora["config"]["database.pdb.name"], "ORCLPDB");
        let mss = debezium_source_spec(
            SourceKind::Sqlserver,
            "abc123",
            "h",
            1433,
            "appdb",
            "ns",
            "creds",
        );
        assert_eq!(
            mss["config"]["connector.class"],
            "io.debezium.connector.sqlserver.SqlServerConnector"
        );
        assert_eq!(mss["config"]["snapshot.mode"], "initial");
        assert_eq!(mss["config"]["database.names"], "appdb");
    }

    #[test]
    fn sink_targets_topic_regex_and_upsert() {
        let s = jdbc_sink_spec(
            "abc123",
            "jdbc:postgresql://edge-rw:5432/appdb",
            "zyvor-databridge",
            "edge-app",
            "app",
            "password",
            "id",
            false,
        );
        assert_eq!(s["config"]["insert.mode"], "upsert");
        assert_eq!(s["config"]["pk.fields"], "id");
        assert_eq!(s["config"]["auto.create"], false);
        assert_eq!(
            s["config"]["transforms.unwrap.type"],
            "io.debezium.transforms.ExtractNewRecordState"
        );
        assert_eq!(s["config"]["topics.regex"], "dbabc123[.][^.]+[.].*");
    }

    #[test]
    fn sink_auto_creates_for_heterogeneous() {
        let s = jdbc_sink_spec(
            "abc123",
            "jdbc:postgresql://edge-rw:5432/appdb",
            "ns",
            "edge-app",
            "app",
            "password",
            "id",
            true,
        );
        assert_eq!(s["config"]["auto.create"], true);
    }

    #[test]
    fn mysql_edge_sink_takes_the_debezium_envelope() {
        let s = debezium_jdbc_sink_spec(
            "abc123",
            "jdbc:mysql://edge-haproxy:3306/shop",
            "ns",
            "edge-secrets",
            "root",
            "root",
        );
        let c = &s["config"];
        assert_eq!(s["class"], "io.debezium.connector.jdbc.JdbcSinkConnector");
        assert_eq!(c["connection.username"], "root");
        assert_eq!(c["connection.password"], "${secrets:ns/edge-secrets:root}");
        assert_eq!(c["primary.key.mode"], "record_key");
        assert_eq!(c["delete.enabled"], true);
        assert_eq!(c["topics.regex"], "dbabc123[.][^.]+[.].*");
        assert_eq!(
            c["transforms"], "route",
            "no unwrap: the sink reads the envelope"
        );
        assert!(c.get("pk.fields").is_none());
    }

    #[test]
    fn mongo_source_uses_connection_string_not_jdbc() {
        let s = debezium_source_spec(
            SourceKind::Mongodb,
            "abc123",
            "mongo.rds.aws",
            27017,
            "appdb",
            "ns",
            "src-creds",
        );
        assert_eq!(
            s["config"]["connector.class"],
            "io.debezium.connector.mongodb.MongoDbConnector"
        );
        let conn = s["config"]["mongodb.connection.string"].as_str().unwrap();
        assert!(conn.contains("mongo.rds.aws:27017"));
        assert!(conn.contains("replicaSet=rs0"));
        assert!(conn.contains("authSource=admin"));
        assert!(s["config"].get("database.hostname").is_none());
        assert_eq!(s["config"]["snapshot.mode"], "no_data");
    }

    #[test]
    fn mongo_sink_routes_topics_to_collections() {
        let s = mongo_sink_spec(
            "abc123",
            "edge-rs0.zyvor-databridge.svc:27017",
            "appdb",
            "ns",
            "edge-secrets",
            "MONGODB_DATABASE_ADMIN_USER",
            "MONGODB_DATABASE_ADMIN_PASSWORD",
        );
        assert_eq!(
            s["config"]["connector.class"],
            "com.mongodb.kafka.connect.MongoSinkConnector"
        );
        assert_eq!(s["config"]["database"], "appdb");
        assert!(s["config"]["connection.uri"]
            .as_str()
            .unwrap()
            .contains("edge-rs0"));
        assert!(s["config"]["connection.uri"]
            .as_str()
            .unwrap()
            .contains("authSource=admin"));
        assert_eq!(
            s["config"]["change.data.capture.handler"],
            "com.mongodb.kafka.connect.sink.cdc.debezium.mongodb.MongoDbHandler"
        );
        assert_eq!(s["config"]["transforms.route.replacement"], "$1");
        assert_eq!(
            s["config"]["topics.regex"], "(dbabc123[.][^.]+[.].*|[^.]+)",
            "must match Debezium topic and post-RegexRouter collection name"
        );
    }

    #[test]
    fn running_status_parsed() {
        assert!(connector_running(
            &json!({ "connectorStatus": { "connector": { "state": "RUNNING" } } })
        ));
        assert!(!connector_running(
            &json!({ "connectorStatus": { "connector": { "state": "FAILED" } } })
        ));
        assert!(!connector_running(&json!({})));
    }
}
