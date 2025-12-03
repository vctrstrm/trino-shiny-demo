"""
A modular `data_utils` design for branching/time travel + lineage across engines,
implemented with an **Abstract Base Class (ABC)** so your team gets runtime
contract enforcement.

Overview
--------
- **Ref** – Identifies a table version (branch, tag, snapshot id, or timestamp).
- **RunContext** – Immutable context about the job run (Airflow run id, code sha, params, etc.).
- **DataPlatform (ABC)** – Read/write/resolve snapshot operations (abstract API).
- **LineageSink (ABC)** – Persist lineage records in an engine-agnostic way.

Concrete platforms provided:
- **IcebergSparkPlatform** – Spark + Iceberg (branches, tags, snapshots, timestamps).
- **DeltaSparkPlatform** – Spark + Delta Lake (time travel; no native branching; raises on branch ops).
- **SnowflakePlatform** – Skeleton showing how to wire Snowflake (time travel; no branches).

Concrete sinks provided:
- **IcebergLineageSink** – Stores lineage into an Iceberg table.
- **NoopLineageSink** – Disables lineage persistence.

Usage
-----
```
from pyspark.sql import SparkSession
from data_utils import PlatformFactory, ctx_from_airflow, LineageRecord, InputLineage, millis, IcebergLineageSink

spark = SparkSession.builder.getOrCreate()
platform = PlatformFactory.iceberg_spark(spark)
ctx = ctx_from_airflow(airflow_ctx={})  # picks up params/branch overrides from Airflow if present

orders = platform.read_table("lake.sales.orders", ctx.default_input_ref)
orders_snap = platform.resolve_snapshot_id("lake.sales.orders", ctx.default_input_ref)

agg = orders.groupBy("customer_id").sum("amount")
wr = platform.write_table(agg, "lake.analytics.customer_ip_sum", ctx, mode="overwrite")

sink = IcebergLineageSink(spark, full_table="audit.etl_lineage")
sink.ensure()
sink.record(LineageRecord(
    recorded_at_ms=millis(),
    run_id=ctx.run_id,
    dag_id=ctx.dag_id,
    task_id=ctx.task_id,
    code_sha=ctx.code_sha,
    code_branch=ctx.code_branch,
    transform="ip_sum",
    target_table=wr.table,
    target_ref=wr.ref_applied,
    target_snapshot_id=wr.committed_snapshot_id,
    inputs=[InputLineage("lake.sales.orders", ctx.default_input_ref, orders_snap)],
    params_json=json.dumps(ctx.params or {}),
))
```
"""

from __future__ import annotations
import os
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from abc import ABC, abstractmethod
import json
import time

# Optional PySpark types are gated to avoid a hard dependency when used outside Spark
try:  # type: ignore
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql import functions as F  # noqa: F401  (used by users of this module)
except Exception:  # pragma: no cover
    SparkSession = Any  # type: ignore
    DataFrame = Any  # type: ignore
    F = Any  # type: ignore


# =============================================================
# Core dataclasses
# =============================================================
@dataclass(frozen=True)
class Ref:
    """Reference to a specific version of a table.

    Exactly one of the fields is typically set. Engines without branching ignore
    unsupported dimensions (e.g., Delta/Snowflake ignore `branch`/`tag`).

    Attributes
    ----------
    branch:
        Named branch to read/write (e.g., "main", "feature_x"). Iceberg only.
    tag:
        Immutable tag name pointing to a snapshot. Iceberg only.
    snapshot_id:
        Engine-specific numeric version identifier (Iceberg snapshot id, Delta version).
    as_of_ts_millis:
        Milliseconds since epoch for time travel reads.
    """

    branch: Optional[str] = None
    tag: Optional[str] = None
    snapshot_id: Optional[int] = None
    as_of_ts_millis: Optional[int] = None

    def is_unset(self) -> bool:
        """Return True when no addressing dimension is provided."""
        return not any([self.branch, self.tag, self.snapshot_id, self.as_of_ts_millis])

    def pretty(self) -> str:
        """Human-friendly string describing this ref."""
        if self.snapshot_id:
            return f"snapshot:{self.snapshot_id}"
        if self.as_of_ts_millis:
            return f"ts:{self.as_of_ts_millis}"
        if self.branch:
            return f"branch:{self.branch}"
        if self.tag:
            return f"tag:{self.tag}"
        return "latest"


@dataclass(frozen=True)
class RunContext:
    """Immutable execution metadata propagated to all reads/writes.

    Attributes
    ----------
    run_id:
        Unique identifier for the pipeline run (Airflow run id, etc.).
    dag_id, task_id:
        Airflow DAG/task identifiers when available.
    code_sha, code_branch:
        Git metadata to capture exact code provenance.
    params:
        Arbitrary user parameters passed via orchestration.
    default_input_ref:
        Default reference for reading inputs when a transform doesn't specify one.
    target_ref:
        Reference (usually a branch) where writes/commits should land.
    """

    run_id: str
    dag_id: Optional[str] = None
    task_id: Optional[str] = None
    code_sha: Optional[str] = None
    code_branch: Optional[str] = None
    params: Dict[str, Any] | None = None
    default_input_ref: Ref = Ref(branch="main")
    target_ref: Ref = Ref(branch="main")


@dataclass(frozen=True)
class WriteResult:
    """Result returned by :meth:`DataPlatform.write_table`."""

    table: str
    committed_snapshot_id: Optional[int]
    ref_applied: Ref
    extra: Dict[str, Any]


@dataclass(frozen=True)
class InputLineage:
    """Input table lineage element stored in :class:`LineageRecord`."""

    input_table: str
    input_ref: Ref
    input_snapshot_id: Optional[int]


@dataclass(frozen=True)
class LineageRecord:
    """A single lineage record persisted by a :class:`LineageSink`."""

    recorded_at_ms: int
    run_id: str
    dag_id: Optional[str]
    task_id: Optional[str]
    code_sha: Optional[str]
    code_branch: Optional[str]
    transform: str
    target_table: str
    target_ref: Ref
    target_snapshot_id: Optional[int]
    inputs: List[InputLineage]
    params_json: str


# =============================================================
# Abstract interfaces (ABC)
# =============================================================
class BranchingNotSupported(Exception):
    """Raised by platforms that don't implement branch/tag semantics."""
    pass


class DataPlatform(ABC):
    """Abstract base class for all data platform integrations.

    Engines must implement minimal primitives to enable branch/timestamp reads,
    writes, and concrete snapshot resolution for lineage.
    """

    @abstractmethod
    def read_table(self, table: str, ref: Optional[Ref] = None, **kwargs) -> DataFrame:
        """Read a table at a specific reference.

        Parameters
        ----------
        table : str
            Fully qualified table name (e.g., "catalog.db.table").
        ref : Ref, optional
            The :class:`Ref` to read; if omitted, engine defaults apply.
        **kwargs : Any
            Engine-specific read options.

        Returns
        -------
        DataFrame
            A DataFrame-like object for the engine (Spark DataFrame, etc.).
        """
        raise NotImplementedError

    @abstractmethod
    def write_table(
        self,
        df: Any,
        table: str,
        ctx: RunContext,
        mode: str = "append",
        extra_snapshot_props: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> WriteResult:
        """Write a dataset to the target reference and attach run metadata.

        Implementations should stamp snapshot/commit metadata when supported.
        """
        raise NotImplementedError

    @abstractmethod
    def resolve_snapshot_id(self, table: str, ref: Optional[Ref] = None) -> Optional[int]:
        """Resolve a concrete snapshot/version id for the given table and reference.

        Returns ``None`` when the engine doesn't expose a numeric id (e.g., Snowflake).
        """
        raise NotImplementedError

    def default_branch(self) -> Optional[str]:
        """Return the platform's conventional default branch name if any (e.g., "main")."""
        return None


class LineageSink(ABC):
    """Destination for persisting lineage records (e.g., an Iceberg table)."""

    @abstractmethod
    def ensure(self) -> None:
        """Create the sink storage if missing (idempotent)."""
        raise NotImplementedError

    @abstractmethod
    def record(self, rec: LineageRecord) -> None:
        """Persist a lineage record."""
        raise NotImplementedError


# =============================================================
# Helpers
# =============================================================

def millis() -> int:
    """Current time in milliseconds since epoch."""
    return int(time.time() * 1000)


def ctx_from_airflow(airflow_ctx: Dict[str, Any] | None, default_branch: str = "main") -> RunContext:
    """Build a :class:`RunContext` from an Airflow task context dict.

    Recognized params include:
    - ``input_branch``, ``input_tag``, ``input_snapshot_id``, ``input_as_of_ms``
    - ``default_input_branch``
    - ``target_branch``, ``target_tag``, ``target_snapshot_id``, ``target_as_of_ms``
    - ``code_sha``, ``code_branch``
    - All other keys are preserved in ``params``.
    """
    airflow_ctx = airflow_ctx or {}
    run_id = airflow_ctx.get("run_id") or (
        airflow_ctx.get("ti").run_id if airflow_ctx.get("ti") else f"manual-{millis()}"
    )
    dag_id = airflow_ctx.get("dag_id") or (airflow_ctx.get("dag").dag_id if airflow_ctx.get("dag") else None)
    task_id = airflow_ctx.get("task_id") or (airflow_ctx.get("ti").task_id if airflow_ctx.get("ti") else None)

    p = airflow_ctx.get("params", {}) or {}
    in_ref = Ref(
        branch=p.get("input_branch"),
        tag=p.get("input_tag"),
        snapshot_id=int(p["input_snapshot_id"]) if p.get("input_snapshot_id") else None,
        as_of_ts_millis=int(p["input_as_of_ms"]) if p.get("input_as_of_ms") else None,
    )
    if in_ref.is_unset():
        in_ref = Ref(branch=p.get("default_input_branch", default_branch))

    tgt_ref = Ref(
        branch=p.get("target_branch", default_branch),
        tag=p.get("target_tag"),
        snapshot_id=int(p["target_snapshot_id"]) if p.get("target_snapshot_id") else None,
        as_of_ts_millis=int(p["target_as_of_ms"]) if p.get("target_as_of_ms") else None,
    )
    if not any([tgt_ref.branch, tgt_ref.tag]):
        tgt_ref = Ref(branch=default_branch)

    return RunContext(
        run_id=run_id,
        dag_id=dag_id,
        task_id=task_id,
        code_sha=p.get("code_sha"),
        code_branch=p.get("code_branch"),
        params=p,
        default_input_ref=in_ref,
        target_ref=tgt_ref,
    )


# =============================================================
# Iceberg + Spark implementation
# =============================================================
class IcebergSparkPlatform(DataPlatform):
    """Spark + Apache Iceberg implementation of :class:`DataPlatform`.

    Supports reading by branch, tag, snapshot id, or timestamp; writing to branches/tags;
    and resolving concrete snapshot ids for lineage using Iceberg metadata tables.
    """

    def __init__(self, spark: SparkSession):
        """Create an Iceberg platform tied to a Spark session."""
        self.spark = spark

    def read_table(self, table: str, ref: Optional[Ref] = None, **kwargs) -> DataFrame:
        ref = ref or Ref(branch="main")
        rdr = self.spark.read.format("iceberg")
        if ref.snapshot_id:
            rdr = rdr.option("snapshot-id", int(ref.snapshot_id))
        elif ref.as_of_ts_millis:
            rdr = rdr.option("as-of-timestamp", str(int(ref.as_of_ts_millis)))
        elif ref.branch:
            rdr = rdr.option("branch", ref.branch)
        elif ref.tag:
            rdr = rdr.option("tag", ref.tag)
        return rdr.load(table)

    def resolve_snapshot_id(self, table: str, ref: Optional[Ref] = None) -> Optional[int]:
        ref = ref or Ref(branch="main")
        if ref.branch or ref.tag:
            name = ref.branch or ref.tag
            row = self.spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name='" + name + "'").first()
            return int(row[0]) if row and row[0] is not None else None
        if ref.as_of_ts_millis:
            q = f"""
            SELECT snapshot_id
            FROM {table}.history
            WHERE made_current_at <= timestamp_millis({int(ref.as_of_ts_millis)})
            ORDER BY made_current_at DESC
            LIMIT 1
            """
            row = self.spark.sql(q).first()
            return int(row[0]) if row and row[0] is not None else None
        if ref.snapshot_id:
            return int(ref.snapshot_id)
        row = self.spark.sql(
            f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
        ).first()
        return int(row[0]) if row and row[0] is not None else None

    def write_table(
        self,
        df: DataFrame,
        table: str,
        ctx: RunContext,
        mode: str = "append",
        extra_snapshot_props: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> WriteResult:
        props = {
            "snapshot-property.run_id": ctx.run_id,
            "snapshot-property.dag_id": ctx.dag_id or "",
            "snapshot-property.task_id": ctx.task_id or "",
            "snapshot-property.code_sha": ctx.code_sha or "",
            "snapshot-property.code_branch": ctx.code_branch or "",
            "snapshot-property.params": json.dumps(ctx.params or {}),
        }
        if extra_snapshot_props:
            for k, v in extra_snapshot_props.items():
                props[f"snapshot-property.{k}"] = v

        w = df.write.format("iceberg").mode(mode).options(**props)
        if ctx.target_ref.branch and ctx.target_ref.branch != "main":
            w = w.option("branch", ctx.target_ref.branch)
        if ctx.target_ref.tag:
            w = w.option("tag", ctx.target_ref.tag)
        
        # Use saveAsTable for Iceberg to handle table creation automatically
        w.saveAsTable(table)

        committed = self.resolve_snapshot_id(table, ctx.target_ref)
        return WriteResult(table=table, committed_snapshot_id=committed, ref_applied=ctx.target_ref, extra={})

    def default_branch(self) -> Optional[str]:
        return "main"


# =============================================================
# Delta Lake (Databricks / OSS) + Spark implementation
# =============================================================
class DeltaSparkPlatform(DataPlatform):
    """Spark + Delta Lake implementation of :class:`DataPlatform`.

    - Time travel via version/timestamp is supported.
    - Branching is **not** supported and will raise :class:`BranchingNotSupported`.
    - Use isolated schemas/tables or CLONE for branch-like workflows.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_table(self, table: str, ref: Optional[Ref] = None, **kwargs) -> DataFrame:
        ref = ref or Ref()
        if ref.snapshot_id:  # Delta: version as of
            return self.spark.read.format("delta").option("versionAsOf", int(ref.snapshot_id)).table(table)
        if ref.as_of_ts_millis:
            ts_expr = f"timestamp_millis({int(ref.as_of_ts_millis)})"
            return self.spark.sql(f"SELECT * FROM {table} TIMESTAMP AS OF {ts_expr}")
        if ref.branch or ref.tag:
            raise BranchingNotSupported("Delta Lake has no native branches; use isolated tables/schemas or CLONE.")
        return self.spark.table(table)

    def resolve_snapshot_id(self, table: str, ref: Optional[Ref] = None) -> Optional[int]:
        if ref and ref.snapshot_id:
            return int(ref.snapshot_id)
        hist = self.spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()
        if hist is None:
            return None
        try:
            return int(hist[0])
        except Exception:
            try:
                return int(hist[hist.__fields__.index("version")])  # type: ignore
            except Exception:
                return None

    def write_table(
        self,
        df: DataFrame,
        table: str,
        ctx: RunContext,
        mode: str = "append",
        extra_snapshot_props: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> WriteResult:
        if ctx.target_ref.branch or ctx.target_ref.tag:
            raise BranchingNotSupported("Delta Lake write to branch/tag is not supported.")
        df.write.format("delta").mode(mode).saveAsTable(table)
        committed = self.resolve_snapshot_id(table)
        return WriteResult(table=table, committed_snapshot_id=committed, ref_applied=ctx.target_ref, extra={})

    def default_branch(self) -> Optional[str]:
        return None


# =============================================================
# Snowflake implementation (via Snowpark or connector)
# =============================================================
class SnowflakePlatform(DataPlatform):
    """Skeleton :class:`DataPlatform` implementation for Snowflake.

    Snowflake supports time travel using ``AT/BEFORE`` modifiers on queries.
    It does not expose a numeric snapshot id nor branches/tags for table refs.
    """

    def __init__(self, session: Any):
        self.session = session

    def read_table(self, table: str, ref: Optional[Ref] = None, **kwargs) -> Any:
        ref = ref or Ref()
        if hasattr(self.session, "sql"):
            if ref.snapshot_id or ref.branch or ref.tag:
                raise BranchingNotSupported("Snowflake has no snapshot-id or branch addressability for tables.")
            if ref.as_of_ts_millis:
                ts = int(ref.as_of_ts_millis) / 1000.0
                return self.session.sql(f"SELECT * FROM {table} AT(TIMESTAMP => TO_TIMESTAMP({ts}))")
            return self.session.sql(f"SELECT * FROM {table}")
        raise NotImplementedError("Provide a Snowflake session with .sql(...) support.")

    def resolve_snapshot_id(self, table: str, ref: Optional[Ref] = None) -> Optional[int]:
        return None

    def write_table(
        self,
        df: Any,
        table: str,
        ctx: RunContext,
        mode: str = "append",
        extra_snapshot_props: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> WriteResult:
        if hasattr(df, "write") and hasattr(df.write, "save_as_table"):
            df.write.save_as_table(table, mode=mode)  # type: ignore[attr-defined]
        else:
            raise NotImplementedError("Pass a Snowpark DataFrame or implement COPY INTO.")
        return WriteResult(table=table, committed_snapshot_id=None, ref_applied=ctx.target_ref, extra={})

    def default_branch(self) -> Optional[str]:
        return None


# =============================================================
# Lineage sinks
# =============================================================
class IcebergLineageSink(LineageSink):
    """Persist lineage records into an Iceberg table via Spark SQL."""

    def __init__(self, spark: SparkSession, full_table: str = "audit.etl_lineage"):
        self.spark = spark
        self.full_table = full_table

    def ensure(self) -> None:
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.full_table} (
                recorded_at TIMESTAMP,
                run_id STRING,
                dag_id STRING,
                task_id STRING,
                code_sha STRING,
                code_branch STRING,
                transform STRING,
                target_table STRING,
                target_ref STRING,
                target_snapshot_id BIGINT,
                inputs_json STRING,
                params_json STRING
            ) USING iceberg
            """
        )

    def record(self, rec: LineageRecord) -> None:
        inputs = [
            {
                "input_table": i.input_table,
                "input_ref": i.input_ref.pretty(),
                "input_snapshot_id": i.input_snapshot_id,
            }
            for i in rec.inputs
        ]
        vals = (
            f"to_timestamp({rec.recorded_at_ms}/1000.0)",
            f"'{rec.run_id}'",
            f"'{rec.dag_id or ''}'",
            f"'{rec.task_id or ''}'",
            f"'{rec.code_sha or ''}'",
            f"'{rec.code_branch or ''}'",
            f"'{rec.transform}'",
            f"'{rec.target_table}'",
            f"'{rec.target_ref.pretty()}'",
            f"{rec.target_snapshot_id if rec.target_snapshot_id is not None else 'null'}",
            f"'{json.dumps(inputs)}'",
            f"'{rec.params_json}'",
        )
        self.spark.sql(
            f"INSERT INTO {self.full_table} VALUES (" + ", ".join(vals) + ")"
        )

# ============================================================
# DMAP prod data plafrom abstraction
# ============================================================

class dmapProdCatalogPlatform(DataPlatform):
    """Abstract base class for all data platform integrations.

    Engines must implement minimal primitives to enable branch/timestamp reads,
    writes, and concrete snapshot resolution for lineage.
    """

    def __init__(self, spark: SparkSession, config_path: str, branch: str = "master"):
        self.config_path = config_path
        self.config = self._load_catalog(config_path)
        self.catalog_path = self.config["CATALOG_PATH"]

        # build spark using config values, not catalog
        if spark is None:
            self.spark = self._build_spark(local_tmp=self.config["TMP_PATH"])
        else:
            self.spark = spark

        # now load the actual data registry catalog.json
        self.catalog = self._load_catalog(self.catalog_path)   # no branch arg here
        self.default_branch_name = branch

    def _load_catalog(self, path: str) -> Dict[str, Any]:
        if not os.path.exists(path):
            raise FileNotFoundError(f"Local catalog.json not found at {path}")
        with open(path, "r") as f:
            data = json.load(f) or {}
        return data

    def _get_entry(self, rid: str, branch: str = "master") -> Dict[str, Any]:
        # First: validate RID
        if rid not in self.catalog:
            raise KeyError(
                f"RID '{rid}' not found in local catalog {self.catalog_path}. "
                f"Available: {', '.join(sorted(self.catalog.keys()))}"
            )

        # Then: validate branch under that RID
        rid_entry = self.catalog[rid]
        if branch not in rid_entry:
            raise KeyError(
                f"Branch '{branch}' not found for RID '{rid}' in local catalog {self.catalog_path}. "
                f"Available branches: {', '.join(sorted(rid_entry.keys()))}"
            )

        return rid_entry[branch]

    def _build_spark(self, local_tmp=None) -> SparkSession:
        if local_tmp is None:
            local_tmp = self.config['TMP_PATH']
        conda_lib = os.environ.get("CONDA_PREFIX", self.config['CONDA_PATH']) + "/lib"
        os.makedirs(local_tmp, exist_ok=True)
        return (
            SparkSession.builder
            .appName("DataRegistrySampler")
            .config("spark.driver.memory", "10g")
            .config("spark.sql.parquet.compression.codec", "gzip")
            .config("spark.driver.extraLibraryPath", f"{conda_lib}:/lib64")
            .config("spark.executor.extraLibraryPath", f"{conda_lib}:/lib64")
            .config("spark.driverEnv.LD_LIBRARY_PATH", f"{conda_lib}:/lib64")
            .config("spark.executorEnv.LD_LIBRARY_PATH", f"{conda_lib}:/lib64")
            .config("spark.local.dir", local_tmp)
            .config("spark.executor.extraJavaOptions", f"-Djava.io.tmpdir={local_tmp}")
            .config("spark.driver.extraJavaOptions",  f"-Djava.io.tmpdir={local_tmp}")
            .getOrCreate()
        )

    # ---------- Custom schema → StructType ----------
    def _init_spark_types(self) -> None:
        """
        Lazily import pyspark.sql.types so the module can be imported without
        pyspark, but this platform class only works when pyspark is available.
        """
        try:
            from pyspark.sql.types import (
                StructType, StructField,
                StringType, IntegerType, LongType, ShortType, ByteType,
                DoubleType, FloatType, DecimalType,
                BooleanType, TimestampType, DateType, BinaryType,
                ArrayType, MapType,
            )
            import re
        except Exception as e:
            raise RuntimeError(
                "pyspark is required to use dmapProdCatalogPlatform"
            ) from e

        # Attach types and regex to self so other methods can use them
        self.StructType = StructType
        self.StructField = StructField
        self.StringType = StringType
        self.IntegerType = IntegerType
        self.LongType = LongType
        self.ShortType = ShortType
        self.ByteType = ByteType
        self.DoubleType = DoubleType
        self.FloatType = FloatType
        self.DecimalType = DecimalType
        self.BooleanType = BooleanType
        self.TimestampType = TimestampType
        self.DateType = DateType
        self.BinaryType = BinaryType
        self.ArrayType = ArrayType
        self.MapType = MapType
        self._decimal_re = re.compile(r"DECIMAL\((\d+)\s*,\s*(\d+)\)", re.IGNORECASE)

    def _to_dtype(self, entry):

        self._init_spark_types()

        if not isinstance(entry, dict) or "type" not in entry:
            raise ValueError(f"Invalid schema entry: {entry}")

        # If entry["type"] is a dict (e.g., {"type":"STRUCT","subSchemas":[...]}), merge it into entry
        t = entry.get("type")
        while isinstance(t, dict) and "type" in t:
            merged = dict(entry)
            inner = dict(t)
            # pull inner keys (type, subSchemas, arraySubtype, mapKeyType, mapValueType, etc.) up
            merged.update(inner)
            entry = merged
            t = entry.get("type")

        if not isinstance(t, str):
            raise ValueError(f"Unrecognized 'type' payload: {t!r}")
        t = t.upper()

        # DECIMAL(p, s)
        m = self._decimal_re.fullmatch(t)
        if m:
            return self.DecimalType(int(m.group(1)), int(m.group(2)))

        scalars = {
            "STRING": self.StringType(),
            "INT": self.IntegerType(), "INTEGER": self.IntegerType(),
            "BIGINT": self.LongType(), "LONG": self.LongType(),
            "SMALLINT": self.ShortType(),
            "TINYINT": self.ByteType(),
            "DOUBLE": self.DoubleType(),
            "FLOAT": self.FloatType(),
            "BOOLEAN": self.BooleanType(),
            "TIMESTAMP": self.TimestampType(),
            "DATE": self.DateType(),
            "BINARY": self.BinaryType(),
        }
        if t in scalars:
            return scalars[t]

        if t == "ARRAY":
            subtype = entry.get("arraySubtype")
            if not subtype:
                raise ValueError("ARRAY requires 'arraySubtype'.")
            if isinstance(subtype, dict):
                return self.ArrayType(self._to_dtype(subtype))
            else:
                return self.ArrayType(self._to_dtype({"type": subtype}))

        if t == "MAP":
            k = entry.get("mapKeyType")
            v = entry.get("mapValueType")
            if not k or not v:
                raise ValueError("MAP requires 'mapKeyType' and 'mapValueType'.")
            k_entry = k if isinstance(k, dict) else {"type": k}
            v_entry = v if isinstance(v, dict) else {"type": v}
            return self.MapType(self._to_dtype(k_entry), self._to_dtype(v_entry))

        if t == "STRUCT":
            subs = entry.get("subSchemas") or entry.get("fields")
            if not subs or not isinstance(subs, list):
                raise ValueError("STRUCT requires 'subSchemas' (list).")
            return self._to_struct(subs)

        raise ValueError(f"Unsupported type: {t}")


    def _to_struct(self, entries: List[Dict[str, Any]]):
        fields = []
        for e in entries:
            name = e["name"]
            dtype = self._to_dtype(e)
            nullable = bool(e.get("nullable", True))
            meta = e.get("customMetadata") or {}
            fields.append(self.StructField(name, dtype, nullable, metadata=meta))
        return self.StructType(fields)

    def _load_custom_schema(self, schema_path: str):
        with open(schema_path, "r") as f:
            data = json.load(f)

        if isinstance(data, dict) and "fields" in data and data.get("type") == "struct":
            return self.StructType.fromJson(data)
        if isinstance(data, list):
            return self._to_struct(data)

        raise ValueError("Schema JSON must be either a StructType object or a list of column entries.")

    # ---------- DataPlatform interface ----------

    def read_table(
        self,
        rid: str,
        branch: str = "master",
        ref: Optional[Ref] = None,
        **kwargs,
    ) -> DataFrame:
        """
        Read from local files defined in catalog.json.

        `ref` is currently ignored (no branching/time travel).
        """
        entry = self._get_entry(rid, branch)
        schema_path = entry.get("schema")
        file_path = entry.get("files")

        schema = self._load_custom_schema(schema_path)

        files_in_dir = os.listdir(file_path)
        if any(f.endswith(".parquet") for f in files_in_dir):
            fmt = "parquet"
        elif any(f.endswith(".csv") for f in files_in_dir):
            fmt = "csv"
        elif any(f.endswith(".json") for f in files_in_dir):
            fmt = "json"
        else:
            raise ValueError(
                f"Could not determine file format in directory '{file_path}' for RID '{rid}'"
            )

        reader = self.spark.read.format(fmt).schema(schema)
        df = reader.load(file_path)
        return df

    def write_table(
        self,
        df: DataFrame,
        table: str,
        ctx: RunContext,
        mode: str = "append",
        extra_snapshot_props: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> WriteResult:
        # Read-only platform by design
        raise NotImplementedError("dmapProdCatalogPlatform is read-only")

    def resolve_snapshot_id(self, table: str, ref: Optional[Ref] = None) -> Optional[int]:
        """
        No snapshot support for local files → always None.
        """
        return None

    def default_branch(self) -> Optional[str]:
        return None

class NoopLineageSink(LineageSink):
    """Lineage sink that performs no I/O (useful in tests or ad-hoc runs)."""

    def ensure(self) -> None:
        return None

    def record(self, rec: LineageRecord) -> None:
        return None


# =============================================================
# High-level Pipeline API for Data Engineers
# =============================================================
class DataPipeline:
    """
    High-level API that simplifies data engineering workflows.
    Handles Spark session creation, context detection, lineage tracking,
    and platform operations with minimal boilerplate.
    """

    def __init__(self, transform_name: str, platform_type: str = "iceberg", 
                 spark_session: Optional[SparkSession] = None, 
                 lineage_table: str = "audit.etl_lineage",
                 spark_config: Optional[Dict[str, str]] = None):
        """
        Initialize a data pipeline with automatic Spark session configuration.
        
        Parameters
        ----------
        transform_name : str
            Name of this transformation for lineage tracking
        platform_type : str
            Platform type: "iceberg", "delta", "snowflake" (default: "iceberg")
        spark_session : SparkSession, optional
            Pre-configured Spark session. If None, creates one automatically.
        lineage_table : str
            Full table name for lineage storage (default: "audit.etl_lineage")
        spark_config : Dict[str, str], optional
            Additional Spark configuration overrides
        """
        self.transform_name = transform_name
        self.platform_type = platform_type
        
        # Initialize or use provided Spark session
        self.spark = spark_session or self._create_spark_session(platform_type, spark_config)
        
        # Initialize platform adapter
        self.platform = self._create_platform(platform_type, self.spark)
        
        # Initialize context - auto-detects Airflow or creates standalone context
        self.ctx = self._detect_context()
        
        # Initialize lineage with graceful fallback
        self.lineage_sink = self._init_lineage(lineage_table)
        
        # Track inputs for lineage
        self._inputs: List[InputLineage] = []
        
        print(f"🚀 Pipeline '{transform_name}' initialized ({platform_type})")
        print(f"   Context: {self.ctx.run_id}")
        print(f"   Input ref: {self.ctx.default_input_ref.pretty()}")
        print(f"   Target ref: {self.ctx.target_ref.pretty()}")

    def _create_spark_session(self, platform_type: str, extra_config: Optional[Dict[str, str]] = None) -> SparkSession:
        """Create a Spark session configured for the specified platform."""
        builder = SparkSession.builder.appName(f"{self.transform_name}_{platform_type}")
        
        # Platform-specific configurations
        if platform_type == "iceberg":
            # Default Iceberg configuration  
            iceberg_config = {
                "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog", 
                "spark.sql.catalog.iceberg.type": "hive",
                "spark.sql.catalog.iceberg.uri": "thrift://hive-metastore:9083",
                "spark.sql.catalog.iceberg.warehouse": "file:///data/warehouse",
                "spark.sql.defaultCatalog": "iceberg"
            }
            for key, value in iceberg_config.items():
                builder = builder.config(key, value)
                
        elif platform_type == "delta":
            # Default Delta Lake configuration
            delta_config = {
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            }
            for key, value in delta_config.items():
                builder = builder.config(key, value)
                
        elif platform_type == "snowflake":
            # Snowflake typically uses JDBC/connector, minimal Spark config
            snowflake_config = {
                "spark.jars.packages": "net.snowflake:snowflake-jdbc:3.13.30,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3"
            }
            for key, value in snowflake_config.items():
                builder = builder.config(key, value)
        
        # Apply any additional/override configuration from config file or user
        if extra_config:
            for key, value in extra_config.items():
                builder = builder.config(key, str(value))  # Ensure string values
        
        return builder.getOrCreate()

    def _create_platform(self, platform_type: str, spark: SparkSession) -> DataPlatform:
        """Create the appropriate platform adapter."""
        if platform_type == "iceberg":
            return PlatformFactory.iceberg_spark(spark)
        elif platform_type == "delta": 
            return PlatformFactory.delta_spark(spark)
        elif platform_type == "snowflake":
            # Note: Snowflake would need different session handling
            raise NotImplementedError("Snowflake platform requires separate session management")
        else:
            raise ValueError(f"Unsupported platform type: {platform_type}")

    @classmethod
    def from_config(cls, transform_name: str, config_path: Optional[str] = None, **kwargs) -> 'DataPipeline':
        """
        Create a DataPipeline from configuration file.
        
        Parameters
        ----------
        transform_name : str
            Name of this transformation
        config_path : str, optional
            Path to config file. If None, looks for default locations:
            1. ./pipeline_config.yaml 
            2. ./config/pipeline_config.yaml
            3. Environment variable DATA_PIPELINE_CONFIG
        **kwargs : Any
            Override any config file settings
            
        Returns
        -------
        DataPipeline
            Configured pipeline instance
            
        Example config file (pipeline_config.yaml):
        ```yaml
        platform:
          type: "iceberg"  # or "delta", "snowflake"
          spark_config:
            spark.sql.adaptive.enabled: "true"
            spark.sql.adaptive.coalescePartitions.enabled: "true"
          warehouse_path: "s3://my-bucket/warehouse"
          metastore_uri: "thrift://metastore:9083"
          
        lineage:
          enabled: true
          table: "audit.etl_lineage"
          
        environment: "production"  # or "development", "staging"
        ```
        """
        config = cls._load_config(config_path)
        
        # Merge config with kwargs (kwargs take precedence)
        platform_type = kwargs.pop('platform_type', config.get('platform', {}).get('type', 'iceberg'))
        lineage_table = kwargs.pop('lineage_table', config.get('lineage', {}).get('table', 'audit.etl_lineage'))
        spark_config = kwargs.pop('spark_config', config.get('platform', {}).get('spark_config', {}))
        
        return cls(
            transform_name=transform_name,
            platform_type=platform_type,
            lineage_table=lineage_table,
            spark_config=spark_config,
            **kwargs
        )
    
    @classmethod
    def _load_config(cls, config_path: Optional[str] = None) -> Dict[str, Any]:
        """Load configuration from YAML file with fallback locations."""
        import os
        
        # Determine config file path
        if config_path:
            config_file = config_path
        else:
            # Try default locations
            candidates = [
                "./pipeline_config.yaml",
                "./config/pipeline_config.yaml", 
                os.getenv("DATA_PIPELINE_CONFIG", "")
            ]
            config_file = None
            for candidate in candidates:
                if candidate and os.path.exists(candidate):
                    config_file = candidate
                    break
        
        if not config_file or not os.path.exists(config_file):
            print("⚠️  No config file found, using defaults")
            return {}
        
        try:
            import yaml
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f) or {}
            print(f"📋 Loaded config from: {config_file}")
            return config
        except ImportError:
            print("⚠️  PyYAML not installed, using defaults. Install with: pip install pyyaml")
            return {}
        except Exception as e:
            print(f"⚠️  Failed to load config from {config_file}: {e}")
            return {}

    # Convenience constructors (kept for backward compatibility)
    @classmethod
    def for_iceberg(cls, transform_name: str, **kwargs) -> 'DataPipeline':
        """Convenience constructor for Iceberg pipelines."""
        return cls(transform_name, platform_type="iceberg", **kwargs)
    
    @classmethod  
    def for_delta(cls, transform_name: str, **kwargs) -> 'DataPipeline':
        """Convenience constructor for Delta Lake pipelines."""
        return cls(transform_name, platform_type="delta", **kwargs)
        
    @classmethod
    def with_spark_session(cls, transform_name: str, spark: SparkSession, **kwargs) -> 'DataPipeline':
        """Constructor when you already have a configured Spark session."""
        return cls(transform_name, spark_session=spark, **kwargs)

    def _detect_context(self) -> RunContext:
        """Auto-detect execution context (Airflow vs standalone)."""
        # Try to detect Airflow context from environment or task instance
        try:
            # Check if we're running in Airflow
            import os
            airflow_ctx = {}
            
            # Common Airflow environment variables
            if os.getenv('AIRFLOW_CTX_DAG_ID'):
                airflow_ctx = {
                    'dag_id': os.getenv('AIRFLOW_CTX_DAG_ID'),
                    'task_id': os.getenv('AIRFLOW_CTX_TASK_ID'),
                    'run_id': os.getenv('AIRFLOW_CTX_DAG_RUN_ID'),
                    'params': {}  # Could be enhanced to parse from env
                }
                print("📋 Detected Airflow execution context")
            else:
                print("📋 Running in standalone mode")
            
            return ctx_from_airflow(airflow_ctx)
        except Exception:
            return ctx_from_airflow({})

    def _init_lineage(self, lineage_table: str) -> LineageSink:
        """Initialize lineage sink with graceful fallback."""
        try:
            sink = IcebergLineageSink(self.spark, lineage_table)
            sink.ensure()
            print("📊 Lineage tracking enabled")
            return sink
        except Exception as e:
            print(f"⚠️  Lineage tracking disabled: {e}")
            return NoopLineageSink()

    def read_table(self, table: str, ref: Optional[Ref] = None, **kwargs) -> DataFrame:
        """
        Read a table with automatic lineage tracking.
        
        Parameters
        ----------
        table : str
            Fully qualified table name
        ref : Ref, optional
            Override the default input reference for this read
            
        Returns
        -------
        DataFrame
            The loaded DataFrame
        """
        ref = ref or self.ctx.default_input_ref
        print(f"📖 Reading {table} at {ref.pretty()}")
        
        try:
            df = self.platform.read_table(table, ref, **kwargs)
            count = df.count()
            print(f"   Loaded {count:,} records")
            
            # Track for lineage
            snapshot_id = self.platform.resolve_snapshot_id(table, ref)
            self._inputs.append(InputLineage(table, ref, snapshot_id))
            
            return df
        except Exception as e:
            print(f"❌ Failed to read {table}: {e}")
            raise

    def write_table(self, df: DataFrame, table: str, mode: str = "append", **kwargs) -> WriteResult:
        """
        Write a table with automatic lineage tracking and context stamping.
        
        Parameters
        ----------
        df : DataFrame
            Data to write
        table : str
            Target table name
        mode : str
            Write mode (append, overwrite, etc.)
            
        Returns
        -------
        WriteResult
            Write operation result with snapshot info
        """
        print(f"💾 Writing to {table} at {self.ctx.target_ref.pretty()}")
        
        count = df.count()
        extra_props = {
            "transform": self.transform_name,
            "output_records": str(count),
            **kwargs.pop("extra_snapshot_props", {})
        }
        
        result = self.platform.write_table(
            df, table, self.ctx, mode=mode, 
            extra_snapshot_props=extra_props, **kwargs
        )
        
        print(f"   Wrote {count:,} records (snapshot: {result.committed_snapshot_id})")
        
        # Record lineage
        self._record_lineage(table, result)
        
        return result

    def _record_lineage(self, target_table: str, write_result: WriteResult) -> None:
        """Record lineage for the completed transformation."""
        try:
            record = LineageRecord(
                recorded_at_ms=millis(),
                run_id=self.ctx.run_id,
                dag_id=self.ctx.dag_id,
                task_id=self.ctx.task_id,
                code_sha=self.ctx.code_sha,
                code_branch=self.ctx.code_branch,
                transform=self.transform_name,
                target_table=target_table,
                target_ref=self.ctx.target_ref,
                target_snapshot_id=write_result.committed_snapshot_id,
                inputs=self._inputs.copy(),
                params_json=json.dumps(self.ctx.params or {})
            )
            self.lineage_sink.record(record)
            print("📊 Lineage recorded")
        except Exception as e:
            print(f"⚠️  Lineage recording failed: {e}")

    def set_input_ref(self, ref: Ref) -> None:
        """Override the default input reference (for time travel, branch switching)."""
        self.ctx = RunContext(
            run_id=self.ctx.run_id,
            dag_id=self.ctx.dag_id,
            task_id=self.ctx.task_id,
            code_sha=self.ctx.code_sha,
            code_branch=self.ctx.code_branch,
            params=self.ctx.params,
            default_input_ref=ref,
            target_ref=self.ctx.target_ref
        )
        print(f"🔄 Input ref changed to: {ref.pretty()}")

    def set_target_ref(self, ref: Ref) -> None:
        """Override the target reference (for branch-specific writes)."""
        self.ctx = RunContext(
            run_id=self.ctx.run_id,
            dag_id=self.ctx.dag_id,
            task_id=self.ctx.task_id,
            code_sha=self.ctx.code_sha,
            code_branch=self.ctx.code_branch,
            params=self.ctx.params,
            default_input_ref=self.ctx.default_input_ref,
            target_ref=ref
        )
        print(f"🎯 Target ref changed to: {ref.pretty()}")


# =============================================================
# Factory
# =============================================================
class PlatformFactory:
    """Simple constructors for each supported platform."""

    @staticmethod
    def iceberg_spark(spark: SparkSession) -> DataPlatform:
        return IcebergSparkPlatform(spark)

    @staticmethod
    def delta_spark(spark: SparkSession) -> DataPlatform:
        return DeltaSparkPlatform(spark)

    @staticmethod
    def snowflake(session: Any) -> DataPlatform:
        return SnowflakePlatform(session)

    @staticmethod
    def dmap_prod(spark: SparkSession, config_path: str) -> DataPlatform:
        return dmapProdCatalogPlatform(spark, config_path)
