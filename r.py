
# ============================================================
# Adjacency-List BFS for Multi-Target Flipping (No Broadcast)
# Covers: NULL fills, latest rule per src, cycles, pruning, skew
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, concat_ws, array_contains, row_number, size, explode, collect_set, trim
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel
import time


# --- ABSOLUTE NO-BROADCAST GUARANTEE (session-wide) ---
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")          # classic threshold
spark.conf.set("spark.sql.adaptive.enabled", "true")                  # keep AQE
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1") # adaptive broadcast threshold OFF
spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")          # prefer SMJ
spark.conf.set("spark.sql.shuffle.partitions", "512")                 # tune for your cores (20 → 512; increase if spill persists)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.sort.bypassMergeThreshold", "1")    # keep merges predictable

# --- Helper: assert there is NO broadcast in a DataFrame's plan ---
def assert_no_broadcast(df, label=""):
    plan = df._jdf.queryExecution().executedPlan().toString()
    if "BroadcastHashJoin" in plan or "BroadcastNestedLoopJoin" in plan or "BroadcastExchange" in plan:
        raise RuntimeError(f"[NO-BROADCAST VIOLATION] Found broadcast in plan {label}:\n{plan}")


# --------------------------
# Config (tune for your cluster)
# --------------------------
NUM_PARTITIONS = 512               # 512 is a good start for ~20 cores; raise to 768/1024 if needed
MAX_DEPTH = 10                     # chain length bound
MAX_PATHS_PER_SRC = 50             # breadth cap per source
SALT_BUCKETS = 32                  # increase to 64 if a few hot sources dominate
MAX_RUNTIME_SEC = 60 * 45          # optional wall-clock cap; set None to disable

# --------------------------
# Spark settings (no broadcast)
# --------------------------
spark.conf.set("spark.sql.shuffle.partitions", str(NUM_PARTITIONS))
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")    # hard-disable broadcast
spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")    # stable memory
spark.conf.set("spark.sql.shuffle.sort.bypassMergeThreshold", "1")

# --------------------------
# Inputs
# --------------------------
df_flipping_ds = mapping_spark_df
price_data = spark.read.table("apl_silver.supply_chain.price_data_demand_planning")

# ============================================================
# 1) Master build + NULL fills via constrained joins (no explode)
# ============================================================

price_data_master = (
    price_data
    .withColumn("prode", F.substring("sku", 1, 4))
    .withColumn("shade", F.substring("sku", 5, 4))
    .withColumn("pack",  F.substring("sku", 9, 3))
    .select(
        trim(col("depot")).alias("depot"),
        trim(col("prode")).alias("prode"),
        trim(col("shade")).alias("shade"),
        trim(col("pack")).alias("pack")
    )
    .distinct()
    .repartition(NUM_PARTITIONS, col("depot"), col("prode"), col("shade"), col("pack"))
    .persist(StorageLevel.DISK_ONLY)
)
_ = price_data_master.limit(1).count()  # light materialization

# Normalize cond columns for consistent matching
for c in ["sales_office_cond", "prod_code_cond", "shade_code_cond", "pack_code_cond"]:
    df_flipping_ds = df_flipping_ds.withColumn(c, trim(col(c).cast("string")))

COND_TO_MASTER = {
    "sales_office_cond": "depot",
    "prod_code_cond":    "prode",
    "shade_code_cond":   "shade",
    "pack_code_cond":    "pack",
}

master_aliased = price_data_master.selectExpr(
    "depot as m_depot", "prode as m_prode", "shade as m_shade", "pack as m_pack"
)

def fill_one_null(df, null_cond_col):
    """
    Fill exactly one NULL cond column by joining to master using the other three cond columns as constraints.
    No broadcast; hint for MERGE (Sort-Merge join).
    """
    other_cond_cols = [c for c in COND_TO_MASTER.keys() if c != null_cond_col]
    join_cond = F.lit(True)
    for oc in other_cond_cols:
        mc = f"m_{COND_TO_MASTER[oc]}"
        join_cond = join_cond & (col(oc).isNull() | (col(oc) == col(mc)))

    need = df.filter(col(null_cond_col).isNull())
    keep = df.filter(col(null_cond_col).isNotNull())

    filled = (
        need.hint("MERGE")
            .join(master_aliased.hint("MERGE"), join_cond, "left")
            .withColumn(null_cond_col, col(f"m_{COND_TO_MASTER[null_cond_col]}"))
            .drop("m_depot", "m_prode", "m_shade", "m_pack")
    )
    return keep.unionByName(filled).dropDuplicates(df.columns)

df_flipping_ds = (
    df_flipping_ds
    .repartition(NUM_PARTITIONS,
                 col("sales_office_cond"),
                 col("prod_code_cond"),
                 col("shade_code_cond"),
                 col("pack_code_cond"))
    .persist(StorageLevel.DISK_ONLY)
)
_ = df_flipping_ds.limit(1).count()

for col_to_fill in ["prod_code_cond", "shade_code_cond", "pack_code_cond", "sales_office_cond"]:
    df_flipping_ds = fill_one_null(df_flipping_ds, col_to_fill)

# Fill flip columns from cond when flip is null
for code in ["prod", "shade", "pack", "sales_office"]:
    flip_col = f"flip_{code}_code" if code != "sales_office" else "flip_sales_office"
    cond_col = f"{code}_code_cond" if code != "sales_office" else "sales_office_cond"
    df_flipping_ds = df_flipping_ds.withColumn(
        flip_col, when(col(flip_col).isNull(), col(cond_col)).otherwise(col(flip_col))
    )

# ============================================================
# 2) Build latest edges: src -> dst, then adjacency list
# ============================================================

df_edges = (
    df_flipping_ds
    .withColumn("src", concat_ws("", col("sales_office_cond"), col("prod_code_cond"), col("shade_code_cond"), col("pack_code_cond")))
    .withColumn("dst", concat_ws("", col("flip_sales_office"), col("flip_prod_code"), col("flip_shade_code"), col("flip_pack_code")))
    .select("src", "dst", "rule_update_date")
    .repartition(NUM_PARTITIONS, col("src"))
    .persist(StorageLevel.DISK_ONLY)
)
_ = df_edges.limit(1).count()

# Use window to select latest dst per src (no self-join to max)
w_latest = Window.partitionBy("src").orderBy(col("rule_update_date").desc(), col("dst").asc())
edges_latest = (
    df_edges.withColumn("rn", row_number().over(w_latest))
            .filter(col("rn") == 1)
            .select("src", "dst")
            .distinct()
            .repartition(NUM_PARTITIONS, col("src"))
            .persist(StorageLevel.MEMORY_AND_DISK)
)
_ = edges_latest.limit(1).count()

# Build adjacency list (reduce width during expansion)
edges_adj = (
    edges_latest.groupBy("src")
                .agg(collect_set("dst").alias("adj"))    # set of neighbors
                .repartition(NUM_PARTITIONS, col("src"))
                .persist(StorageLevel.MEMORY_AND_DISK)
)
_ = edges_adj.limit(1).count()

# Universe of nodes (sources + destinations)
all_nodes = (
    edges_latest.select(col("src").alias("node")).union(edges_latest.select(col("dst").alias("node")))
    .distinct()
    .repartition(NUM_PARTITIONS, col("node"))
    .persist(StorageLevel.DISK_ONLY)
)
_ = all_nodes.limit(1).count()

# ============================================================
# 3) Node-centric BFS expansion using adjacency + explode
# ============================================================

paths_src = (
    all_nodes.select(
        col("node").alias("src"),
        col("node").alias("current"),
        F.array(col("node")).alias("history"),
        F.lit(0).alias("depth"),
        F.lit(False).alias("stopped")
    )
    .repartition(NUM_PARTITIONS, col("src"))
    .persist(StorageLevel.DISK_ONLY)
)
_ = paths_src.limit(1).count()

start_ts = time.time()
for depth in range(1, MAX_DEPTH + 1):
    if MAX_RUNTIME_SEC and (time.time() - start_ts > MAX_RUNTIME_SEC):
        print(f"[INFO] Time cap hit at depth {depth-1}; stopping.")
        break

    # Join to adjacency; explode neighbors to create new paths
    joined = (
        paths_src.alias("p")
                 .hint("MERGE")
                 .join(edges_adj.alias("e").hint("MERGE"), col("p.current") == col("e.src"), how="left")
                 .select(
                     col("p.src"),
                     col("p.current"),
                     col("p.history"),
                     col("p.depth"),
                     col("p.stopped"),
                     col("e.adj").alias("adj")   # array of neighbors or null
                 )
    )

    # Unchanged: no adjacency (dead end) -> stop
    unchanged = joined.filter(col("adj").isNull()).select(
        col("src"), col("current"), col("history"), col("depth"), F.lit(True).alias("stopped")
    )

    # New paths: explode adjacency; cycle check; increment depth
    exploded = joined.filter(col("adj").isNotNull()).select(
        col("src"),
        explode(col("adj")).alias("next"),
        col("history"), col("depth")
    )

    expanded = (
        exploded.withColumn("is_cycle", array_contains(col("history"), col("next")))
                .select(
                    col("src"),
                    col("next").alias("current"),
                    F.concat(col("history"), F.array(col("next"))).alias("history"),
                    (col("depth") + 1).alias("depth"),
                    F.lit(False).alias("stopped"),
                    col("is_cycle")
                )
    )

    # Stop cycles; keep only non-cycles to continue
    non_cycles = expanded.filter(~col("is_cycle")).select("src", "current", "history", "depth", "stopped")
    cycled = expanded.filter(col("is_cycle")).select("src", "current", "history", "depth", F.lit(True).alias("stopped"))

    combined = unchanged.unionByName(non_cycles).unionByName(cycled)

    # Skew mitigation: salt by current node; prune in two stages
    combined = combined.withColumn("salt", F.pmod(F.hash(col("current")), F.lit(SALT_BUCKETS)))

    w1 = Window.partitionBy("src", "salt").orderBy(col("depth").asc(), size(col("history")).asc())
    pruned_s1 = (
        combined.withColumn("rn1", row_number().over(w1))
                .filter(col("rn1") <= MAX_PATHS_PER_SRC)
                .drop("rn1")
    )

    w2 = Window.partitionBy("src").orderBy(col("depth").asc(), size(col("history")).asc())
    paths_src = (
        pruned_s1.drop("salt")
                 .dropDuplicates(["src", "current", "history"])
                 .withColumn("rn2", row_number().over(w2))
                 .filter(col("rn2") <= MAX_PATHS_PER_SRC)
                 .drop("rn2")
                 .repartition(NUM_PARTITIONS, col("src"))
                 .persist(StorageLevel.DISK_ONLY)
    )
    _ = paths_src.limit(1).count()  # light trigger only

    any_new = paths_src.filter(col("depth") >= depth).limit(1).count() > 0
    if not any_new:
        print(f"[INFO] Early stop at depth={depth}: no new paths.")
        break

# ============================================================
# 4) Seeds lookup (fast path)
# ============================================================

seeds = (
    price_data.withColumn("depot_sku", concat_ws("", col("depot"), col("sku")))
              .select("depot_sku").distinct()
              .repartition(NUM_PARTITIONS, col("depot_sku"))
              .persist(StorageLevel.DISK_ONLY)
)
_ = seeds.limit(1).count()

result = (
    seeds.alias("s").hint("MERGE")
         .join(paths_src.alias("p").hint("MERGE"), col("s.depot_sku") == col("p.src"), "left")
         .select(
             col("s.depot_sku"),
             col("p.current").alias("final_depot_sku_flip"),
             F.concat_ws("->", col("p.history")).alias("history"),
             col("p.stopped"),
             col("p.depth")
         )
         .dropDuplicates(["depot_sku", "final_depot_sku_flip", "history"])
         .repartition(NUM_PARTITIONS, col("depot_sku"))
         .persist(StorageLevel.DISK_ONLY)
)
_ = result.limit(1).count()

display(result.limit(100))
