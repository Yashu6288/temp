# ============================================================
# PRODUCTION: Graph BFS Flip Resolution (NO BROADCAST)
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, when, concat_ws, row_number, array_contains,
    explode, collect_set, size, trim
)
from pyspark.storagelevel import StorageLevel
import time

# ============================================================
# 0. HARD GUARANTEES — NO BROADCAST EVER
# ============================================================

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.sort.bypassMergeThreshold", "1")

# ============================================================
# 1. TUNABLE PARAMETERS
# ============================================================

NUM_PARTITIONS        = 512
MAX_DEPTH             = 10
MAX_PATHS_PER_SRC     = 40
SALT_BUCKETS          = 32
MAX_RUNTIME_SEC       = 60 * 45   # 45 mins safety

spark.conf.set("spark.sql.shuffle.partitions", str(NUM_PARTITIONS))

# ============================================================
# 2. INPUTS
# ============================================================

df_rules = mapping_spark_df
price_data = spark.read.table(
    "apl_silver.supply_chain.price_data_demand_planning"
)

# ============================================================
# 3. BUILD MASTER DIM (SKU DECOMPOSITION)
# ============================================================

price_master = (
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
    .repartition(NUM_PARTITIONS, "depot", "prode", "shade", "pack")
    .persist(StorageLevel.DISK_ONLY)
)
price_master.count()

# ============================================================
# 4. NORMALIZE RULE CONDITIONS
# ============================================================

COND_COLS = [
    "sales_office_cond",
    "prod_code_cond",
    "shade_code_cond",
    "pack_code_cond"
]

for c in COND_COLS:
    df_rules = df_rules.withColumn(c, trim(col(c).cast("string")))

# ============================================================
# 5. NULL FILL (CONSTRAINED MERGE JOIN — SAFE)
# ============================================================

master = price_master.selectExpr(
    "depot m_depot",
    "prode m_prode",
    "shade m_shade",
    "pack  m_pack"
)

COND_MAP = {
    "sales_office_cond": "m_depot",
    "prod_code_cond":    "m_prode",
    "shade_code_cond":   "m_shade",
    "pack_code_cond":    "m_pack"
}

def fill_single_null(df, null_col):
    others = [c for c in COND_COLS if c != null_col]
    join_cond = F.lit(True)
    for oc in others:
        join_cond &= (col(oc).isNull() | (col(oc) == col(COND_MAP[oc])))

    need = df.filter(col(null_col).isNull())
    keep = df.filter(col(null_col).isNotNull())

    filled = (
        need.hint("MERGE")
            .join(master.hint("MERGE"), join_cond, "left")
            .withColumn(null_col, col(COND_MAP[null_col]))
            .drop("m_depot", "m_prode", "m_shade", "m_pack")
    )

    return keep.unionByName(filled).dropDuplicates()

for c in COND_COLS:
    df_rules = fill_single_null(df_rules, c)

# ============================================================
# 6. DEFAULT FLIP = CONDITION (SELF LOOP SAFE)
# ============================================================

for base in ["prod", "shade", "pack", "sales_office"]:
    flip = f"flip_{base}_code" if base != "sales_office" else "flip_sales_office"
    cond = f"{base}_code_cond" if base != "sales_office" else "sales_office_cond"
    df_rules = df_rules.withColumn(
        flip, when(col(flip).isNull(), col(cond)).otherwise(col(flip))
    )

# ============================================================
# 7. BUILD EDGES (LATEST RULE PER SRC)
# ============================================================

edges_raw = (
    df_rules
    .withColumn("src", concat_ws(
        "", "sales_office_cond", "prod_code_cond", "shade_code_cond", "pack_code_cond"
    ))
    .withColumn("dst", concat_ws(
        "", "flip_sales_office", "flip_prod_code", "flip_shade_code", "flip_pack_code"
    ))
    .select("src", "dst", "rule_update_date")
    .repartition(NUM_PARTITIONS, "src")
)

w_latest = Window.partitionBy("src").orderBy(
    col("rule_update_date").desc(), col("dst")
)

edges = (
    edges_raw
    .withColumn("rn", row_number().over(w_latest))
    .filter(col("rn") == 1)
    .select("src", "dst")
    .persist(StorageLevel.MEMORY_AND_DISK)
)
edges.count()

# ============================================================
# 8. ADJACENCY LIST
# ============================================================

adj = (
    edges
    .groupBy("src")
    .agg(collect_set("dst").alias("adj"))
    .repartition(NUM_PARTITIONS, "src")
    .persist(StorageLevel.MEMORY_AND_DISK)
)
adj.count()

# ============================================================
# 9. NODE UNIVERSE
# ============================================================

nodes = (
    edges.select(col("src").alias("node"))
         .union(edges.select(col("dst").alias("node")))
         .distinct()
         .repartition(NUM_PARTITIONS, "node")
         .persist(StorageLevel.DISK_ONLY)
)
nodes.count()

# ============================================================
# 10. BFS (NODE-CENTRIC, PRUNED, CYCLE-SAFE)
# ============================================================

paths = (
    nodes.select(
        col("node").alias("src"),
        col("node").alias("current"),
        F.array(col("node")).alias("history"),
        F.lit(0).alias("depth"),
        F.lit(False).alias("stopped")
    )
    .repartition(NUM_PARTITIONS, "src")
    .persist(StorageLevel.DISK_ONLY)
)
paths.count()

start = time.time()

for d in range(1, MAX_DEPTH + 1):

    if time.time() - start > MAX_RUNTIME_SEC:
        break

    joined = (
        paths.alias("p")
             .hint("MERGE")
             .join(adj.alias("a").hint("MERGE"),
                   col("p.current") == col("a.src"),
                   "left")
             .select("p.*", "a.adj")
    )

    dead = joined.filter(col("adj").isNull()) \
        .withColumn("stopped", F.lit(True)) \
        .select("src", "current", "history", "depth", "stopped")

    expanded = (
        joined.filter(col("adj").isNotNull())
              .select("src", explode("adj").alias("next"), "history", "depth")
              .withColumn("is_cycle", array_contains("history", "next"))
    )

    next_paths = (
        expanded.filter(~col("is_cycle"))
                .select(
                    "src",
                    col("next").alias("current"),
                    F.concat("history", F.array("next")).alias("history"),
                    (col("depth") + 1).alias("depth"),
                    F.lit(False).alias("stopped")
                )
    )

    combined = dead.unionByName(next_paths)

    combined = combined.withColumn(
        "salt", F.pmod(F.hash("current"), F.lit(SALT_BUCKETS))
    )

    w1 = Window.partitionBy("src", "salt").orderBy("depth", size("history"))
    w2 = Window.partitionBy("src").orderBy("depth", size("history"))

    paths = (
        combined
        .withColumn("r1", row_number().over(w1))
        .filter(col("r1") <= MAX_PATHS_PER_SRC)
        .drop("r1", "salt")
        .withColumn("r2", row_number().over(w2))
        .filter(col("r2") <= MAX_PATHS_PER_SRC)
        .drop("r2")
        .persist(StorageLevel.DISK_ONLY)
    )

    if paths.filter(col("depth") == d).limit(1).count() == 0:
        break

# ============================================================
# 11. APPLY TO SEEDS
# ============================================================

seeds = (
    price_data
    .withColumn("depot_sku", concat_ws("", "depot", "sku"))
    .select("depot_sku")
    .distinct()
    .repartition(NUM_PARTITIONS, "depot_sku")
)

final = (
    seeds.alias("s")
         .hint("MERGE")
         .join(paths.alias("p").hint("MERGE"),
               col("s.depot_sku") == col("p.src"),
               "left")
         .select(
             col("s.depot_sku"),
             col("p.current").alias("final_depot_sku_flip"),
             F.concat_ws("->", "p.history").alias("flip_chain"),
             "p.depth",
             "p.stopped"
         )
         .dropDuplicates()
         .persist(StorageLevel.DISK_ONLY)
)

display(final.limit(100))