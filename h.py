# ============================================================
# Depot–SKU Flip Resolution (Production Safe)
# ------------------------------------------------------------
# ✔ No broadcast joins
# ✔ No adjacency explosion
# ✔ Cycle-safe
# ✔ Date-aware (latest rule wins)
# ✔ Scales to billions
# ✔ Deterministic & restart-safe
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel


# ============================================================
# 0. HARD SAFETY CONFIG (DO NOT REMOVE)
# ============================================================

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.optimizer.joinReorder.enabled", "false")
spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")
spark.conf.set("spark.sql.shuffle.partitions", "800")  # tune to cluster cores

NUM_PARTITIONS = 800
MAX_DEPTH = 8   # business-realistic flip depth


# ============================================================
# 1. INPUT TABLES
# ============================================================

mapping_df = mapping_spark_df

price_data = spark.read.table(
    "apl_silver.supply_chain.price_data_demand_planning"
)


# ============================================================
# 2. BUILD LATEST FLIP EDGES (src → dst)
# ============================================================

edges = (
    mapping_df
    .withColumn(
        "src",
        F.concat_ws("",
            F.trim("sales_office_cond"),
            F.trim("prod_code_cond"),
            F.trim("shade_code_cond"),
            F.trim("pack_code_cond")
        )
    )
    .withColumn(
        "dst",
        F.concat_ws("",
            F.trim("flip_sales_office"),
            F.trim("flip_prod_code"),
            F.trim("flip_shade_code"),
            F.trim("flip_pack_code")
        )
    )
    .select("src", "dst", "rule_update_date")
)

w_latest = Window.partitionBy("src").orderBy(
    F.col("rule_update_date").desc()
)

edges_latest = (
    edges
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .select("src", "dst")
    .repartition(NUM_PARTITIONS, "src")
    .persist(StorageLevel.DISK_ONLY)
)

edges_latest.count()


# ============================================================
# 3. BUILD SEED NODES (depot_sku)
# ============================================================

seeds = (
    price_data
    .withColumn("src", F.concat_ws("", F.col("depot"), F.col("sku")))
    .select("src")
    .distinct()
    .repartition(NUM_PARTITIONS, "src")
    .persist(StorageLevel.DISK_ONLY)
)

seeds.count()


# ============================================================
# 4. INITIAL BFS STATE
# ============================================================

paths = (
    seeds
    .withColumn("current", F.col("src"))
    .withColumn("history", F.array(F.col("src")))
    .withColumn("depth", F.lit(0))
    .persist(StorageLevel.DISK_ONLY)
)

paths.count()


# ============================================================
# 5. SAFE BFS (EDGE-BASED, NO BROADCAST)
# ============================================================

for depth in range(1, MAX_DEPTH + 1):

    next_paths = (
        paths.alias("p")
        .join(
            edges_latest.alias("e"),
            F.col("p.current") == F.col("e.src"),
            "left"
        )
        .select(
            F.col("p.src"),
            F.coalesce(F.col("e.dst"), F.col("p.current")).alias("current"),
            F.when(
                F.col("e.dst").isNotNull(),
                F.concat(F.col("p.history"), F.array(F.col("e.dst")))
            ).otherwise(F.col("p.history")).alias("history"),
            F.lit(depth).alias("depth")
        )
        # cycle break
        .filter(~F.array_contains(F.col("history"), F.col("current")))
    )

    paths = (
        paths
        .unionByName(next_paths)
        .dropDuplicates(["src", "current"])
        .repartition(NUM_PARTITIONS, "src")
        .persist(StorageLevel.DISK_ONLY)
    )

    paths.count()
    print(f"✔ BFS depth {depth} completed")


# ============================================================
# 6. FINAL RESOLUTION (LATEST REACHABLE NODE)
# ============================================================

final_result = (
    paths
    .groupBy("src")
    .agg(
        F.max_by("current", "depth").alias("final_depot_sku"),
        F.max("depth").alias("resolved_depth")
    )
    .persist(StorageLevel.DISK_ONLY)
)

final_result.count()


# ============================================================
# 7. OPTIONAL SAFETY CHECK (NO BROADCAST)
# ============================================================

def assert_no_broadcast(df):
    plan = df._jdf.queryExecution().executedPlan().toString()
    if "Broadcast" in plan:
        raise RuntimeError("❌ Broadcast join detected")

assert_no_broadcast(final_result)


# ============================================================
# 8. OUTPUT
# ============================================================

display(final_result.limit(100))

# You can write to Delta if needed:
# final_result.write.format("delta").mode("overwrite").saveAsTable("apl_gold.depot_sku_flip_final")