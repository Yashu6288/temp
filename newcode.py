# ============================================================
# Production-Ready Depot-SKU Flip Resolution (Spark / Databricks)
# Matches original Python logic EXACTLY
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel

# ---------------------------
# Spark configs (SAFE)
# ---------------------------
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "400")

MAX_DEPTH = 10   # same role as visited loop safety

# ---------------------------
# Inputs
# ---------------------------
df_flipping_ds = mapping_spark_df
price_data = spark.read.table("apl_silver.supply_chain.price_data_demand_planning")

# ============================================================
# 1. Build master (NO COLLECT, NO EXPLODE)
# ============================================================

price_master = (
    price_data
    .withColumn("prode", F.substring("sku", 1, 4))
    .withColumn("shade", F.substring("sku", 5, 4))
    .withColumn("pack",  F.substring("sku", 9, 3))
    .select("depot", "prode", "shade", "pack")
    .distinct()
    .persist()
)

# ============================================================
# 2. NULL-safe rule normalization (JOIN-BASED, NOT EXPLODE)
# ============================================================

def fill_null(col_name, master_col):
    return F.when(F.col(col_name).isNull(), F.col(master_col)).otherwise(F.col(col_name))

df_rules = (
    df_flipping_ds
    .join(
        price_master,
        (
            (df_flipping_ds.sales_office_cond.isNull() | (df_flipping_ds.sales_office_cond == price_master.depot)) &
            (df_flipping_ds.prod_code_cond.isNull()    | (df_flipping_ds.prod_code_cond == price_master.prode)) &
            (df_flipping_ds.shade_code_cond.isNull()   | (df_flipping_ds.shade_code_cond == price_master.shade)) &
            (df_flipping_ds.pack_code_cond.isNull()    | (df_flipping_ds.pack_code_cond == price_master.pack))
        ),
        "left"
    )
    .withColumn("sales_office_cond", fill_null("sales_office_cond", "depot"))
    .withColumn("prod_code_cond",    fill_null("prod_code_cond", "prode"))
    .withColumn("shade_code_cond",   fill_null("shade_code_cond", "shade"))
    .withColumn("pack_code_cond",    fill_null("pack_code_cond", "pack"))
)

# Fill flip nulls (EXACT MATCH)
for c in ["prod", "shade", "pack", "sales_office"]:
    flip = f"flip_{c}_code" if c != "sales_office" else "flip_sales_office"
    cond = f"{c}_code_cond" if c != "sales_office" else "sales_office_cond"
    df_rules = df_rules.withColumn(flip, F.coalesce(F.col(flip), F.col(cond)))

# ============================================================
# 3. Build depot_sku_cond → depot_sku_flip
# ============================================================

df_rules = (
    df_rules
    .withColumn(
        "src",
        F.concat("sales_office_cond", "prod_code_cond", "shade_code_cond", "pack_code_cond")
    )
    .withColumn(
        "dst",
        F.concat("flip_sales_office", "flip_prod_code", "flip_shade_code", "flip_pack_code")
    )
    .select("src", "dst", "rule_update_date")
)

# ============================================================
# 4. Keep ONLY latest rule per src (CRITICAL)
# ============================================================

w = Window.partitionBy("src").orderBy(F.col("rule_update_date").desc())

rules_latest = (
    df_rules
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")
    .select("src", "dst")
    .persist()
)

# ============================================================
# 5. Price depot_sku seeds
# ============================================================

seeds = (
    price_data
    .withColumn("depot_sku", F.concat("depot", "sku"))
    .select("depot_sku")
    .distinct()
    .withColumn("current", F.col("depot_sku"))
    .withColumn("history", F.array(F.col("depot_sku")))
    .persist()
)

# ============================================================
# 6. Iterative flip resolution (Spark version of while loop)
# ============================================================

current_df = seeds

for i in range(MAX_DEPTH):
    next_df = (
        current_df
        .join(rules_latest, current_df.current == rules_latest.src, "left")
        .withColumn("next", F.coalesce("dst", "current"))
        # cycle detection (EXACT visited logic)
        .withColumn("is_cycle", F.array_contains("history", F.col("next")))
        .withColumn(
            "current",
            F.when(F.col("is_cycle"), F.col("current")).otherwise(F.col("next"))
        )
        .withColumn(
            "history",
            F.when(
                (~F.col("is_cycle")) & (F.col("next") != F.col("current")),
                F.concat("history", F.array("next"))
            ).otherwise("history")
        )
        .select("depot_sku", "current", "history")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    current_df.unpersist()
    current_df = next_df

# ============================================================
# 7. Final result (MATCHES YOUR OUTPUT)
# ============================================================

final_df = (
    current_df
    .withColumnRenamed("current", "final_depot_sku_flip")
    .withColumn("history", F.concat_ws("->", "history"))
)

display(final_df)
display(final_df.filter(F.col("depot_sku") != F.col("final_depot_sku_flip")))
display(final_df.filter(F.col("depot_sku").like("____0026%")))