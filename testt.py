
from pyspark.sql.functions import when, col
from pyspark.sql.functions import concat
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc
from pyspark.sql.functions import explode, array, lit
from pyspark.sql import SparkSession, functions as F

#reading dataframe flipping_ds
# df_flipping_ds = spark.read.table("apl_bronze.supply_chain.flipping_ds")
df_flipping_ds=mapping_spark_df

#reading price data 
price_data = spark.read.table("apl_silver.supply_chain.price_data_demand_planning")


# Create master lists for prode, shade, pack, and depot from price_data
price_data_master = (
    price_data
    .withColumn("prode", F.substring("sku", 1, 4))
    .withColumn("shade", F.substring("sku", 5, 4))
    .withColumn("pack", F.substring("sku", 9, 3))
    .distinct()
)
prode_master = [row['prode'] for row in price_data_master.select("prode").distinct().collect()]
shade_master = [row['shade'] for row in price_data_master.select("shade").distinct().collect()]
pack_master = [row['pack'] for row in price_data_master.select("pack").distinct().collect()]
depot_master = [row['depot'] for row in price_data_master.select("depot").distinct().collect()]

# Expand nulls in *_cond columns to all possible values from master lists
for col_name, master_list in [
    ("prod_code_cond", prode_master),
    ("shade_code_cond", shade_master),
    ("pack_code_cond", pack_master),
    ("sales_office_cond", depot_master)
]:
    # Step 1: replace nulls with array of all master values, else wrap value in array
    df_flipping_ds = df_flipping_ds.withColumn(
        col_name,
        when(
            col(col_name).isNull(),
            array([lit(x) for x in master_list])
        ).otherwise(array(col(col_name)))
    )
    # Step 2: explode the array to create multiple rows
    df_flipping_ds = df_flipping_ds.withColumn(col_name, explode(col(col_name)))



#Filling null values.
for code in ["prod", "shade", "pack", "sales_office"]:
    flip_col = f"flip_{code}_code" if code != "sales_office" else "flip_sales_office"
    cond_col = f"{code}_code_cond" if code != "sales_office" else "sales_office_cond"
    df_flipping_ds = df_flipping_ds.withColumn(
        flip_col,
        when(col(flip_col).isNull(), col(cond_col)).otherwise(col(flip_col))
    )

# ascending order by date in rule_update_date.
df_flipping_ds = df_flipping_ds.orderBy("rule_update_date")




# adding columns depot_sku_code and depot_sku_flip.
df_flipping_ds = df_flipping_ds.withColumn(
    "depot_sku_cond",
    concat(
        col("sales_office_cond"),
        col("prod_code_cond"),
        col("shade_code_cond"),
        col("pack_code_cond")
    )
).withColumn(
    "depot_sku_flip",
    concat(
        col("flip_sales_office"),
        col("flip_prod_code"),
        col("flip_shade_code"),
        col("flip_pack_code")
    )
)


# adding column depot_sku
price_data = price_data.withColumn("depot_sku", concat(col("depot"), col("sku")))

# distinct on depot_sku
distinct_depot_sku_pricing = price_data.select("depot_sku").distinct()

# adding a column for flipping
distinct_depot_sku_pricing = distinct_depot_sku_pricing.withColumn("depot_sku_flip", col("depot_sku"))
display(distinct_depot_sku_pricing)

df_flipping_ds = df_flipping_ds.select("depot_sku_cond", "depot_sku_flip", "rule_update_date")


# apply distinct on column depot_sku_cond and keep only latest rule_update_date if duplicate found
window_spec = Window.partitionBy("depot_sku_cond").orderBy(desc("rule_update_date"))
df_flipping_ds = df_flipping_ds.withColumn("rn", row_number().over(window_spec)).filter(col("rn") == 1).drop("rn")

df_flipping_ds = df_flipping_ds.orderBy("rule_update_date")


# Build rules dictionary from flipping DataFrame
rules_map = {
    row['depot_sku_cond']: row['depot_sku_flip']
    # for row in df_flipping_ds.collect()
    for row in df_flipping_ds.toLocalIterator()
}

# Collect pricing rows
pricing_rows = distinct_depot_sku_pricing.collect()

flipped_results = []
for row in pricing_rows:
    depot_sku = row['depot_sku']
    current_flip = row['depot_sku_flip']
    history = [current_flip]
    visited = set()
    while current_flip in rules_map and current_flip not in visited:
        visited.add(current_flip)
        new_flip = rules_map[current_flip]
        history.append(new_flip)
        current_flip = new_flip
    flipped_results.append((depot_sku, current_flip, "->".join(history)))

# Convert back to DataFrame
flipped_df = spark.createDataFrame(
    flipped_results,
    ["depot_sku", "final_depot_sku_flip", "history"]
)

display(flipped_df)
display(flipped_df.filter(col("depot_sku") != col("final_depot_sku_flip")))
display(flipped_df.filter(col("depot_sku").like("____0026%")))
