# instacart_spark_pipeline_dil.py

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, ArrayType, StructType, StructField, FloatType
import math

SPARK_APP_NAME = "InstacartDILPreprocessing"

# ---------- SPARK SESSION ----------
def start_spark():
    spark = SparkSession.builder \
        .appName(SPARK_APP_NAME) \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .getOrCreate()
    return spark

# ---------- LOAD CSV TABLES ----------
def load_tables(spark, orders_path, order_products_path, products_path):
    orders = spark.read.option("header", True).csv(orders_path, inferSchema=True)
    order_products = spark.read.option("header", True).csv(order_products_path, inferSchema=True)
    products = spark.read.option("header", True).csv(products_path, inferSchema=True)
    orders = orders.withColumn("order_date", F.to_date("order_date"))
    return orders, order_products, products

# ---------- CREATE WINDOWS ----------
def get_windows(orders_df, freq_days=7):
    min_date = orders_df.agg(F.min("order_date")).collect()[0][0]
    windowed = orders_df.withColumn("days_since", F.datediff(F.col("order_date"), F.lit(min_date)))
    windowed = windowed.withColumn("window_id", (F.col("days_since") / F.lit(freq_days)).cast("int"))
    return windowed.select("order_id", "window_id")

# ---------- CO-OCCURRENCE ----------
def cooccurrence_by_window(spark, order_products_df, order_windows_df):
    joined = order_products_df.join(order_windows_df, on="order_id", how="inner")
    orders_products = joined.groupBy("window_id","order_id").agg(F.collect_list("product_id").alias("products"))

    # UDF to create all pairs
    def pairs_udf(products):
        unique = sorted(set(int(p) for p in products))
        res = []
        for i in range(len(unique)):
            for j in range(i+1, len(unique)):
                res.append((unique[i], unique[j]))
        return res

    pairs_schema = ArrayType(StructType([
        StructField("i", IntegerType()), StructField("j", IntegerType())
    ]))
    pairs_udf_spark = F.udf(pairs_udf, pairs_schema)

    exploded = orders_products.withColumn("pairs", pairs_udf_spark("products"))\
                              .select("window_id","order_id", F.explode("pairs").alias("pair"))
    exploded = exploded.select("window_id", exploded.pair.i.alias("i"), exploded.pair.j.alias("j"))

    cooc = exploded.groupBy("window_id","i","j").count().withColumnRenamed("count","cooc_count")
    items = joined.groupBy("window_id","product_id").agg(F.countDistinct("order_id").alias("item_count"))
    total_orders = order_windows_df.groupBy("window_id").agg(F.countDistinct("order_id").alias("total_orders"))
    return cooc, items, total_orders

# ---------- COMPUTE SCORES ----------
def compute_weights_and_filter(spark, cooc_df, items_df, totals_df, norm_threshold=0.002, use_npmi=True):
    items_i = items_df.selectExpr("window_id as w1", "product_id as i_item", "item_count as count_i")
    items_j = items_df.selectExpr("window_id as w2", "product_id as j_item", "item_count as count_j")

    c = cooc_df.join(items_i, on=[(cooc_df.window_id == F.col("w1")) & (cooc_df.i == F.col("i_item"))]).drop("w1")
    c = c.join(items_j, on=[(c.window_id == F.col("w2")) & (c.j == F.col("j_item"))]).drop("w2")
    c = c.join(totals_df, on="window_id")

    def norm_score(count_ij, count_i, count_j):
        return float(count_ij) / math.sqrt(max(count_i * count_j, 1))

    def npmi_func(count_ij, count_i, count_j, total_orders):
        p_i = count_i / total_orders
        p_j = count_j / total_orders
        p_ij = count_ij / total_orders
        if p_ij == 0 or p_i == 0 or p_j == 0:
            return None
        pmi = math.log(p_ij / (p_i * p_j))
        npmi = pmi / (-math.log(p_ij)) if p_ij < 1 else 0.0
        return float(npmi)

    norm_udf = F.udf(norm_score, FloatType())
    npmi_udf = F.udf(npmi_func, FloatType())

    c = c.withColumn("norm_score", norm_udf("cooc_count","count_i","count_j"))
    if use_npmi:
        c = c.withColumn("npmi", npmi_udf("cooc_count","count_i","count_j","total_orders"))
        filtered = c.filter((F.col("npmi").isNotNull()) & (F.col("norm_score") >= norm_threshold) & (F.col("npmi") > 0))
    else:
        filtered = c.filter(F.col("norm_score") >= norm_threshold)

    return filtered.select("window_id","i","j","cooc_count","count_i","count_j","norm_score","npmi")

# ---------- PARTITION GRAPH ----------
def partition_graph(filtered_df, num_partitions=4):
    assign_partition = F.udf(lambda x: hash(x) % num_partitions, IntegerType())
    df = filtered_df.withColumn("i_partition", assign_partition("i")) \
                    .withColumn("j_partition", assign_partition("j"))
    df = df.withColumn("edge_partition", F.least("i_partition","j_partition"))

    # Identify boundary nodes (nodes connecting multiple partitions)
    boundary_nodes = df.filter(F.col("i_partition") != F.col("j_partition")) \
                       .select(F.col("i").alias("node"), F.col("i_partition").alias("partition")) \
                       .union(df.filter(F.col("i_partition") != F.col("j_partition"))
                              .select(F.col("j").alias("node"), F.col("j_partition").alias("partition"))) \
                       .distinct()

    partitioned_dfs = {}
    for p in range(num_partitions):
        partitioned_dfs[p] = df.filter(F.col("edge_partition") == p)

    return partitioned_dfs, boundary_nodes

# ---------- EXPORT ----------
def export_partitioned_edges(partitioned_dfs, boundary_nodes, out_prefix="partitioned_edges"):
    for pid, df in partitioned_dfs.items():
        path_parquet = f"{out_prefix}/partition_{pid}"
        path_csv = f"{out_prefix}_csv/partition_{pid}"
        df.write.mode("overwrite").parquet(path_parquet)
        df.write.mode("overwrite").option("header", True).csv(path_csv)
        print(f"Partition {pid} exported to Parquet: {path_parquet} and CSV: {path_csv}")

    # Export boundary nodes for synchronization
    boundary_nodes.write.mode("overwrite").parquet(f"{out_prefix}/boundary_nodes")
    boundary_nodes.write.mode("overwrite").option("header", True).csv(f"{out_prefix}_csv/boundary_nodes")
    print("Boundary nodes exported for synchronization.")

# ---------- MAIN ----------
if __name__ == "__main__":
    spark = start_spark()

    # CSV paths
    orders_path = "orders.csv"
    order_products_path = "order_products.csv"
    products_path = "products.csv"

    # Load tables
    orders, order_products, products = load_tables(spark, orders_path, order_products_path, products_path)
    print("Loaded tables:")
    orders.show(5)
    order_products.show(5)
    products.show(5)

    # Windows
    order_windows = get_windows(orders)
    print("Order windows:")
    order_windows.show(5)

    # Co-occurrences
    cooc, items, totals = cooccurrence_by_window(spark, order_products, order_windows)
    print("Co-occurrences (first 5 rows):")
    cooc.show(5)
    print("Item counts (first 5 rows):")
    items.show(5)
    print("Total orders per window (first 5 rows):")
    totals.show(5)

    # Compute filtered edges
    filtered = compute_weights_and_filter(spark, cooc, items, totals)
    print("Filtered edges (first 5 rows):")
    filtered.show(5)

    # Partition the graph
    num_partitions = 4
    partitioned_graphs, boundary_nodes = partition_graph(filtered, num_partitions=num_partitions)

    # Export partitioned edges and boundary nodes
    export_partitioned_edges(partitioned_graphs, boundary_nodes, "partitioned_edges")
