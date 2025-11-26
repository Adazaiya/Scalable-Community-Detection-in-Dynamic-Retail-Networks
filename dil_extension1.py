# dil_extension1.py
# extension functions for DIL computations
# distributed local optimization 
from pyspark.sql import SparkSession
from pyspark import SparkConf
import networkx as nx
from louvain_baseline import classical_louvain, compute_modularity
import time

def start_spark():
    conf = SparkConf().setAppName("DIL_Extension1").setMaster("local[4]")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    return spark

def load_partition_edges(spark, partition_id):
    """Load edges for a specific partition"""
    path = f"partitioned_edges/partition_{partition_id}"
    df = spark.read.parquet(path)
    edges = df.select("i", "j", "norm_score").collect()
    
    # Build NetworkX graph for this partition
    G = nx.Graph()
    for row in edges:
        G.add_edge(row.i, row.j, weight=row.norm_score)
    
    return G

def local_louvain_on_partition(partition_graph):
    """
    Run Louvain on a single partition (this runs on a Spark worker)
    Returns: (partition_id, communities_dict)
    """
    print(f"Processing partition with {partition_graph.number_of_nodes()} nodes")
    communities = classical_louvain(partition_graph)
    Q = compute_modularity(partition_graph, communities)
    print(f"Partition modularity: {Q:.4f}")
    return communities

def distributed_local_optimization(spark, num_partitions=4):
    """
    Extension 1: Run Louvain in parallel across partitions
    """
    print("=== Extension 1: Distributed Local Optimization ===\n")
    
    # Load all partitions
    partition_graphs = []
    for p in range(num_partitions):
        G = load_partition_edges(spark, p)
        partition_graphs.append((p, G))
        print(f"Partition {p}: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    
    # Distribute partitions to Spark workers using RDD
    partitions_rdd = spark.sparkContext.parallelize(partition_graphs, numSlices=num_partitions)
    
    # Map: Run Louvain on each partition in parallel
    start_time = time.time()
    results = partitions_rdd.map(
        lambda x: (x[0], local_louvain_on_partition(x[1]))
    ).collect()
    runtime = time.time() - start_time
    
    # Collect results
    all_communities = {}
    for partition_id, communities in results:
        all_communities[partition_id] = communities
    
    print(f"\n=== Distributed optimization completed in {runtime:.2f} seconds ===")
    
    return all_communities, runtime

if __name__ == "__main__":
    spark = start_spark()
    
    # Run Extension 1
    communities_by_partition, runtime = distributed_local_optimization(spark, num_partitions=4)
    
    # Analyze results
    for pid, communities in communities_by_partition.items():
        num_comms = len(set(communities.values()))
        print(f"Partition {pid}: {len(communities)} nodes, {num_comms} communities")
    
    spark.stop()