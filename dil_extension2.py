# cross partition synchronization
# dil_extension2.py
from pyspark.sql import SparkSession
# from dil_extension1 import distributed_local_optimization, load_partition_edges # This line is causing the error
import networkx as nx

def load_boundary_nodes(spark):
    """Load boundary nodes identified during preprocessing"""
    df = spark.read.parquet("partitioned_edges/boundary_nodes")
    boundary_info = df.collect()

    # Group by node
    boundaries = {}
    for row in boundary_info:
        node = row.node
        partition = row.partition
        if node not in boundaries:
            boundaries[node] = []
        boundaries[node].append(partition)

    return boundaries

def exchange_boundary_information(communities_by_partition, boundary_nodes):
    """
    Extension 2: Share information about boundary nodes across partitions
    """
    print("\n=== Extension 2: Cross-Partition Synchronization ===\n")

    boundary_context = {}

    for node, partitions in boundary_nodes.items():
        # This node appears in multiple partitions
        boundary_context[node] = {}

        for partition_id in partitions:
            if partition_id in communities_by_partition:
                communities = communities_by_partition[partition_id]
                if node in communities:
                    boundary_context[node][partition_id] = communities[node]

    return boundary_context

def reoptimize_boundaries(communities_by_partition, boundary_context, partition_graphs):
    """
    Re-optimize boundary nodes considering global context
    """
    changes = 0

    for node, partition_communities in boundary_context.items():
        if len(partition_communities) <= 1:
            continue

        # Node appears in multiple partitions with potentially different community assignments
        assignments = list(partition_communities.values())

        # Simple strategy: majority vote
        from collections import Counter
        majority_community = Counter(assignments).most_common(1)[0][0]

        # Update all partitions to use majority assignment
        for partition_id in partition_communities.keys():
            if communities_by_partition[partition_id][node] != majority_community:
                communities_by_partition[partition_id][node] = majority_community
                changes += 1

    print(f"Synchronized {changes} boundary node assignments")
    return communities_by_partition

def dil_with_synchronization(spark, num_partitions=4):
    """
    Complete DIL with Extension 1 + Extension 2
    """
    # Extension 1: Distributed local optimization
    communities_by_partition, runtime = distributed_local_optimization(spark, num_partitions)

    # Load boundary information
    boundary_nodes = load_boundary_nodes(spark)
    print(f"\nBoundary nodes identified: {len(boundary_nodes)}")

    # Extension 2: Synchronization
    boundary_context = exchange_boundary_information(communities_by_partition, boundary_nodes)

    # Load partition graphs for context
    partition_graphs = {
        p: load_partition_edges(spark, p) for p in range(num_partitions)
    }

    communities_synchronized = reoptimize_boundaries(
        communities_by_partition, boundary_context, partition_graphs
    )

    return communities_synchronized

if __name__ == "__main__":
    spark = SparkSession.builder.appName("DIL_Extension2").master("local[4]").getOrCreate()

    communities = dil_with_synchronization(spark, num_partitions=4)

    print("\n=== Final Communities (after synchronization) ===")
    for pid, comms in communities.items():
        unique_comms = len(set(comms.values()))
        print(f"Partition {pid}: {unique_comms} communities")

    spark.stop()
