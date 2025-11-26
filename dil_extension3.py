# dynamic adaptation 
#dil_extension3.py
from dil_extension2 import dil_with_synchronization
from louvain_baseline import classical_louvain
import networkx as nx
import time

def identify_affected_communities(graph_delta, current_communities):
    """
    Extension 3: Identify which communities are affected by new edges
    """
    affected_nodes = set()
    
    for u, v in graph_delta['added_edges']:
        affected_nodes.add(u)
        affected_nodes.add(v)
    
    for u, v in graph_delta.get('removed_edges', []):
        affected_nodes.add(u)
        affected_nodes.add(v)
    
    # Find communities containing affected nodes
    affected_communities = set()
    for node in affected_nodes:
        if node in current_communities:
            affected_communities.add(current_communities[node])
    
    return affected_nodes, affected_communities

def expand_reoptimization_region(G, affected_communities, current_communities, hops=2):
    """
    Expand affected region by k hops
    """
    reopt_nodes = set()
    
    # Start with all nodes in affected communities
    for node, comm in current_communities.items():
        if comm in affected_communities:
            reopt_nodes.add(node)
    
    # Expand by k hops
    for _ in range(hops):
        new_nodes = set()
        for node in reopt_nodes:
            if node in G:
                new_nodes.update(G.neighbors(node))
        reopt_nodes.update(new_nodes)
    
    return reopt_nodes

def incremental_update(G, current_communities, graph_delta, expansion_hops=2):
    """
    Extension 3: Incremental community update
    """
    print("\n=== Extension 3: Dynamic Adaptation ===\n")
    
    # Step 1: Identify affected regions
    affected_nodes, affected_communities = identify_affected_communities(
        graph_delta, current_communities
    )
    
    print(f"Affected nodes: {len(affected_nodes)}")
    print(f"Affected communities: {len(affected_communities)}")
    
    # Step 2: Expand region
    reopt_region = expand_reoptimization_region(
        G, affected_communities, current_communities, hops=expansion_hops
    )
    
    print(f"Reoptimization region: {len(reopt_region)} nodes ({100*len(reopt_region)/len(G):.1f}% of graph)")
    
    # Step 3: Extract subgraph
    subgraph = G.subgraph(reopt_region).copy()
    
    # Step 4: Re-run Louvain on affected subgraph
    start_time = time.time()
    updated_communities = classical_louvain(subgraph)
    reopt_time = time.time() - start_time
    
    # Step 5: Merge with unchanged regions
    final_communities = current_communities.copy()
    for node in reopt_region:
        if node in updated_communities:
            final_communities[node] = updated_communities[node]
    
    print(f"Reoptimization completed in {reopt_time:.2f} seconds")
    
    return final_communities, len(reopt_region)

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("DIL_Extension3").master("local[4]").getOrCreate()
    
    # Initial communities (Extensions 1 + 2)
    print("Computing initial communities...")
    communities = dil_with_synchronization(spark, num_partitions=4)
    
    # Flatten communities from all partitions
    all_communities = {}
    for partition_comms in communities.values():
        all_communities.update(partition_comms)
    
    # Load full graph
    from dil_extension1 import load_partition_edges
    G = nx.Graph()
    for p in range(4):
        G_part = load_partition_edges(spark, p)
        G = nx.compose(G, G_part)
    
    # Simulate new edges arriving
    graph_delta = {
        'added_edges': [(101, 115), (102, 109)],  # New connections
        'removed_edges': []
    }
    
    print(f"\nGraph delta: {len(graph_delta['added_edges'])} edges added")
    
    # Add new edges to graph
    for u, v in graph_delta['added_edges']:
        G.add_edge(u, v, weight=0.5)
    
    # Extension 3: Incremental update
    updated_communities, nodes_reprocessed = incremental_update(
        G, all_communities, graph_delta
    )
    
    print(f"\nIncremental update reprocessed {nodes_reprocessed}/{len(G)} nodes")
    print(f"Savings: {100*(1 - nodes_reprocessed/len(G)):.1f}% of nodes not recomputed")
    
    spark.stop()
