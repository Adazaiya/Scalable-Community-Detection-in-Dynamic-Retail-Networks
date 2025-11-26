# louvain_baseline.py
import networkx as nx
from collections import defaultdict
import time
import random

def load_graph_from_partitions(num_partitions=4):
    """
    Load the full graph by combining all partitions
    """
    print("Loading graph from partitioned edges...")
    G = nx.Graph()
    
    total_edges = 0
    for p in range(num_partitions):
        csv_path = f"partitioned_edges_csv/partition_{p}"
        
        try:
            # Read CSV files in partition directory
            import os
            import csv
            
            for filename in os.listdir(csv_path):
                if filename.endswith('.csv') and not filename.startswith('_'):
                    filepath = os.path.join(csv_path, filename)
                    
                    with open(filepath, 'r') as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            i = int(row['i'])
                            j = int(row['j'])
                            weight = float(row['norm_score'])
                            
                            # Add edge (NetworkX handles duplicates automatically)
                            if not G.has_edge(i, j):
                                G.add_edge(i, j, weight=weight)
                                total_edges += 1
            
            print(f"  Partition {p}: loaded")
        
        except FileNotFoundError:
            print(f"  Warning: Partition {p} not found, skipping...")
    
    print(f"\nGraph loaded: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    return G

def compute_modularity(G, communities):
    """
    Calculate modularity Q
    Q = (1/2m) * Σ[A_ij - (k_i * k_j)/(2m)] * δ(c_i, c_j)
    """
    if len(communities) == 0:
        return 0.0
    
    m = sum(dict(G.degree(weight='weight')).values()) / 2  # Total edge weight
    
    if m == 0:
        return 0.0
    
    Q = 0.0
    
    for i, j, data in G.edges(data=True):
        weight = data.get('weight', 1.0)
        k_i = G.degree(i, weight='weight')
        k_j = G.degree(j, weight='weight')
        
        # δ(c_i, c_j) = 1 if same community, 0 otherwise
        if communities.get(i) == communities.get(j):
            Q += weight - (k_i * k_j) / (2 * m)
    
    Q = Q / (2 * m)
    return Q

def modularity_gain(G, node, current_comm, new_comm, communities, degrees, m):
    """
    Calculate modularity gain from moving node to new_comm
    Simplified calculation for efficiency
    """
    if current_comm == new_comm:
        return 0.0
    
    k_i = degrees[node]
    
    # Sum of edges from node to new community
    k_i_in_new = 0.0
    for neighbor in G.neighbors(node):
        if communities.get(neighbor) == new_comm:
            k_i_in_new += G[node][neighbor].get('weight', 1.0)
    
    # Sum of edges from node to current community
    k_i_in_current = 0.0
    for neighbor in G.neighbors(node):
        if communities.get(neighbor) == current_comm:
            k_i_in_current += G[node][neighbor].get('weight', 1.0)
    
    # Sum of degrees in communities
    sum_tot_new = sum(degrees[n] for n in G.nodes() if communities.get(n) == new_comm)
    sum_tot_current = sum(degrees[n] for n in G.nodes() if communities.get(n) == current_comm)
    
    # Calculate change in modularity
    delta_q = (k_i_in_new - k_i_in_current) / m - \
              (k_i * (sum_tot_new - sum_tot_current + k_i)) / (2 * m * m)
    
    return delta_q

def classical_louvain(G, max_iterations=50, epsilon=0.0001, verbose=True):
    """
    Classical Louvain algorithm for community detection
    
    Args:
        G: NetworkX graph
        max_iterations: Maximum number of iterations
        epsilon: Convergence threshold
        verbose: Print progress
    
    Returns:
        communities: dict {node: community_id}
    """
    if verbose:
        print("\n" + "="*60)
        print("Running Classical Louvain Algorithm")
        print("="*60)
    
    # Initialize: each node in its own community
    communities = {node: i for i, node in enumerate(G.nodes())}
    
    # Precompute degrees
    degrees = dict(G.degree(weight='weight'))
    m = sum(degrees.values()) / 2  # Total edge weight
    
    if m == 0:
        print("Warning: Graph has no edges!")
        return communities
    
    iteration = 0
    improvement = True
    
    while improvement and iteration < max_iterations:
        improvement = False
        iteration += 1
        
        # Process nodes in random order
        nodes = list(G.nodes())
        random.shuffle(nodes)
        
        moves = 0
        
        for node in nodes:
            current_community = communities[node]
            
            # Find neighbor communities and their connection weights
            neighbor_communities = defaultdict(float)
            for neighbor in G.neighbors(node):
                neighbor_comm = communities[neighbor]
                edge_weight = G[node][neighbor].get('weight', 1.0)
                neighbor_communities[neighbor_comm] += edge_weight
            
            if len(neighbor_communities) == 0:
                continue
            
            # Find best community to move to
            best_community = current_community
            best_gain = 0.0
            
            for candidate_comm, edge_weight_to_comm in neighbor_communities.items():
                if candidate_comm == current_community:
                    continue
                
                gain = modularity_gain(
                    G, node, current_community, candidate_comm,
                    communities, degrees, m
                )
                
                if gain > best_gain + epsilon:
                    best_gain = gain
                    best_community = candidate_comm
            
            # Move node if beneficial
            if best_community != current_community:
                communities[node] = best_community
                improvement = True
                moves += 1
        
        # Calculate current modularity
        Q = compute_modularity(G, communities)
        
        if verbose:
            num_communities = len(set(communities.values()))
            print(f"Iteration {iteration:2d}: Moves={moves:4d}, Communities={num_communities:3d}, Q={Q:.4f}")
        
        if moves == 0:
            break
    
    if verbose:
        print("="*60)
        print(f"Converged after {iteration} iterations")
        print("="*60 + "\n")
    
    return communities

def analyze_communities(G, communities, top_n=5):
    """
    Analyze and display community statistics
    """
    print("\n" + "="*60)
    print("Community Analysis")
    print("="*60)
    
    # Count community sizes
    community_sizes = defaultdict(int)
    for node, comm in communities.items():
        community_sizes[comm] += 1
    
    # Sort by size
    sorted_communities = sorted(community_sizes.items(), key=lambda x: x[1], reverse=True)
    
    print(f"\nTotal communities: {len(sorted_communities)}")
    print(f"Largest community: {sorted_communities[0][1]} nodes")
    print(f"Smallest community: {sorted_communities[-1][1]} nodes")
    print(f"Average community size: {sum(community_sizes.values()) / len(sorted_communities):.1f}")
    
    print(f"\nTop {top_n} largest communities:")
    for i, (comm_id, size) in enumerate(sorted_communities[:top_n], 1):
        nodes_in_comm = [n for n, c in communities.items() if c == comm_id]
        print(f"  {i}. Community {comm_id}: {size} nodes")
        print(f"     Nodes: {nodes_in_comm[:10]}{'...' if len(nodes_in_comm) > 10 else ''}")
    
    print("="*60 + "\n")

def compare_to_product_categories():
    """
    Optional: Compare communities to product categories if products.csv exists
    """
    try:
        import csv
        
        print("\n" + "="*60)
        print("Comparing to Product Categories")
        print("="*60)
        
        # Load product categories
        product_categories = {}
        with open('products.csv', 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                product_id = int(row['product_id'])
                department = row['department']
                product_categories[product_id] = department
        
        print(f"\nLoaded {len(product_categories)} product categories")
        print("Product categories:", set(product_categories.values()))
        print("="*60 + "\n")
        
        return product_categories
    
    except FileNotFoundError:
        print("\nNote: products.csv not found, skipping category comparison\n")
        return None

def save_results(communities, output_file="communities_baseline.txt"):
    """
    Save community assignments to file
    """
    with open(output_file, 'w') as f:
        f.write("node,community\n")
        for node, comm in sorted(communities.items()):
            f.write(f"{node},{comm}\n")
    
    print(f"Results saved to {output_file}")

def main():
    print("\n" + "="*70)
    print(" "*15 + "CLASSICAL LOUVAIN BASELINE")
    print("="*70 + "\n")
    
    # Load graph
    G = load_graph_from_partitions(num_partitions=4)
    
    if G.number_of_nodes() == 0:
        print("Error: Graph is empty! Make sure to run instacart_spark_pipeline.py first.")
        return
    
    # Load product categories (optional)
    product_categories = compare_to_product_categories()
    
    # Run Classical Louvain
    start_time = time.time()
    communities = classical_louvain(G, max_iterations=50, epsilon=0.0001, verbose=True)
    runtime = time.time() - start_time
    
    # Calculate final metrics
    Q = compute_modularity(G, communities)
    num_communities = len(set(communities.values()))
    
    # Display results
    print("\n" + "="*70)
    print("FINAL RESULTS")
    print("="*70)
    print(f"Runtime:              {runtime:.2f} seconds")
    print(f"Final Modularity Q:   {Q:.4f}")
    print(f"Communities found:    {num_communities}")
    print(f"Nodes processed:      {G.number_of_nodes()}")
    print(f"Edges processed:      {G.number_of_edges()}")
    print("="*70)
    
    # Analyze communities
    analyze_communities(G, communities, top_n=5)
    
    # Save results
    save_results(communities, "communities_baseline.txt")
    
    print("\nClassical Louvain baseline complete!")
    print(f"   Use this as reference: Q = {Q:.4f}, Runtime = {runtime:.2f}s\n")

if __name__ == "__main__":
    main()