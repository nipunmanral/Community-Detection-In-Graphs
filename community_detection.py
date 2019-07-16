from pyspark import SparkConf, SparkContext
import argparse
import time
from itertools import combinations
from collections import deque
import copy

# spark-submit community_detection.py 7 ~/Downloads/sample_data.csv ~/Downloads/output_betweenness.txt ~/Downloads/output_community.txt

def create_adjacency_graph(user_edges_rdd):
    rdd_1 = user_edges_rdd.groupByKey().mapValues(set)
    rdd_2 = user_edges_rdd.map(lambda x: (x[1], x[0])).groupByKey().mapValues(set)
    return rdd_1.union(rdd_2).reduceByKey(lambda x, y: x.union(y))


def breadth_first_search(start_node, user_adjacency_dict):
    """
    Step 1 of Girvan-Newman Algorithm
    :return: Returns a dictionary which contains the level wise node graph
    Eg:{0: ['B'], 1: ['A', 'K', 'L', 'M'], 2: ['C', 'Y'], 3: ['D', 'E', 'F'], 4: ['Z']}
    """
    visited_nodes = [start_node]
    nodes_current_level_list = [start_node]
    level_index = 0
    bfs_graph_dict = {}

    while len(nodes_current_level_list) != 0:
        bfs_graph_dict[level_index] = nodes_current_level_list
        level_index += 1
        nodes_next_level_list = []
        for current_node in nodes_current_level_list:
            current_neighbors = user_adjacency_dict[current_node]
            for neighbor in current_neighbors:
                if neighbor not in visited_nodes:
                    visited_nodes.append(neighbor)
                    nodes_next_level_list.append(neighbor)
        nodes_current_level_list = nodes_next_level_list
    return bfs_graph_dict


def generate_node_weights(bfs_graph_dict, user_adjacency_dict):
    """
    Step 2 of Girvan-Newman Algorithm
    :return:
    """
    node_weights = {}
    levels = len(bfs_graph_dict)
    for root_level_node in bfs_graph_dict[0]:
        node_weights[root_level_node] = 1.0

    for current_level in range(1, levels):
        previous_level_nodes = set(bfs_graph_dict[current_level-1])
        current_level_nodes = set(bfs_graph_dict[current_level])
        for node in current_level_nodes:
            node_neighbors = set(user_adjacency_dict[node])
            parent_nodes = previous_level_nodes.intersection(node_neighbors)
            sum = 0.0
            for parent in parent_nodes:
                sum += node_weights[parent]
            node_weights[node] = sum
    return node_weights


def generate_edge_weights(node_weights, bfs_graph_dict, user_adjacency_dict):
    """
    Step 3 of Girvan-Newman Algorithm
    :return:
    """
    edge_weights = {}
    node_credits = {}
    levels = len(bfs_graph_dict)
    for last_level_node in bfs_graph_dict[levels - 1]:
        node_credits[last_level_node] = 1

    for current_level in range(levels - 2, -1, -1):
        current_level_nodes = set(bfs_graph_dict[current_level])
        next_level_nodes = set(bfs_graph_dict[current_level + 1])
        for node in current_level_nodes:
            node_neighbors = set(user_adjacency_dict[node])
            child_nodes = next_level_nodes.intersection(node_neighbors)
            sum = 1.0 if current_level != 0 else 0.0
            for child in child_nodes:
                value = (node_credits[child]/node_weights[child]) * node_weights[node]
                edge_weights_sort_key = tuple(sorted([node, child]))
                edge_weights[edge_weights_sort_key] = value
                sum += value
            node_credits[node] = sum
    return edge_weights


def calculate_betweenness(start_node, user_adjacency_dict):
    bfs_graph_dict = breadth_first_search(start_node, user_adjacency_dict)
    node_weights = generate_node_weights(bfs_graph_dict, user_adjacency_dict)
    edge_weights = generate_edge_weights(node_weights, bfs_graph_dict, user_adjacency_dict)
    return edge_weights.items()


def fetch_connected_communities():
    visited = []
    connected_components = []
    for start_node in community_user_adjacency_dict.keys():
        detected_community = []
        queue = deque([start_node])
        while queue:
            node = queue.popleft()
            if node not in visited:
                visited.append(node)
                detected_community.append(node)
                node_neighbors = community_user_adjacency_dict[node]
                for neighbor in node_neighbors:
                    queue.append(neighbor)
        if len(detected_community) != 0:
            detected_community.sort()
            connected_components.append(detected_community)
    return connected_components


def calculate_modularity(community_list):
    modularity_sum = 0
    for community in community_list:
        if len(community) > 1:
            for node_i_index in range(0, len(community)):
                for node_j_index in range(node_i_index, len(community)):
                    node_i = community[node_i_index]
                    node_j = community[node_j_index]
                    modularity_sort_key = tuple(sorted([node_i, node_j]))
                    adjacent_matrix_value = 1.0 if modularity_sort_key in original_user_edges_list else 0.0
                    # neighbors_node_i = user_adjacency_dict[node_i]
                    # if node_j in neighbors_node_i:
                    #     adjacent_matrix_value = 1.0
                    # else:
                    #     adjacent_matrix_value = 0.0
                    value = adjacent_matrix_value - (node_degrees_dict[node_i] * node_degrees_dict[node_j] * formula_first_part)
                    modularity_sum += value
    modularity_sum = modularity_sum * formula_first_part
    return modularity_sum


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("filter_threshold", type=int, help="Enter the filter threshold to generate edges between "
                                                           "user nodes")
    parser.add_argument("input_file_path", type=str, help="Enter the input file path")
    parser.add_argument("betweenness_output", type=str, help="Enter the path of the betweenness output file")
    parser.add_argument("community_output", type=str, help="Enter the path of the community output file")
    args = parser.parse_args()

    start = time.time()
    conf = SparkConf().setAppName("h4_task1").setMaster("local[*]").set("spark.driver.memory", "4g")\
        .set("spark.executor.memory", "4g")
    sc = SparkContext(conf=conf)

    threshold = args.filter_threshold

    input_data_rdd = sc.textFile(args.input_file_path)
    header_line = input_data_rdd.first()
    input_data_rdd = input_data_rdd.filter(lambda x: x != header_line).map(lambda y: y.split(","))
    users_rdd = input_data_rdd.map(lambda x: (x[0], x[1])).groupByKey().mapValues(set)
    users_rdd.persist()
    users_dict = dict(users_rdd.collect())
    distinct_users = users_rdd.keys().collect()

    original_user_edges_list = []
    for temp_user in combinations(distinct_users, 2):
        if len(users_dict[temp_user[0]].intersection(users_dict[temp_user[1]])) >= threshold:
            edge_sort_key = tuple(sorted([temp_user[0], temp_user[1]]))
            original_user_edges_list.append(edge_sort_key)

    num_user_edges_original_graph = len(original_user_edges_list)

    user_edges_rdd = sc.parallelize(original_user_edges_list).persist()
    user_adjacency_rdd = create_adjacency_graph(user_edges_rdd)
    user_adjacency_dict = user_adjacency_rdd.collectAsMap()
    node_degrees_dict = user_adjacency_rdd.map(lambda x: (x[0], len(x[1]))).collectAsMap()

    edge_betweenness_rdd = user_adjacency_rdd.keys().flatMap(lambda x: calculate_betweenness(x, user_adjacency_dict))\
        .reduceByKey(lambda a, b: a+b).mapValues(lambda y: y/2.0).sortBy(lambda z: (-z[1], z[0]), ascending=True)

    edge_betweenness_values = edge_betweenness_rdd.collect()
    with open(args.betweenness_output, 'w') as file:
        for line in edge_betweenness_values:
            line_write = str(line[0]) + ", " + str(line[1]) + "\n"
            file.write(line_write)

    community_edges = deque(edge_betweenness_values)
    community_user_adjacency_dict = copy.deepcopy(user_adjacency_dict)

    formula_first_part = (1 / (2 * num_user_edges_original_graph))
    global_maximum_modularity = -1.0
    final_communities = []

    while len(community_edges) != 0:
        removed_edge = community_edges.popleft()[0]
        community_user_adjacency_dict[removed_edge[0]].remove(removed_edge[1])
        community_user_adjacency_dict[removed_edge[1]].remove(removed_edge[0])
        # node_degrees_dict[removed_edge[0]] -= 1
        # node_degrees_dict[removed_edge[1]] -= 1
        connected_communities = fetch_connected_communities()
        community_modularity = calculate_modularity(connected_communities)
        if community_modularity > global_maximum_modularity:
            global_maximum_modularity = community_modularity
            final_communities = connected_communities
        community_user_adjacency_rdd = sc.parallelize(community_user_adjacency_dict.items())
        community_edges_betweenness = community_user_adjacency_rdd.keys().\
            flatMap(lambda x: calculate_betweenness(x, community_user_adjacency_dict)).reduceByKey(lambda a, b: a + b).\
            mapValues(lambda y: y / 2.0).sortBy(lambda z: (-z[1], z[0]), ascending=True).collect()
        community_edges = deque(community_edges_betweenness)

    # print(global_maximum_modularity)
    # print(len(final_communities))

    final_communities = sorted(final_communities, key=lambda x: (len(x), x))
    with open(args.community_output, 'w') as file:
        for community in final_communities:
            value = str(community).replace('[', '').replace(']', '') + "\n"
            file.write(value)
    end = time.time()

    # print("Duration: ", end-start)
