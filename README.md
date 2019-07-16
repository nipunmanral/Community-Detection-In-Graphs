# Community Detection In Graphs

## Objective
In this project, we will implement the Girvan-Newman algorithm to detect communities in graphs where each community is a set of users who have a similar business taste, using the Yelp dataset. We will identify these communities using map-reduce, Spark RDD and standard python libraries.

## Environment Setup
Python Version = 3.6

Spark Version = 2.3.3

## Dataset
The dataset `sample_data.csv` is a sub-dataset which has been generated from the [Yelp review dataset](https://www.yelp.com/dataset). Each line in this dataset contains a user_id and business_id.

## Code Execution
Run the python code using the below command:

    spark-submit community_detection.py <filter_threshold> <input_file_path> <betweenness_output_file_path> <community_output_file_path>

- filter_threshold: the filter threshold to generate edges between user nodes.
- input_file_path: the path to the input file including path, file name and extension.
- betweenness_output_file_path: the path to the betweenness output file including path, file name and extension.
- community_output_file_path: the path to the community output file including path, file name and extension.

## Approach
(Reference: 'Mining of Massive Datasets' by Jure Leskovec, Anand Rajaraman and Jeffrey D. Ullman)
### Graph Construction
We first construct the social network graph, where each node represents a user. An edge exists between two nodes if the number of times that the two users review the same business is greater than or equivalent to the filter threshold. For example, suppose user1 reviews [business1, business2, business3] and user2 reviews [business2, business3, business4, business5]. If the threshold is 2, then there exists an edge between user1 and user2. If a user node has no edge, then we do not include that node in the graph.

### Betweenness Calculation
In this part, we calculate the betweenness of each edge in the original graph and save the result in a txt file. The betweenness of an edge (a, b) is defined as the number of pairs of nodes x and y such that the edge (a ,b) lies on the shortest path between x and y. We use the [Girvan-Newman Algorithm](https://en.wikipedia.org/wiki/Girvan%E2%80%93Newman_algorithm) to calculate the number of shortest paths going through each edge. In this algorithm, we visit each node X once and compute the number of shortest paths from X to each of the other nodes that go through each of the edges as shown in the below steps:
1. First, perform a breadth-first search (BFS) of the graph, starting at node X.
2. Next, label each node by the number of shortest paths that reach it from the root. Start by labelling the root 1. Then, from the top down, label each node Y by the sum of the labels of its parents.
3. Calculate for each edge e, the sum over all nodes Y (of the fraction) of the shortest paths from the root X to Y that go through edge e.

To complete the betweenness calculation, we have to repeat this calculation for every node as the root and sum the contributions. Finally, we must divide by 2 to get the true betweenness, since every shortest path will be discovered twice, once for each of its endpoints.

### Community Detection
We use [modularity](https://en.wikipedia.org/wiki/Modularity_(networks)) to identify the communities by taking the graph and all its edges, and then removing edges with the highest betweenness, until the graph has broken into a suitable number of connected components. Thus, we divide the graph into suitable communities, which reaches the global highest modularity.

The formula of modularity is shown below:

    𝑸 = ∑s∈S [(# edges within group s) – (expected # edges within group s)]

    𝑸(𝑮,𝑺) = (1/(2 * m)) * ∑𝒔∈𝑺 ∑𝒊∈𝒔 ∑𝒋∈𝒔 (Aij − ((ki * kj)/(2 * m)))

According to the Girvan-Newman algorithm, after removing one edge, we should re-compute the betweenness. The “m” in the formula represents the edge number of the original graph. The “A” in the formula is the adjacent matrix of the original graph where Aij is 1 if i connects j, else it is 0. ki is the node degree of node i. In each remove step, 'm', 'A', 'ki' and 'kj' should not be changed. If the community only has one user node, we still regard it as a valid community. We save the results to an output text file.

## Output file format
The betweenness calculation output is saved in the path specified by the `<betweenness_output_file_path>` parameter in the execution script. This output is saved in a txt file which contains the betweenness of each edge in the original graph. The format of each line is `(‘user_id1’, ‘user_id2’), betweenness value`. The results are firstly sorted by the betweenness values in the descending order and then the first user_id in the tuple in lexicographical order (the user_id is of type string). The two user_ids in each tuple are also in lexicographical order.

The community detection output is saved in the path specified by the `<community_output_file_path>` parameter in the execution script. The output which is the resulting communities are saved in a txt file. Each line represents one community and the format is: `‘user_id1’, ‘user_id2’, ‘user_id3’, ‘user_id4’, ...`. The results are firstly sorted by the size of communities in the ascending order and then the first user_id in the community in lexicographical order (the user_id is of type string). The user_ids in each community are also in the lexicographical order. If there is only one node in the community, we still regard it as a valid community.
