# Fast Cluster-based Computation of Exact Betweenness Centrality in Large Graphs
___
## Introduction
This repository contains the source code of the *E1C-FastBC* algorithm, a cluster-based and pivot-based solution for the exact computation of the (Node) Betweenness Centrality in large undirected graphs. For further details see the paper [Fast Cluster-based Computation of Exact Betweenness Centrality in Large Graphs] . 

## Installation
The algorithm requires a running Spark cluster ([Apache Spark 2.2.0]).

The first step involves generating the *.jar* file to submit to the Spark cluster. To do that, run the following command in the directory where the *pom.xml* file is located:

```sh
mvn package
```
(Alternatively, the jar file can be generated within a Scala IDE)

A jar file named *FastBetweennessCentrality-1.0.1-SNAPSHOT-jar-with-dependencies.jar* will be created in the *target* directory.

The second step involves running the *spark-submit* command. The syntax is:

```sh
spark-submit --master <master-url> --deploy-mode <deploy-mode> --class unisannio.algorithm.ExactOneCFastBC --name 1CFastBCExact --driver-memory <driver-memory> --executor-memory <executor-memory> --total-executor-cores <total-executor-cores> --executor-cores <executor-cores> --conf "spark.default.parallelism=<default-parallelism>" target\FastBetweennessCentrality-1.0.1-SNAPSHOT-jar-with-dependencies.jar <algo_args>
```

```<algo_args>``` has the following form:

```sh
<master-url> <input-file> <logging> <stats-dir> <stats-file> <n-executors> <profiling> <n-mapper> <louvain-epsilon>
```

- ```<master-url>```: url of the master node (same as ```<master-url>``` in the spark-submit command)
- ```<input-file>```: file storing the graph to analyze (see Input file format)
- ```<logging>```: true/false depending on whether enable/disable logging
- ```<stats-dir>```: directory where the output files (one file containing BC values and one file containing some other statistics such as the number of cluster, number of border nodes etc.) have to be saved. 
- ```<stats-file>```: name of the file containing the statistics
- ```<n-executors>```: number of executors
- ```<profiling>```: true/false depending on whether enable/disable profiling
- ```<n-mapper>```: number of mapper (for parallel executions of louvain)
- ```<louvain-epsilon>```: threshold to be used as a stop condition between one iteration of louvain and the next (difference between the modularity scores of two iterations within the same execution)

## Input file format
The input file stores the graph as *adjacency list*. In particular, the file must have a line for each node of the graph. Node identifiers must start with ```0``` and must be contiguous. Hence, if the graph has ```n``` nodes, the identifiers range from ```0``` to ```n-1```. In the file, identifiers are separated by ";": the first number represents the node; the other numbers represent the neighbors. At the moment, the algorithm only works on unweighted graphs. However, the extension to weighted graphs is pretty straightforward. Indeed, it's just necessary to replace BFS-based explorations with Dijkstra-based ones. 

The following is an example of a valid input file.

```sh
0;1;2;3
1;0;2
2;0;1
3;0;
```

## Usage
The following example shows how to run the algorithm on one of the graphs provided in the *example* directory of the project using a driver with 1GB and one executor with 1GB of RAM and 2 cores. In particular, logging and profiling are disabled, the number of executors is set to 1, the number of mappers is set to 10 and the louvain-epsilon is set to 0.1.

```sh
spark-submit --master spark://xxx.xxx.xxx.xxx:7077 --deploy-mode client --class unisannio.algorithm.ExactOneCFastBC --name 1CFastBCExact --driver-memory 1G --executor-memory 1G --total-executor-cores 2 --executor-cores 2 --conf "spark.default.parallelism=2" target\FastBetweennessCentrality-1.0.1-SNAPSHOT-jar-with-dependencies.jar spark://xxx.xxx.xxx.xxx:7077 examples\BA_6250_m1.csv false . ./stats.csv 1 false 10 0.1
```


[Fast Cluster-based Computation of Exact Betweenness Centrality in Large Graphs]: <https://www.researchsquare.com/article/rs-321493/v1>
[Apache Spark 2.2.0]: <https://spark.apache.org/docs/2.2.0/cluster-overview.html>
