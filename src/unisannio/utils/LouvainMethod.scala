package unisannio.utils

import org.apache.spark.SparkContext

object LouvainMethod {

  def louvainExecution(sc: SparkContext, dataset_b: org.apache.spark.broadcast.Broadcast[Array[Array[Int]]], n_mappers: Int, epsilon_precision: Double, number_of_partitions: Int) = {
    val mappers_rdd = sc.parallelize((1 to n_mappers).toSeq, number_of_partitions)
    
    var graph_mapping = sc.parallelize(0.to(dataset_b.value.length-1), number_of_partitions).map(pos => (pos, pos)).collectAsMap
    
    //map (each map run Louvain Method instance with dataset shuffled), after map, get the instance that have best modularity
    var best_result = mappers_rdd.map(mapper => louvainMapInstance(new CommunityFinder(dataset_b.value, epsilon_precision, graph_mapping))).max()(Ordering.by(_._2))
    var improvement = best_result._1
    var nb_pass_done = 1
    //table node to community for association tracking
    var n2c = best_result._3
    //create new graph by substitution node->community
    var new_graph = generateNewGraph(sc, dataset_b.value, n2c, graph_mapping, number_of_partitions)
    
    //new iteration if there's improvement of modularity
	  var early_stop = 0
    
	  while (improvement){
	    graph_mapping = sc.parallelize(0.to(new_graph.length-1), number_of_partitions).map(pos => (new_graph(pos).head, pos)).collectAsMap
      best_result = mappers_rdd.map(mapper => louvainMapInstance(new CommunityFinder(new_graph, epsilon_precision, graph_mapping))).max()(Ordering.by(_._2))
      improvement = best_result._1
      nb_pass_done = nb_pass_done + 1
      var new_n2c = best_result._3
      //community update
      n2c = communityUpdate(n2c, new_n2c, sc, graph_mapping, number_of_partitions)
      //create new graph by substitution node->community
      new_graph = generateNewGraph(sc, new_graph, new_n2c, graph_mapping, number_of_partitions)
      
	    early_stop+=1
	    
    }
    val number_of_clusters = new_graph.filter(_.length > 0).length
    println("Iteration n. " + nb_pass_done + ", modularity: " + best_result._2 + ", number of communities: " + number_of_clusters)
    
    (n2c, number_of_clusters)
  }

  def louvainMapInstance(c: CommunityFinder) = {
    (c.oneLouvainIteration, c.modularity, c.n2c)
  }

  def newGraph(node: Int, neigh: Array[Int], n2c: Array[Int], graph_mapping: scala.collection.Map[Int, Int]) = {
    val new_node_community_id = Array(n2c(graph_mapping(node)))
    val new_neigh = neigh.map(n => n2c(graph_mapping(n)))
    new_node_community_id ++ new_neigh
  }
  
  def noDup(arrlinks: Array[Array[Int]], sc: SparkContext) = {
    val new_dataset_array = arrlinks.groupBy(nodelinks => nodelinks.head).map(a => noDupMap(a)).toArray
    new_dataset_array
  }

  def noDupMap(a: (Int, Array[Array[Int]])) = {
    var arr = a._2(0)
    for (i <- 1 to a._2.length - 1) {
      arr = arr ++ a._2(i).tail
    }
    arr
  }
  
  def generateNewGraph(sc: SparkContext, graph: Array[Array[Int]], n2c: Array[Int], graph_mapping: scala.collection.Map[Int, Int], number_of_partitions: Int) = {
    val nodes_RDD = sc.parallelize(0.to(graph.length-1), number_of_partitions)
    val new_dataset_array = nodes_RDD.map(node => newGraph(graph(node).head, graph(node).tail, n2c, graph_mapping)).collect()
    //remove duplicate nodes
    val new_graph = noDup(new_dataset_array, sc)
    new_graph
  }

  def communityUpdate(n2c: Array[Int], new_n2c: Array[Int], sc: SparkContext, graph_mapping: scala.collection.Map[Int, Int], number_of_partitions:Int) = {
    val n2c_updated = Array.fill[Int](n2c.length)(-1)
    sc.parallelize(0.to(n2c.length-1), number_of_partitions).map(node => (node, new_n2c(graph_mapping(n2c(node))))).collect().foreach(node_cluster => {
      n2c_updated(node_cluster._1) = node_cluster._2 
    })
    n2c_updated
  }
}