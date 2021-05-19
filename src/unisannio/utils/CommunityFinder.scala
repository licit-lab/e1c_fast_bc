package unisannio.utils

import scala.util.Random

class CommunityFinder(graph: Array[Array[Int]], epsilon_precision: Double, graph_mapping: scala.collection.Map[Int, Int]) {
  val graphInfo = new GraphInfo(graph)
  val nodes = graphInfo.nodes
  val total_graph_degree = graphInfo.totalGraphDegree.toDouble
  val epsilon = epsilon_precision
  val n2c = Array.fill[Int](graph.length)(-1) //table node/community (at begin nodeID=commID)
  val community_weight_mapped = scala.collection.mutable.Map[Int, Int]() //table community/weight (weight represent the amount of links from a node to a certain community)
  val community_tot_internal_mapped = scala.collection.mutable.Map[Int, (Int, Int)]() //community degree and selfloops
  nodes.foreach(node => tableInit(node))
  val community_tot_in_clone = community_tot_internal_mapped.clone //table node/degree/selfloop
  
  def tableInit(node: Int) = {
    n2c(node) = node
    community_weight_mapped(node) = (-1)
    community_tot_internal_mapped(node) = (graphInfo.degree(node), graphInfo.selfLoop(node, graph(node).head))
  }

  def modularity() = {
    community_tot_internal_mapped.keys.map(comm => modularityMap(community_tot_internal_mapped(comm))).reduce(_ + _)
  }

  def modularityMap(degree_selfloops: (Int, Int)) = {
    var q = 0.0
    if (degree_selfloops._1 > 0) {
      q = degree_selfloops._2 / total_graph_degree - (degree_selfloops._1 / total_graph_degree) * (degree_selfloops._1 / total_graph_degree)
    }
    q
  }

  def resetNeighWeight(arr_neighbors: Iterable[Int]) = {
    arr_neighbors.foreach(neigh => {
      val community_id = n2c(graph_mapping(neigh))
      if (community_weight_mapped(community_id) != (-1)) {
        community_weight_mapped(community_id) = (-1)
      }
    })
  }

  def neighCommunity(node: Int) = {
    val neigh_occurrence = graphInfo.neighbors(node).groupBy(neigh => neigh).mapValues(_.size)
    //reset community weight
    resetNeighWeight(neigh_occurrence.keys)
    //for current node setting to 0
    val community_id = n2c(node)
    community_weight_mapped(community_id) = 0
    //map (for each neighbor calculates community weight)
    neigh_occurrence.map(neigh_occ => neighCommunityMap(node, neigh_occ))
  }

  def neighCommunityMap(node: Int, neighnode_occurrence: (Int, Int)) = {
    if (graph_mapping(neighnode_occurrence._1) != node) {
      //searching neighbor's community
      val community_id = n2c(graph_mapping(neighnode_occurrence._1))
      //reading weight of that community
      synchronized {
        var weight_community = community_weight_mapped(community_id)
        if (weight_community == (-1)) {
          weight_community = 0
        }
        //weight increment
        community_weight_mapped(community_id) = weight_community + neighnode_occurrence._2
      }
    }
  }

  def insert(node: Int, community_id: Int, dnodecomm: Int) = {
    val (degree_community, self_loop_community) = community_tot_internal_mapped(community_id)
    //update degree and selfloop of community
    community_tot_internal_mapped(community_id) = (degree_community + community_tot_in_clone(node)._1, self_loop_community + (2 * dnodecomm) + community_tot_in_clone(node)._2)
    //update association node->community
    n2c(node) = community_id
  }

  def remove(node: Int, comm: Int, dnodecomm: Int) = {
    val (degree_comm, sl_comm) = community_tot_internal_mapped(comm)
    //update degree and selfloop of community
    community_tot_internal_mapped(comm) = (degree_comm - community_tot_in_clone(node)._1, sl_comm - (2 * dnodecomm) - community_tot_in_clone(node)._2)
  }

  def bestCommunity(neigh: Int, node_degree: Int) = {
    val community_id = n2c(graph_mapping(neigh))
    val weight_community = community_weight_mapped(community_id)
    val increase = modularityGain(community_id, weight_community, node_degree)
    (community_id, weight_community, increase)
  }

  def modularityGain(comm: Int, dnodecomm: Int, degree: Int) = {
    val tot_degree_community = community_tot_internal_mapped(comm)._1.toDouble
    dnodecomm - (tot_degree_community * degree / total_graph_degree)
  }

  def oneLouvainIteration() = {
    var improvement = false
    var nb_moves = 0
    var new_mod = modularity
    var cur_mod = new_mod
    do {
      cur_mod = new_mod
      nb_moves = 0
      val random_node_evaluation = Random.shuffle(nodes.toSeq)
      nb_moves = random_node_evaluation.map(node => oneNodeLouvain(node)).reduce(_ + _)      
      new_mod = modularity
      if (nb_moves > 0) {
        improvement = true
      }
    } while (nb_moves > 0 && new_mod - cur_mod > epsilon)
    improvement
  }

  def oneNodeLouvain(node: Int) = {
    val node_comm = n2c(node)
    val degree = community_tot_in_clone(node)._1
    var nb_moves = 0
    //node's neighbors evaluation
    neighCommunity(node)
    val weight_community = community_weight_mapped(node_comm)
    //remove current node from its community
    remove(node, node_comm, weight_community)
    //find best community
    var best_comm = node_comm
    var best_nblinks = 0
    val arr_node_neigh = graph(node)
    val best = arr_node_neigh.map(n => bestCommunity(n, degree)).maxBy(_._3)
    best_comm = best._1
    best_nblinks = best._2
    //insert current node in best community (best modularity increment)
    insert(node, best_comm, best_nblinks)
    if (best_comm != node_comm) {
      nb_moves = 1
    }
    nb_moves
  }

}