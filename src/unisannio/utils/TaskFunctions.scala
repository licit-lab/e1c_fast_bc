package unisannio.utils

import scala.collection.mutable.Stack
import scala.collection.mutable.Queue

object TaskFunctions {
  
  /*
   * This function checks if a node is a border node.
   * A border node is a node having at least one neighbor belonging to another cluster.
   * @param node 								the node to check
   * @param dataset_b 					the broadcasted graph
   * @param node_to_cluster_b 	the broadcasted array storing the node-cluster association
   * @return a tuple (true/false, node_id) depending on whether the node is a border node or not
   */
  def is_bordernode_task(node: Int, dataset_b: org.apache.spark.broadcast.Broadcast[Array[Array[Int]]], node_to_cluster_b: org.apache.spark.broadcast.Broadcast[Array[Int]]) = {
    (dataset_b.value(node).tail.find(node_to_cluster_b.value(_) != node_to_cluster_b.value(node)).isDefined, node)
  }
  
  /*
   * This function performs a local (intra-cluster) BFS. Only border nodes have neighbours belonging to other clusters,
   * hence only when the current node is a border node a filter operation is performed. Checking the number of border nodes
   * visited is only an early stop condition.    
   * @param source_node 				source border node of the BFS
   * @param graph 							the broadcasted graph
   * @param node_to_cluster_b 	the broadcasted array storing the node-cluster association
   * @param bordernodes_set_b 	the broadcasted set of border nodes
   * @return a tuple (source_bnode_id, Map[dest_node_id, Array[predecessor_node_id]]) representing the shortest path tree
   * 				 rooted in the source border node
   */
  def compute_bfs_task(source_node: Int, graph: org.apache.spark.broadcast.Broadcast[Array[Array[Int]]], node_to_cluster_b: org.apache.spark.broadcast.Broadcast[Array[Int]], bordernodes_set_b: org.apache.spark.broadcast.Broadcast[Set[Int]]) = {
    
		val stack = Stack[Int]()
		val queue = Queue[Int](source_node)
    val predecessors_sigma_distance = scala.collection.mutable.Map[Int, (Array[Int], Long, Double)](source_node -> (Array[Int](), 1L, 0.0))
    
    var count = 0
    var neighbours = Array.emptyIntArray
    
    while (!queue.isEmpty && (count < bordernodes_set_b.value.size)) {
      val v = queue.dequeue()
      
      stack.push(v)
      
      neighbours = graph.value(v).tail
      
      if(bordernodes_set_b.value.contains(v)) {
    	  count += 1
        neighbours = neighbours.filter(node_to_cluster_b.value(_) == node_to_cluster_b.value(source_node))
      }
      
      neighbours.foreach(neighbour => {
        update_queue_distance(v, neighbour, predecessors_sigma_distance, queue)
        update_predecessors_sigma(v, neighbour, predecessors_sigma_distance)
      })
    }
    
    (source_node, predecessors_sigma_distance.map(predecessor_sigma_distance => (predecessor_sigma_distance._1, predecessor_sigma_distance._2._1)))
  }
  
  /*
   * This function updates queue and distance during the BFS.
   * @param v 														the current node
   * @param 															neighbour the current neighbour
   * @param predecessors_sigma_distance 	the predecessors, number of shortest paths and distance
   * @param queue the queue used during the BFS
   */
  def update_queue_distance(v: Int, neighbour: Int, predecessors_sigma_distance: scala.collection.mutable.Map[Int,(Array[Int], Long, Double)], queue: scala.collection.mutable.Queue[Int]) = {
 
    if (predecessors_sigma_distance.getOrElseUpdate(neighbour, (Array[Int](), 0L, -1.0))._3 < 0) {
      queue.enqueue(neighbour)
      predecessors_sigma_distance(neighbour) = (predecessors_sigma_distance(neighbour)._1, predecessors_sigma_distance(neighbour)._2, predecessors_sigma_distance(v)._3 + 1.0)
    }
  }
  
  /*
   * This function updates predecessors during the BFS.
   * @param v 														the current node
   * @param neighbour 										the current neighbour
   * @param predecessors_sigma_distance 	the predecessors, number of shortest paths and distance
   */
  def update_predecessors_sigma(v: Int, neighbour: Int, predecessors_sigma_distance: scala.collection.mutable.Map[Int,(Array[Int], Long, Double)]) = {
    
    if (predecessors_sigma_distance(neighbour)._3 == predecessors_sigma_distance(v)._3 + 1.0) 
      predecessors_sigma_distance(neighbour) = ((scala.collection.mutable.ArrayBuffer[Int](predecessors_sigma_distance(neighbour)._1:_*) += v).toArray, predecessors_sigma_distance(neighbour)._2 + predecessors_sigma_distance(v)._2, predecessors_sigma_distance(neighbour)._3)
  }
  
  /*
   * This function finds HSN nodes of a shortest path tree by performing a DFS starting from each destination border nodes 
   * up to the source border node. A node belongs to the HSN if and only if it's a border node or it's a node lying on a 
   * shortest path between a pair of border nodes. 
   * @param bordernodes_set_b 			the broadcasted set of border nodes
   * @param bnodesource_dest_stats 	the tuple (source_node_id, Map[dest_node_id, Array[predecessor_node_id]]) representing 
   * 																the shortest path tree
   * @return the array of HSN nodes
   */
  def find_hsn_nodes_task(bordernodes_set_b: org.apache.spark.broadcast.Broadcast[Set[Int]], bnodesource_dest_stats: (Int, scala.collection.mutable.Map[Int, Array[Int]])) = {

    var stack = Stack[Int]()
    var node = -1
    val visited = scala.collection.mutable.Set[Int]()
    
    bnodesource_dest_stats._2.foreach(dest => {
      if (bordernodes_set_b.value.contains(dest._1) && dest._1 != bnodesource_dest_stats._1) { 
        stack.push(dest._1)
          
        while (!stack.isEmpty) {
          node = stack.pop()

          visited += node
            
          bnodesource_dest_stats._2(node).foreach(predecessor => {
            if(!visited.contains(predecessor)) {
              visited += predecessor
              stack.push(predecessor)
            }
          })
        }
      }
    })
    visited += bnodesource_dest_stats._1
    visited.toArray
  }
  
  /*
   * This function performs a local (HSN) BFS. Only nodes belonging to the HSN must be taken into account, hence for each  
   * node a filter operation is performed. Checking the number of border nodes visited is only an early stop condition.    
   * @param source_node 				source border node of the BFS
   * @param graph 							the broadcasted graph
   * @param hsn_nodes_set 			the set of HSN nodes
   * @param bordernodes_set_b 	the broadcasted set of border nodes
   * @return a tuple (source_bnode_id, Map[dest_node_id, (Array[predecessor_node_id], dist)]) representing the shortest path tree
   * 				 rooted in the source border node. Here, the distances are also returned.
   */
  def compute_bfs_with_distance_task(source_node: Int, graph: org.apache.spark.broadcast.Broadcast[Array[Array[Int]]], hsn_nodes_set: scala.collection.Set[Int], bordernodes_set_b: org.apache.spark.broadcast.Broadcast[Set[Int]]) = {
    
		val stack = Stack[Int]()
		val queue = Queue[Int](source_node)
    val predecessors_sigma_distance = scala.collection.mutable.Map[Int, (Array[Int], Long, Double)](source_node -> (Array[Int](), 1L, 0.0))
    
    //variables used to stop exploration when all border nodes have been visited (early stop)
    var count = 0
    //val bordernodes_set = bordernodes_set_b.value
    
    while (!queue.isEmpty && (count < bordernodes_set_b.value.size)) {
      val v = queue.dequeue()
      
      if(bordernodes_set_b.value.contains(v))
        count += 1
        
      stack.push(v)
      /*
      graph.value(v).tail.filter(hsn_nodes_set.contains(_)).foreach(neighbour => {
        update_queue_distance(v, neighbour, predecessors_sigma_distance, queue)
        update_predecessors_sigma(v, neighbour, predecessors_sigma_distance)
      })
      */
      
      graph.value(v).tail.foreach(neighbour => {
        if(hsn_nodes_set.contains(neighbour)) {
          update_queue_distance(v, neighbour, predecessors_sigma_distance, queue)
          update_predecessors_sigma(v, neighbour, predecessors_sigma_distance)
        }
      })
    }
    
    (source_node, predecessors_sigma_distance.map(predecessor_sigma_distance => (predecessor_sigma_distance._1, (predecessor_sigma_distance._2._1, predecessor_sigma_distance._2._3))))
  }
  
  /*
   * This function updates clusters and computes a part of the backpropagation map.
   * @param node_to_cluster_b 			the broadcasted array storing the node-cluster association
   * @param bordernodes_set_b 			the broadcasted set of border nodes
   * @param bnodesource_dest_stats 	the tuple (source_node_id, Map[dest_node_id, (Array[predecessor_node_id], dist)]) 
   * 															 	representing the shortest path tree with distances.
   * @return a tuple (Array[(ext_node_id, Array[cluster_id])], Map[source_bnode_id, Map[dest_bnode_id, (sigma, distance)]])
   * The first field stores the ids of the clusters an external node belongs to; the second field stores the distance and 
   * the number of shortest paths between each pair of border nodes belonging to the same cluster.  
   */
  def update_cluster_and_compute_backpropagation_map_task(node_to_cluster_b: org.apache.spark.broadcast.Broadcast[Array[Int]], bordernodes_set_b: org.apache.spark.broadcast.Broadcast[Set[Int]], bnodesource_dest_stats: (Int, scala.collection.mutable.Map[Int, (Array[Int], Double)])) = {
    val node_to_cluster_updated = scala.collection.mutable.Map[Int, scala.collection.mutable.ArrayBuffer[Int]]()
    val bn_prevbn_sp_dist = scala.collection.mutable.Map[Int, scala.collection.mutable.Map[Int, (Long, Double)]]()
    val stack = Stack[Int]()
    var node = -1
    
    bnodesource_dest_stats._2.foreach(dest => {
       
      if (bordernodes_set_b.value.contains(dest._1) && (dest._1 != bnodesource_dest_stats._1) && (node_to_cluster_b.value(dest._1) == node_to_cluster_b.value(bnodesource_dest_stats._1))) {
        
        stack.push(dest._1)
          
        while (!stack.isEmpty) {
          node = stack.pop()
            
          bnodesource_dest_stats._2(node)._1.foreach(predecessor => {
              
            if(node_to_cluster_b.value(predecessor) != node_to_cluster_b.value(bnodesource_dest_stats._1)) {
              node_to_cluster_updated.update(predecessor, node_to_cluster_updated.getOrElse(predecessor, scala.collection.mutable.ArrayBuffer()) += node_to_cluster_b.value(bnodesource_dest_stats._1))
              stack.push(predecessor)    
            } else {
              if((node_to_cluster_b.value(node) != node_to_cluster_b.value(bnodesource_dest_stats._1)) && (predecessor == bnodesource_dest_stats._1)) {
                bn_prevbn_sp_dist.update(predecessor, 
                  { var tmp = bn_prevbn_sp_dist.getOrElse(predecessor, scala.collection.mutable.Map[Int, (Long, Double)]())
                    tmp.update(dest._1, (tmp.getOrElse(dest._1,(0L, 0.0))._1 + 1L, dest._2._2))
                    tmp
                  })
              }
            }
          })
        }
      }
    })
    (node_to_cluster_updated.mapValues(_.toArray.distinct).toArray, bn_prevbn_sp_dist)
  }
  
  /*
   * This function is used to compute local stats.
   * TODO: complete with javadoc tags. 
   */
  def compute_local_stats_task(source_node: Int, graph: org.apache.spark.broadcast.Broadcast[Array[Array[Int]]], node_to_cluster_b: org.apache.spark.broadcast.Broadcast[Array[Int]], bordernodes_set_b: org.apache.spark.broadcast.Broadcast[Set[Int]], node_to_cluster_updated: Array[Array[Boolean]]) = {
    
    val predecessors_sigma_distance_and_stack = breadth_first_search_with_stack(source_node, node_to_cluster_b.value(source_node), graph, node_to_cluster_updated, true)
    val partial_betweenness = scala.collection.mutable.Map[Int, Double]()
    
    back_propagation(source_node, predecessors_sigma_distance_and_stack._2, partial_betweenness, predecessors_sigma_distance_and_stack._3, node_to_cluster_b.value)

    val sigma_normalized_distance_to_borders = sigma_distance_to_bordernodes(bordernodes_set_b.value, predecessors_sigma_distance_and_stack._2, node_to_cluster_b.value, source_node)
    
    //val max_sigma_norm_t = sigma_normalized_distance_to_borders.maxBy(_._2)
    //val max_dist_norm_t = sigma_normalized_distance_to_borders.maxBy(_._3)
    
    val sigma_normalized_distance_to_borders_sorted = sigma_normalized_distance_to_borders.toArray.sortBy(_._1)
    val str_builder = new StringBuilder("")
    
    sigma_normalized_distance_to_borders_sorted.foreach(t => {str_builder ++= (t._1.toString() + t._2.toString() + t._3.toString())})
    
    (source_node, partial_betweenness.toArray, str_builder.toString())
    //(source_node, partial_betweenness.toArray, sigma_normalized_distance_to_borders.toArray.deep.hashCode)
    
    //(source_node, sigma_normalized_distance_to_borders)
    //(source_node, partial_betweenness, sigma_normalized_distance_to_borders)
  }
  
  /*
   * This function is used to compute a BFS (here the stack is returned).
   * TODO: complete with javadoc tags. 
   */
  def breadth_first_search_with_stack(source_node: Int, source_cluster: Int, graph: org.apache.spark.broadcast.Broadcast[Array[Array[Int]]], node_to_cluster_updated: Array[Array[Boolean]], local: Boolean) = {
    
		val stack = Stack[Int]()
		val queue = Queue[Int](source_node)
    val predecessors_sigma_distance = scala.collection.mutable.Map[Int, (Array[Int], Long, Double)](source_node -> (Array[Int](), 1L, 0.0))
    var neighbours = Array.emptyIntArray
    
    while (!queue.isEmpty) {
      val v = queue.dequeue()
      stack.push(v)
      
      neighbours = graph.value(v).tail
      
      if(local)
        neighbours = neighbours.filter(node_to_cluster_updated(source_cluster)(_))
      
      neighbours.foreach(neighbour => {
        update_queue_distance(v, neighbour, predecessors_sigma_distance, queue)
        update_predecessors_sigma(v, neighbour, predecessors_sigma_distance)
      })
    }
    
    (source_node, predecessors_sigma_distance, stack)
  }
  
  /*
   * This function is used to compute local stats employing Set.
   * TODO: complete with javadoc tags. 
   */
  def compute_local_stats_task_set(source_node: Int, graph: org.apache.spark.broadcast.Broadcast[Array[Array[Int]]], node_to_cluster_b: org.apache.spark.broadcast.Broadcast[Array[Int]], bordernodes_set_b: org.apache.spark.broadcast.Broadcast[Set[Int]], node_to_cluster_updated: Array[scala.collection.mutable.Set[Int]]) = {
    
    val predecessors_sigma_distance_and_stack = breadth_first_search_with_stack_set(source_node, node_to_cluster_b.value(source_node), graph, node_to_cluster_updated, true)
    val partial_betweenness = scala.collection.mutable.Map[Int, Double]()
    
    back_propagation(source_node, predecessors_sigma_distance_and_stack._2, partial_betweenness, predecessors_sigma_distance_and_stack._3, node_to_cluster_b.value)

    val sigma_normalized_distance_to_borders = sigma_distance_to_bordernodes(bordernodes_set_b.value, predecessors_sigma_distance_and_stack._2, node_to_cluster_b.value, source_node)
    
    (source_node, partial_betweenness, sigma_normalized_distance_to_borders.toArray.deep.hashCode)
    //(source_node, sigma_normalized_distance_to_borders)
    //(source_node, partial_betweenness, sigma_normalized_distance_to_borders)
  }
  
  /*
   * This function is used to compute a BFS (here the stack is returned) employing Set.
   * TODO: complete with javadoc tags. 
   */
  def breadth_first_search_with_stack_set(source_node: Int, source_cluster: Int, graph: org.apache.spark.broadcast.Broadcast[Array[Array[Int]]], node_to_cluster_updated: Array[scala.collection.mutable.Set[Int]], local: Boolean) = {
    
		val stack = Stack[Int]()
		val queue = Queue[Int](source_node)
    val predecessors_sigma_distance = scala.collection.mutable.Map[Int, (Array[Int], Long, Double)](source_node -> (Array[Int](), 1L, 0.0))
    var neighbours = Array.emptyIntArray
    
    while (!queue.isEmpty) {
      val v = queue.dequeue()
      stack.push(v)
      
      neighbours = graph.value(v).tail
      
      if(local)
        neighbours = neighbours.filter(node_to_cluster_updated(_).contains(source_cluster))
      
      neighbours.foreach(neighbour => {
        update_queue_distance(v, neighbour, predecessors_sigma_distance, queue)
        update_predecessors_sigma(v, neighbour, predecessors_sigma_distance)
      })
    }
    
    (source_node, predecessors_sigma_distance, stack)
  }
  
  /*
   * This function is used to backpropagate local BC contributions.
   * TODO: complete with javadoc tags. 
   */
  def back_propagation(source_node: Int, predecessors_sigma_distance: scala.collection.mutable.Map[Int, (Array[Int], Long, Double)], partial_betweenness: scala.collection.mutable.Map[Int, Double], stack: scala.collection.mutable.Stack[Int], node_to_cluster: Array[Int]) = {
   
    var node_w = -1
    var update_delta, coefficient = -1.0
    
    while (!stack.isEmpty) {
      node_w = stack.pop
      
      predecessors_sigma_distance(node_w)._1.foreach(node_v => {        
        coefficient = predecessors_sigma_distance(node_v)._2.toDouble/predecessors_sigma_distance(node_w)._2.toDouble
        update_delta = coefficient*partial_betweenness.getOrElse(node_w, 0.0)
        
        if(node_to_cluster(node_w) == node_to_cluster(source_node))
          update_delta += coefficient
          
        if(update_delta > 0)
          partial_betweenness(node_v) = partial_betweenness.getOrElse(node_v, 0.0)  + update_delta
      })
    }
    
    partial_betweenness -= source_node
  }
  
  /*
   * This function is used to compute the topological characteristics of nodes of a cluster.
   * TODO: complete with javadoc tags. 
   */
  def sigma_distance_to_bordernodes(bordernodes_set: scala.collection.Set[Int], predecessors_sigma_distance: scala.collection.mutable.Map[Int, (Array[Int], Long, Double)], node_to_cluster: Array[Int], source_node: Int) = {
    val predecessors_sigma_distance_to_bordernodes = predecessors_sigma_distance.filterKeys(node => (bordernodes_set.contains(node) && (node_to_cluster(node) == node_to_cluster(source_node))))
    val min_distance = predecessors_sigma_distance_to_bordernodes.minBy(_._2._3)._2._3 
    val min_sigma = 1L.max(predecessors_sigma_distance_to_bordernodes.minBy(_._2._2)._2._2)//should be at least equal to 1
    val sigma_normalized_distance_to_bordernodes = predecessors_sigma_distance_to_bordernodes.map(bordernode => (bordernode._1, bordernode._2._2.toDouble/min_sigma.toDouble, bordernode._2._3 - min_distance))
    //val sigma_normalized_distance_to_bordernodes = predecessors_sigma_distance_to_bordernodes.map(bordernode => (bordernode._1, bordernode._2._2.toDouble, bordernode._2._3 - min_distance))
    
    sigma_normalized_distance_to_bordernodes
  }
  
  /*
   * This function is used to compute the topological characteristics of nodes of a cluster.
   * TODO: complete with javadoc tags. 
   */
  def sigma_distance_to_bordernodes_v1(bordernodes_set: scala.collection.Set[Int], predecessors_sigma_distance: scala.collection.mutable.Map[Int, (Array[Int], Long, Double)], node_to_cluster: Array[Int], source_node: Int) = {
    val predecessors_sigma_distance_to_bordernodes = predecessors_sigma_distance.filterKeys(node => (bordernodes_set.contains(node) && (node_to_cluster(node) == node_to_cluster(source_node))))
    val min_distance = predecessors_sigma_distance_to_bordernodes.minBy(_._2._3)._2._3 
    val min_sigma = 1L.max(predecessors_sigma_distance_to_bordernodes.minBy(_._2._2)._2._2)//should be at least equal to 1
    val sigma_normalized_distance_to_bordernodes_hashed = predecessors_sigma_distance_to_bordernodes.map(bordernode => (bordernode._1, bordernode._2._2.toDouble/min_sigma.toDouble, bordernode._2._3 - min_distance).##).sum
    
    sigma_normalized_distance_to_bordernodes_hashed
  }
  
  /*
   * This function is used to compute the topological characteristics of nodes of a cluster.
   * TODO: complete with javadoc tags. 
   */
  def sigma_distance_to_bordernodes2(bordernodes_set: scala.collection.Set[Int], predecessors_sigma_distance: scala.collection.mutable.Map[Int, (Array[Int], Long, Double)], node_to_cluster: Array[Int], source_node: Int) = {
    val predecessors_sigma_distance_to_bordernodes = predecessors_sigma_distance.filterKeys(node => (bordernodes_set.contains(node) && (node_to_cluster(node) == node_to_cluster(source_node))))
    val min_distance = predecessors_sigma_distance_to_bordernodes.minBy(_._2._3)._2._3 
    val sigma_normalized_distance_to_bordernodes = predecessors_sigma_distance_to_bordernodes.map(bordernode => (bordernode._1, predecessors_sigma_distance_to_bordernodes(bordernode._1)._2, predecessors_sigma_distance_to_bordernodes(bordernode._1)._3 - min_distance))
    sigma_normalized_distance_to_bordernodes
  }
  
  /*
   * This function is used to select a pivot in a class.
   * TODO: complete with javadoc tags. 
   */
  def select_source_first_task(cn: Array[Int]) = {
    (cn(0), cn.size)
  }
  
  /*
   * This function is used to compute the global BC contributions.
   * TODO: complete with javadoc tags. 
   */
  def compute_global_bc_task(pivot_cardinality: (Int, Int), dataset_broadcasted: org.apache.spark.broadcast.Broadcast[Array[Array[Int]]], backpropagation_map: scala.collection.Map[Int, scala.collection.Map[Int, (Long, Double)]], node_to_cluster_b: org.apache.spark.broadcast.Broadcast[Array[Int]]) = {
    
    val predecessors_sigma_distance_and_stack = breadth_first_search_with_stack(pivot_cardinality._1, node_to_cluster_b.value(pivot_cardinality._1), dataset_broadcasted, null,false)
    val partial_betweenness = scala.collection.mutable.Map[Int, Double]()
    
    bi_clustered_back_propagation(pivot_cardinality._1, pivot_cardinality._2, predecessors_sigma_distance_and_stack._2, partial_betweenness, node_to_cluster_b.value, predecessors_sigma_distance_and_stack._3, backpropagation_map) 
    
    (pivot_cardinality._1, partial_betweenness.toArray)
  }
  
  /*
   * This function is used to backpropagate global BC contributions.
   * TODO: complete with javadoc tags. 
   */
  def bi_clustered_back_propagation(source_node: Int, cardinality: Int, predecessors_sigma_distance: scala.collection.mutable.Map[Int, (Array[Int], Long, Double)], partial_betweenness:  scala.collection.mutable.Map[Int, Double], node_to_cluster:  Array[Int], stack: scala.collection.mutable.Stack[Int], backpropagation_map : scala.collection.Map[Int, scala.collection.Map[Int, (Long, Double)]]) = {
    
    //val contribution_per_cluster = scala.collection.mutable.Map[Int, scala.collection.mutable.Map[String,Double]]()
    val contribution_per_cluster = Array.fill[Array[Double]](node_to_cluster.length)(Array.fill[Double](2)(0.0))
    
    var increment, coeff, new_coeff = 0.0
    var node_w = -1
    
    0.to(node_to_cluster.length-1).foreach(node => {
      partial_betweenness += (node ->  0.0)
      //contribution_per_cluster += (node -> scala.collection.mutable.Map[String,Double](("not_cluster_v" -> 0.0), ("cluster_v" -> 0.0)))      
    })

    while (!stack.isEmpty) {
      node_w = stack.pop
      
      if (backpropagation_map.contains(node_w) && (node_to_cluster(node_w) != node_to_cluster(source_node))){
        backpropagation_map(node_w).foreach(previous_w_sigma_distance => {
          if((predecessors_sigma_distance(node_w)._3 + previous_w_sigma_distance._2._2) == predecessors_sigma_distance(previous_w_sigma_distance._1)._3) {
            new_coeff = previous_w_sigma_distance._2._1 *(predecessors_sigma_distance(node_w)._2.toDouble / predecessors_sigma_distance(previous_w_sigma_distance._1)._2.toDouble)
            increment += new_coeff*(1+contribution_per_cluster(previous_w_sigma_distance._1)(1))
          }      
        })
        
        contribution_per_cluster(node_w)(0) -= increment
        contribution_per_cluster(node_w)(1) += increment
        
        increment = 0.0
      }
      
      predecessors_sigma_distance(node_w)._1.foreach(node_v => {
        coeff = predecessors_sigma_distance(node_v)._2.toDouble / predecessors_sigma_distance(node_w)._2.toDouble
        
        if (node_to_cluster(node_w) != node_to_cluster(source_node)){
          if (node_to_cluster(node_w) == node_to_cluster(node_v)) {
            contribution_per_cluster(node_v)(1) += coeff*(1+contribution_per_cluster(node_w)(1))
            contribution_per_cluster(node_v)(0) += coeff*contribution_per_cluster(node_w)(0)
          } else {
            contribution_per_cluster(node_v)(0) += coeff*(1+ contribution_per_cluster(node_w)(1) + contribution_per_cluster(node_w)(0))
          }
        } else{
          contribution_per_cluster(node_v)(0) += coeff*(contribution_per_cluster(node_w)(0))
        }
      })
    }

    0.to(contribution_per_cluster.length-1).filter(pos => node_to_cluster(pos) != node_to_cluster(source_node)).foreach(node => {
      partial_betweenness(node) = cardinality *(contribution_per_cluster(node)(0) + 2*contribution_per_cluster(node)(1))
    })
  }
  
  /*
   * This function is used to compute the global BC contributions.
   * TODO: complete with javadoc tags. 
   */
  def compute_global_bc_task2(pivot_cardinality: (Int, Int), dataset_broadcasted: org.apache.spark.broadcast.Broadcast[Array[Array[Int]]], backpropagation_map: scala.collection.Map[Int, scala.collection.Map[Int, (Long, Double)]], node_to_cluster_b: org.apache.spark.broadcast.Broadcast[Array[Int]], number_of_clusters: Int) = {
    
    val predecessors_sigma_distance_and_stack = breadth_first_search_with_stack(pivot_cardinality._1, node_to_cluster_b.value(pivot_cardinality._1), dataset_broadcasted, null,false)
    val partial_betweenness = scala.collection.mutable.Map[Int, Double]()
    
    full_clustered_back_propagation2(pivot_cardinality._1, pivot_cardinality._2, predecessors_sigma_distance_and_stack._2, partial_betweenness, node_to_cluster_b.value, predecessors_sigma_distance_and_stack._3, backpropagation_map, number_of_clusters) 
   
   (pivot_cardinality._1, partial_betweenness.toArray)
  }
  
  /*
   * This function is used to backpropagate global BC contributions.
   * TODO: complete with javadoc tags. 
   */
  def full_clustered_back_propagation(source_node: Int, cardinality: Int, predecessors_sigma_distance: scala.collection.mutable.Map[Int, (Array[Int], Long, Double)], partial_betweenness:  scala.collection.mutable.Map[Int, Double], node_to_cluster:  Array[Int], stack: scala.collection.mutable.Stack[Int], backpropagation_map : scala.collection.Map[Int, scala.collection.Map[Int, (Int, Double)]], number_of_clusters: Int) = {
    
    //val contribution_per_cluster = scala.collection.mutable.Map[Int, scala.collection.mutable.Map[String,Double]]()
    val contribution_per_cluster = Array.fill[Array[Double]](node_to_cluster.length)(Array.fill[Double](number_of_clusters)(0.0))
    
    var coeff, temp_acc = 0.0
    var node_w = -1
    
    0.to(node_to_cluster.length-1).foreach(node => {
      partial_betweenness += (node ->  0.0)
      //contribution_per_cluster += (node -> scala.collection.mutable.Map[String,Double](("not_cluster_v" -> 0.0), ("cluster_v" -> 0.0)))      
    })

    while (!stack.isEmpty) {
      node_w = stack.pop
      
      predecessors_sigma_distance(node_w)._1.foreach(node_v => {
        coeff = predecessors_sigma_distance(node_v)._2.toDouble / predecessors_sigma_distance(node_w)._2.toDouble
        
        if(node_to_cluster(node_w) != node_to_cluster(source_node))
          contribution_per_cluster(node_v)(node_to_cluster(node_w)) += coeff
        
        0.to(number_of_clusters-1).foreach(cluster => {
          contribution_per_cluster(node_v)(cluster) += coeff*contribution_per_cluster(node_w)(cluster)
        })        
      })

      if(node_to_cluster(node_w) != node_to_cluster(source_node)) {
        0.to(number_of_clusters-1).foreach(cluster => {
          if(cluster != node_to_cluster(node_w))
            temp_acc += contribution_per_cluster(node_w)(cluster)
        })
        
        partial_betweenness(node_w) = cardinality*(temp_acc + 2*contribution_per_cluster(node_w)(node_to_cluster(node_w)))
        
        temp_acc = 0.0
      }
    }      
  }
  
  /*
   * This function is used to backpropagate global BC contributions.
   * TODO: complete with javadoc tags. 
   */
  def full_clustered_back_propagation2(source_node: Int, cardinality: Int, predecessors_sigma_distance: scala.collection.mutable.Map[Int, (Array[Int], Long, Double)], partial_betweenness:  scala.collection.mutable.Map[Int, Double], node_to_cluster:  Array[Int], stack: scala.collection.mutable.Stack[Int], backpropagation_map : scala.collection.Map[Int, scala.collection.Map[Int, (Long, Double)]], number_of_clusters: Int) = {
    
    //val contribution_per_cluster = scala.collection.mutable.Map[Int, scala.collection.mutable.Map[String,Double]]()
    val contribution_per_cluster = Array.fill[Array[Double]](node_to_cluster.length)(Array.fill[Double](number_of_clusters)(0.0))
    
    var coeff, temp_acc = 0.0
    var node_w = -1
    
    var node_w_not_source, acc = false
    
    0.to(node_to_cluster.length-1).foreach(node => {
      partial_betweenness += (node ->  0.0)
      //contribution_per_cluster += (node -> scala.collection.mutable.Map[String,Double](("not_cluster_v" -> 0.0), ("cluster_v" -> 0.0)))      
    })

    while (!stack.isEmpty) {
      node_w = stack.pop
      
      if(node_to_cluster(node_w) != node_to_cluster(source_node))
        node_w_not_source = true
      
      predecessors_sigma_distance(node_w)._1.foreach(node_v => {
        coeff = predecessors_sigma_distance(node_v)._2.toDouble / predecessors_sigma_distance(node_w)._2.toDouble
        
        if(node_to_cluster(node_w) != node_to_cluster(source_node))
          contribution_per_cluster(node_v)(node_to_cluster(node_w)) += coeff
        
        0.to(number_of_clusters-1).foreach(cluster => {
          contribution_per_cluster(node_v)(cluster) += coeff*contribution_per_cluster(node_w)(cluster)
          
          if(node_w_not_source && (cluster != node_to_cluster(node_w))) {
            temp_acc += contribution_per_cluster(node_w)(cluster)
            acc = true
          }
        })
        
        if(acc) {
          node_w_not_source = false
          acc = false
        }
      })

      if(node_to_cluster(node_w) != node_to_cluster(source_node)) {
        partial_betweenness(node_w) = cardinality*(temp_acc + 2*contribution_per_cluster(node_w)(node_to_cluster(node_w)))
        temp_acc = 0.0
      }
    }      
  }
}