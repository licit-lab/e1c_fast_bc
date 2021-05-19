package unisannio.utils

import scala.collection.mutable.Stack
import scala.collection.mutable.Queue
import scala.collection.immutable.Map
import java.io.PrintWriter
import java.io.FileWriter
import java.io.File
import scala.collection.mutable.ArrayBuffer


class GraphInfo(
    node_graph_and_neighbors: Array[Array[Int]], 
    clusters_broadcasted: org.apache.spark.broadcast.Broadcast[Array[Array[Array[Int]]]],
    dataset_broadcasted: (Boolean, org.apache.spark.broadcast.Broadcast[Array[Array[Int]]])) {

  def this(node_graph_and_neighbors: Array[Array[Int]]) = this(node_graph_and_neighbors, null, null)
  def this(clusters_broadcasted: org.apache.spark.broadcast.Broadcast[Array[Array[Array[Int]]]]) = this(null, clusters_broadcasted, null)
  def this(dataset_broadcasted: (Boolean, org.apache.spark.broadcast.Broadcast[Array[Array[Int]]])) = this(null, null, dataset_broadcasted)
  
  def display() = {
    (0.to(node_graph_and_neighbors.length-1)).foreach(node => {
      println("\nNode: " + node + " links:\n")
      node_graph_and_neighbors(node).tail.foreach(println) // tail because first element is the node
    })
  }

  def nodes() = {
    (0.to(node_graph_and_neighbors.length-1)).toArray
  }

  def neighbors(node: Int) = {
    node_graph_and_neighbors(node).tail
  }

  def degree(node: Int) = {
    node_graph_and_neighbors(node).tail.length
  }

  def selfLoop(node: Int, node_mapped: Int) = {
    node_graph_and_neighbors(node).tail.count(neigh => neigh == node_mapped)
  }

  def totalGraphDegree() = {
    nodes.map(node => degree(node)).reduce(_ + _)
  }
  
  def betwennessSingleSourceCentrality(node: Int, file_prefix:String, log: Boolean) = {
    val start = System.currentTimeMillis()
    val nodelink = dataset_broadcasted._2.value(node)
    val (predecessors_sigma_distance, partial_betweenness, stack, queue) = initialization(nodelink)
    while (!queue.isEmpty) {
      val v = queue.dequeue
      stack.push(v.head)
      //neighbor of extracted node evaluation
      v.tail.foreach(neighnode => {
        pathDiscovery(predecessors_sigma_distance, queue, v, neighnode)
        pathCounting(predecessors_sigma_distance, v, neighnode)
      })
    }
    backPropagation(nodelink, predecessors_sigma_distance, partial_betweenness, stack)
    val delta =  (System.currentTimeMillis() - start) / 1000.0
    if(log){
      val file = new PrintWriter(new FileWriter(new File(file_prefix, "single_source_bc.csv"), true))
      file.write(node + "," + delta + "\n")
      file.close()
    }
    (node, partial_betweenness)
  }
 
  
  def initialization(nodelink: Array[Int]) = {
    val predecessors_sigma_distance=scala.collection.mutable.Map[Int, (Array[Int], Double, Double)]()
    val partial_betweenness=scala.collection.mutable.Map[Int, Double]()
    val stack = Stack[Int]()
    //for current node set sigma to 1 and dist to 0
    val tuple = (Array[Int](), 1.0, 0.0)
    predecessors_sigma_distance(nodelink.head) = tuple
    val queue = Queue[Array[Int]]()
    //adding current node to queue
    queue.enqueue(nodelink)
    (predecessors_sigma_distance, partial_betweenness, stack, queue)
  }
  
  def pathDiscovery(predecessors_sigma_distance: scala.collection.mutable.Map[Int,(Array[Int], Double, Double)], queue: scala.collection.mutable.Queue[Array[Int]], v: Array[Int], neighnode: Int) = {
    //if first visit
    if (predecessors_sigma_distance.getOrElseUpdate(neighnode, (Array[Int](), 0.0, -1.0))._3 < 0) {
      //insert node in queue
      queue.enqueue(dataset_broadcasted._2.value(neighnode))
      //update distance
      val tuple_new_distance = (predecessors_sigma_distance(neighnode)._1, predecessors_sigma_distance(neighnode)._2, predecessors_sigma_distance(v.head)._3 + 1)
      predecessors_sigma_distance(neighnode)=tuple_new_distance  
    }
  }
  
  
  def clusterPathDiscovery(predecessors_sigma_distance: scala.collection.mutable.Map[Int,(Array[Int], Double, Double)]
                          ,queue: scala.collection.mutable.Queue[Array[Int]], v: Array[Int], neighnode: Int, comm: Int) = {
    //if first visit
    if (predecessors_sigma_distance.getOrElseUpdate(neighnode, (Array[Int](), 0.0, -1.0))._3 < 0) {
      //insert node in queue
      queue.enqueue(clusters_broadcasted.value(comm)(neighnode))
      //update distance
      val tuple_new_distance = (predecessors_sigma_distance(neighnode)._1, predecessors_sigma_distance(neighnode)._2, predecessors_sigma_distance(v.head)._3 + 1)
      predecessors_sigma_distance(neighnode)=tuple_new_distance  
    }
  }
  
  def pathCounting(predecessors_sigma_distance: scala.collection.mutable.Map[Int,(Array[Int], Double, Double)], v: Array[Int], neighnode: Int) = {
    //if minimum path to neighnode by v
    if (predecessors_sigma_distance(neighnode)._3 == predecessors_sigma_distance(v.head)._3 + 1) {
      //update sigma and add v to neighnode predecessor list
      val tuple_new_sigma_and_predecessor=(predecessors_sigma_distance(neighnode)._1:+(v.head), predecessors_sigma_distance(neighnode)._2+predecessors_sigma_distance(v.head)._2, predecessors_sigma_distance(neighnode)._3)
      predecessors_sigma_distance(neighnode)=tuple_new_sigma_and_predecessor
    }
  }
  
  def backPropagation(nodelink: Array[Int], predecessors_sigma_distance: scala.collection.mutable.Map[Int,(Array[Int], Double, Double)]
                    , partial_betweenness: scala.collection.mutable.Map[Int,Double], stack: scala.collection.mutable.Stack[Int]) = {
    while (!stack.isEmpty) {
      val node = stack.pop
      predecessors_sigma_distance(node)._1.foreach(nodeP => {
        //update delta (partial betwenness contribute from this source)
        val update_delta=partial_betweenness.getOrElseUpdate(nodeP, 0.0) + ((predecessors_sigma_distance(nodeP)._2.toDouble / predecessors_sigma_distance(node)._2.toDouble) * (1 + partial_betweenness.getOrElseUpdate(node, 0.0)))
        partial_betweenness(nodeP)=update_delta
      })
    }
    //set betweenness of source node to 0
    partial_betweenness(nodelink.head)= 0.0
  }
  
  //modified version of Brandes algorithm for distance and sigma calculation only to border nodes
  def modifiedBetwennessSingleSourceCentrality(node: Int, bordernodes: scala.collection.immutable.Map[Int,Boolean], comm: Int, version:String
      , file_prefix:String, log:Boolean, file: PrintWriter) = {
    val nodelink = clusters_broadcasted.value(comm)(node)
    
    val (predecessors_sigma_distance, partial_betweenness, stack, queue) = initialization(nodelink)
    while (!queue.isEmpty) {
      val v = queue.dequeue
      stack.push(v.head)
      
      //neighbor of extracted node evaluation
      v.tail.foreach(neighnode => {
        clusterPathDiscovery(predecessors_sigma_distance, queue, v, neighnode, comm)
        pathCounting(predecessors_sigma_distance, v, neighnode)
      })
    }
    
    backPropagation(nodelink, predecessors_sigma_distance, partial_betweenness, stack)
    //calculate for this source the sigma and normalized distance to each bordernode
    val sigma_normalized_distance_to_borders = sigmaDistanceToBordernodes(node, bordernodes, predecessors_sigma_distance, comm, file_prefix, log)
    (nodelink.head, partial_betweenness, sigma_normalized_distance_to_borders.toArray.deep.hashCode)
  }
  
    def modifiedBetwennessSingleSourceCentrality2CFastBC(node: Int, bordernodes: scala.collection.immutable.Map[Int,Boolean], comm: Int, version:String
      , file_prefix:String, log:Boolean, file: PrintWriter) = {
    val nodelink = clusters_broadcasted.value(comm)(node)
    
    val (predecessors_sigma_distance, partial_betweenness, stack, queue) = initialization(nodelink)
    while (!queue.isEmpty) {
      val v = queue.dequeue
      stack.push(v.head)
      
      //neighbor of extracted node evaluation
      v.tail.foreach(neighnode => {
        clusterPathDiscovery(predecessors_sigma_distance, queue, v, neighnode, comm)
        pathCounting(predecessors_sigma_distance, v, neighnode)
      })
    }
    
    backPropagation(nodelink, predecessors_sigma_distance, partial_betweenness, stack)
    //calculate for this source the sigma and normalized distance to each bordernode
    val sigma_normalized_distance_to_borders = sigmaDistanceToBordernodes(node, bordernodes, predecessors_sigma_distance, comm, file_prefix, log)
    (nodelink.head, partial_betweenness, sigma_normalized_distance_to_borders)
  }
  
    
    
  //modified version of Brandes algorithm for distance and sigma calculation only to border nodes
  def modifiedBetwennessSingleSourceCentrality1CFastBCExact(node: Int, bordernodes: scala.collection.immutable.Map[Int,Boolean]
                                                          , community: Int, version:String, file_prefix:String, log:Boolean) = {
    /***
     * GOAL: calculate partial betweenness and sigma + distance for a node to border nodes of its community
     * INPUT:
     * node
     * bordernodes: map with node as key and boolean, true if border nodes in the community
     * community: community of the node
     * 
     * OUTPUT: node, partial betweenness, sigma normalized and distance to border nodes, predecessor_sigma_distance in the community
     */
    //links of the node inside the community it belongs to
    val nodelink = clusters_broadcasted.value(community)(node)
    //Brandes algorithm for the node inside its cluster
    val (predecessors_sigma_distance, partial_betweenness, stack, queue) = initialization(nodelink)
    while (!queue.isEmpty) {
      val v = queue.dequeue
      stack.push(v.head)
      //neighbor of extracted node evaluation
      v.tail.foreach(neighnode => {
        clusterPathDiscovery(predecessors_sigma_distance, queue, v, neighnode, community)
        pathCounting(predecessors_sigma_distance, v, neighnode)
      })
    }
    (nodelink.head, predecessors_sigma_distance)
  }
  
  
  def findShortestPathsToBorderNodes(node: Int, bordernodes: scala.collection.immutable.Map[Int,Boolean]
                                                          , community: Int, log:Boolean){
    
    val nodelink = clusters_broadcasted.value(community)(node)
    val (predecessors_sigma_distance, partial_betweenness, stack, queue) = initialization(nodelink)
    var borderNodeCount = 0
    
    while (borderNodeCount < bordernodes.size){
      while (!queue.isEmpty){
        val v = queue.dequeue
        v.tail.foreach(neighnode => {
          clusterPathDiscovery(predecessors_sigma_distance, queue, v, neighnode, community)
          pathCounting(predecessors_sigma_distance, v, neighnode)
        }
       )
      }
    }
    val file_prefix = "" 
    val sigma_normalized_distance_to_borders = sigmaDistanceToBordernodes(node, bordernodes, predecessors_sigma_distance, community
                                                                        , file_prefix, log)
    
    
  }
  
  def printStats(file_prefix: String, bordernodes: scala.collection.immutable.Map[Int,Boolean], comm:Int, the_node:Int, sigma_normalized_distance_to_borders:scala.collection.Iterable[(Int, Double, Double)]) ={
    val file = new PrintWriter(new FileWriter(new File(file_prefix, "sigma_normalized_distances_for_louvain.csv"), true))
    file.write("Comm:" + comm + ",bordernodes:" + bordernodes.keys.mkString(",") + "\n" + 
        the_node + ":" + sigma_normalized_distance_to_borders.toArray.mkString(",") + "\n")
    file.close()
  }
  
  def sigmaDistanceToBordernodes(the_node: Int, bordernodes: scala.collection.immutable.Map[Int,Boolean]
                                , predecessors_sigma_distance: scala.collection.mutable.Map[Int,(Array[Int], Double, Double)]
                                , community: Int
                                , file_prefix:String, log:Boolean) = {
    val pred_sigma_distance_to_borders = predecessors_sigma_distance.filterKeys(node=>bordernodes.contains(node))
    val min_distance=pred_sigma_distance_to_borders.minBy(_._2._3)._2._3

    val sigma_normalized_distance_to_borders = pred_sigma_distance_to_borders.map(bnode => (bnode._1, 
                                                                          pred_sigma_distance_to_borders(bnode._1)._2,
                                                                          pred_sigma_distance_to_borders(bnode._1)._3 - min_distance))
    if(log){
      printStats(file_prefix, bordernodes, community, the_node, sigma_normalized_distance_to_borders)
    }
    sigma_normalized_distance_to_borders
  }
}