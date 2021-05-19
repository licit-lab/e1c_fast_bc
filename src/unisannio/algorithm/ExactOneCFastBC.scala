package unisannio.algorithm

import unisannio.utils._
import org.apache.spark.rdd.RDD
import java.io.File
import java.util.Date
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.io.PrintWriter
import java.io.FileWriter
import java.io.File
import org.apache.spark.SparkConf
import org.apache.log4j.{ Level, Logger }
import java.nio.file.Files
import org.apache.log4j.BasicConfigurator
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
//import org.apache.spark.mllib.clustering.MCL
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.clustering._
import org.apache.spark.sql.SparkSession
//import org.apache.spark
import org.apache.spark.ml.feature.{OneHotEncoder, CountVectorizer}
import org.apache.spark.ml.feature
import scala.io.Source
import org.apache.spark.SparkEnv


object ExactOneCFastBC {
  var stats_map = scala.collection.mutable.Map[(Date, String, String, Int, Int), scala.collection.mutable.Map[Int, (String, Long)]]()

  /*
   * ONECFASTBC ALGORITHM
   */
  def main(args: Array[String]) {
    if (args.length != 9) {
      System.err.println("Usage: <master> <input_file> <logging> <stats_dir> <stats_file> <n_executors> <profiling> <n_mapper> <louvain_epsilon>")
      System.exit(1)
    }
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    
    val parameterUtils = new ParameterUtils(args(0), args(1),  args(5).toInt, args(4), args(7).toInt, args(2).toBoolean, args(6).toBoolean, args(8).toDouble, args(3))
    
    val conf = {
      if (args(0).contains("local")){
        new SparkConf()
          .setMaster(args(0))
         .setAppName("BetweennessCentralityApp")
      }
      else 
        new SparkConf()
    }
    
    val sc = SparkContextFactory.getSparkContext(conf)
    
    
    val start = System.currentTimeMillis
    
    var date = new Date(start)
    val profiling = parameterUtils.get_profiling
    val directory = new File(parameterUtils.get_file_prefix)
    if (!directory.exists())
      directory.mkdirs()
    
    val stats_key = (date, parameterUtils.get_path, parameterUtils.get_master, parameterUtils.get_num_exec, parameterUtils.get_mapper)   
    var step_id = 0
   
    
    //READING DATASET AND BROADCAST
    val broadcast_start = System.currentTimeMillis
    
    val graph = read_dataset(sc, parameterUtils.get_path, parameterUtils.get_num_exec)    
    val dataset_b = sc.broadcast(graph)   
    
    val broadcast_end = System.currentTimeMillis
    
    if (profiling)
      stats_map += stats_key -> scala.collection.mutable.Map(step_id -> ("Graph Broadcast", broadcast_end - broadcast_start))
   
    
    //LOUVAIN CLUSTERING 
    val louvain_start = System.currentTimeMillis
    
    val (node_to_cluster, number_of_clusters) = LouvainMethod.louvainExecution(sc, dataset_b, parameterUtils.get_mapper, parameterUtils.get_louvainEps, parameterUtils.get_num_exec)
    val node_to_cluster_b = sc.broadcast(node_to_cluster)
    
    val louvain_finish = System.currentTimeMillis
    
    if (profiling){
      Utils.print_node_to_cluster(new PrintWriter(new FileWriter(new File(parameterUtils.get_file_prefix, "node_to_cluster.csv"), false)), node_to_cluster)
      
      step_id += 1
      stats_map(stats_key) += step_id -> ("louvain",louvain_finish - louvain_start)
      println("*** Performing Louvain clustering took " + (louvain_finish - louvain_start) / 1000.0 + " s.")
    } 

     
    //IDENTIFYING BORDER NODES
    val bnodes_start = System.currentTimeMillis
    
    val bordernodes_set = sc.parallelize(0.to(node_to_cluster.length-1), parameterUtils.get_num_exec).map(node => TaskFunctions.is_bordernode_task(node, dataset_b, node_to_cluster_b)).filter(_._1).map(_._2).collect().toSet
    val bordernodes_set_b = sc.broadcast(bordernodes_set)
    
    val bnodes_end = System.currentTimeMillis
    
    println("Number of border nodes: " + bordernodes_set.size + ".")
    
    if (profiling){
      Utils.print_bordernodes(new PrintWriter(new FileWriter(new File(parameterUtils.get_file_prefix, "bordernodes.csv"), false)), bordernodes_set)
      
      step_id +=1
      stats_map(stats_key) += step_id -> ("Border Nodes Identification",bnodes_end - bnodes_start)
      println("*** Searching for border nodes over " + number_of_clusters + " clusters took " 
        + (bnodes_end - bnodes_start) / 1000.0 + " s.\n*** Number of border nodes: " + bordernodes_set.size + ".")
    }
    
     
    //COMPUTING LOCAL BFS FOR EACH BORDER NODE 
    val local_bfs_start = System.currentTimeMillis
    
    val local_stats_rdd = sc.parallelize(bordernodes_set.toSeq, parameterUtils.get_num_exec).map(border_node => TaskFunctions.compute_bfs_task(border_node, dataset_b, node_to_cluster_b, bordernodes_set_b))
    
    if (profiling){
      //local_stats_rdd is cached because it's also used later (see "FINDING HSN NODES" section).
      local_stats_rdd.cache()
      val local_stats = local_stats_rdd.collect
      
      val local_bfs_end = System.currentTimeMillis
      
      Utils.print_local_stats(new PrintWriter(new FileWriter(new File(parameterUtils.get_file_prefix, "local_stats.csv"), false)), local_stats)
      
      step_id += 1
      stats_map(stats_key) += step_id -> ("local BC (border nodes)", local_bfs_end - local_bfs_start)
      println("*** Computing local bfs took " + (local_bfs_end - local_bfs_start) / 1000.0 + " s.")
    }
    
    
    //FINDING HSN NODES
    val hsn_start = System.currentTimeMillis
    
    val hsn_nodes_set = local_stats_rdd.map(bordernodesource_dest_stats => TaskFunctions.find_hsn_nodes_task(bordernodes_set_b, bordernodesource_dest_stats)).reduce(_++_).toSet
    
    val hsn_end = System.currentTimeMillis

    if (profiling){
      Utils.print_hsn_nodes(new PrintWriter(new FileWriter(new File(parameterUtils.get_file_prefix, "hsn.csv"), false)), hsn_nodes_set)
      
      step_id += 1
      stats_map(stats_key) += step_id -> ("HSN Building",hsn_end - hsn_start)
      //println("HSN Building: "+(hsn_end - hsn_start))
      println("*** Finding HSN nodes took " + (hsn_end - hsn_start) / 1000.0 + " s.")
      
      //unpersist the local_stats_rdd cached before (see "COMPUTING LOCAL BFS FOR EACH BORDER NODE" section).
      local_stats_rdd.unpersist(false)
    }
    
    
    //COMPUTING BFS OVER THE HSN FOR EACH BORDER NODE
    val hsn_bfs_start = System.currentTimeMillis
       
    val hsn_stats_rdd = sc.parallelize(bordernodes_set.toSeq, parameterUtils.get_num_exec).map(border_node => TaskFunctions.compute_bfs_with_distance_task(border_node, dataset_b, hsn_nodes_set, bordernodes_set_b))
      
    if (profiling){
      //hsn_stats_rdd is cached because it's also used later (see "UPDATING CLUSTERS AND BUILDING BACKPROPAGATION MAP" section).
      hsn_stats_rdd.cache()
      val hsn_stats = hsn_stats_rdd.collect
      
      val hsn_bfs_end = System.currentTimeMillis
      
      Utils.print_hsn_stats(new PrintWriter(new FileWriter(new File(parameterUtils.get_file_prefix, "hsn_stats.csv"), false)), hsn_stats)
      
      step_id += 1
      stats_map(stats_key) += step_id -> ("HSN Stats",hsn_bfs_end - hsn_bfs_start)
      println("*** Computing bfs over the HSN took " + (hsn_bfs_end - hsn_bfs_start) / 1000.0 + " s.")
    }
    
    
    //UPDATING CLUSTERS AND BUILDING BACKPROPAGATION MAP
    val clusters_update_start = System.currentTimeMillis

    val (node_to_cluster_updated_array, backpropagation_map) = hsn_stats_rdd.map(bnodesource_dest_stats => TaskFunctions.update_cluster_and_compute_backpropagation_map_task(node_to_cluster_b, bordernodes_set_b, bnodesource_dest_stats)).reduce((tuple1, tuple2) => {
      val new_node_to_cluster_updated = scala.collection.mutable.ArrayBuffer[(Int, Array[Int])]()
      val new_bn_prevbn_sp_dist = scala.collection.mutable.Map[Int, scala.collection.mutable.Map[Int, (Long, Double)]]()
      
      val tmpArray = (tuple1._1.toSeq ++ tuple2._1.toSeq).groupBy(_._1).mapValues(_.map(_._2)).toArray
      tmpArray.foreach(elem => {
        new_node_to_cluster_updated += ((elem._1, elem._2.reduce(_ ++ _).distinct))
      })
      
      new_bn_prevbn_sp_dist ++= (tuple1._2.toSeq ++ tuple2._2.toSeq)      
      
      (new_node_to_cluster_updated.toArray, new_bn_prevbn_sp_dist)
    })
    

    val node_to_cluster_updated_matrix = Array.fill[Boolean](number_of_clusters, node_to_cluster.length)(false)
    0.to(node_to_cluster.length-1).foreach(node => {
      node_to_cluster_updated_matrix(node_to_cluster(node))(node) = true
    })
    
    node_to_cluster_updated_array.foreach(node_clusters => {
      node_clusters._2.foreach(cluster => {
        node_to_cluster_updated_matrix(cluster)(node_clusters._1) = true
      })
    })
    
    val clusters_update_end = System.currentTimeMillis
    
    if (profiling){
      Utils.print_backpropagation_map(new PrintWriter(new FileWriter(new File(parameterUtils.get_file_prefix, "backpropagation_map.csv"), false)), backpropagation_map)
      Utils.print_node_to_cluster_updated(new PrintWriter(new FileWriter(new File(parameterUtils.get_file_prefix, "node_to_cluster_updated.csv"), false)), node_to_cluster_updated_matrix)
    
      step_id += 1
      stats_map(stats_key) +=  step_id -> ("Cluster Modification", clusters_update_end - clusters_update_start)
      println("*** Updating clusters and building backpropagation map took " + (clusters_update_end - clusters_update_start) / 1000.0 + " s.")
      
      //unpersist hsn_stats_rdd cached before (see "COMPUTING BFS OVER THE HSN FOR EACH BORDER NODE" section).
      hsn_stats_rdd.unpersist(false)    
    }
    
    val number_ext_nodes = node_to_cluster_updated_array.length
    
    println("Number of external nodes: " + number_ext_nodes + ".")
    
    //COMPUTING LOCAL BC (AND PART OF THE GLOBAL BC) AND IDENTIFYING CLASSES
    val local_bc_start = System.currentTimeMillis
     
    val full_local_stats_rdd = sc.parallelize(0.to(node_to_cluster.length-1), parameterUtils.get_num_exec).map(node => TaskFunctions.compute_local_stats_task(node, dataset_b, node_to_cluster_b, bordernodes_set_b, node_to_cluster_updated_matrix))
    
    //full_local_stats_rdd is cached because it's also used later (see "SELECTING PIVOTS" section).
    full_local_stats_rdd.cache()
    
    
    val class_nodes_rdd = full_local_stats_rdd.map(node_partial_bc_hash => (node_partial_bc_hash._1, node_partial_bc_hash._3)).groupBy(_._2).map(class_nodes => class_nodes._2.map(node_class => node_class._1))
    val local_betweenness_rdd = full_local_stats_rdd.flatMap(node_partial_bc_hash => node_partial_bc_hash._2).reduceByKey(_ + _)
    val local_bc = local_betweenness_rdd.collect
    
    val local_bc_end = System.currentTimeMillis
    
    bordernodes_set_b.unpersist(false)
      
    if (profiling){
      //class_nodes_rdd is cached because it's also used later (see "SELECTING PIVOTS" section).
      class_nodes_rdd.cache()
      val class_nodes = class_nodes_rdd.collect()
      
      val class_identification_end = System.currentTimeMillis
      
      Utils.print_bc_v2(new PrintWriter(new FileWriter(new File(parameterUtils.get_file_prefix, "local_bc.csv"), false)), local_bc, false)
      Utils.print_classes(new PrintWriter(new FileWriter(new File(parameterUtils.get_file_prefix, "classes.csv"), false)), class_nodes)
      
      step_id += 1
      stats_map(stats_key) +=  step_id -> ("New Local BC computation", local_bc_end - local_bc_start)
      
      println("*** Computing local BC and statistics for class identification took " + (local_bc_end - local_bc_start) / 1000.0 + " s.")
      println("*** Identifying classes took " + (local_bc_end - class_identification_end) / 1000.0 + " s.")
      
      //unpersist full_local_stats_rdd cached before (see "COMPUTING LOCAL BC (AND PART OF THE GLOBAL BC) AND IDENTIFYING CLASSES" section).
      full_local_stats_rdd.unpersist(false)
    }
    
    
    //SELECTING PIVOTS
    val pivots_start = System.currentTimeMillis
    
    val pivots_cardinalities_rdd = class_nodes_rdd.map(classes => TaskFunctions.select_source_first_task(classes.toArray)) 
    
    //pivots_cardinalities_rdd is cached because it's also used later (see "COMPUTING GLOBAL BC" section).
    pivots_cardinalities_rdd.cache()
    
    val pivots_cardinalities = pivots_cardinalities_rdd.collect
    
    val pivots_end = System.currentTimeMillis
    
    println("Number of pivots: " + pivots_cardinalities.length + ".")
    
    if (profiling){
      Utils.print_pivots_cardinalities(new PrintWriter(new FileWriter(new File(parameterUtils.get_file_prefix, "pivots.csv"), false)), pivots_cardinalities)
      
      step_id += 1
      stats_map(stats_key) +=  step_id -> ("Sources Selection", pivots_end - pivots_start)
      println("*** Selecting pivots took " + (pivots_end - pivots_start) / 1000.0 + " s.\n*** Number of pivots: " + pivots_cardinalities.length + ".")
      
      //unpersist class_nodes_rdd cached before (see "COMPUTING LOCAL BC (AND PART OF THE GLOBAL BC) AND IDENTIFYING CLASSES" section).
      class_nodes_rdd.unpersist(false)
    } else {
      //unpersist full_local_stats_rdd cached before (see "COMPUTING LOCAL BC (AND PART OF THE GLOBAL BC) AND IDENTIFYING CLASSES" section).
      full_local_stats_rdd.unpersist(false)
    }
       
    //COMPUTING GLOBAL BC
    val global_bc_start = System.currentTimeMillis
  
    val partial_global_bc_rdd = pivots_cardinalities_rdd.map(pivot_cardinality => TaskFunctions.compute_global_bc_task(pivot_cardinality, dataset_b, backpropagation_map, node_to_cluster_b))    
    //val partial_global_bc_rdd = pivots_cardinalities_rdd.map(pivot_cardinality => TaskFunctions.compute_global_bc_task2(pivot_cardinality, dataset_b, backpropagation_map, node_to_cluster_b, number_of_clusters))
    
    val global_betweenness_rdd = partial_global_bc_rdd.flatMap(node_partial_bc => node_partial_bc._2).reduceByKey(_ + _)
    val global_bc = global_betweenness_rdd.collect
    
    val global_bc_end = System.currentTimeMillis
    
    //unpersist pivots_cardinalities_rdd cached before (see "SELECTING PIVOTS" section).
    pivots_cardinalities_rdd.unpersist(false)
    
    node_to_cluster_b.unpersist(false)
    dataset_b.unpersist(false)
    
    if (profiling){
      Utils.print_bc_v2(new PrintWriter(new FileWriter(new File(parameterUtils.get_file_prefix, "global_bc.csv"), false)), global_bc, false)
      
      step_id += 1
      stats_map(stats_key) +=  step_id -> ("Brandes No Local", global_bc_end - global_bc_start)
      println("*** Computing global BC took " + (global_bc_end - global_bc_start) / 1000.0 + " s.")
    } 
    
    
    //SUMMING UP ALL CONTRIBUTIONS (COMPUTING FINAL BC)
    val final_bc_start = System.currentTimeMillis
    
    val final_bc = new Array[Double](node_to_cluster.length)
    local_bc.foreach(node_bc => { final_bc(node_bc._1) = node_bc._2 })
    global_bc.foreach(node_bc => { final_bc(node_bc._1) += node_bc._2 })
     
    val final_bc_end = System.currentTimeMillis
    
    val end = System.currentTimeMillis
    
    println("*** Computing exact BC took " + (end - start) / 1000.0 + " s.")
    
    if (profiling){
      step_id += 1
      stats_map(stats_key) +=  step_id -> ("Final Brandes Reduce", final_bc_end - final_bc_start)
      println("\n*** Computing final BC (summing all contributions) took " + (final_bc_end - final_bc_start) / 1000.0 + " s.")
      stats_map(stats_key) +=  -1 -> ("Total Computation Time", end - start)
    }
    else stats_map += stats_key -> scala.collection.mutable.Map(-1 -> ("Total Computation Time", end - start))
    
    var stats_str = "[pivots:"+pivots_cardinalities.length+"_clusters:"+number_of_clusters+"_bnodes:"+bordernodes_set.size+"_extnodes:" + number_ext_nodes + "]"
    stats_map(stats_key) += -2 -> (stats_str, 0.toLong)
    
    Utils.print_bc(new PrintWriter(new FileWriter(new File(parameterUtils.get_file_prefix, "final_bc.csv"), false)), final_bc, true)

    val file_stats = new PrintWriter(new FileWriter(new File(parameterUtils.get_file_prefix, "other_stats.csv"), false))
    file_stats.write("start_time," + date + "\n") 
    file_stats.write("num_pivots," + pivots_cardinalities.length + "\n")
    file_stats.write("num_clusters," + number_of_clusters + "\n")
    file_stats.write("num_border_nodes," + bordernodes_set.size + "\n")
    file_stats.write("num_external_nodes," + number_ext_nodes + "\n")
    file_stats.write("hsn_size," + hsn_nodes_set.size + "\n")
    file_stats.close
    
    sc.stop()

  }
  
  def getStatsMap() = stats_map

  def read_dataset(sc: org.apache.spark.SparkContext, dataset_path: String, outputPartitions: Int) = {
    val node_neighbors_rdd = sc.textFile(dataset_path, outputPartitions)
    val graph_rdd = node_neighbors_rdd.map(line => line.split(";").map(_.toInt))
    
    val number_of_nodes = Source.fromFile(dataset_path).getLines.length
    
    val graph = new Array[Array[Int]](number_of_nodes)
    
    graph_rdd.collect().foreach(adjacency_list => { graph(adjacency_list.head) = adjacency_list })
    
    graph
  }
    
  /*def compute_bfs_wrapper_task(bordernodes: Array[Int], cluster: scala.collection.Map[Int, Array[Int]]) = {  
    bordernodes.map(compute_bfs_task(_, cluster, bordernodes))
  }*/
}