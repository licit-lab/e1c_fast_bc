package unisannio.utils

import scala.io.Source
import java.io.PrintWriter

object Utils {
  def read_node_to_cluster(filename: String, separator: String, number_of_nodes: Int) = {
    val node_to_cluster = new Array[Int](number_of_nodes)
    for (line <- Source.fromFile(filename).getLines) {
      val linesplitted = line.split(separator)
      node_to_cluster(linesplitted(0).toInt) = linesplitted(1).toInt
    }
    node_to_cluster
  }
  
  def print_node_to_cluster(pw: PrintWriter, node_to_cluster: Array[Int]) = {
    //pw.println("node_id, cluster_id")
    0.to(node_to_cluster.length-1).foreach(node => {
      pw.println(node + "," + node_to_cluster(node))
    })
    pw.close()
  }
  
  def print_bordernodes(pw: PrintWriter, bordernodes_set: scala.collection.Set[Int]) = {
    //pw.println("bordernode_id")
    bordernodes_set.foreach(bordernode => pw.println(bordernode))
    pw.close()
  }
  
  def print_hsn_nodes(pw: PrintWriter, hsn_nodes_set: scala.collection.Set[Int]) = {
    //pw.println("hsn_node_id")
    hsn_nodes_set.foreach(bordernode => pw.println(bordernode))
    pw.close()
  }
  
  def print_node_to_cluster_updated(pw: PrintWriter, node_to_cluster_updated: Array[Array[Boolean]]) = {
    //pw.println("node_id, cluster_ids")
    val number_of_nodes = node_to_cluster_updated(0).length
    0.to(number_of_nodes-1).foreach(node => {
      pw.print(node + ",")
      0.to(node_to_cluster_updated.length-1).filter(node_to_cluster_updated(_)(node)).foreach(elem => {
        pw.print(elem + ",")
      })
      pw.println()
    })
    pw.close()
  }
  
  def print_node_to_cluster_updated(pw: PrintWriter, node_to_cluster_updated: Array[scala.collection.mutable.Set[Int]]) = {
    //pw.println("node_id, cluster_ids")
    0.to(node_to_cluster_updated.length-1).foreach(node => {
      pw.print(node + ",")
      node_to_cluster_updated(node).foreach(cluster => { pw.print(cluster + ",")})
      pw.println()
    })
    pw.close()
  }
  
  def print_backpropagation_map(pw: PrintWriter, backpropagation_map: scala.collection.mutable.Map[Int, scala.collection.mutable.Map[Int, (Long, Double)]]) = {
    backpropagation_map.foreach(bnode_s => {
      bnode_s._2.foreach(bnode_d => {
        pw.println(bnode_s._1 + "," + bnode_d._1 + "," + bnode_d._2._1 + "," + bnode_d._2._2)
      })
    })
    pw.close()
  }
  
  def print_pivots_cardinalities(pw: PrintWriter, pivots_cardinalities: Array[(Int, Int)]) = {
    //pw.println("pivot_id, cardinality")
    pivots_cardinalities.foreach(pivot_cardinality => {
      pw.println(pivot_cardinality._1 + "," + pivot_cardinality._2)
    })
    pw.close()
  }
  
  def print_classes(pw: PrintWriter, classes: Array[Iterable[Int]]) = {
    classes.foreach(eq_class => {
      eq_class.foreach(node => { pw.print(node + ",")})
      pw.println()
    })
    pw.close()
  }
  
  def print_bc(pw: PrintWriter, bc: Array[Double], divide: Boolean) = {
    //pw.println("node_id, bc")
    0.to(bc.length-1).foreach(node => {
      pw.println(node + "," + (if(divide) (bc(node)/2.0) else bc(node)))
    })
    pw.close()
  }
  
  def print_bc_v2(pw: PrintWriter, bc: Array[(Int, Double)], divide: Boolean) = {
    //pw.println("node_id, bc")
    bc.foreach(node => {
      pw.println(node._1 + "," + (if(divide) (node._2/2.0) else node._2))
    })
    pw.close()
  }
  
  def print_full_local_stats(pw: PrintWriter, full_local_stats: Array[(Int, scala.collection.mutable.Map[Int, Double], Int)]) = {
    full_local_stats.foreach(node_bc_hash => {
      pw.print(node_bc_hash._1 + ",")
      node_bc_hash._2.foreach(node_bc => {
        pw.print(node_bc._1 + "," + node_bc._2 + ",")
      })
      pw.println(node_bc_hash._3)
    })
    pw.close()
  }
  
  def print_local_stats(pw: PrintWriter, local_stats: Array[(Int, scala.collection.mutable.Map[Int, Array[Int]])]) = {
    local_stats.foreach(local_stat => {
    	local_stat._2.foreach(dest_stats => {
      	pw.print(local_stat._1 + "," + dest_stats._1 + ",")
        dest_stats._2.foreach(predecessor => { pw.print(predecessor + ",")})
        pw.println()
      })
    })
    pw.close()
  }
  
  def print_hsn_stats(pw: PrintWriter, hsn_stats: Array[(Int, scala.collection.mutable.Map[Int, (Array[Int], Double)])]) = {
    hsn_stats.foreach(hsn_stat => {
      hsn_stat._2.foreach(dest_stats => {
        pw.print(hsn_stat._1 + "," + dest_stats._1 + ",")
        dest_stats._2._1.foreach(predecessor => { pw.print(predecessor + ",")})
        pw.println(dest_stats._2._2)
      })
    })
    pw.close()
  }
  
  def print_stats(pw: PrintWriter, stats: Array[(Int, scala.collection.mutable.Map[Int, (Array[Int], Int, Double)])]) = {
    stats.foreach(stat => {
      stat._2.foreach(dest_stats => {
        pw.print(stat._1 + "," + dest_stats._1 + ",")
        dest_stats._2._1.foreach(predecessor => { pw.print(predecessor + ",")})
        pw.println(dest_stats._2._2 + "," + dest_stats._2._3)
      })
    })
    pw.close()
  }
  
  def print_stats2(pw: PrintWriter, stats: Array[(Int, Iterable[(Int, Double, Double)])]) = {
    stats.foreach(stat => {
      pw.print(stat._1 + ",")
      stat._2.foreach(bnode_nsigma_ndist => {
        pw.print(bnode_nsigma_ndist._1 + "," + bnode_nsigma_ndist._2 + "," + bnode_nsigma_ndist._3 + ",")
      })
      pw.println()
    })
    pw.close()
  }  
}