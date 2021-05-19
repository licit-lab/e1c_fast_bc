

package unisannio.utils

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

/**
 * Spark Context Factory
 * This class return an instance of
 * spark context (SQL or SparkContext)
 */
object SparkContextFactory {
  
  /**
   * Get spark context
   * @param config
   */
  def getSparkContext(config: SparkConf): SparkContext = {
    SparkSession.builder().config(config).getOrCreate().sparkContext
  }
  
  /**
   * Get SQL context
   * @param
   */
  def getSqlContext(config: SparkConf): SQLContext = {
    return new SQLContext(getSparkContext(config))
  }
  
  /**
   * Get SQL context from existing spark context
   * @param sc
   */
  def getSqlContext(sc: SparkContext): SQLContext = {
    return new SQLContext(sc)
  }
}