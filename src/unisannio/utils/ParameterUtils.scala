

package unisannio.utils

class ParameterUtils(master : String, path: String,total_num_executor : Int, stat_file: String, mapper : Int
    , log : Boolean, profiling: Boolean, louvainEps : Double, filePrexif : String) extends Serializable {
      
  def get_master : String = master
  def get_path : String = path
  def get_mapper : Int = mapper
  def get_log : Boolean = log
  def get_louvainEps : Double = louvainEps
  def get_file_prefix : String = filePrexif
  def get_stat_file: String = stat_file
  def get_num_exec : Int = total_num_executor
  def get_profiling: Boolean = profiling
}