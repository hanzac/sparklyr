package sparklyr

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object WorkerUtils {

  def buildStructTypeForLongField(): StructType = {
    val fields = Array(StructField("id", LongType, false))
    StructType(fields)
  }

  def mapRddLongToRddRow(rdd: RDD[Long]): RDD[Row] = {
    rdd.map(x => org.apache.spark.sql.Row(x))
  }

}
