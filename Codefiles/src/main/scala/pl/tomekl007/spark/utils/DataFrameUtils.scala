package pl.tomekl007.spark.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * Created by tomasz.lelek on 18/03/16.
  */
object DataFrameUtils {
  def dataFrameToVect(df: DataFrame): RDD[org.apache.spark.mllib.linalg.Vector] = {
    df.rdd
      .map(row => row.getAs[org.apache.spark.mllib.linalg.Vector]("result"))
  }

}
