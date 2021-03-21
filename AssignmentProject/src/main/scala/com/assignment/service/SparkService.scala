package com.assignment.service

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author Anupam.Patel
 *
 */
object SparkService {

  val _logger: Logger = Logger.getLogger(this.getClass.getName)

  private var _SPARK_SESSION:SparkSession = null
 
  def getSparkSession:SparkSession ={
    if(_SPARK_SESSION == null){
      createSparkSession(getSparkConf(Map()))
    }
    return  _SPARK_SESSION
  }

  def getSparkConf(conf:Map[String,String]):SparkConf = {
    var sparkConf = new SparkConf
    conf.foreach(entry => sparkConf.set(entry._1,entry._2))
    //sparkConf.set("spark.sql.warehouse.dir","Your Path")
    return sparkConf
  }

  def createSparkSession(sparkConf: SparkConf):SparkSession = {
    _SPARK_SESSION =  SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    return _SPARK_SESSION
  }


}
