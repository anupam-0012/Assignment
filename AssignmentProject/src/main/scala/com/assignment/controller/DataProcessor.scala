package com.assignment

import org.apache.log4j.Logger
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import com.assignment.service.SparkService
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import com.amazonaws.services.glue.GlueContext

/**
 * @author Anupam.Patel
 *
 */
object DataProcessor {
	val _logger: Logger = Logger.getLogger(this.getClass.getName)
			var _debugString: String = ""

			def main(args: Array[String]): Unit = {
			    
	         var inputPath: String = ""
	         var outputPath: String = ""
	        
					if (args == null || args.length == 0) {
						_logger.error(" No arguments found. Please pass InputFolder  and OutPutFolder Path")
						System.exit(1)
					} else {
						inputPath = args(0)
						outputPath = args(1)
						
						_logger.info(s"Arguments received $inputPath and $outputPath")
					}

					var tranSoucePath = inputPath+"/transaction";
					var vendorSoucePath = inputPath+"/vendor";
					var vendorTypeSoucePath = inputPath+"/vendor_type";
					var augTargetPath = inputPath+"/augment_result";
					var geoupByTargetPath = inputPath+"/groupby_result";
					
					//Code below is required when to run this on AWS Glue
					/*val sparkCont: SparkContext = new SparkContext()
          val glueContext: GlueContext = new GlueContext(sparkCont)
          val sparkSession: SparkSession = glueContext.getSparkSession*/
					
					_logger.info("Starting spark application")
					val sparkConf = SparkService.getSparkConf(Map())

					sparkConf.setAppName("DataProcessor Assignment App")

					_logger.info("Starting spark session")
					val spark = SparkService.createSparkSession(sparkConf)

					//reading transaction data source
					val transDF = spark.read.parquet(tranSoucePath)

					//reading vendor data source
					val vendorDF = spark.read.parquet(vendorSoucePath)

					//reading vendor type data source
					val venTypeDF = spark.read.parquet(vendorTypeSoucePath)

					//Joinning data sources for augmented result
					val resultDF = transDF.join(vendorDF,transDF("vendor_id") ===  vendorDF("vendor_id"),"leftouter").join(venTypeDF,vendorDF("vendor_type_code") === venTypeDF("vendor_type_code"),"leftouter").select(transDF("tx_id"),transDF("vendor_id"),vendorDF("vendor_name"),vendorDF("vendor_type_code"),venTypeDF("name").as("vendor_type_name"),transDF("amount"))

					//Dropping vendor_type_code as it's not required on output file
					val augDF = resultDF.drop("vendor_type_code")

					//Storing Augment result as parequest data source. Ask is to write the output in single fiel that's why coalesec used
					augDF.coalesce(1).write.mode(SaveMode.Overwrite).parquet(augTargetPath)

					// Group by vendor_type_code   
					val groupDf=resultDF.groupBy("vendor_type_code").agg(sum("amount").as("amount"))

					//Making Join with Vendor Data Source for the Second type of output
					val finalGroupDf=groupDf.join(venTypeDF,groupDf("vendor_type_code") ===  venTypeDF("vendor_type_code"),"leftouter").select(groupDf("amount"),venTypeDF("name").as("venor_type_name"),venTypeDF("description"))

					finalGroupDf.printSchema()

					//writing the group by result to parequet. Ask is to write the output in single fiel that's why coalesec used
					finalGroupDf.coalesce(1).write.mode(SaveMode.Overwrite).parquet(geoupByTargetPath)

					_logger.info("Execution of spark app completed")

					spark.close
					spark.stop()

	}

}