package com.abidi

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object ClientHistory {
  def updateClientsStatus(clientsHistory: DataFrame, updatedClientsInfo: DataFrame): DataFrame = {

    val updatedClientsInfoDeduplicate = updatedClientsInfo.dropDuplicates()

    val existingInput = clientsHistory
      .join(updatedClientsInfo, Seq("name","surname","address","startDate"),"inner")

    val updatedClientsInfoDeduplicated = updatedClientsInfoDeduplicate
      .join(existingInput, Seq("name","surname","address","startDate"), "left_anti")

    val clientsHistoryWithUpdateClientSchema = clientsHistory.drop("endDate","isEffective")

    val closedOldClient = clientsHistoryWithUpdateClientSchema
      .union(updatedClientsInfoDeduplicated)

    val windowSpec = Window.partitionBy("name","surname").orderBy("startDate")

    val clientsUpdatedHistory = closedOldClient
      .withColumn("endDate", lead("startDate",1).over(windowSpec))
      .withColumn("isEffective", when(col("endDate") =!= "null", false).otherwise(true))

    clientsUpdatedHistory
  }
}