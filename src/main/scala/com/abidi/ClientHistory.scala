package com.abidi

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.BooleanType

object ClientHistory {
  def updateClientsStatus(clientsHistory: DataFrame, updatedClientsInfo: DataFrame): DataFrame = {

    val updateEndDate = updatedClientsInfo
      .select(
        col("name"),
        col("surname"),
        col("startDate").alias("endDate")
      )

    val closedOldClient = clientsHistory
      .join(updateEndDate, Seq("name", "surname"), "inner")
      .withColumn("isEffective", lit(false).cast(BooleanType))

    val clientsHistoryList = clientsHistory
      .select(
        col("name"),
        col("surname")
      )

    val newAddressClient = updatedClientsInfo
      .join(clientsHistoryList, Seq("name", "surname"), "left")
      .withColumn("endDate", lit(null))
      .withColumn("isEffective", lit(true).cast(BooleanType))

    val clientAddressUpdate = closedOldClient
      .union(newAddressClient)

    val clientsUpdateList = updatedClientsInfo
      .select(
        col("name"),
        col("surname")
      )

    val activeClients = clientsHistory
      .join(clientsUpdateList, Seq("name", "surname"), "left_anti")
      .withColumn("endDate", lit(null))
      .withColumn("isEffective", lit(true).cast(BooleanType))

    val updatedClientsStatus = clientAddressUpdate
      .union(activeClients)

    updatedClientsStatus
  }
}