package com.abidi

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.BooleanType

object ClientHistory {
  def updateClientsStatus(clientsHistory: DataFrame, updatedClientsInfo: DataFrame): DataFrame = {

    val updatedClientsInfoDropDup = updatedClientsInfo.dropDuplicates()

    val existingInput = clientsHistory
      .join(updatedClientsInfo, Seq("name","surname","address","startDate"),"inner")

    val updatedClientsInfoClean = updatedClientsInfoDropDup
      .join(existingInput, Seq("name","surname","address","startDate"), "left_anti")


    val updateEndDate = updatedClientsInfoClean
      .select(
        col("name"),
        col("surname"),
        col("startDate").alias("endDate")
      )
    val clientsHistoryWithoutEndDate = clientsHistory.drop("endDate","isEffective")

    val closedOldClient = clientsHistoryWithoutEndDate
      .join(updateEndDate, Seq("name", "surname"), "inner")
      .withColumn("isEffective", lit(false).cast(BooleanType))

    val clientsHistoryList = clientsHistory
      .select(
        col("name"),
        col("surname")
      )

    val newAddressClient = updatedClientsInfoClean
      .join(clientsHistoryList, Seq("name", "surname"), "left")
      .withColumn("endDate", lit(null))
      .withColumn("isEffective", lit(true).cast(BooleanType))

    val clientAddressUpdate = closedOldClient
      .union(newAddressClient)

    val clientsUpdateList = updatedClientsInfoClean
      .select(
        col("name"),
        col("surname")
      )

    val activeClients = clientsHistory
      .join(clientsUpdateList, Seq("name", "surname"), "left_anti")
      .withColumn("endDate", lit(null))
      .withColumn("isEffective", lit(true).cast(BooleanType))

    val alreadyExistingClient = clientsHistory
      .join(updatedClientsInfoClean, Seq("name","surname","address","startDate"),"inner")
      .withColumn("endDate", lit(null))
      .withColumn("isEffective", lit(true).cast(BooleanType))

    val firstVersion = clientAddressUpdate
      .union(activeClients)

    val updatedClientsStatus = firstVersion
      .union(alreadyExistingClient)

    updatedClientsStatus
  }
}