package com.abidi

import java.nio.file
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.BooleanType
import io.delta.tables._

object ClientHistoryDelta {

  def updateClientsStatus(clientsHistory: DataFrame, updatedClientsInfo: DataFrame, Path: file.Path) = {

    val updatedClientsInfoDeduplicate = updatedClientsInfo.dropDuplicates()
      .withColumn("endDate", lit(null))
      .withColumn("isEffective", lit(true).cast(BooleanType))

    val historyTable = DeltaTable.forPath("Path")

    val clientsToBeClosed = updatedClientsInfoDeduplicate.as("updates")
      .join(clientsHistory.as("history"), Seq("name", "surname"))
      .where("history.address <> updates.address AND history.isEffective = true")
      .selectExpr(
        "name",
        "surname",
        "history.address",
        "history.startDate",
        "updates.startDate as endDate",
        "false as isEffective")

    val stagedUpdates = clientsToBeClosed
      .selectExpr( "(name || surname) as mergeKey", "*")
      .union(
        updatedClientsInfoDeduplicate.selectExpr("Null as mergeKey", "*")
      )

    historyTable.as("History")
      .merge(
        stagedUpdates.as("Update"), "History.name||History.surname = mergeKey")
      .whenMatched("History.address <> Update.address AND History.isEffective = true")
      .updateAll()
      .whenNotMatched()
      .insertAll()
      .execute

    historyTable
  }
}
