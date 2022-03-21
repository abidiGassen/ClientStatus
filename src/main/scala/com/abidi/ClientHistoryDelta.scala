package com.abidi

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.BooleanType


object ClientHistoryDelta {

  def updateClientsStatus(clientsHistory: DataFrame, updatedClientsInfo: DataFrame) = {

    val updatedClientsInfoDeduplicate = updatedClientsInfo.dropDuplicates()
      .withColumn("endDate", lit(null))
      .withColumn("isEffective", lit(true).cast(BooleanType))

    //todo: replace hardcoded Path
    val historyTable = DeltaTable.forPath("C:\\Users\\abidi\\IdeaProjects\\ClientStatus\\target\\History")

    val newAddresses = updatedClientsInfoDeduplicate.as("updates")
      .join(clientsHistory.as("history"), Seq("name", "surname"))
      .where("history.address <> updates.address AND history.isEffective = true")
      .select(
        "name",
        "surname",
        "updates.address",
        "updates.startDate",
        "updates.endDate",
        "updates.isEffective")

    val stagedUpdates = newAddresses
      .selectExpr("Null as mergeKey", "updates.*")
      .union(
        updatedClientsInfoDeduplicate.selectExpr("(name || surname) as mergeKey", "*")
      )

    historyTable.as("History")
      .merge(
        stagedUpdates.as("Update"), "History.name||History.surname = mergeKey")
      .whenMatched("History.address <> Update.address AND History.isEffective = true")
      .updateExpr(Map(
        "endDate" -> "Update.startDate",
        "isEffective" -> "false"))
      .whenNotMatched()
      .insertAll()
      .execute

    historyTable
  }
}
