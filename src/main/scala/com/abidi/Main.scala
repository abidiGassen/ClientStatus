package com.abidi

import com.abidi.ClientHistory.updateClientsStatus
import org.apache.spark.sql.SparkSession

object Main {
   def main(args: Array[String]): Unit = {

      val spark = SparkSession.builder()
        .master("local[*]")
        .appName("Client Status")
        .getOrCreate()

      val clientsHistory = spark.read.format("delta")
                                      .load("s3n://deltalake101test/client-history.csv")
      val updatedClientsInfo = spark.read.format("delta")
                                          .load("s3n://deltalake101test/update.csv")

      updateClientsStatus(clientsHistory, updatedClientsInfo)
   }
}