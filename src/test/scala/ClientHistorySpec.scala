//case class HistoryClient(name: String, surname: String, address: String, startDate: String, endDate: String, isEffective: Boolean)

//case class UpdateClient(name: String, surname: String, address: String, startDate: String)

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.GivenWhenThen
import org.scalatest.matchers.should.Matchers._
import com.abidi.ClientHistory.updateClientsStatus
import org.apache.spark.sql.functions._

class ClientHistorySpec extends AnyFlatSpec with GivenWhenThen {

  val spark: SparkSession = SparkSession.builder()
    .appName("client status")
    .master("local[*]")
    .getOrCreate()

  "updateClientsStatus" should "return client with the date of changing his address as an end date and a " +
    "false effectiveness, client with his new address and a true effectiveness  " in {
    Given("clientsInfo and updatedClientsInfo")
    val clientsInfo = Seq(
      HistoryClient("Mohamed", "Sehli", "California", "2017/08/25", null, true)
    )
    val updatedClientsInfo = Seq(
      UpdateClient("Mohamed", "Sehli", "Zurich", "2018/06/25")
    )
    import spark.implicits._
    val clientsInfoDF: DataFrame = clientsInfo.toDF()
    val updatedClientsInfoDF: DataFrame = updatedClientsInfo.toDF()

    When("updateClientsStatus is invoked")
    val result = updateClientsStatus(clientsInfoDF, updatedClientsInfoDF)

    Then("The client Mohamed Sehli should be returned two times: one with his old address, " +
      "an end date(start date of new address) and false effectiveness ,the other row with his new address ,its start date(effectiveness true)")
    val expectedResult: DataFrame = Seq(
      HistoryClient("Mohamed", "Sehli", "California", "2017/08/25", "2018/06/25", false),
      HistoryClient("Mohamed", "Sehli", "Zurich", "2018/06/25", null, true)
    ).toDF()
    result.collect() should contain theSameElementsAs expectedResult.collect()
  }

  "updateClientsStatus" should "return clients existing only in the history table with a true effectiveness" in {
    Given("clientsInfo and updatedClientsInfo")
    val clientsInfo = Seq(
      HistoryClient("Ala", "Noumi", "LA", "2021/07/12", null, true)
    )
    import spark.implicits._
    val clientsInfoDF: DataFrame = clientsInfo.toDF()
    val updatedClientsInfoDF: DataFrame = Seq.empty[UpdateClient].toDF()

    When("updateClientsStatus is invoked")
    val result = updateClientsStatus(clientsInfoDF, updatedClientsInfoDF)

    Then("the client Ala Noumi LA should be returned with true effectiveness and his already existing start date")
    val expectedResult: DataFrame = Seq(
      HistoryClient("Ala", "Noumi", "LA", "2021/07/12", null, true)
    ).toDF()
    result.collect() should contain theSameElementsAs expectedResult.collect()
  }

  "updateClientsStatus" should "return new clients existing only in the update table with a start date and a true effectiveness" in {
    Given("clientsInfo and updatedClientsInfo")
    val updatedClientsInfo = Seq(
      UpdateClient("Tarak", "Marzougui", "NY", "2021/12/25")
    )
    import spark.implicits._
    val clientsInfoDF: DataFrame = Seq.empty[UpdateClient].toDF()
    val updatedClientsInfoDF: DataFrame = updatedClientsInfo.toDF()

    When("updateClientsStatus is invoked")
    val result = updateClientsStatus(clientsInfoDF, updatedClientsInfoDF)

    Then("The client Tarak Marzougui NY should be returned with a true effectiveness and a start date as the event time")
    val expectedResult: DataFrame = Seq(
      HistoryClient("Tarak", "Marzougui", "NY", "2021/12/25", null, true)
    ).toDF()
    result.collect() should contain theSameElementsAs expectedResult.collect()
  }

  "updateClientStatus" should "deduplicate data in the update table" in {
    Given("clientsInfo and updatedClientsInfo")
    val updatedClientsInfo = Seq(
      UpdateClient("Tarak", "Marzougui", "NY", "2021/12/25"),
      UpdateClient("Tarak", "Marzougui", "NY", "2021/12/25")
    )
    import spark.implicits._
    val clientsInfoDF: DataFrame = Seq.empty[UpdateClient].toDF()
    val updatedClientsInfoDF: DataFrame = updatedClientsInfo.toDF()

    When("updateClientsStatus is invoked")
    val result = updateClientsStatus(clientsInfoDF, updatedClientsInfoDF)

    Then("clients Tarak Marzougui and Ala Noumi should be returned only once")
    val expectedResult: DataFrame = Seq(
      HistoryClient("Tarak", "Marzougui", "NY", "2021/12/25", null, true)
    ).toDF()
    result.collect() should contain theSameElementsAs expectedResult.collect()
  }

  "updateClientStatus" should "maintain the history table inchanged by duplicated data from the update" in {
    Given("clientsInfo and updatedClientsInfo")
    val clientsInfo = Seq(
      HistoryClient("Ala", "Noumi", "LA", "2021/07/01", null, true)
    )
    val updatedClientsInfo = Seq(
      UpdateClient("Ala", "Noumi", "LA", "2021/07/01")
    )
    import spark.implicits._
    val clientsInfoDF: DataFrame = clientsInfo.toDF()
    val updatedClientsInfoDF: DataFrame = updatedClientsInfo.toDF()

    When("updateClientsStatus is invoked")
    val result = updateClientsStatus(clientsInfoDF, updatedClientsInfoDF)

    Then("The client Tarak Marzougui NY should be returned with a true effectiveness and a start date as the event time")
    val expectedResult: DataFrame = Seq(
      HistoryClient("Ala", "Noumi", "LA", "2021/07/01", null, true)
    ).toDF()
    result.collect() should contain theSameElementsAs expectedResult.collect()
  }

  "updateClientStatus" should "take in consideration late arriving data" in {
    Given("clientsInfo and updatedClientsInfo")
    val clientsInfo = Seq(
      HistoryClient("Ala", "Noumi", "LA", "2021/07/01", null, true)
    )
    val updatedClientsInfo = Seq(
      UpdateClient("Ala", "Noumi", "CH", "2021/06/01"),
      UpdateClient("Ala", "Noumi", "NY", "2021/05/01")
    )
    import spark.implicits._
    val clientsInfoDF: DataFrame = clientsInfo.toDF()
    val updatedClientsInfoDF: DataFrame = updatedClientsInfo.toDF()

    When("updateClientsStatus is invoked")
    val result = updateClientsStatus(clientsInfoDF, updatedClientsInfoDF)

    Then("The client Ala Noumi should be returned ")
    val expectedResult: DataFrame = Seq(
      HistoryClient("Ala", "Noumi", "NY", "2021/05/01", "2021/06/01", false),
      HistoryClient("Ala", "Noumi", "CH", "2021/06/01", "2021/07/01", false),
      HistoryClient("Ala", "Noumi", "LA", "2021/07/01", null, true)
    ).toDF()
    result.show()
    expectedResult.show()
     result.collect() should contain theSameElementsAs expectedResult.collect()
  }
  "" should "" in {
    Given("clientsInfo and updatedClientsInfo")
    val clientsInfo = Seq(
      HistoryClient("Ala", "Noumi", "LA", "2016/07/01", null, true),
      HistoryClient("Dhia", "Mansouri", "CAL", "2015/05/01", null, true),
      HistoryClient("Anwer", "Hichri", "Aus", "2021/06/01", null, true)
    )
    val updatedClientsInfo = Seq(
      UpdateClient("Ala", "Noumi", "CH", "2017/05/01"),
      UpdateClient("Ala", "Noumi", "NY", "2021/06/01"),
      UpdateClient("Dhia", "Mansouri", "NY", "2018/07/01"),
      UpdateClient("Tayeb", "Ouni", "SPA", "2021/07/01")
    )
    import spark.implicits._
    val clientsInfoDF: DataFrame = clientsInfo.toDF()
    val updatedClientsInfoDF: DataFrame = updatedClientsInfo.toDF()
    When("")
    val result = updateClientsStatus(clientsInfoDF, updatedClientsInfoDF)

    result.show()
  }
}