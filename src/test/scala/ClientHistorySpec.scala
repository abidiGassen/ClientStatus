case class HistoryClient(name: String, surname: String, address: String, startDate: String, endDate: String, isEffective: Boolean)

case class UpdateClient(name: String, surname: String, address: String, startDate: String)

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.GivenWhenThen
import org.scalatest.matchers.should.Matchers._
import com.abidi.ClientHistory.updateClientsStatus

class ClientHistorySpec extends AnyFlatSpec with GivenWhenThen {

  val spark: SparkSession = SparkSession.builder()
    .appName("client status")
    .master("local[*]")
    .getOrCreate()

  val trueEffectiveness: Boolean = true
  val falseEffectiveness: Boolean = false

  "updateClientsStatus" should "return client with the date of changing his address as an end date and a " +
    "false effectiveness, client with his new address and a true effectiveness  " in {
    Given("clientsInfo and updatedClientsInfo")
    val clientsInfo = Seq(
      UpdateClient("Mohamed", "Sehli", "California", "25/08/2017")
    )
    val updatedClientsInfo = Seq(
      UpdateClient("Mohamed", "Sehli", "Zurich", "25/06/2018")
    )
    import spark.implicits._
    val clientsInfoDF: DataFrame = clientsInfo.toDF()
    val updatedClientsInfoDF: DataFrame = updatedClientsInfo.toDF()

    When("updateClientsStatus is invoked")
    val result = updateClientsStatus(clientsInfoDF, updatedClientsInfoDF)

    Then("The client Mohamed Sehli should be returned two times: one with his old address, " +
      "an end date(start date of new address) and false effectiveness ,the other row with his new address ,its start date(effectiveness true)")
    val expectedResult: DataFrame = Seq(
      HistoryClient("Mohamed", "Sehli", "California", "25/08/2017", "25/06/2018", false),
      HistoryClient("Mohamed", "Sehli", "Zurich", "25/06/2018", null, true)
    ).toDF()
    result.collect() should contain theSameElementsAs expectedResult.collect()
  }

  "updateClientsStatus" should "return clients existing only in the history table with a true effectiveness" in {
    Given("clientsInfo and updatedClientsInfo")
    val clientsInfo = Seq(
      UpdateClient("Ala", "Noumi", "LA", "12/07/2021")
    )
    import spark.implicits._
    val clientsInfoDF: DataFrame = clientsInfo.toDF()
    val updatedClientsInfoDF: DataFrame = Seq.empty[UpdateClient].toDF()

    When("updateClientsStatus is invoked")
    val result = updateClientsStatus(clientsInfoDF, updatedClientsInfoDF)

    Then("the client Ala Noumi LA should be returned with true effectiveness and his already existing start date")
    val expectedResult: DataFrame = Seq(
      HistoryClient("Ala", "Noumi", "LA", "12/07/2021", null, true)
    ).toDF()
    result.collect() should contain theSameElementsAs expectedResult.collect()
  }

  "updateClientsStatus" should "return new clients existing only in the update table with a start date and a true effectiveness" in {
    Given("clientsInfo and updatedClientsInfo")
    val updatedClientsInfo = Seq(
      UpdateClient("Tarak", "Marzougui", "NY", "25/12/2021")
    )
    import spark.implicits._
    val clientsInfoDF: DataFrame = Seq.empty[UpdateClient].toDF()
    val updatedClientsInfoDF: DataFrame = updatedClientsInfo.toDF()

    When("updateClientsStatus is invoked")
    val result = updateClientsStatus(clientsInfoDF, updatedClientsInfoDF)

    Then("The client Tarak Marzougui NY should be returned with a true effectiveness and a start date as the event time")
    val expectedResult: DataFrame = Seq(
      HistoryClient("Tarak", "Marzougui", "NY", "25/12/2021", null, true)
    ).toDF()
    result.collect() should contain theSameElementsAs expectedResult.collect()
  }

  "updateClientStatus" should "delete duplicated data " in {
    Given("clientsInfo and updatedClientsInfo")
    val clientsInfo = Seq(
      UpdateClient("Ala", "Noumi", "LA", "12/07/2021"),
      UpdateClient("Ala", "Noumi", "LA", "12/07/2021")
    )
    val updatedClientsInfo = Seq(
      UpdateClient("Tarak", "Marzougui", "NY", "25/12/2021"),
      UpdateClient("Tarak", "Marzougui", "NY", "25/12/2021")
    )
    import spark.implicits._
    val clientsInfoDF: DataFrame = clientsInfo.toDF()
    val updatedClientsInfoDF: DataFrame = updatedClientsInfo.toDF()

    When("updateClientsStatus is invoked")
    val result = updateClientsStatus(clientsInfoDF, updatedClientsInfoDF)

    Then("clients Tarak Marzougui and Ala Noumi should be returned only once")
    val expectedResult: DataFrame = Seq(
      HistoryClient("Tarak", "Marzougui", "NY", "25/12/2021", null, true),
      HistoryClient("Ala", "Noumi", "LA", "12/07/2021", null, true)
    ).toDF()
    result.collect() should contain theSameElementsAs expectedResult.collect()
  }
}