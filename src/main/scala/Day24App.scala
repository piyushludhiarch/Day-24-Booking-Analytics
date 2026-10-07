import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day24App {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Booking Analytics")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    import spark.implicits._

    val customers = Seq(
      (1,"Amit","India"),
      (2,"Sara","India"),
      (3,"John","USA"),
      (4,"Neha","India")
    ).toDF("customer_id","customer","country")

    val hotels = Seq(
      (101,"Grand Hotel","Delhi"),
      (102,"Lake View","Hyderabad"),
      (103,"Beach Resort","Goa")
    ).toDF("hotel_id","hotel","city")

    val bookings = Seq(
      (1,1,101,3,12000,"Confirmed"),
      (2,2,102,2,8000,"Confirmed"),
      (3,3,103,5,25000,"Cancelled"),
      (4,4,102,4,16000,"Confirmed"),
      (5,1,103,2,10000,"Confirmed")
    ).toDF(
      "booking_id",
      "customer_id",
      "hotel_id",
      "nights",
      "amount",
      "status"
    )

    println("===== BOOKING ANALYTICS =====")

    val data = bookings
      .join(customers, Seq("customer_id"))
      .join(hotels, Seq("hotel_id"))

    println("BOOKINGS:")
    data.show(false)

    println("CITY REVENUE:")

    data.filter(col("status") === "Confirmed")
      .groupBy("city")
      .agg(
        sum("amount").as("revenue"),
        count("*").as("bookings")
      )
      .orderBy(desc("revenue"))
      .show(false)

    println("BOOKING STATUS:")

    data.groupBy("status")
      .count()
      .show(false)

    println("BOOKING CATEGORY:")

    data.withColumn(
      "booking_category",
      when(col("amount") >= 15000, "Premium")
        .when(col("amount") >= 10000, "Standard")
        .otherwise("Budget")
    ).show(false)

    spark.stop()
  }
}
