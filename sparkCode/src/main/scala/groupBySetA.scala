import org.apache.spark.sql.SparkSession


object groupBySetA {
  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.functions._

    val spark = SparkSession.builder.appName("whenOtherwise").master("local[4]").getOrCreate()
    import spark.implicits._
    // ---------------------------------------1-----------------------------------------//

    val data =List((1, "ProductA", "Electronics", 1000.0),
    (2, "ProductB", "Clothing", 500.0),
    (3, "ProductC", "Electronics", 800.0),
    (4, "ProductD", "Clothing", 300.0),
    (5, "ProductE", "Electronics", 1200.0))

    val Df = data.toDF("product_id","product_name","product_cat","product_Price")

//    val totalSales = Df.groupBy(col("product_cat")).agg(sum("product_price"))
//    totalSales.show()

    // ---------------------------------------2-----------------------------------------//

//    val avgPrice = Df.groupBy(col("product_cat")).agg(avg("product_Price").as("avg_price"))
//      .sort(desc("avg_Price"))
//       avgPrice.show()
    // ---------------------------------------3-----------------------------------------//

//    val totalSales = Df.groupBy(col("product_cat")).agg(sum("product_price")
//      .as("Total_sales")).sort(desc("Total_sales"))
//       totalSales.show(1)
    // ---------------------------------------4-----------------------------------------//

//    val maxMinSales = Df.groupBy(col("product_cat")).agg(max("product_price")
//      ,min("product_price"))
//    maxMinSales.show()
    // ---------------------------------------5-----------------------------------------//

//    val avgSales = Df.groupBy(col("product_cat")).agg(avg("product_price")
//      .as("calculated_avg"))
//        avgSales.show()
    // ---------------------------------------6-----------------------------------------//
    val data1 = List((1, "ProductA", "Electronics", 2023, 1000.0),
    (2, "ProductB", "Clothing", 2023, 500.0),
    (3, "ProductC", "Electronics", 2022, 800.0),
    (4, "ProductD", "Clothing", 2022, 300.0),
    (5, "ProductE", "Electronics", 2023, 1200.0))

    val Df1 = data1.toDF("product_id","product_name","product_cat","year","product_Price")

//    val yearlySales = Df1.groupBy(col("product_cat"),col("year")).agg(sum("product_price")
//      .as("product_price"))
//
//    yearlySales.show()
    // ---------------------------------------7-----------------------------------------//

//    val productSales = Df1.groupBy(col("product_cat"), col("product_name")).agg(sum("product_price")
//          .as("product_price"))
//
//    productSales.show()

    //**** not able to find single product from each catagery ***//

    // ---------------------------------------8-----------------------------------------//
    val data3 = List((1, "ProductA", "Electronics", "2023-01-15", 1000.0),
    (2, "ProductB", "Clothing", "2023-02-20", 500.0),
    (3, "ProductC", "Electronics", "2023-04-10", 800.0),
    (4, "ProductD", "Clothing", "2023-05-05", 300.0),
    (5, "ProductE", "Electronics", "2023-01-25", 1200.0))

    val Df2 = data3.toDF("product_id","product_name","product_cat","year","product_Price")

//    val quaterSales = Df2.groupBy(col("product_cat"),col("year"))
//      .agg(sum("product_price").as("Total_sales"))
//
//    quaterSales.show()
    // ---------------------------------------9-----------------------------------------//

//    val quaterSales = Df2.groupBy(col("product_cat"), col("year"))
//      .agg(sum("product_price").as("Total_sales"))
//
//    quaterSales.show()

    //***** problem 8 and 9 *****//
  }
}