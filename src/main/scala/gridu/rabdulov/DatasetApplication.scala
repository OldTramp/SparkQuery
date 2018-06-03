package gridu.rabdulov

import gridu.rabdulov.Model._
import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object DatasetApplication {

  def writeToMysql(df: DataFrame, tableName: String): Unit = {
    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", "root")
//    prop.setProperty("password", "pass")
//    val url = "jdbc:mysql://localhost:3306/rabdulov"
    val url = "jdbc:mysql://10.0.0.21:3306/rabdulov"

    df.write.mode("overwrite").jdbc(url, tableName, prop)
  }

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName("Dataset queries")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .config("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive","true")
      .getOrCreate

    import spark.implicits._

    val purchaseSchema = StructType(
      List(
        StructField("productCategory", DataTypes.StringType),
        StructField("productName", DataTypes.StringType),
        StructField("productPrice", DataTypes.DoubleType),
        StructField("purchaseDateTime", DataTypes.TimestampType),
        StructField("clientIp", DataTypes.StringType)
      )
    )

    val fsPrefix = "hdfs:///user/rabdulov/"
    val eventsDF = spark.read.schema(purchaseSchema).csv(fsPrefix + "events/2018/02/*")

    val purchases = eventsDF.as[Purchase]
    purchases.createOrReplaceTempView("purchase")

    val topCategories = spark.sql(
      "SELECT productCategory, count(*) cnt " +
        "FROM purchase " +
        "GROUP BY productCategory " +
        "ORDER BY cnt DESC LIMIT 10")

    writeToMysql(topCategories, "spark_ds_top_categories")
//    println("Top Categories:")
//    topCategories.collect.foreach(println)


    val topCategoryProducts = spark.sql(
      "SELECT productCategory, productName, cnt " +
        "FROM ( " +
         " SELECT productCategory, productName, cnt, ROW_NUMBER() OVER (PARTITION BY productCategory ORDER BY cnt DESC) rank " +
            "FROM ( " +
              "SELECT productCategory, productName, count(*) cnt " +
              "FROM purchase " +
              "GROUP BY productCategory, productName" +
            ") grouped" +
        ") ranked " +
        "WHERE rank <= 10"
      )

    writeToMysql(topCategoryProducts, "spark_ds_top_category_products")
//    println("Top Products per Category:")
//    topCategoryProducts.collect.foreach(println)




    val networksDF = spark.read
      .option("header","true")
      .option("inferSchema", "true")
      .csv(fsPrefix + "GeoLite2-Country-Blocks-IPv4.csv")


    val countriesDF = spark.read
      .option("header","true")
      .option("inferSchema", "true")
      .csv(fsPrefix + "GeoLite2-Country-Locations-en.csv")


    val countryNetworkDS = countriesDF.join(networksDF, "geoname_id")
      .select($"country_name".alias("country"), $"network")
      .as[CountryNetwork]

    countryNetworkDS.createOrReplaceTempView("countryNetwork")

    val addrFromSubnetUDF = spark.udf.register("addr_from_subnet",
      (ipAddress: String, network: String) => new SubnetUtils(network).getInfo.isInRange(ipAddress))

    val topCountries = spark.sql(
      "SELECT cn.country, round(sum(p.productPrice), 2) moneySpent " +
        "FROM purchase p JOIN countryNetwork cn ON addr_from_subnet(p.clientIp, cn.network) " +
        "GROUP BY country " +
        "ORDER BY moneySpent DESC " +
        "LIMIT 10")


    writeToMysql(topCountries, "spark_ds_top_countries")
//    println("Top Countries:")
//    topCountries.collect.foreach(println)


    println("end")
    spark.stop

  }

//  object PurchaseEncoders {
//    implicit def barEncoder: org.apache.spark.sql.Encoder[Purchase] =
//      org.apache.spark.sql.Encoders.kryo[Purchase]
//  }
}

