package gridu.rabdulov

import gridu.rabdulov.Model._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}


object DatasetApplication {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName("Dataset queries")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
//      .config("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive","true")
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

    val eventsDF = spark.read.schema(purchaseSchema).csv("/Users/rabdulov/Downloads/hadoop/rabdulov/events/2018/02/*")

    val purchases = eventsDF.as[Purchase]
    purchases.createOrReplaceTempView("purchase")

    val topCategories = spark.sql(
      "SELECT productCategory, count(*) cnt " +
        "FROM purchase " +
        "GROUP BY productCategory " +
        "ORDER BY cnt DESC LIMIT 10")

    println("Top Categories:")
    //TODO send result to MySQL
    topCategories.collect.foreach(println)


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

    println("Top Products per Category:")
    //TODO send result to MySQL
    topCategoryProducts.collect.foreach(println)






//    val networksRDD = sc.textFile("/Users/rabdulov/Downloads/hadoop/rabdulov/GeoLite2-Country-Blocks-IPv4.csv")
//    val header1 = networksRDD.first
//    val networks = networksRDD.filter(_ != header1).map(getTokens)
//      .filter(l => !l(0).isEmpty && !l(1).isEmpty).map(TempIP.parse).keyBy(_.geonameId)
//
//    val countriesRDD = sc.textFile("/Users/rabdulov/Downloads/hadoop/rabdulov/GeoLite2-Country-Locations-en.csv")
//    val header2 = countriesRDD.first
//    val countries = countriesRDD.filter(_ != header2).map(getTokens)
//      .filter(l => !l(0).isEmpty && !l(5).isEmpty).map(TempLoc.parse).keyBy(_.geonameId)
//    countries.partitionBy(new HashPartitioner(networks.partitions.length))
//
//
//    val countryNetwork = countries.join(networks).map(j => CountryNetwork(j._2._1.country, j._2._2.network))
//
//    val byCountry = countryNetwork.keyBy(_.country).mapValues(_.network).groupByKey()
//
//    val purchaseCollection = purchases.map(p => (p.clientIp, p.productPrice)).collect()
//    sc.broadcast(purchaseCollection)
//
//    val withPurchase = byCountry.flatMapValues(_.iterator)
//      .mapValues(c => purchaseCollection.toStream
//        .filter(p => new SubnetUtils(c).getInfo.isInRange(p._1)).map(_._2).sum)
//
//    val topCountries = withPurchase.reduceByKey(_+_).sortBy(-_._2).take(10)

//    println("Top Countries:")
//    //TODO send result to MySQL
//    topCountries.foreach(println)


    println("end")
    spark.stop

  }


//  private def getTokens(value: String): Array[String] = {
//    if (!"".equals(value)) {
//      var tokens: Array[String] = csvParser.parseLine(value)
//      return tokens
//    }
//    return null
//  }

  object PurchaseEncoders {
    implicit def barEncoder: org.apache.spark.sql.Encoder[Purchase] =
      org.apache.spark.sql.Encoders.kryo[Purchase]
  }
}

