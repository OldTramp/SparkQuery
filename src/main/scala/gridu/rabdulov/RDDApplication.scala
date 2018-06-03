package gridu.rabdulov

import com.opencsv.CSVParser
import gridu.rabdulov.Model.{CountryNetwork, TempIP, TempLoc, TopCategoryProducts}
import org.apache.commons.net.util.SubnetUtils
import gridu.rabdulov.DatasetApplication.writeToMysql
import org.apache.spark.sql.SQLContext
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object RDDApplication {

  val csvParser = new CSVParser()

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("RDD queries")
      .set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive","true")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

//    val localPrefix = "/Users/rabdulov/Downloads/hadoop/rabdulov/"
    val fsPrefix = "hdfs:///user/rabdulov/"

    val eventsRDD = sc.textFile(fsPrefix + "events/2018/02/*")

    val purchases = eventsRDD.map(getTokens).map(Model.Purchase.parse)


    val byCategory = purchases.map(p => (p.productCategory, 1)).reduceByKey(_+_)
    val topCategories = byCategory.sortBy(_._2, ascending = false).take(10)

//    println("Top Categories:")
//    topCategories.foreach(println)
    writeToMysql(sqlContext.createDataFrame(topCategories), "spark_rdd_top_categories")




    val byCategoryAndProduct = purchases.map(p => ((p.productCategory, p.productName), 1)).reduceByKey(_+_)
    val topCategoryProducts = byCategoryAndProduct
      .map(p => TopCategoryProducts(p._1._1, p._1._2, p._2))
      .keyBy(p => p.productCategory)
      .aggregateByKey(List[TopCategoryProducts]())(
        (l, r) => (l :+ r).sortBy(-_.count).take(10),
        (l1, l2) => (l1 ++ l2).sortBy(-_.count).take(10))
      .values.flatMap(list => list.iterator)

//    println("Top Products per Category:")
//    topCategoryProducts.foreach(println)
    writeToMysql(sqlContext.createDataFrame(topCategoryProducts), "spark_rdd_top_category_products")




    val networksRDD = sc.textFile(fsPrefix + "GeoLite2-Country-Blocks-IPv4.csv")
    val header1 = networksRDD.first
    val networks = networksRDD.filter(_ != header1).map(getTokens)
      .filter(l => !l(0).isEmpty && !l(1).isEmpty).map(TempIP.parse).keyBy(_.geonameId)

    val countriesRDD = sc.textFile(fsPrefix + "GeoLite2-Country-Locations-en.csv")
    val header2 = countriesRDD.first
    val countries = countriesRDD.filter(_ != header2).map(getTokens)
      .filter(l => !l(0).isEmpty && !l(5).isEmpty).map(TempLoc.parse).keyBy(_.geonameId)
    countries.partitionBy(new HashPartitioner(networks.partitions.length))


    val countryNetwork = countries.join(networks).map(j => CountryNetwork(j._2._1.country, j._2._2.network))

    val byCountry = countryNetwork.keyBy(_.country).mapValues(_.network).groupByKey()

    val purchaseCollection = purchases.map(p => (p.clientIp, p.productPrice)).collect()
    sc.broadcast(purchaseCollection)

    val withPurchase = byCountry.flatMapValues(_.iterator)
      .mapValues(c => purchaseCollection.toStream
        .filter(p => new SubnetUtils(c).getInfo.isInRange(p._1)).map(_._2).sum)

    val topCountries = withPurchase.reduceByKey(_+_).sortBy(-_._2).take(10)

//    println("Top Countries:")
//    topCountries.foreach(println)
    writeToMysql(sqlContext.createDataFrame(topCountries), "spark_rdd_top_countries")


    println("end")

  }


  private def getTokens(value: String): Array[String] = {
    if (!"".equals(value)) {
      var tokens: Array[String] = csvParser.parseLine(value)
      return tokens
    }
    return null
  }

}

