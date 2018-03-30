package gridu.rabdulov

import com.opencsv.CSVParser
import gridu.rabdulov.Model.{CountryNetwork, TempIP, TempLoc, TopCategoryProducts}
import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object RDDApplication {

  val csvParser = new CSVParser()

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("RDD queries")
//      .set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive","true")

    val sc = new SparkContext(conf)

    val eventsRDD = sc.textFile("/Users/rabdulov/Downloads/hadoop/rabdulov/events/2018/02/*")

    val purchases = eventsRDD.map(getTokens).map(Model.Purchase.parse)

    purchases.cache()

    val byCategory = purchases.map(p => (p.productCategory, 1)).reduceByKey(_+_)
    val topCategories = byCategory.sortBy(_._2, ascending = false).take(10)

//    println("Top Categories:")
//    //TODO send result to MySQL
//    topCategories.foreach(println)




    val byCategoryAndProduct = purchases.map(p => ((p.productCategory, p.productName), 1)).reduceByKey(_+_)
    val topCategoryProducts = byCategoryAndProduct
      .map(p => TopCategoryProducts(p._1._1, p._1._2, p._2))
      .keyBy(p => p.productCategory)
      .aggregateByKey(List[TopCategoryProducts]())(
        (l, r) => (l :+ r).sortBy(-_.count).take(10),
        (l1, l2) => (l1 ++ l2).sortBy(-_.count).take(10))
      .values.flatMap(list => list.iterator)

//    println("Top Products per Category:")
//    //TODO send result to MySQL
//    topCategoryProducts.foreach(println)






    val networksRDD = sc.textFile("/Users/rabdulov/Downloads/hadoop/rabdulov/GeoLite2-Country-Blocks-IPv4.csv")
    val header1 = networksRDD.first
    val networks = networksRDD.filter(_ != header1).map(getTokens)
      .filter(l => !l(0).isEmpty && !l(1).isEmpty).map(TempIP.parse).keyBy(_.geonameId)

    val countriesRDD = sc.textFile("/Users/rabdulov/Downloads/hadoop/rabdulov/GeoLite2-Country-Locations-en.csv")
    val header2 = countriesRDD.first
    val countries = countriesRDD.filter(_ != header2).map(getTokens)
      .filter(l => !l(0).isEmpty && !l(5).isEmpty).map(TempLoc.parse).keyBy(_.geonameId)
    countries.partitionBy(new HashPartitioner(networks.partitions.length))


    val countryNetwork = countries.join(networks).map(j => CountryNetwork(j._2._1.country, j._2._2.network))

    val countryAddresses = countryNetwork.keyBy(_.country).mapValues(e => new SubnetUtils(e.network).getInfo.getAllAddresses)

    println("===============")
    countryAddresses.take(10).foreach(println)

    countryAddresses.groupByKey().flatMapValues()


//    val filtered = countryNetwork.cartesian(purchases)
      //.filter(e => new SubnetUtils(e._1.network).getIn fo.isInRange(e._2.clientIp))


    println("end")

  }
  //new SubnetUtils(subnet.toString).getInfo.isInRange(address.toString);

  private def getTokens(value: String): Array[String] = {
    if (!"".equals(value)) {
      var tokens: Array[String] = csvParser.parseLine(value)
      return tokens
    }
    return null
  }

}

