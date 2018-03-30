package gridu.rabdulov

import java.sql.Timestamp
import java.text.{ParseException, SimpleDateFormat}

object Model extends Serializable {
  case class Purchase(
                       productCategory: String,
                       productName: String,
                       productPrice: Double,
                       purchaseDateTime: Timestamp,
                       clientIp: String
                     )

  object Purchase {
    def parse(i: Array[String]) = {
      val fmt1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
      val fmt2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm")

      var timestamp:Timestamp = null

      try {
        timestamp = new Timestamp(fmt1.parse(i(3)).getTime)
      } catch {
        case e: ParseException => timestamp = new Timestamp(fmt2.parse(i(3)).getTime)
      }

      Purchase(i(0).toString, i(1).toString, i(2).toDouble, timestamp, i(4).toString)
    }
  }

  case class TopCategoryProducts(
                                  productCategory: String,
                                  productName: String,
                                  count: Int
                                )



  case class TempIP(
                     network: String,
                     geonameId: Long
                   )

  object TempIP {
    def parse(i: Array[String]) = {

      TempIP(i(0).toString, i(1).toLong)
    }
  }

  case class TempLoc(
                      geonameId: Long,
                      country: String
                    )

  object TempLoc {
    def parse(i: Array[String]) = {

      TempLoc(i(0).toLong, i(5).toString)
    }
  }

  case class CountryNetwork(
//                             geonameId: Long,
                             country: String,
                             network: String
                           )
//  object CountryNetwork {
//    def parse(i: Array[String]) = {
//
//      CountryNetwork(i(0).toLong, i(1).toString, i(2).toString)
//    }
//  }

}
