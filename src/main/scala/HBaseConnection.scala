import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory}



object HBaseConnection {

  final val RATINGS_TABLE : String = "ratings"
  final val RATING_COLUMNFAMILY : String = "rating"


  def createConnection(configFile : String) : (Connection, Admin) = {
    val conf : Configuration = HBaseConfiguration.create()

    conf.addResource(configFile)//"/usr/local/hbase-2.2.4/conf/hbase-site.xml"
    val conn : Connection = ConnectionFactory.createConnection(conf)

    (conn, conn.getAdmin)
  }

  def closeConnection(connection: Connection, admin: Admin) : Unit = {
    admin.close()
    connection.close()
  }
}
