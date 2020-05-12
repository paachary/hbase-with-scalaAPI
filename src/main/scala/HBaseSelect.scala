import org.apache.hadoop.hbase.{CellUtil, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, Get, Result, Table }
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.LoggerFactory

/**
 * This class provides a Select method for fetching data from the ratings table.
 * This class can be made completely generic by passing the table name as an argument to the program
 */
class HBaseSelect {

  def checkTableExistance(conn : Connection, admin : Admin) : Table = {

    val logger = LoggerFactory.getLogger(classOf[HBaseSelect])

    val tableName : TableName = TableName.valueOf(HBaseConnection.RATINGS_TABLE)

    if (admin.isTableAvailable(tableName)) {

    conn.getTable(tableName)
  } else {
      logger.error("Table "+ HBaseConnection.RATINGS_TABLE +" does not exist..")
      null
    }
  }

  /**
   *
   * @param table -> Table object
   * @param userid -> userid used as the rowkey to fetch details from the table
   */
  def selectFromTable(table: Table, userid : String) = {
    val logger = LoggerFactory.getLogger(classOf[HBaseCreateInsert])

    val get : Get = new Get(Bytes.toBytes(userid))

    val result : Result = table.get(get)

    if (result != null) {
      for (cell <- result.rawCells()) {
        val family = CellUtil.cloneFamily(cell)
        val column = CellUtil.cloneQualifier(cell)
        val value = CellUtil.cloneValue(cell)
        println(Bytes.toString(family)+ ":{" +
          Bytes.toString(column) + ":" + Bytes.toString(value) +"}")
      }
    } else {
      logger.debug("Error retrieving table data")
    }
  }
}

/**
 * Companion object which orchestrates the selection of of movie ratings data from the corresponding table
 */
object HBaseSelect extends App{
  val logger = LoggerFactory.getLogger(classOf[HBaseCreateInsert])

  if (args.length > 1 ) {
    val hbaseConfigFile = args(0)
    val userid = args(1)

    val hBaseSelect = new HBaseSelect

    val (conn, admin) = HBaseConnection.createConnection(hbaseConfigFile)

    hBaseSelect.selectFromTable(hBaseSelect.checkTableExistance(conn = conn, admin = admin), userid)

    HBaseConnection.closeConnection(conn, admin)
  } else {
    logger.error("Wrong number of arguments passed to the program ...." +
      "\t\t Usage>>" +
      "\t\t\t  HBaseCreateInsert <hbase config file> <user_id to be queried>")
  }
}
