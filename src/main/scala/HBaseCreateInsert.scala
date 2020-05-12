import java.util

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Admin, ColumnFamilyDescriptorBuilder, Connection, Put, Table, TableDescriptor, TableDescriptorBuilder}
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.LoggerFactory

import scala.io.Source

/**
 * Class for creating an HBase table (ratings) and inserting data from the ratings file, accepted as an argument to the
 * program
 */
class HBaseCreateInsert {

  /**
   * This function prepares the dataset from the movie ratings file and uploads into the ratings HTable
   * The function uses a bulk insert approach.
   * The function also ensures batching data passed to the bulk insert function to avoid memory issues.
   *
   * @param fileName -> Filename containing movie ratings data
   * @param table -> The HTable to which the ratings data will be inserted
   */
  def preparingDataSet(fileName: String, table: Table) : Unit = {

    val logger = LoggerFactory.getLogger(classOf[HBaseCreateInsert])

    val source = Source.fromFile(fileName)

    var upperLimit : Int = 50000
    var puts: util.List[Put] = new util.ArrayList[Put](upperLimit)

    val ratingCF = Bytes.toBytes("rating")

    var noLines= 1

    val lines = source.getLines().toList

    val length =  lines.length

    if (length < upperLimit)
      upperLimit = length

    logger.debug("length = "+ length + ": upperlimit = "+ upperLimit)
    var calculatedLength = length

    for (line <- lines) {
      processLine(line)
      if ( noLines == upperLimit ) {
        logger.debug("length of the array => "+ calculatedLength + ": no lines processed ="+ noLines)
        calculatedLength = calculatedLength - noLines
        logger.debug("... remaining length of array = "+calculatedLength+"\n")
        if (calculatedLength < upperLimit)
          upperLimit = calculatedLength
        logger.debug("new upper limit = "+ upperLimit)
        noLines = 1
        bulkInsert(table, puts)
        puts = new util.ArrayList[Put](upperLimit)
      } else
        noLines += 1
    }

    /**
     * Function to process each line of the file (array) and adds to the Put method, used later for bulk insertion
     * @param line -> Each line of the file (array)
     */
    def processLine(line: String): Unit = {
      val fields = line.split(",")
      val put: Put = new Put(Bytes.toBytes(fields(0)))
      put.addColumn(ratingCF, Bytes.toBytes(fields(1)), Bytes.toBytes(fields(2)))
      puts.add(put)
    }
  }

  /**
   *  Function drops an existing table and recreates it.
   * @param conn -> Connection object
   * @param admin -> Admin object
   * @return -> returns the table object
   */
  def createTable(conn : Connection, admin : Admin) : Table = {

    val tableName : TableName = TableName.valueOf(HBaseConnection.RATINGS_TABLE)

    val tableDescriptor  : TableDescriptor = TableDescriptorBuilder.
      newBuilder(tableName).
      setColumnFamily(ColumnFamilyDescriptorBuilder.of(HBaseConnection.RATING_COLUMNFAMILY)).build()

    if (admin.isTableAvailable(tableName)) {
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
    }
    admin.createTable(tableDescriptor)

    conn.getTable(tableName)
  }

  /**
   * Function to perform bulk insertion
   * @param table -> Table object into which the data is inserted
   * @param puts -> Bulk Put object
   */
  def bulkInsert(table: Table, puts : util.List[Put]) : Unit = {
    val logger = LoggerFactory.getLogger(classOf[HBaseCreateInsert])
    try{
      table.put(puts)
      logger.debug("Inserted into the table")
    } catch {
      case exception: Exception => exception.printStackTrace()
    }
  }
}

/**
 * Companion object which orchestrates the creation and insertion of movie ratings data from a file
 */
object HBaseCreateInsert extends  App{
  val logger = LoggerFactory.getLogger(classOf[HBaseCreateInsert])

  if (args.length > 1 ) {
    val hbaseConfigFile = args(0)
    val fileName = args(1)

    val createInsert = new HBaseCreateInsert

    val (conn, admin) = HBaseConnection.createConnection(hbaseConfigFile)

    val table: Table = createInsert.createTable(conn, admin)

    createInsert.preparingDataSet(fileName = fileName,
      table = table)

    HBaseConnection.closeConnection(conn, admin)
  } else {
    logger.error("Wrong number of arguments passed to the program ...." +
      "\t\t Usage>>" +
      "\t\t\t  HBaseCreateInsert <hbase config file> <ratings file to be loaded>")
  }
}
