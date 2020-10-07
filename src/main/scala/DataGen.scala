import java.sql.{Connection, DriverManager}

import scala.util.Random

object DataGen extends App {

  /*
  TO RUN THE SCRIPT
  -----------------------------------------------------------------------------------------------------------------
  -Using the command line we `cd` into the root of the directory, the root is where the build.sbt resides
  -We the run the command `sbt run`
  -We wait as the data is fetched using GET request and is inserted into the database
  -After completion, the script will run functions that print out the number of unique items in the database


  DDL Queries
  ------------------
  Create Table
 ->CREATE TABLE HW1_Q1_CONTACT(`First Name` varchar(255), `Last Name` varchar(255),State varchar(255),City varchar(255)))

  Prepared statement to insert Customers into Table
 ->INSERT INTO HW1_Q1_CONTACT(`First Name`, `Last Name` ,State ,City ) VALUES(?,?,?,?)

 DML Queries
 -------------------
    Confirm unique customer Rows by counting them to confirm they match up to 5 million
 -> SELECT count(*) FROM (
        SELECT DISTINCT `First Name`, `Last Name` ,State ,City AS customer FROM HW1_Q1_CONTACT)
          AS t)

   Query to get unique cities in table
 -> SELECT COUNT( DISTINCT City) as cities FROM HW1_Q1_CONTACT;"

  Query to get unique First Names in table
 -> SELECT COUNT( DISTINCT `First Name`) as firstNames FROM HW1_Q1_CONTACT;

   Query to get unique Last Names in table
 -> SELECT COUNT( DISTINCT `Last Name`) as lastNames FROM HW1_Q1_CONTACT;

   Query to get unique States in table
 -> SELECT COUNT( DISTINCT State) as states FROM HW1_Q1_CONTACT;


 -------------------------------------------------------------------------------------
 Total Unique States 66
 Total Unique Cities 19813
 Total Unique First Names 77413
 Total Unique Last Names 71854


 Count for all generated Distinct Row items equals to 5 million


   */


  /*

   The source for First Names  and Last Names are from the following Github repository

   https://github.com/smashew/NameDatabases

   Each link resource stores a name per line.
   We use a GET request to fetch the firstnames and lastnames and store each in a List

   Links:

   FIRSTNAMES
   https://raw.githubusercontent.com/smashew/NameDatabases/master/NamesDatabases/surnames/all.txt

   LASTNAME
   https://raw.githubusercontent.com/smashew/NameDatabases/master/NamesDatabases/surnames/all.txt
   */

  var firstNameList = scala.io.Source
    .fromURL("https://raw.githubusercontent.com/smashew/NameDatabases/master/NamesDatabases/surnames/all.txt")
    .getLines()
    .map { firstNameItem =>
      /*
      We remove trailing quotes from the list items
       */
      var doubleQuoteClean = firstNameItem.stripPrefix("\"").stripSuffix("\"")
      var singleQuoteClean = doubleQuoteClean.stripPrefix("\'").stripSuffix("\'")
      singleQuoteClean
    }
    .toArray

  var lastNameList = scala.io.Source
    .fromURL("https://raw.githubusercontent.com/smashew/NameDatabases/master/NamesDatabases/surnames/all.txt")
    .getLines()
    .map { lastNameItem =>
      /*
      We remove trailing quotes from the list items
       */
      var doubleQuoteClean = lastNameItem.stripPrefix("\"").stripSuffix("\"")
      var singleQuoteClean = doubleQuoteClean.stripPrefix("\'").stripSuffix("\'")
      singleQuoteClean
    }
    .toArray


  /*

    The source for cities is stored in a CSV format from the following Github repository.

    https://github.com/dr5hn/countries-states-cities-database

    Link
    CITIES
    https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/master/csv/cities.csv

    Each city item is stored in a CSV row.
    The columns of interest are the City name, first index(1) and country code, fifth index(5).

    We split each line of the csv into an array and filter out all Cities not in the US
    i.e only allow if the fifth item in CSV row == "US"
    after that we remove any trailing quotes that might be in the field.
    We're left with all cities in the US.

   */

  var allCities = scala.io.Source
    .fromURL("https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/master/csv/cities.csv")
    .getLines().toList.tail
    .filter {
      cityRowWithCountry =>
        cityRowWithCountry.split(",")(5) == "US"
    }
    .map { cityRow =>
      cityRow.split(",")(1)
    }
    .map { cityItem =>
      var doubleQuoteClean = cityItem.stripPrefix("\"").stripSuffix("\"")
      var singleQuoteClean = doubleQuoteClean.stripPrefix("\'").stripSuffix("\'")
      singleQuoteClean
    }
    .toArray

  /*
    The source for States is stored in a CSV format from the following Github repository.

    https://github.com/dr5hn/countries-states-cities-database

    Link
    STATES
    https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/master/csv/states.csv


    Each State item is stored in a CSV row.
    The columns of interest are the State name, first index(1) and country code, third index(3).

    We split each line of the csv into an array and filter out all States not in the US
    i.e only allow if the third item in CSV row == "US"
    after that we remove any trailing quotes that might be in the field.
    We take the State name from the first index.
    We're left with all states in the US.

   */
  var allStates = scala.io.Source
    .fromURL("https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/master/csv/states.csv")
    .getLines().toList.tail
    .filter { stateRowWithCountry =>
      stateRowWithCountry.split(",")(3) == "US"
    }
    .map { stateRow =>
      stateRow.split(",")(1)
    }
    .map { stateItem =>
      var doubleQuoteClean = stateItem.stripPrefix("\"").stripSuffix("\"")
      var singleQuoteClean = doubleQuoteClean.stripPrefix("\'").stripSuffix("\'")
      singleQuoteClean
    }
    .toArray


  // connect to the database named "customers" on the localhost

  val driver = "com.mysql.jdbc.Driver"
  /*
  To improve performance we set these flags to JDBC url below useServerPrepStmts=false and rewriteBatchedStatements=true
  as solved here https://stackoverflow.com/a/10617768/5828058.
  This will help our batch inserts work quicker
   */
  val url = "jdbc:mysql://localhost/customers?useServerPrepStmts=false&rewriteBatchedStatements=true"
  val username = "root"
  val password = ""

  var connection: Connection = null

  try {
    // make the connection
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
    val tableStatement = connection.createStatement()
    /*
    We create a statement to create the HW1_Q1_CONTACT table
     */
    val createTableRes = tableStatement.execute("CREATE TABLE HW1_Q1_CONTACT" +
      "(`First Name` varchar(255), " +
      "`Last Name` varchar(255)," +
      "State varchar(255)," +
      "City varchar(255));")

    try {

      var totalUsers = 5000000
      var batchSize = 0;
      var SQL: String = "INSERT INTO HW1_Q1_CONTACT(`First Name`, `Last Name` ,State ,City ) VALUES(?,?,?,?)";
      val createCustomerStatement = connection.prepareStatement(SQL)
      do {
        /*
         For each item, First Name, Last Name , State and City, We choose a random item from their respective list.

         We then create a prepare statement that populates the database with the respective random item.
         */
        var state = allStates(Random.nextInt(allStates.length - 1))
        val city = allCities(Random.nextInt(allCities.length - 1))
        val firstName = firstNameList(Random.nextInt(firstNameList.length - 1))
        val lastName = lastNameList(Random.nextInt(lastNameList.length - 1))
        createCustomerStatement.setString(1, firstName)
        createCustomerStatement.setString(2, lastName)
        createCustomerStatement.setString(3, state)
        createCustomerStatement.setString(4, city)
        /*
        For perfomance, we add batching to the query,
        We add items to the batch variable, whenever the batch gets to 10000 we perform a batch insert of 10000 items.
         */
        batchSize += 1
        val resultSet = createCustomerStatement.addBatch()
        if (batchSize == 10000) {
          /*
          Reset batch size
           */
          batchSize = 0
          createCustomerStatement.executeLargeBatch()
          createCustomerStatement.clearBatch()
        }
        println(totalUsers)

        totalUsers -= 1
      } while (totalUsers >= 0)
      createCustomerStatement.close()
    } catch {
      case ex: Exception =>
        ex.printStackTrace

    }
  } catch {
    case e => e.printStackTrace
  }
  connection.close()

  /*
      We count all the unique generated Item Rows using the DISTINCT keyword in with following SQL statement

      The number of distinct Rows should equal to 5 million, we check that by doing an equality check with
      the count result i.e. Counted Unique result == 5 million

     SELECT count(*) FROM (
        SELECT DISTINCT `First Name`, `Last Name` ,State ,City AS customer FROM HW1_Q1_CONTACT)
          AS t)


     */
  def confirmItemsAreUnique(): Boolean = {
    var countResult = 0
    try {
      val driver = "com.mysql.jdbc.Driver"
      val url = "jdbc:mysql://localhost/customers"
      val username = "root"
      val password = ""
      // make the connection
      Class.forName(driver)
      var connection = DriverManager.getConnection(url, username, password)
      val tableStatement = connection.createStatement()
      // create the statement, and run the select query
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("" +
        "SELECT count(*) FROM" +
        " (SELECT DISTINCT `First Name`, `Last Name` ,State ,City AS customer FROM HW1_Q1_CONTACT) AS t" +
        "")
      while (resultSet.next()) {
        countResult = resultSet.getInt("count(*)")
      }
      connection.close()
      return countResult == 5000000
    } catch {
      case e => e.printStackTrace
        return false
    }

  }

  /*
    We count all the unique cities using the DISTINCT keyword in with following SQL statement
     SELECT COUNT( DISTINCT City) as cities FROM HW1_Q1_CONTACT;"
   */

  def uniqueCitiesCount(): Int = {
    var uniqueCities = 0
    try {
      val driver = "com.mysql.jdbc.Driver"
      val url = "jdbc:mysql://localhost/customers"
      val username = "root"
      val password = ""
      // make the connection
      Class.forName(driver)
      var connection = DriverManager.getConnection(url, username, password)
      val tableStatement = connection.createStatement()
      // create the statement, and run the select query
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("" +
        "  SELECT COUNT( DISTINCT City) as cities" +
        "  FROM HW1_Q1_CONTACT;" +
        "")
      while (resultSet.next()) {
        return resultSet.getInt("cities")
      }
      connection.close()
      return uniqueCities
    } catch {
      case e => e.printStackTrace
        uniqueCities
    }
  }

  /*
   We count all the unique First Names using the DISTINCT keyword in with following SQL statement
    SELECT COUNT( DISTINCT `First Name`) as firstNames FROM HW1_Q1_CONTACT;
 */
  def uniqueFirstNameCount(): Int = {
    var uniqueFirstNames = 0
    try {
      val driver = "com.mysql.jdbc.Driver"
      val url = "jdbc:mysql://localhost/customers"
      val username = "root"
      val password = ""
      // make the connection
      Class.forName(driver)
      var connection = DriverManager.getConnection(url, username, password)
      val tableStatement = connection.createStatement()
      // create the statement, and run the select query
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("" +
        "  SELECT COUNT( DISTINCT `First Name`) as firstNames" +
        "  FROM HW1_Q1_CONTACT;" +
        "")
      while (resultSet.next()) {
        return resultSet.getInt("firstNames")
      }
      connection.close()
      return uniqueFirstNames
    } catch {
      case e => e.printStackTrace
        uniqueFirstNames
    }
  }

  /*
   We count all the unique Last Names using the DISTINCT keyword in with following SQL statement
    SELECT COUNT( DISTINCT `Last Name`) as lastNames FROM HW1_Q1_CONTACT;
 */
  def uniqueLastNameCount(): Int = {
    var uniqueLastNames = 0
    try {
      val driver = "com.mysql.jdbc.Driver"
      val url = "jdbc:mysql://localhost/customers"
      val username = "root"
      val password = ""
      // make the connection
      Class.forName(driver)
      var connection = DriverManager.getConnection(url, username, password)
      val tableStatement = connection.createStatement()
      // create the statement, and run the select query
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("" +
        "  SELECT COUNT( DISTINCT `Last Name`) as lastNames" +
        "  FROM HW1_Q1_CONTACT;" +
        "")
      while (resultSet.next()) {
        return resultSet.getInt("lastNames")
      }
      connection.close()
      return uniqueLastNames
    } catch {
      case e => e.printStackTrace
        uniqueLastNames
    }
  }

  /*
   We count all the unique States using the DISTINCT keyword in with following SQL statement
    SELECT COUNT( DISTINCT State) as states FROM HW1_Q1_CONTACT;"
  */
  def uniqueStatesCount(): Int = {
    var uniqueStates = 0
    try {
      val driver = "com.mysql.jdbc.Driver"
      val url = "jdbc:mysql://localhost/customers"
      val username = "root"
      val password = ""
      // make the connection
      Class.forName(driver)
      var connection = DriverManager.getConnection(url, username, password)
      val tableStatement = connection.createStatement()
      // create the statement, and run the select query
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("" +
        "  SELECT COUNT( DISTINCT State) as states" +
        "  FROM HW1_Q1_CONTACT;" +
        "")
      while (resultSet.next()) {
        return resultSet.getInt("states")
      }
      connection.close()
      return uniqueStates
    } catch {
      case e => e.printStackTrace
        uniqueStates
    }
  }

  println(s"All Generated rows equal to 5 million ${confirmItemsAreUnique()}")
  println(s"Total of unique cities counted ${uniqueCitiesCount()}")
  println(s"Total of unique States counted ${uniqueStatesCount()}")
  println(s"Total of First Names counted ${uniqueFirstNameCount()}")
  println(s"Total of Last Names counted ${uniqueLastNameCount()}")
}