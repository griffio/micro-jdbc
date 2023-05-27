package griffio

import griffio.CoffeesTable
import java.sql.*

internal class CoffeesTable(private val con: Connection?, private val dbName: String, private val dbms: String) {
    @Throws(SQLException::class)
    fun createTable() {
        val createString =
            "create table COFFEES (COF_NAME varchar(32) NOT NULL, SUP_ID int NOT NULL, PRICE numeric(10,2) NOT NULL, SALES integer NOT NULL, TOTAL integer NOT NULL, PRIMARY KEY (COF_NAME), FOREIGN KEY (SUP_ID) REFERENCES SUPPLIERS (SUP_ID))".formatted()
        try {
            con!!.createStatement().use { stmt -> stmt.executeUpdate(createString) }
        } catch (e: SQLException) {
        }
    }

    @Throws(SQLException::class)
    fun populateTable() {
        try {
            con!!.createStatement().use { stmt ->
                stmt.executeUpdate(
                    "insert into COFFEES " +
                            "values('Colombian', 00101, 7.99, 0, 0)"
                )
                stmt.executeUpdate(
                    "insert into COFFEES " +
                            "values('French_Roast', 00049, 8.99, 0, 0)"
                )
                stmt.executeUpdate(
                    "insert into COFFEES " +
                            "values('Espresso', 00150, 9.99, 0, 0)"
                )
                stmt.executeUpdate(
                    "insert into COFFEES " +
                            "values('Colombian_Decaf', 00101, 8.99, 0, 0)"
                )
                stmt.executeUpdate(
                    "insert into COFFEES " +
                            "values('French_Roast_Decaf', 00049, 9.99, 0, 0)"
                )
            }
        } catch (e: SQLException) {
        }
    }

    @Throws(SQLException::class)
    fun updateCoffeeSales(salesForWeek: HashMap<String?, Int?>) {
        val updateString = "update COFFEES set SALES = ? where COF_NAME = ?"
        val updateStatement = "update COFFEES set TOTAL = TOTAL + ? where COF_NAME = ?"
        try {
            con!!.prepareStatement(updateString).use { updateSales ->
                con.prepareStatement(updateStatement).use { updateTotal ->
                    con.autoCommit = false
                    for ((key, value) in salesForWeek) {
                        updateSales.setInt(1, value!!)
                        updateSales.setString(2, key)
                        updateSales.executeUpdate()
                        updateTotal.setInt(1, value)
                        updateTotal.setString(2, key)
                        updateTotal.executeUpdate()
                        con.commit()
                    }
                }
            }
        } catch (e: SQLException) {
            if (con != null) {
                try {
                    System.err.print("Transaction is being rolled back")
                    con.rollback()
                } catch (excep: SQLException) {
                }
            }
        }
    }

    @Throws(SQLException::class)
    fun modifyPrices(percentage: Float) {
        try {
            con!!.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE).use { stmt ->
                val uprs = stmt.executeQuery("SELECT * FROM COFFEES")
                while (uprs.next()) {
                    val f = uprs.getFloat("PRICE")
                    uprs.updateFloat("PRICE", f * percentage)
                    uprs.updateRow()
                }
            }
        } catch (e: SQLException) {
        }
    }

    @Throws(SQLException::class)
    fun modifyPricesByPercentage(
        coffeeName: String,
        priceModifier: Float,
        maximumPrice: Float
    ) {
        con!!.autoCommit = false
        var rs: ResultSet? = null
        val priceQuery = "SELECT COF_NAME, PRICE FROM COFFEES " +
                "WHERE COF_NAME = ?"
        val updateQuery = "UPDATE COFFEES SET PRICE = ? " +
                "WHERE COF_NAME = ?"
        try {
            con.prepareStatement(priceQuery, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
                .use { getPrice ->
                    con.prepareStatement(updateQuery).use { updatePrice ->
                        val save1 = con.setSavepoint()
                        getPrice.setString(1, coffeeName)
                        if (!getPrice.execute()) {
                            println("Could not find entry for coffee named $coffeeName")
                        } else {
                            rs = getPrice.resultSet
                            rs.first()
                            val oldPrice = rs.getFloat("PRICE")
                            val newPrice = oldPrice + oldPrice * priceModifier
                            System.out.printf("Old price of %s is $%.2f%n", coffeeName, oldPrice)
                            System.out.printf("New price of %s is $%.2f%n", coffeeName, newPrice)
                            println("Performing update...")
                            updatePrice.setFloat(1, newPrice)
                            updatePrice.setString(2, coffeeName)
                            updatePrice.executeUpdate()
                            println("\nCOFFEES table after update:")
                            viewTable(con)
                            if (newPrice > maximumPrice) {
                                System.out.printf(
                                    "The new price, $%.2f, is greater " +
                                            "than the maximum price, $%.2f. " +
                                            "Rolling back the transaction...%n",
                                    newPrice, maximumPrice
                                )
                                con.rollback(save1)
                                println("\nCOFFEES table after rollback:")
                                viewTable(con)
                            }
                            con.commit()
                        }
                    }
                }
        } catch (e: SQLException) {
        } finally {
            con.autoCommit = true
        }
    }

    @Throws(SQLException::class)
    fun insertRow(
        coffeeName: String?, supplierID: Int, price: Float,
        sales: Int, total: Int
    ) {
        try {
            con!!.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE).use { stmt ->
                val uprs = stmt.executeQuery("SELECT * FROM COFFEES")
                uprs.moveToInsertRow()
                uprs.updateString("COF_NAME", coffeeName)
                uprs.updateInt("SUP_ID", supplierID)
                uprs.updateFloat("PRICE", price)
                uprs.updateInt("SALES", sales)
                uprs.updateInt("TOTAL", total)
                uprs.insertRow()
                uprs.beforeFirst()
            }
        } catch (e: SQLException) {
        }
    }

    @Throws(SQLException::class)
    fun batchUpdate() {
        con!!.autoCommit = false
        try {
            con.createStatement().use { stmt ->
                stmt.addBatch(
                    "INSERT INTO COFFEES " +
                            "VALUES('Amaretto', 49, 9.99, 0, 0)"
                )
                stmt.addBatch(
                    "INSERT INTO COFFEES " +
                            "VALUES('Hazelnut', 49, 9.99, 0, 0)"
                )
                stmt.addBatch(
                    "INSERT INTO COFFEES " +
                            "VALUES('Amaretto_decaf', 49, 10.99, 0, 0)"
                )
                stmt.addBatch(
                    "INSERT INTO COFFEES " +
                            "VALUES('Hazelnut_decaf', 49, 10.99, 0, 0)"
                )
                val updateCounts = stmt.executeBatch()
                con.commit()
            }
        } catch (b: BatchUpdateException) {
        } catch (ex: SQLException) {
        } finally {
            con.autoCommit = true
        }
    }

    @get:Throws(SQLException::class)
    val keys: Set<String>
        get() {
            val keys = HashSet<String>()
            val query = "select COF_NAME from COFFEES"
            try {
                con!!.createStatement().use { stmt ->
                    val rs = stmt.executeQuery(query)
                    while (rs.next()) {
                        keys.add(rs.getString(1))
                    }
                }
            } catch (e: SQLException) {
            }
            return keys
        }

    @Throws(SQLException::class)
    fun dropTable() {
        try {
            con!!.createStatement().use { stmt ->
                if (dbms == "mysql") {
                    stmt.executeUpdate("DROP TABLE IF EXISTS COFFEES")
                } else if (dbms == "derby") {
                    stmt.executeUpdate("DROP TABLE COFFEES")
                }
            }
        } catch (e: SQLException) {
        }
    }

    companion object {
        @Throws(SQLException::class)
        fun viewTable(con: Connection?) {
            val query = "select COF_NAME, SUP_ID, PRICE, SALES, TOTAL from COFFEES"
            try {
                con!!.createStatement().use { stmt ->
                    val rs = stmt.executeQuery(query)
                    while (rs.next()) {
                        val coffeeName = rs.getString("COF_NAME")
                        val supplierID = rs.getInt("SUP_ID")
                        val price = rs.getFloat("PRICE")
                        val sales = rs.getInt("SALES")
                        val total = rs.getInt("TOTAL")
                        println(
                            coffeeName + ", " + supplierID + ", " + price +
                                    ", " + sales + ", " + total
                        )
                    }
                }
            } catch (e: SQLException) {
            }
        }

        @Throws(SQLException::class)
        fun alternateViewTable(con: Connection) {
            val query = "select COF_NAME, SUP_ID, PRICE, SALES, TOTAL from COFFEES"
            try {
                con.createStatement().use { stmt ->
                    val rs = stmt.executeQuery(query)
                    while (rs.next()) {
                        val coffeeName = rs.getString(1)
                        val supplierID = rs.getInt(2)
                        val price = rs.getFloat(3)
                        val sales = rs.getInt(4)
                        val total = rs.getInt(5)
                        println(
                            coffeeName + ", " + supplierID + ", " + price +
                                    ", " + sales + ", " + total
                        )
                    }
                }
            } catch (e: SQLException) {
            }
        }

        @JvmStatic
        fun main(args: Array<String>) {
            var myConnection: Connection? = null
            myConnection = null
            !! // Java DB does not have an SQL create database command; it does require createDatabase
            //      JDBCTutorialUtilities.createDatabase(myConnection,
            //                                           myJDBCTutorialUtilities.dbName,
            //                                           myJDBCTutorialUtilities.dbms);
            //
            //      JDBCTutorialUtilities.initializeTables(myConnection,
            //                                             myJDBCTutorialUtilities.dbName,
            //                                             myJDBCTutorialUtilities.dbms);
            CoffeesTable
            myCoffeeTable = CoffeesTable(
                myConnection, myJDBCTutorialUtilities.dbName,
                myJDBCTutorialUtilities.dbms
            )
            println("\nContents of COFFEES table:")
            viewTable(myConnection)
            println("\nRaising coffee prices by 25%")
            myCoffeeTable.modifyPrices(1.25f)
            println("\nInserting a new row:")
            myCoffeeTable.insertRow("Kona", 150, 10.99f, 0, 0)
            viewTable(myConnection)
            println("\nUpdating sales of coffee per week:")
            val salesCoffeeWeek = HashMap<String, Int>()
            salesCoffeeWeek["Colombian"] = 175
            salesCoffeeWeek["French_Roast"] = 150
            salesCoffeeWeek["Espresso"] = 60
            salesCoffeeWeek["Colombian_Decaf"] = 155
            salesCoffeeWeek["French_Roast_Decaf"] = 90
            myCoffeeTable.updateCoffeeSales(salesCoffeeWeek)
            viewTable(myConnection)
            println("\nModifying prices by percentage")
            myCoffeeTable.modifyPricesByPercentage("Colombian", 0.10f, 9.00f)
            println("\nCOFFEES table after modifying prices by percentage:")
            myCoffeeTable.viewTable(myConnection)
            println("\nPerforming batch updates; adding new coffees")
            myCoffeeTable.batchUpdate()
            myCoffeeTable.viewTable(myConnection)

//      System.out.println("\nDropping Coffee and Suplliers table:");
//      
//      myCoffeeTable.dropTable();
//      mySuppliersTable.dropTable(
        }
    }
}
