package griffio.coffees

import griffio.micro.*
import java.lang.System.*
import java.sql.*
import java.sql.ResultSet.*

//https://docs.oracle.com/javase/tutorial/displayCode.html?code=https://docs.oracle.com/javase/tutorial/jdbc/basics/examples/JDBCTutorial/src/com/oracle/tutorial/jdbc/CoffeesTable.java
class CoffeesTable(private val con: Connection, private val dbName: String, private val dbms: String) {
    fun createTable() = closeableScope {
        val createCoffees = """
                |create table COFFEES (
                |COF_NAME varchar(32) NOT NULL,
                |SUP_ID int NOT NULL,
                |PRICE numeric(10,2) NOT NULL,
                |SALES integer NOT NULL,
                |TOTAL integer NOT NULL, 
                |PRIMARY KEY (COF_NAME))""".trimMargin()
        con.createStatement().closing()
            .executeUpdate(createCoffees)
            .run { println("Created COFFEES table in $dbName database") }
    }

    fun populateTable() = closeableScope {
        con.createStatement().closing().run {
            executeUpdate("insert into COFFEES values('Colombian', 00101, 7.99, 0, 0)")
            executeUpdate("insert into COFFEES values('French_Roast', 00049, 8.99, 0, 0)")
            executeUpdate("insert into COFFEES values('Espresso', 00150, 9.99, 0, 0)")
            executeUpdate("insert into COFFEES values('Colombian_Decaf', 00101, 8.99, 0, 0)")
            executeUpdate("insert into COFFEES values('French_Roast_Decaf', 00049, 9.99, 0, 0)").run {
                println("Populated COFFEES table")
            }
        }
    }

    fun updateCoffeeSales(salesForWeek: Map<String, Int>) {
        val updateString = "update COFFEES set SALES = ? where COF_NAME = ?"
        val updateStatement = "update COFFEES set TOTAL = TOTAL + ? where COF_NAME = ?"
        con.transaction {
            closeableScope {
                val updateSales = prepareStatement(updateString).closing()
                val updateTotal = prepareStatement(updateStatement).closing()
                for ((key, value) in salesForWeek) {
                    updateSales.bindInt(1, value).bindString(2, key).executeUpdate()
                    updateTotal.bindInt(1, value).bindString(2, key).executeUpdate()
                }
            }
        }
    }

    fun modifyPrices(percentage: Float) = closeableScope {
        con.createStatement(TYPE_SCROLL_SENSITIVE, CONCUR_UPDATABLE).closing()
            .executeQuery("SELECT * FROM COFFEES").closing().run {
                while (next()) {
                    val f = getFloat("PRICE")
                    updateFloat("PRICE", f * percentage)
                    updateRow()
                }
            }
    }

    fun modifyPricesByPercentage(
        coffeeName: String,
        priceModifier: Float,
        maximumPrice: Float,
    ) {
        con.transaction {
            val priceQuery = "SELECT COF_NAME, PRICE FROM COFFEES WHERE COF_NAME = ?"
            val updateQuery = "UPDATE COFFEES SET PRICE = ? WHERE COF_NAME = ?"
            val priceReader = ResultSetReader { readFloat("PRICE") }
            closeableScope {
                val oldPrice = prepareStatement(priceQuery, TYPE_SCROLL_INSENSITIVE, CONCUR_READ_ONLY).closing()
                    .bindString(1, coffeeName)
                    .executeQuery().closing()
                    .readOne(priceReader)
                val save1 = setSavepoint()
                val newPrice = oldPrice + oldPrice * priceModifier
                out.printf("Old price of %s is $%.2f%n", coffeeName, oldPrice)
                out.printf("New price of %s is $%.2f%n", coffeeName, newPrice)
                println("Performing update...")
                prepareStatement(updateQuery).closing()
                    .bindFloat(1, newPrice)
                    .bindString(2, coffeeName)
                    .executeUpdate()
                println("\nCOFFEES table after update:")
                viewTable()
                if (newPrice > maximumPrice) {
                    out.printf(
                        "The new price, $%.2f, is greater " +
                                "than the maximum price, $%.2f. " +
                                "Rolling back the transaction...%n",
                        newPrice, maximumPrice
                    )
                    rollback(save1)
                    println("\nCOFFEES table after rollback:")
                    viewTable()
                }
            }
        }
    }

    fun insertRow(
        coffeeName: String, supplierID: Int, price: Float,
        sales: Int, total: Int,
    ) = closeableScope {
        con.createStatement(TYPE_SCROLL_SENSITIVE, CONCUR_UPDATABLE).closing()
            .executeQuery("SELECT * FROM COFFEES").closing().run {
                moveToInsertRow()
                updateString("COF_NAME", coffeeName)
                updateInt("SUP_ID", supplierID)
                updateFloat("PRICE", price)
                updateInt("SALES", sales)
                updateInt("TOTAL", total)
                insertRow()
                beforeFirst()
            }
    }

    fun batchUpdate() {

        val coffeeBatch = listOf(
            "INSERT INTO COFFEES VALUES('Amaretto', 49, 9.99, 0, 0)",
            "INSERT INTO COFFEES VALUES('Hazelnut', 49, 9.99, 0, 0)",
            "INSERT INTO COFFEES VALUES('Amaretto_decaf', 49, 10.99, 0, 0)",
            "INSERT INTO COFFEES VALUES('Hazelnut_decaf', 49, 10.99, 0, 0)"
        )

        val updateCounts = con.transaction {
            closeableScope {
                createStatement().closing()
                    .addBatch(coffeeBatch)
                    .executeBatch()
            }
        }

        updateCounts?.size == coffeeBatch.size || error("Expected ${coffeeBatch.size} updates but got ${updateCounts?.size}")
    }

    fun getKeys() = closeableScope {
        val query = "select COF_NAME from COFFEES"
        con.createStatement().closing()
            .executeQuery(query).closing()
            .readAll {
                readString("COF_NAME")
            }.toSet()
    }

    fun dropTable() = closeableScope {
        val drop = if (dbms == "mysql") "DROP TABLE IF EXISTS COFFEES" else "DROP TABLE COFFEES"
        con.createStatement().closing().executeUpdate(drop)
    }

    fun viewTable() = closeableScope {
        val query = "select COF_NAME, SUP_ID, PRICE, SALES, TOTAL from COFFEES"
        con.createStatement().closing().executeQuery(query).closing().readAll {
            val coffeeName = readString("COF_NAME")
            val supplierID = readInt("SUP_ID")
            val price = readFloat("PRICE")
            val sales = readInt("SALES")
            val total = readInt("TOTAL")
            println("$coffeeName, $supplierID, $price, $sales, $total")
        }
    }
}

//const val jdbc = "jdbc:hsqldb:mem:coffee-database"
const val jdbc = "jdbc:postgresql://localhost:5432/coffees"

fun main(): Unit = closeableScope {

    val myConnection = DriverManager.getConnection(jdbc).closing()

    val myCoffeeTable = CoffeesTable(
        myConnection, "coffees",
        "postgres"
    )

    myCoffeeTable.createTable()
    myCoffeeTable.populateTable()
    println("\nContents of COFFEES table:")
    myCoffeeTable.viewTable()
    println("\nRaising coffee prices by 25%")
    myCoffeeTable.modifyPrices(1.25f)
    println("\nInserting a new row:")
    myCoffeeTable.insertRow("Kona", 150, 10.99f, 0, 0)
    myCoffeeTable.viewTable()
    println("\nUpdating sales of coffee per week:")
    val salesCoffeeWeek = HashMap<String, Int>()
    salesCoffeeWeek["Colombian"] = 175
    salesCoffeeWeek["French_Roast"] = 150
    salesCoffeeWeek["Espresso"] = 60
    salesCoffeeWeek["Colombian_Decaf"] = 155
    salesCoffeeWeek["French_Roast_Decaf"] = 90
    myCoffeeTable.updateCoffeeSales(salesCoffeeWeek)
    myCoffeeTable.viewTable()
    println("\nModifying prices by percentage")
    myCoffeeTable.modifyPricesByPercentage("Colombian", 0.10f, 9.00f)
    println("\nCOFFEES table after modifying prices by percentage:")
    myCoffeeTable.viewTable()
    println("\nPerforming batch updates; adding new coffees")
    myCoffeeTable.batchUpdate()
    myCoffeeTable.viewTable()

    println("\nDropping Coffee and Suppliers table:")

    val dropTable = myCoffeeTable.dropTable();

    println("dropped table: $dropTable")
}
