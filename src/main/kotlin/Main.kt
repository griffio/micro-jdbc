import griffio.micro.*
import java.sql.DriverManager

const val jdbc = "jdbc:hsqldb:mem:example-database"

fun main() = closeableScope {

    val connection = DriverManager.getConnection(jdbc).closing()

    val ddl = connection.createStatement().closing()

    ddl.execute("create table people (name varchar(255), age int, the_one boolean)")
    ddl.execute("insert into people (name, age, the_one) values ('Morpheus', 42, false)")
    ddl.execute("insert into people (name, age, the_one) values ('Neo', 31, true)")

    data class Person(val name: String, val age: Int, val theOne: Boolean)

    val personResultSetReader = ResultSetReader {
        val name = readString("name")
        val age = readInt("age")
        val theOne = readBoolean("the_one")
        Person(name, age, theOne)
    }

    val people = connection.createStatement().closing()
        .executeQuery("select * from people order by name").closing()
        .readAll(personResultSetReader)

    people.joinToString("\n") { "${it.name}${if (it.theOne) " 'The One'" else ""} is ${it.age} years old" }
        .also(::println)
    
    val theOne = connection.prepareStatement("select * from people where the_one = ? order by name").closing()
        .bindBoolean(1, true)
        .executeQuery().closing()
        .readAll(personResultSetReader)

    theOne.joinToString("\n") { "${it.name}${if (it.theOne) " 'The One'" else ""} is ${it.age} years old" }
        .also(::println)

}
