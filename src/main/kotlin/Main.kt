import griffio.micro.*
import java.sql.DriverManager
import java.sql.Statement.RETURN_GENERATED_KEYS

const val jdbc = "jdbc:hsqldb:mem:matrix-database"

fun main() = closeableScope {

    val connection = DriverManager.getConnection(jdbc).closing()

    val ddl = connection.createStatement().closing()

    ddl.execute("create table avatars (id int primary key generated always as identity (START WITH 1), name varchar(255), age int, the_one boolean)")
    ddl.execute("insert into avatars (name, age, the_one) values ('Morpheus', 42, false)")
    ddl.execute("insert into avatars (name, age, the_one) values ('Neo', 31, true)")

    data class Avatar(val id: Int, val name: String, val age: Int, val theOne: Boolean)

    val avatarResultSetReader = ResultSetReader {
        val id: Int = readInt("id")
        val name = readString("name")
        val age = readInt("age")
        val theOne = readBoolean("the_one")
        Avatar(id, name, age, theOne)
    }

    val avatars = connection.createStatement().closing()
        .executeQuery("select * from avatars order by name").closing()
        .readAll(avatarResultSetReader)

    avatars.joinToString("\n") { "${it.name}${if (it.theOne) " 'The One'" else ""} is ${it.age} years old" }
        .also(::println)

    val theOne = connection.transaction {
        prepareStatement("select * from avatars where the_one = ? order by name").closing()
            .bindBoolean(1, true)
            .executeQuery().closing()
            .readOne(avatarResultSetReader)
    }

    println(theOne)

    val sql = "insert into avatars (name, age, the_one) values(?, ?, ?)";

    val trinityId = connection.prepareStatement(sql, RETURN_GENERATED_KEYS).closing()
        .bindString(1, "Trinity")
        .bindInt(2, 33)
        .bindBoolean(3, false)
        .executeUpdateAndGeneratedKeys().closing().readOne {
            readInt("id")
        }

    println("Trinity is $trinityId")

    val morpheus = connection.transaction {

        prepareStatement("select * from avatars where name = ?").closing()
            .bindString(1, "Morpheus")
            .executeQuery().closing()
            .readOneOrNull(avatarResultSetReader)
    }

    println(morpheus)

    fun newAvatar(name: String, age: Int, theOne: Boolean): Avatar = closeableScope {
        connection.transaction {
            prepareStatement(
                "insert into avatars (name, age, the_one) values(?, ?, ?)",
                RETURN_GENERATED_KEYS
            ).closing()
                .bindString(1, name)
                .bindInt(2, age)
                .bindBoolean(3, theOne)
                .executeUpdateAndGeneratedKeys().closing()
                .readOne { Avatar(readInt("id"), name, age, theOne) }
        }
    }

    val agentSmith = newAvatar("Agent Smith", 46, false)

    println(agentSmith)

}
