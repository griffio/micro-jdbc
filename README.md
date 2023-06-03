# Micro Jdbc for Kotlin

Instead of trying to guess your [data access with Kotlin](https://griffio.github.io/programming/2023/04/20/More-Kotlin-Sql-Persistence-Comparision/),
this is a collection of Kotlin extension methods for Jdbc that can be adapted to your own projects initially.

There is no library and the utility methods can be copy pasted and extended as required.

This is the first rung on the ladder to decide if you need a jdbc wrapper library like https://github.com/jdbi/jdbi or https://github.com/target/lite-for-jdbc and [so on](https://griffio.github.io/programming/2023/04/20/More-Kotlin-Sql-Persistence-Comparision/).

A couple of ideas with extensions for cleaning up using sequential Jdbc resources and extracting result sets.

### Example

Jdbc autoClosable objects are closed in the scope of the lambda

`closing` is an extension method on `AutoClosable` types

[ClosableScope.kt](https://github.com/griffio/micro-jdbc/blob/master/src/main/kotlin/griffio/micro/ClosableScope.kt)

```kotlin

    data class Person(val name: String, val age: Int)

    val personResultSetReader = ResultSetReader {
        val name = readString("name")
        val age = readInt("age")
        Person(name, age)
    }

    closeableScope {
        val people = DriverManager.getConnection("jdbc:h2:mem:example").closing()
            .createStatement().closing()
            .executeQuery("select * from people order by name").closing()
            .readAll(personResultSetReader)
    }
```

#### Transaction scope

``` kotlin
fun main() = closeableScope {
    
    val connection = DriverManager.getConnection("jdbc:h2:mem:matrix").closing()
    
    val ddl = connection.createStatement().closing()

    ddl.execute("""
        |create table avatars (id int primary key generated always as identity (START WITH 1),
        |name varchar(255),
        |age int,
        |the_one boolean)""".trimMargin())
            
    fun newAvatar(name: String, age: Int, theOne: Boolean): Avatar = closeableScope {
        connection.prepareStatement(
            "insert into avatars (name, age, the_one) values(?, ?, ?)",
            RETURN_GENERATED_KEYS
        ).closing()
            .bindString(1, name)
            .bindInt(2, age)
            .bindBoolean(3, theOne)
            .executeUpdateAndGeneratedKeys().closing()
            .readOne { Avatar(readInt("id"), name, age, theOne) }

    }

    connection.transaction {
        val agentSmith = newAvatar("Agent Smith", 46, false)
        val theOracle = newAvatar("The Oracle", 101, false)
        println(agentSmith)
        println(theOracle)
    }

}   
```

### Todo

Context receivers
https://github.com/Kotlin/KEEP/blob/master/proposals/context-receivers.md#use-cases
