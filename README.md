# Micro Jdbc for Kotlin

A couple of ideas with extensions for using sequential Jdbc resources and extracting result sets

### Example

Jdbc autoClosable objects are closed in the scope of the lambda

`closing` is an extension method on `AutoClosable` types

[ClosableScope.kt](https://github.com/griffio/micro-jdbc/blob/master/src/main/kotlin/griffio/micro/ClosableScope.kt)

```kotlin

    data class Person(val name: String, val age: Int, val theOne: Boolean)

    val personResultSetReader = ResultSetReader {
        val name = readString("name")
        val age = readInt("age")
        val theOne = readBoolean("the_one")
        Person(name, age, theOne)
    }

    closeableScope {
        val people = DriverManager.getConnection("jdbc:h2:mem:test").closing()
            .createStatement().closing()
            .executeQuery("select * from people order by name").closing()
            .readAll(personResultSetReader)
    }
```


#### Transaction scope

``` kotlin
fun main() = closeableScope {
    
    val connection = DriverManager.getConnection("jdbc:h2:mem:matrix").closing()
    
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
}   
```
