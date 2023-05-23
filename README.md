# Micro Jdbc for Kotlin

### Example

Jdbc autoClosable objects are closed in the scope of the lambda

`closing` is an extension method on `AutoClosable` types

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
