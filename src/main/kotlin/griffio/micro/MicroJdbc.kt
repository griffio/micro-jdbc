package griffio.micro

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement

// see https://github.com/elizarov/SerializationByConvention#reading-objects-from-db
// see https://github.com/spring-projects/spring-framework/blob/main/spring-jdbc/src/main/java/org/springframework/jdbc/support/JdbcUtils.java
fun interface ResultSetReader<out T> {
    fun ResultSet.read(): T
}

fun ResultSet.readInt(name: String): Int = getInt(name)

fun ResultSet.readString(name: String): String = getString(name)

fun ResultSet.readBoolean(name: String): Boolean = getBoolean(name)

fun ResultSet.readFloat(name: String): Float = getFloat(name)

fun <T> ResultSet.readAll(reader: ResultSetReader<T>): List<T> {
    val list = mutableListOf<T>()
    while (next()) list.add(read(reader))
    return list
}

fun <T> ResultSet.readOne(reader: ResultSetReader<T>): T {
    check(next()) { "Expected one result, empty result found" }
    val result = read(reader)
    check(!next()) { "Expected one result, more results found" }
    return result
}

fun <T> ResultSet.readOneOrNull(reader: ResultSetReader<T>): T? = if (next()) read(reader) else null

fun <T> ResultSet.read(reader: ResultSetReader<T>): T = with(reader) { read() }

fun Statement.addBatch(batchSql: List<String>): Statement = apply { batchSql.forEach { addBatch(it) } }

fun PreparedStatement.bindInt(index: Int, value: Int): PreparedStatement = apply { setInt(index, value) }

fun PreparedStatement.bindString(index: Int, value: String): PreparedStatement = apply { setString(index, value) }

fun PreparedStatement.bindBoolean(index: Int, value: Boolean): PreparedStatement = apply { setBoolean(index, value) }

fun PreparedStatement.bindFloat(index: Int, value: Float): PreparedStatement = apply { setFloat(index, value) }

fun PreparedStatement.executeUpdateAndGeneratedKeys(): ResultSet = executeUpdate().let {
    it > 0 || throw SQLException("Expected updates, zero updates returned")
    generatedKeys
}

// A transaction must be committed or rolled back before closing the connection,
// autoCommit can also throw SQLException as can attempt to commit
fun <T> Connection.transaction(
    body: Connection.() -> T,
): T = try {
    autoCommit = false // cannot call commit with autoCommit true
    body().also { commit() }
} catch (eCommit: Throwable) {
    try {
        rollback()
    } catch (eRollback: Exception) {
        eCommit.addSuppressed(eRollback) // gotta catch 'em all
    }
    throw eCommit
} finally {
    autoCommit = true // SQLException could be thrown and will propagate
}



