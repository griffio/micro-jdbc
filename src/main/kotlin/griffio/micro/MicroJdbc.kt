package griffio.micro

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

// see https://github.com/elizarov/SerializationByConvention#reading-objects-from-db

fun interface ResultSetReader<out T> {
    fun ResultSet.read(): T
}

fun ResultSet.readInt(name: String): Int = getInt(name)

fun ResultSet.readString(name: String): String = getString(name)

fun ResultSet.readBoolean(name: String): Boolean = getBoolean(name)

fun <T> ResultSet.readAll(reader: ResultSetReader<T>): List<T> {
    val list = mutableListOf<T>()
    while (next()) list.add(read(reader))
    return list
}

fun <T> ResultSet.read(reader: ResultSetReader<T>): T = with(reader) { read() }

fun PreparedStatement.bindInt(index: Int, value: Int): PreparedStatement = apply { setInt(index, value) }

fun PreparedStatement.bindString(index: Int, value: String): PreparedStatement = apply { setString(index, value) }

fun PreparedStatement.bindBoolean(index: Int, value: Boolean): PreparedStatement = apply { setBoolean(index, value) }

fun <T> Connection.transaction(
    body: Connection.() -> T,
): T {
    autoCommit = false
    return try {
        body().let {
            commit()
            it
        }
    } catch (e: Throwable) {
        rollback()
        throw e
    } finally {
        autoCommit = true
    }
}



