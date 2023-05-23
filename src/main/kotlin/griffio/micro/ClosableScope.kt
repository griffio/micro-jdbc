package griffio.micro

// maintains AutoClosable resources and closes them in reverse order
class CloseableScope {

    private val scope = mutableListOf<AutoCloseable>()

    // return null or exception with zero or more suppressed exceptions
    fun closeAll() {
        scope.foldRight(null) { closeable: AutoCloseable, cause: Throwable? ->
            try {
                closeable.close()
                null
            } catch (closeException: Throwable) {
                cause?.addSuppressed(closeException)?.let { cause } ?: closeException
            }
        }?.let { throw it }
    }

    fun <T : AutoCloseable> T.closing(): T {
        scope.add(this)
        return this
    }
}
// e.g fun doSomeWork() = closeableScope { a.closing() b.closing() c.closing() }
// Can throw exception with suppressed exceptions
fun closeableScope(block: CloseableScope.() -> Unit) {
    val cs = CloseableScope()
    try {
        cs.block()
    } finally {
        cs.closeAll()
    }
}
