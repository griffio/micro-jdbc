package griffio.micro

import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import kotlin.test.DefaultAsserter.assertEquals

class TestCloseableScope {

    private val exceptionOnClose = AutoCloseable {
        throw Exception("close error")
    }

    class OrderedOnClose(private val list: MutableList<AutoCloseable> = mutableListOf() ) : AutoCloseable {
        var isClosed = false
        override fun close() {
            check(!isClosed) { "Already closed" }
            isClosed = true
            list.add(this)
        }
    }

    @Test
    fun `closeableScope throws an exception`() {

        val exception: Exception = assertThrows(Exception::class.java) {
            closeableScope {
                exceptionOnClose.closing()
            }
        }

        assertEquals(message = "Exception expected", expected = "close error", actual = exception.message)
    }

    @Test
    fun `closeableScope succeeds`() {
        val check = OrderedOnClose()

        closeableScope {
            check.closing()
        }

        assertEquals(message = "Expect closed", expected = true, actual = check.isClosed)
    }

    @Test
    fun `closeableScopes succeeds`() {

        val check1 = OrderedOnClose()
        val check2 = OrderedOnClose()

        closeableScope {
            check1.closing()
            check2.closing()
        }

        assertEquals(message = "Expect closed", expected = true, actual = check1.isClosed)
        assertEquals(message = "Expect closed", expected = true, actual = check2.isClosed)
    }

    @Test
    fun `closeableScopes in reverse order`() {

        val orderOnClose = mutableListOf<AutoCloseable>()
        val check1 = OrderedOnClose(orderOnClose)
        val check2 = OrderedOnClose(orderOnClose)

        closeableScope {
            check1.closing()
            check2.closing()
        }

        assertEquals(message = "Check 2 expected", expected = check2, actual = orderOnClose.first())
        assertEquals(message = "Check 1 expected", expected = check1, actual = orderOnClose.last())
    }

    @Test
    fun `closeableScopes have suppressed exceptions`() {

        val orderOnClose = mutableListOf<AutoCloseable>()
        val check0 = exceptionOnClose
        val check1 = exceptionOnClose
        val check2 = OrderedOnClose(orderOnClose)
        val exception: Exception = assertThrows(Exception::class.java) {
            closeableScope {
                check0.closing()
                check1.closing()
                check2.closing()
            }
        }

        assertEquals(message = "Exception expected", expected = "close error", actual = exception.message)
        assertEquals(message = "1 suppressed exception expected", expected = 1, actual = exception.suppressedExceptions.size)
        assertEquals(message = "Check 2 expected", expected = check2, actual = orderOnClose.first())
    }
}
