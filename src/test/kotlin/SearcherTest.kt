package websitesearcher

import io.mockk.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.io.BufferedReader
import java.util.regex.Pattern

class SearcherTest {

    @Test
    fun `CSVFileReader should remove the CSV header and parse one URL from the second field of each line`() {
        val bufferedReader = mockk<BufferedReader>()
        every { bufferedReader.readLine() } returnsMany listOf(
            "ignored header line",
            "1,\"facebook.com/\",stuff we don't care about",
            "2,\"twitter.com/\",more stuff we don't care about",
            null
        )
        val reader = CSVFileReader(bufferedReader)
        assertEquals("facebook.com/", reader.read())
        assertEquals("twitter.com/", reader.read())
        assertNull(reader.read())
    }

    @Test
    fun `Searcher should download URLs and write URLs with text matching pattern to output file`() {

        val download = mockk<(String) -> String>()
        every { download("http://a.com/") } returns "There's no place like home"
        every { download("http://b.com/") } returns "A bird in the hand is worth two in the bush"

        val read = mockk<() -> String?>()
        every { read() } returnsMany listOf(
            "a.com/",
            "b.com/",
            null
        )
        val output = ArrayList<String>()
        val searcher = Searcher(download, 1)
        searcher.start(Pattern.compile("[b]ird?"), read, { line -> output.add(line) })

        assertEquals(1, output.size)
        assertEquals("http://b.com/: A bird in the hand is worth two in the bush", output.get(0))
    }
}

