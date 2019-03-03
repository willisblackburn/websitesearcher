import org.jsoup.Jsoup
import java.io.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import java.lang.Math.max
import java.lang.Math.min
import java.util.regex.Pattern
import kotlin.concurrent.thread
import kotlin.system.exitProcess

const val MAX_CONCURRENT_REQUESTS = 20
const val HTTP_TIMEOUT = 5000
const val CONTEXT_LENGTH = 40
const val EOF = ""

/**
 * Searcher word input output
 *
 * word: a word we're searching for
 * input: a file containing the URLs.
 * output: the file to which we will write the matches.
 */
fun main(args: Array<String>) {

    if (args.size < 3) {
        println("Usage: Searcher word input output")
        exitProcess(-1)
    }

    val pattern = Pattern.compile(args[0], Pattern.CASE_INSENSITIVE)
    val inputName = args[1]
    val outputName = args[2]

    Searcher().start(pattern, inputName, outputName)
}

class Searcher {

    fun start(pattern: Pattern, inputName: String, outputName: String) {
        val searcherQueue = LinkedBlockingQueue<String>(MAX_CONCURRENT_REQUESTS)
        val outputQueue = LinkedBlockingQueue<String>(MAX_CONCURRENT_REQUESTS)

        val readerThread = startReaderThread(inputName, searcherQueue)
        val searcherCount = AtomicInteger(MAX_CONCURRENT_REQUESTS)
        for (id in 0 until MAX_CONCURRENT_REQUESTS) {
            startSearcherThread(id, searcherCount, pattern, searcherQueue, outputQueue)
        }
        val writerThread = startWriterThread(outputName, outputQueue, readerThread)

        readerThread.join()
        writerThread.join()
    }

    private fun startReaderThread(
        inputName: String,
        searcherQueue: BlockingQueue<String>
    ) = thread(name = "Reader") {
        log("Started")
        var count = 0
        try {
            BufferedReader(FileReader(inputName)).use { reader ->
                // The file is a CSV in which the first field is a rank and the second file is a scheme-less URL
                // surrounded by quotes. The first line is the header, so we'll just throw that away.
                reader.readLine()
                var line = reader.readLine()
                while (line != null) {
                    val values = line.split(',')
                    if (values.size >= 2) {
                        val partialUrl = values[1].trim('"')
                        log("Found $partialUrl")
                        searcherQueue.put(partialUrl)
                        count++
                    }
                    line = reader.readLine()
                }
                log("EOF")
            }
        } catch (e: InterruptedException) {
            log("Interrupted", e)
        } catch (e: InterruptedIOException) {
            log("Interrupted", e)
        } catch (e: Exception) {
            log("Failed", e)
        }
        log("Found $count URLs")
        searcherQueue.put(EOF)
        log("Shutting down")
    }

    private fun startWriterThread(
        outputName: String,
        outputQueue: BlockingQueue<String>,
        readerThread: Thread
    ) = thread(name = "Writer") {
        log("Started")
        var count = 0
        try {
            PrintWriter(FileWriter(outputName)).use { writer ->
                while (true) {
                    val match = outputQueue.take()
                    if (match == EOF) {
                        log("Received EOF")
                        return@use
                    }
                    writer.println(match)
                    count++
                }
            }
        } catch (e: Exception) {
            log("Failed", e)
            // If the writer thread failed, it's possible that the reader is still running and is blocked writing
            // to the searcher queue. Interrupt it to get it to stop.
            readerThread.interrupt()
        }
        log("Wrote $count URLs")
        log("Shutting down")
    }

    private fun startSearcherThread(
        id: Int, searcherCount: AtomicInteger, pattern: Pattern,
        searcherQueue: BlockingQueue<String>, outputQueue: BlockingQueue<String>
    ) = thread(name = "Searcher $id") {
        log("Started")
        while (true) {
            val partialUrl = searcherQueue.take()
            if (partialUrl == EOF) {
                val remaining = searcherCount.decrementAndGet()
                log("Received EOF, $remaining searcher(s) remaining")
                if (remaining == 0) {
                    // Last searcher has exited, so signal the writer to exit too.
                    outputQueue.put(EOF)
                } else {
                    // More searchers running, so put EOF back in the queue.
                    searcherQueue.put(EOF)
                }
                break
            }
            // Try HTTP first and rely on redirect to identify HTTPS resources.
            val url = "http://$partialUrl"
            log("Requesting $url")
            try {
                val match = search(url, pattern)
                if (match != null) {
                    outputQueue.put(match)
                }
            } catch (e: Exception) {
                // Just report the error and carry on.
                log("$url: Failed", e)
            }
        }
        log("Shutting down")
    }

    private fun search(url: String, pattern: Pattern): String? {
        val body = Jsoup.connect(url).timeout(HTTP_TIMEOUT).execute().body()
        log("$url: Received ${body.length} characters")
        val text = Jsoup.parse(body).text()
        val matcher = pattern.matcher(text)
        if (matcher.find()) {
            val context = text.substring(
                max(matcher.start() - CONTEXT_LENGTH, 0),
                min(matcher.end() + CONTEXT_LENGTH, text.length)
            )
            return "$url: $context"
        }
        return null
    }

    private fun log(message: String, e: Exception? = null) {
        val output = "${Thread.currentThread().name}: $message" + (e?.let { ": ${it.message}" } ?: "")
        println(output)
    }
}