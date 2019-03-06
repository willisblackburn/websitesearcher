package websitesearcher

import org.jsoup.Jsoup
import java.io.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import java.lang.Math.max
import java.lang.Math.min
import java.util.regex.Pattern
import kotlin.concurrent.thread
import kotlin.system.exitProcess

/**
 * Searcher regex input output
 *
 * regex: a regex we're searching for
 * input: a file containing the URLs.
 * output: the file to which we will write the matches.
 */
fun main(args: Array<String>) {

    if (args.size < 3) {
        println("Usage: Searcher regex input output")
        exitProcess(-1)
    }

    val pattern = Pattern.compile(args[0], Pattern.CASE_INSENSITIVE)

    CSVFileReader(BufferedReader(FileReader(args[1]))).use { reader ->
        PrintWriter(FileWriter(args[2])).use { writer ->
            Searcher().start(pattern, reader::read, writer::println)
        }
    }
}

/**
 * The file is a CSV in which the first field is a rank and the second field is a scheme-less URL
 * surrounded by quotes.
 */
class CSVFileReader(private val reader: BufferedReader) : Closeable {

    init {
        // The first line is the header, so we'll just throw that away.
        reader.readLine()
    }

    fun read(): String? {
        while (true) {
            val line = reader.readLine() ?: return null
            val values = line.split(',')
            if (values.size >= 2) {
                return values[1].trim('"')
            }
        }
    }

    override fun close() {
        reader.close()
    }
}

const val HTTP_TIMEOUT_MILLIS = 5000

fun downloadUsingJsoup(url: String): String {
    val body = Jsoup.connect(url).timeout(HTTP_TIMEOUT_MILLIS).execute().body()
    log("$url: Received ${body.length} characters")
    return Jsoup.parse(body).text()
}

const val DEFAULT_MAX_CONCURRENT_REQUESTS = 20
const val DEFAULT_CONTEXT_LENGTH = 40

const val EOF = ""

/**
 * Main searcher class.
 * This class manages the threads and applies the regex.
 * We leave I/O to some other class (e.g., FileIO) and just read URLs from a supplier and write matching
 * lines to a consumer.
 */
class Searcher(
    private val download: (String) -> String = ::downloadUsingJsoup,
    private val maxConcurrentRequests: Int = DEFAULT_MAX_CONCURRENT_REQUESTS,
    private val contextLength: Int = DEFAULT_CONTEXT_LENGTH
) {

    fun start(pattern: Pattern, read: () -> String?, write: (String) -> Unit) {

        // Allocate space in the queues for one pending item to/from each searcher.
        val searcherQueue = LinkedBlockingQueue<String>(maxConcurrentRequests)
        val outputQueue = LinkedBlockingQueue<String>(maxConcurrentRequests)

        // Set up the threads.
        // The searcher threads terminate when they receive an EOF from the reader, which will happen if the
        // reader runs out of data, throws an exception, or is interrupted by the writer.
        val readerThread = startReaderThread(read, searcherQueue)
        val searcherCount = AtomicInteger(maxConcurrentRequests)
        val searcherThreads = (0 until maxConcurrentRequests).map { id ->
            startSearcherThread(id, searcherCount, pattern, searcherQueue, outputQueue)
        }

        // In this thread we'll receive the URLs from the output queue and write them out.
        var count = 0
        try {
            while (true) {
                val match = outputQueue.take()
                if (match == EOF) {
                    log("Received EOF")
                    break
                }
                write(match)
                count++
            }
        } catch (e: Exception) {
            log("Failed", e)
            // If the writer thread failed, it's possible that the read is still running and is blocked writing
            // to the searcher queue. Interrupt it to get it to stop.
            readerThread.interrupt()
            searcherThreads.forEach(Thread::interrupt)
        }
        log("Wrote $count matches")
    }

    private fun startReaderThread(
        reader: () -> String?,
        searcherQueue: BlockingQueue<String>
    ) = thread(name = "Reader") {
        log("Started")
        var count = 0
        try {
            while (true) {
                val partialUrl = reader()
                if (partialUrl == null) {
                    log("EOF")
                    break
                }
                log("Found $partialUrl")
                searcherQueue.put(partialUrl)
                count++
            }
        } catch (e: Exception) {
            log("Failed", e)
        }
        log("Found $count URLs")
        searcherQueue.put(EOF)
        log("Shutting down")
    }

    private fun startSearcherThread(
        id: Int, searcherCount: AtomicInteger, pattern: Pattern,
        searcherQueue: BlockingQueue<String>, outputQueue: BlockingQueue<String>
    ) = thread(name = "Searcher $id") {
        log("Started")
        while (true) {
            try {
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
                val match = search(url, pattern)
                if (match != null) {
                    outputQueue.put(match)
                }
            } catch (e: Exception) {
                log("Failed", e)
            }
        }
        log("Shutting down")
    }

    private fun search(url: String, pattern: Pattern): String? {
        try {
            val text = download(url)
            val matcher = pattern.matcher(text)
            if (matcher.find()) {
                val context = text.substring(
                    max(matcher.start() - contextLength, 0),
                    min(matcher.end() + contextLength, text.length)
                )
                return "$url: $context"
            }
        } catch (e: InterruptedException) {
            throw e
        } catch (e: Exception) {
            // Just report the error and carry on.
            log("$url: Failed", e)
        }
        return null
    }
}

/**
 * Log a message to the console, identifying the thread.
 */
fun log(message: String, e: Exception? = null) {
    val output = "${Thread.currentThread().name}: $message" + (e?.let { ": ${it.message}" } ?: "")
    println(output)
}
