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
    val reader = openInputFile(args[1])
    val writer = openOutputFile(args[2])

    Searcher().start(pattern, reader, writer)
}

fun openInputFile(inputName: String): BufferedReader {
    return BufferedReader(FileReader(inputName))
}

fun openOutputFile(outputName: String): PrintWriter {
    return PrintWriter(FileWriter(outputName))
}

const val DEFAULT_MAX_CONCURRENT_REQUESTS = 20
const val DEFAULT_CONTEXT_LENGTH = 40

const val HTTP_TIMEOUT_MILLIS = 5000

fun downloadUsingJsoup(url: String): String {
    val body = Jsoup.connect(url).timeout(HTTP_TIMEOUT_MILLIS).execute().body()
    log("$url: Received ${body.length} characters")
    return Jsoup.parse(body).text()
}

const val EOF = ""

class Searcher(
    private val maxConcurrentRequests: Int = DEFAULT_MAX_CONCURRENT_REQUESTS,
    private val contextLength: Int = DEFAULT_CONTEXT_LENGTH,
    private val download: (String) -> String = ::downloadUsingJsoup
) {

    fun start(pattern: Pattern, reader: BufferedReader, writer: PrintWriter) {

        // Allocate space in the queues for one pending item to/from each searcher.
        val searcherQueue = LinkedBlockingQueue<String>(maxConcurrentRequests)
        val outputQueue = LinkedBlockingQueue<String>(maxConcurrentRequests)

        // Set up the threads.
        // The searcher threads terminate when they receive an EOF from the reader, which will happen if the
        // reader runs out of data, throws an exception, or is interrupted by the writer.
        val readerThread = startReaderThread(reader, searcherQueue)
        val searcherCount = AtomicInteger(maxConcurrentRequests)
        for (id in 0 until maxConcurrentRequests) {
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
                writer.println(match)
                count++
            }
        } catch (e: Exception) {
            log("Failed", e)
            // If the writer thread failed, it's possible that the reader is still running and is blocked writing
            // to the searcher queue. Interrupt it to get it to stop.
            readerThread.interrupt()
        } finally {
            writer.close()
        }
        log("Wrote $count URLs")
    }

    private fun startReaderThread(
        reader: BufferedReader,
        searcherQueue: BlockingQueue<String>
    ) = thread(name = "Reader") {
        log("Started")
        var count = 0
        try {
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
        } catch (e: InterruptedException) {
            log("Interrupted", e)
        } catch (e: InterruptedIOException) {
            log("Interrupted", e)
        } catch (e: Exception) {
            log("Failed", e)
        } finally {
            reader.close()
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
        val text = download(url)
        val matcher = pattern.matcher(text)
        if (matcher.find()) {
            val context = text.substring(
                max(matcher.start() - contextLength, 0),
                min(matcher.end() + contextLength, text.length)
            )
            return "$url: $context"
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
