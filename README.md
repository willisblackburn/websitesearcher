# Website Searcher Homework Project

## How to Build

Maven and Java must exist on the path.

Just run

    mvn package
    java -jar target/websitesearcher-1.0-SNAPSHOT-jar-with-dependencies.jar word urls.txt results.txt

Replace "word" with the word you want to search for. This can be a regular expression.

The second argument is the file from which to read URLs (this is assumed to be a CSV similar to the example urls.txt) 
and the third is the output file.

## Implementation Notes

`Searcher` is the class that does most of the work. It accepts the pattern to search for and also two functions, `read`
and `write`, which are responsible for supplying URLs and saving matches respectively.

`Searcher` starts a thread to read URLs from `read`, plus 20 searcher threads (this value is configurable via the constructor)
to download pages and search for matches.
(The spec said not to use `ExecutorService` etc., so I figured that the point of the exercise was to start and manage
threads and not to implement the searcher using, say, Vert.x's asynchronous I/O APIs.)
The threads communicate with each other through two `BlockingQueue` instances.
One queue fans out from the reader thread, and the other collects the matches.
After creating the reader and searcher threads, the main thead drains the collector queue until it receives an empty
string, which it interprets as EOF.

`Searcher` doesn't read or write files. Another class, `CSVFileReader`, parses the CSV and delivers URLs when it is
called by the `Searcher` `read` fuction. The `write` function is just `PrintWriter.println`.

I use jsoup to download the web pages and parse the HTML. The program only searches the text of the web pages, not tags.

The output file contains the URLs of the matched pages plus some of the text surrounding the match.

There are a couple of unit tests. I wanted to write some timing tests using `CyclicBarrier` but there just wasn't enough time.

