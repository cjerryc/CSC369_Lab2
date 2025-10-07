Author: Jerry Chang
Lab 2: MapReduce / Hadoop
------------------------------------------------------------------------------------------------------------------------
In the implementation of the reports, I follow a similar structure with the use of the Map-Reduce framework. However, the number
of Map-Reduce jobs used and implementation differs between reports to capture and order different data.
1. The mapper takes in the text of Apache logs, and outputs a key-value pair of (URL, 1) to feed to the reducer. After the
job shuffles and groups on the URL keys, the reducer takes in the pairs as Text and corresponding iterables of Ints, which
are used to sum the 1's associated with each URl. The resulting (URL, Count) pairs are then written to an intermediate file,
which is used for input into the second Map-Reduce job. In the second Map-Reduce job, the mapper takes in the text of the
(URL, Count) pairs, and outputs a key-value pair of (Count, URL). The shuffle phase will sort and group on the Count key,
thus sorting the Request Counts in ascending order as desired. The Reducer thus emits the resulting (Count, URL) pairs.

2. The mapper takes in the text of Apache logs, and outputs key-value pairs of (HTTP Response Code, 1). The shuffle phase
will then group and sort on the HTTP Response Code. The reducer will take in the text of HTTP Response Code and their
corresponding iterable of 1's. The reducer will sum over the 1's to produce a count for each HTTP Response Code. The results
are stored in an intermediate file to be used as input text to the second job. In the second job, the mapper takes in the text
of (HTTP Response Code, Count) pairs to create key-value pairs of (Count, HTTP Response Code). The shuffle phase will group
and sort based on the Counts, so that the output will now be sorted in ascending order of request counts. The reducer takes
in the input of pairs and simply emits them.

3. The mapper takes in the text of Apache logs, and outputs key-value pairs of (Hostname/Client IP, Bytes) based on a
hardcoded IP address to search for. The shuffle phase takes all of those pairs and adds the number of bytes in each event
into an iterable to attach to the Hostname/IP address that is being matched on. The reduced sums over the iterable to find
the total number of bytes and emits the Hostname/IP address with its total number of bytes.

4. The mapper takes in the text of Apache logs, and outputs key-value pairs of (Hostname/IPv4 Address, 1) based on a hardcoded URL.
The shuffle phase groups and sorts on the Client Hostname/IPv4 Address keys. The reducer sums over the 1's to write the
(Hostname/IPv4 Address, Count) pairs to an intermediate file to use as input into the second Map-Reduce job.
In the second job, the mapper reads in the text file of (Hostname/IPv4 Address, Count) to output key-value pairs of
(Count, Hostname/IPv4 Address). The shuffle phase groups and sorts on the counts, thus sorting by the request count in
ascending order.

5. The mapper takes in the text of Apache logs, and outputs key-value pairs of (monthYear, 1). The shuffle phase groups and
sorts on the monthYear. The pairs are then given to the reducer, which sum over the 1's for each key of monthYear and outputs
the Month, Year, and the total number of requests made. The key to sorting in chronological order here is to change the month
to a numerical string and have the Year go first in the mapper output. In doing so, the shuffle phase will sort by Year first,
then sort by numerical representation of month to ensure chronological order. In the reducer, the position and month text may
be swapped back for readability.

6. The mapper takes in the text of Apache logs, and outputs key-value pairs of (Date, Bytes), where Date is the MM-DD-YYYY format.
The shuffle phase groups the days together and sorts them. The reducer takes in these pairs and sums over all the bytes sent on
that Date and writes these pairs to an intermediate file to be used as input into a second Map-Reduce job. In the second Map-Reduce
job, the mapper takes in the text file of (Date, Total Bytes) pairs and outputs (Total Bytes, Date). In the shuffle phase,
the pairs are grouped and sorted on the total number of Bytes, thus sorting the report by total bytes sent in ascending order.
The reduce simply emits the pairs.








*** How to Run Program ***
Run program:
(report 1)
./gradlew run --args="RequestCountByURL input_access_log out_test"

(report 2)
./gradlew run --args="RequestCountByHTTPCode input_access_log out_test"

(report 3)
./gradlew run --args="BytesByClient input_access_log out_test"

(report 4)
./gradlew run --args="RequestCountByClientAndURL input_access_log out_test

(report 5)
./gradlew run --args="RequestCountByMonthYear input_access_log out_test"

(report 6)
./gradlew run --args="BytesByDay input_access_log out_test"

View Output Report:
All reports are stored in "out_test/part-r-00000" and may be read using: "cat out_test/part-r-00000" after running the
above input commands in terminal.


################################################## Lab Requirements: ###########################################################
Using the Java Hadoop MapReduce framework, produce the following reports based on input that consists of Apache HTTP log file(s):
(1) Request count for each URL path, sorted by request count (ascending)
(2) Request count for each HTTP response code, sorted by response code (ascending)
(3) Total bytes sent to the client with a specified hostname or IPv4 address (you may hard code an address)
(4) Based on a given URL (hard coded), compute a request count for each client (hostname or IPv4) who accessed that URL, sorted by request count (ascending)
(5) Request count for each calendar month and year, sorted chronologically
(6) For each calendar day that appears in the file, return total bytes sent. Sort by total bytes in ascending order.


Notes/Requirements
Your program should follow the conventions from the hadoop1 example projectLinks to an external site.
The example code should run using JDK version 1.8+, using the following command: ./gradlew run --args="<JobName> <input folder> <output folder>"
The sample project includes an access.log file (in the input_acces_log/ directory).
You may use this for testing. I also encourage you to test against additional data that follows the Apache HTTP log format
(either simulated data, or sample files that you locate on your own).
You may assume that the access log file follows Apache log format, described here:

     https://httpd.apache.org/docs/current/logs.html#commonLinks to an external site.

If a given report requires multiple Map/Reduce phases, describe the input and output of each map reduce job in your README file.
All output should be in text format, using the default Hadoop directory structure. You do not need to post-process the data in any way.




