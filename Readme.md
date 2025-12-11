o facilitate learning and exploring, the Spark distribution comes with a set of sample
applications for each of Spark’s components. You are welcome to peruse the examples
directory in your installation location to get an idea of what’s available.
From the installation directory on your local machine, you can run one of the several
Java or Scala sample programs that are provided using the command bin/runexample <class> [params]. For example:
$ ./bin/run-example JavaWordCount README.md
This will spew out INFO messages on your console along with a list of each word in
the README.md file and its count (counting words is the “Hello, World” of dis‐
tributed computing).