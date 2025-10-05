# CSC 369 Hadoop Examples

Note that these examples use the MRUnit package to simulate a Hadoop cluster.

Hadoop job configuration can be found in `src/main/java/csc369/HadoopApp.java`

To run the sample jobs, used the gradlew command as follows:
`./gradlew run --args="<job name> <input directory> <output directory>"`

This will compile all code, configure a local Hadoop instance and run the requested job for all records in the provided input directory.

For example:
`./gradlew run --args="AccessLog input_access_log out_test"`
