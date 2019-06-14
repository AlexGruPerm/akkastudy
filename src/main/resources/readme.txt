--------------------------------
build:
--------------------------------
set SBT_OPTS="-Xmx2G"

sbt "set test in assembly := {}" clean compile assembly

--------------------------------
run:
--------------------------------
-Xmx sets the maximum amount of memory that can be allocated to the JVM heap; here it is being set to 1024 megabytes.
-Xms sets the initial amount of memory allocated to the JVM heap; here it is being set to 256 megabytes.

JAVA_OPTS="-Xmx1024m -Xms256m"

export JAVA_OPTS="-Xmx4G -XX:+UseConcMarkSweepGC"
echo %JAVA_OPTS%
scala ticksloader.jar