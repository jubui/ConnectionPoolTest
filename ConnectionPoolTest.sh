#!/usr/bin/env bash

mkdir -p classes
javac -classpath "./deps/commons-dbcp2-2.8.0.jar:./deps/commons-logging-1.2.jar:./deps/commons-pool2-2.8.1.jar:./deps/commons-cli-1.5.0.jar" -d classes QueryTimer.java

DRIVERS=mariadb-java-client-2.7.2.jar
DRIVERS=$DRIVERS:mysql-connector-java-5.1.49.jar
DRIVERS=$DRIVERS:ojdbc8-19.7.jar
DRIVERS=$DRIVERS:postgresql-42.2.18.jar
DRIVERS=$DRIVERS:mssql-jdbc-8.4.1.jre8.jar
DRIVERS=$DRIVERS:db2jcc4-4.31.10.jar
DRIVERS=$DRIVERS:./deps/commons-dbcp2-2.8.0.jar
DRIVERS=$DRIVERS:./deps/commons-pool2-2.8.1.jar
DRIVERS=$DRIVERS:./deps/commons-logging-1.2.jar
DRIVERS=$DRIVERS:./deps/commons-cli-1.5.0.jar

java -cp classes:$DRIVERS com.appian.QueryTimer $*

