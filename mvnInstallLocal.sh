#!/usr/bin/env bash
#
# Install some custom jars into your local maven repo
mvn install:install-file -Dfile=lib/behemoth-core-1.0-SNAPSHOT.jar -DgroupId=com.digitalpebble -DartifactId=behemoth-core -Dversion=1.0 -Dpackaging=jar

