#!/bin/sh
export CLASSPATH=.:lib/*:conf/:bin/
java  net.sathis.export.sql.SQLToNoSQLImporter
