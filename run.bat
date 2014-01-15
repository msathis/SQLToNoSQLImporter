@echo off
SET SUBDIR=%~dp0
set CLASSPATH=%SUBDIR%bin\;%SUBDIR%conf\;%SUBDIR%lib\*;%SUBDIR%lib\couch\*;%SUBDIR%lib\mysql\*;%CLASSPATH%
java net.sathis.export.sql.SQLToNoSQLImporter
pause