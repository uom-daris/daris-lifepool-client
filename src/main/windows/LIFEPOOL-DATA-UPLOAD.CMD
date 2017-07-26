@echo off

if not exist %~dp0\daris-lifepool-client.jar echo "Error: could not find daris-lifepool-client.jar"

java -cp %~dp0\daris-lifepool-client.jar daris.lifepool.client.DataUpload %*