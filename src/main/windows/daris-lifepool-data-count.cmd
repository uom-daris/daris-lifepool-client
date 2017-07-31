@echo off

cmd /k java -cp "%~dp0\daris-lifepool-client.jar" daris.lifepool.client.cli.DataCountCLI %*
