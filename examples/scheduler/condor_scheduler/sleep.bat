@echo off
echo The current time is: %TIME%
ping -n 30 127.0.0.1 > nul
echo The current time is: %TIME%