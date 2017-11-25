@echo off
SETLOCAL ENABLEDELAYEDEXPANSION

for /l %%p in (0,1,2) do (
	
	start java Initiator "192.168.0.101" "1099" %%p "peer_config.txt" %%p

)