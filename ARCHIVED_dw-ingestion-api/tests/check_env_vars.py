# check_env_vars.py
import os

print("Current environment variables used by tests:")
print(f"TEST_DB_NAME     = {os.getenv('TEST_DB_NAME', 'not set')}")
print(f"TEST_DB_USER     = {os.getenv('TEST_DB_USER', 'not set')}")
print(f"TEST_DB_PASSWORD = {os.getenv('TEST_DB_PASSWORD', 'not set')}")
print(f"TEST_DB_HOST     = {os.getenv('TEST_DB_HOST', 'not set')}")
print(f"TEST_DB_PORT     = {os.getenv('TEST_DB_PORT', 'not set')}")
