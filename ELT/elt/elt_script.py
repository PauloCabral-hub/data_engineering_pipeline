import subprocess
import time

def wait_for_postgres(host, max_retries=5, delay_secs=5):
    retries = 0
    while retries < max_retries:
        try:
            result = subprocess.run(["pg_isready", "-h", host],
                                    check=True,
                                    capture_output=True,
                                    text=True)
            if "accepting connections" in result.stdout:
                print("Connection to PostgreSQL was successful")
                return True
        except subprocess.CalledProcessError as e:
            print(f"Failed to connect to PostgreSQL: {e}")
            retries += 1
            print(f"Another attempt in {delay_secs} seconds... (Attempt: {retries} of {max_retries})")
            time.sleep(delay_secs)
    print("Reached maximum number of attempts.")
    return False

if not wait_for_postgres(host="source_postgres"):
    exit(1)

print("Starting ELT script...")

source_config = {
    'dbname': 'source_db',
    'user': 'postgres',
    'password': 'secret',
    'host': 'source_postgres'
}

destination_config = {
    'dbname': 'destination_db',
    'user': 'postgres',
    'password': 'secret',
    'host': 'destination_postgres'
}

# Dump the data from source_postgres
dump_command = [
    'pg_dump',
    '-h', source_config['host'],
    '-U', source_config['user'],
    '-d', source_config['dbname'],
    '-f', 'data_dump.sql',
    '-w'
]

# Pass the password as an environment variable
subprocess_env = dict(PGPASSWORD=source_config['password'])

try:
    subprocess.run(dump_command, env=subprocess_env, check=True)
    print("Data dumped successfully.")
except subprocess.CalledProcessError as e:
    print(f"Error dumping data: {e}")
    exit(1)

# Load the data into destination_postgres
load_command = [
    'psql',
    '-h', destination_config['host'],
    '-U', destination_config['user'],
    '-d', destination_config['dbname'],
    '-a', '-f', 'data_dump.sql'
]

subprocess_env = dict(PGPASSWORD=destination_config['password'])

try:
    subprocess.run(load_command, env=subprocess_env, check=True)
    print("Data loaded successfully.")
except subprocess.CalledProcessError as e:
    print(f"Error loading data: {e}")
    exit(1)

print("End ELT.")