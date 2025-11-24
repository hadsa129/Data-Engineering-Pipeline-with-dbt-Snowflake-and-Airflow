"""
## Astronaut ETL example DAG

This DAG queries the list of astronauts currently in space from the
Open Notify API and prints each astronaut's name and flying craft.
"""
from datetime import datetime
from airflow.decorators import dag, task
import requests

@dag(
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["example", "astronauts"],
)
def example_astronauts():
    @task
    def get_astronaut_data():
        """Get current astronauts data from the Open Notify API."""
        response = requests.get("http://api.open-notify.org/astros.json")
        response.raise_for_status()
        data = response.json()
        return data['people']  # Return just the people list directly

    @task
    def print_astronaut_info(astronaut):
        """Print astronaut information."""
        print(f"{astronaut['name']} is on the {astronaut['craft']}")
        return f"Processed {astronaut['name']}"

    @task
    def log_astronaut_count(astronaut_data):
        """Log the total number of astronauts in space."""
        count = len(astronaut_data)
        print(f"Total astronauts in space: {count}")
        return count

    # Get the data (this returns the people list directly)
    people_in_space = get_astronaut_data()
    
    # Log the total count
    log_astronaut_count(people_in_space)
    
    # Process each astronaut in parallel
    # Pass the list directly to expand
    print_astronaut_info.expand(astronaut=people_in_space)

# Create the DAG
astronauts_dag = example_astronauts()