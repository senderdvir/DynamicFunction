# Dynamic Function Executor

This repository contains a Python project that demonstrates how to execute functions dynamically at runtime based on configurations specified in a CSV file. The project reads operational settings from a CSV file, loads the relevant data files, and applies specified functions to the data. This approach is useful for automating and managing various data processing and testing tasks.

## Project Structure

- **main.py**: The main script that reads the operational configuration, processes the data files, and dynamically executes the specified functions.
- **operational_table.csv**: A sample configuration file containing the details of the tests to be run, including file names, test IDs, and function names.

## Requirements

- Python 3.x
- Pandas

You can install the required dependencies using:

```bash
pip install -r requirements.txt
