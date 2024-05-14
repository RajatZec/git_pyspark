# JSON Flattener using PySpark

This project utilizes PySpark and Python to flatten JSON data, extract relevant information, and export it into CSV format. It's designed to efficiently handle large-scale JSON datasets and transform them into a structured format for further analysis or integration with other systems.

## Installation

1. Ensure you have Python installed on your system. If not, you can download it from [Python's official website](https://www.python.org/downloads/).

2. Install PySpark using pip:

    ```
    pip install pyspark
    ```

3. Clone this repository to your local machine:

    ```
    git clone https://github.com/RajatZec/git_pyspark
    ```

## Usage

1. Navigate to the project directory:

    ```
    cd git_pyspark
    ```

2. Place your JSON data files in the `input/json` directory.

3. Modify the `.env` file according to your Json directory.

4. Run the script:

    ```
    python git_task_flatten_json.py
    ```

5. The flattened data will be saved as CSV files in the `output/` directory.

## Project Structure

- `git_task_flatten_json.py`: Main script for flattening JSON data and extracting relevant information.
- `input/json`: Directory to store input JSON data files.
- `output/`: Directory where flattened data will be saved as CSV files.

## Example

Suppose you have a JSON file `input.json` with the following structure:

```json
{
  "id": 1,
  "name": "John Doe",
  "address": {
    "street": "123 Main St",
    "city": "New York",
    "zip": "10001"
  }
}

Running the script will flatten the JSON and save the data in CSV format:

id	name	address.street	address.city	address.zip
1	John Doe	123 Main St	New York	10001