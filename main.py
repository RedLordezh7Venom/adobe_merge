# Import necessary libraries
import json
import requests
import time
import random
import os
from dotenv import load_dotenv
from database import list_all_tables,get_table_schema

load_dotenv()
# Global variable to keep track of the total number of tokens
total_tokens = 0

# Function to load input file
def load_input_file(file_path):
    """
    Load input file which is a list of dictionaries.
    
    :param file_path: Path to the input file
    :return: List of dictionaries
    """
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data
# Get database schema for all tables

db_name = os.getenv("DB_NAME", "postgres")
user = os.getenv("DB_USER", "postgres")
password = os.getenv("DB_PASSWORD", "root")
host = os.getenv("DB_HOST", "localhost")
port = os.getenv("DB_PORT", "5432")

# Fetch all tables
tables = list_all_tables(db_name, user, password, host, port)

# Build schema description
schema_description = ""
for table in tables:
    table_schema = get_table_schema(db_name, user, password, table,host,port)
    schema_description += f"\nTable '{table}':\n"
    for column, dtype in table_schema.items():
        schema_description += f"- {column} ({dtype})\n"
system_prompt = (
        f"You are a database expert who generates precise, correct PostgreSQL queries from natural language. "
        f"Output only the query with no explanations. The schema is as follows:\n\n"
        f"{schema_description}\n\n"
        f"Always use the schema provided above to generate the SQL queries."
    )
system_prompt = (
        f"You are a database expert who generates precise, correct PostgreSQL queries from natural language. "
        f"Output only the query with no explanations. The schema is as follows:\n\n"
        f"{schema_description}\n\n"
        f"Always use the schema provided above to generate the SQL queries."
    )
# Function to generate SQL statements
def generate_sqls(data):
    """
    Generate SQL statements from the NL queries using the database schema and Groq API.
    
    :param data: List of NL queries (dictionaries with "NL" key)
    :return: List of dictionaries with "NL" and "Query" keys
    """
    sql_statements = []

    # Process each NL query
    for item in data:
        nl_query = item.get("NL", "")
        if not nl_query:
            continue
        delay = random.uniform(0.1, 0.5)
        time.sleep(delay)
    
        
        # Construct prompt for LLM
        prompt = (
            f"Generate a PostgreSQL query for the following request, based on the schema already provided:\n"
            f"{nl_query}\n\n"
            f"Return only the SQL statement in one line without any addition or markdown"
        )

        # Call Groq API
        try:
            response, _ = call_groq_api(
                api_key=os.getenv("GROQ_KEY"),
                model="qwen-2.5-coder-32b",
                messages=[{"role": "system", "content": system_prompt},{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=500
            )
            sql_query = response['choices'][0]['message']['content'].strip()
        except Exception as e:
            sql_query = f"/* Error generating SQL: {str(e)} */"

        # Append result
        sql_statements.append({
            "NL": nl_query,
            "Query": sql_query
        })

    return sql_statements

# Function to call the Groq API

def call_groq_api(api_key, model, messages, temperature=0.0, max_tokens=1000, n=1):
    """
    NOTE: DO NOT CHANGE/REMOVE THE TOKEN COUNT CALCULATION 
    Call the Groq API to get a response from the language model.
    :param api_key: API key for authentication
    :param model: Model name to use
    :param messages: List of message dictionaries
    :param temperature: Temperature for the model
    :param max_tokens: Maximum number of tokens to generate (these are max new tokens)
    :param n: Number of responses to generate
    :return: Response from the API
    """
    global total_tokens
    url = "https://api.groq.com/openai/v1/chat/completions"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }

    data = {
        "model": model,
        "messages": messages,
        'temperature': temperature,
        'max_tokens': max_tokens,
        'n': n
    }

    response = requests.post(url, headers=headers, json=data)
    response_json = response.json()


    # Update the global token count
    total_tokens += response_json.get('usage', {}).get('completion_tokens', 0)

    # You can get the completion from response_json['choices'][0]['message']['content']
    return response_json, total_tokens

# Main function
def main(): 
    input_file_path_1 = 'nl_test.json' 
    # Load data from input file
    data_1 = load_input_file(input_file_path_1)
    
    start = time.time()
    # Generate SQL statements
    #system prompt
    
    call_groq_api(
                api_key=os.getenv("GROQ_KEY"),
                model="qwen-2.5-coder-32b",
                messages=[
    {"role": "system", "content": system_prompt},
],
                temperature=0.1,
                max_tokens=500
    )
        
    sql_statements = generate_sqls(data_1)
    generate_sqls_time = time.time() - start
    
    start = time.time()
    assert len(data_1) == len(sql_statements) # If no answer, leave blank
    
    # TODO: Process the outputs
    
    
    # Get the outputs as a list of dicts with keys 'NL' and 'Query'
    with open('output_sql_generation_task.json', 'w') as f:
        json.dump(sql_statements, f,indent = 4)
    
    return generate_sqls_time



if __name__ == "__main__":
    generate_sqls_time= main()
    print(f"Time taken to generate SQLs: {generate_sqls_time} seconds")
    print(f"Total tokens: {total_tokens}")
