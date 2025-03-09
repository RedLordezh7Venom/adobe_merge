import json
import psycopg2
import pandas as pd
from psycopg2.extras import RealDictCursor
import hashlib
import os
from dotenv import load_dotenv

# Load environment variables from .env file (if present)
load_dotenv()

def get_db_connection():
    """Create and return a connection to the PostgreSQL database."""
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        database=os.getenv('DB_NAME', 'postgres'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD', 'root'),
        port=os.getenv('DB_PORT', '5432')
    )

def execute_query(query):
    """Execute a SQL query and return the results as a pandas DataFrame.
    Uses a new connection for each query to avoid transaction problems."""
    conn = None
    try:
        conn = get_db_connection()
        conn.autocommit = True  # Set autocommit to avoid transaction issues
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            results = cur.fetchall()
            # Convert to pandas DataFrame for easier comparison
            df = pd.DataFrame(results)
            return df, None
    except Exception as e:
        return None, str(e)
    finally:
        if conn:
            conn.close()

def compare_query_results(df1, df2):
    """
    Compare two dataframes and check if they contain the same data.
    Returns True if they match, False otherwise.
    """
    if df1 is None or df2 is None:
        return False
    
    # If one is empty and the other isn't, they don't match
    if df1.empty != df2.empty:
        return False
    
    # If both are empty, they match
    if df1.empty and df2.empty:
        return True
    
    # Sort both dataframes to ensure consistent ordering
    # First make sure they have the same columns
    if sorted(df1.columns.tolist()) != sorted(df2.columns.tolist()):
        return False
    
    # Sort by all columns to ensure consistent ordering
    df1_sorted = df1.sort_values(by=df1.columns.tolist()).reset_index(drop=True)
    df2_sorted = df2.sort_values(by=df2.columns.tolist()).reset_index(drop=True)
    
    # Compare the dataframes
    return df1_sorted.equals(df2_sorted)

def generate_result_signature(df):
    """
    Generate a signature/hash for dataframe results to help with debugging.
    """
    if df is None or df.empty:
        return "empty_result"
    
    # Convert dataframe to string and hash it
    df_str = df.to_string()
    return hashlib.md5(df_str.encode()).hexdigest()

def evaluate_queries(test_file, output_file):
    """
    Evaluate the accuracy of SQL queries in output_file compared to test_file
    by executing them and comparing the results.
    
    Returns:
        tuple: (accuracy percentage, list of errors, matching count, total count)
    """
    # Load the files
    with open(test_file, 'r') as f:
        test_data = json.load(f)
    
    with open(output_file, 'r') as f:
        output_data = json.load(f)
    
    # Create dictionaries with NL as keys for easier matching
    test_dict = {item["NL"]: item["Query"] for item in test_data}
    output_dict = {item["NL"]: item["Query"] for item in output_data}
    
    # Count matches and collect errors
    total = 0
    matches = 0
    errors = []
    
    for nl, test_query in test_dict.items():
        if nl in output_dict:
            total += 1
            output_query = output_dict[nl]
            
            # Execute both queries with separate connections
            test_results, test_error = execute_query(test_query)
            output_results, output_error = execute_query(output_query)
            
            # If either query failed, it's not a match
            if test_error or output_error:
                errors.append({
                    "NL": nl,
                    "Expected": test_query,
                    "Actual": output_query,
                    "Expected_Error": test_error,
                    "Actual_Error": output_error
                })
                continue
            
            # Compare results
            if compare_query_results(test_results, output_results):
                matches += 1
            else:
                errors.append({
                    "NL": nl,
                    "Expected": test_query,
                    "Actual": output_query,
                    "Expected_Signature": generate_result_signature(test_results),
                    "Actual_Signature": generate_result_signature(output_results),
                    "Expected_Shape": test_results.shape if test_results is not None else None,
                    "Actual_Shape": output_results.shape if output_results is not None else None
                })
    
    # Calculate accuracy
    accuracy = (matches / total * 100) if total > 0 else 0
    
    return accuracy, errors, matches, total

def main():
    test_file = "nl_test.json"
    output_file = "output_sql_generation_task.json"
    
    accuracy, errors, matches, total = evaluate_queries(test_file, output_file)
    
    print(f"Evaluation Results:")
    print(f"Total queries: {total}")
    print(f"Matching queries: {matches}")
    print(f"Accuracy: {accuracy:.2f}%")
    
    if errors:
        print(f"\nFound {len(errors)} mismatches:")
        for i, error in enumerate(errors, 1):
            print(f"\nMismatch #{i}:")
            print(f"NL: {error['NL']}")
            print(f"\nExpected query:")
            print(f"{error['Expected']}")
            print(f"\nActual query:")
            print(f"{error['Actual']}")
            
            # Show error details if present
            if 'Expected_Error' in error and error['Expected_Error']:
                print(f"\nExpected query error: {error['Expected_Error']}")
            if 'Actual_Error' in error and error['Actual_Error']:
                print(f"\nActual query error: {error['Actual_Error']}")
            
            # Show result signatures if present
            if 'Expected_Signature' in error:
                print(f"\nExpected result signature: {error['Expected_Signature']}")
            if 'Actual_Signature' in error:
                print(f"\nActual result signature: {error['Actual_Signature']}")
            
            # Show result shapes if present
            if 'Expected_Shape' in error:
                print(f"\nExpected result shape: {error['Expected_Shape']}")
            if 'Actual_Shape' in error:
                print(f"\nActual result shape: {error['Actual_Shape']}")
            
            print("=" * 80)

    # Also save results to a file
    with open("evaluation_results.json", "w") as f:
        json.dump({
            "accuracy": accuracy,
            "total": total,
            "matches": matches,
            "mismatches": total - matches,
            "errors": errors
        }, f, indent=2)
    
    print(f"\nDetailed results saved to evaluation_results.json")

if __name__ == "__main__":
    main()