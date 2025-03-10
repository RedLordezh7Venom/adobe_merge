import json
import psycopg2
from psycopg2.extras import RealDictCursor
from decimal import Decimal
from datetime import date
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Custom JSON encoder to handle Decimal and date objects
class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)  # Convert Decimal to float
        if isinstance(obj, date):
            return obj.isoformat()  # Convert date to ISO format string
        return super().default(obj)

# Database connection function
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        database=os.getenv('DB_NAME', 'postgres'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD', 'root'),
        port=os.getenv('DB_PORT', '5432')
    )

# Function to execute a query and return the results
def execute_query(query):
    """Execute a SQL query and return the results as a list of dictionaries."""
    conn = None
    try:
        conn = get_db_connection()
        conn.autocommit = True  # Set autocommit to avoid transaction issues
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            results = cur.fetchall()
            return results, None
    except Exception as e:
        return None, str(e)
    finally:
        if conn:
            conn.close()

# Function to compare two query results
def compare_query_results(result1, result2):
    """
    Compare two query results (lists of dictionaries).
    Returns True if they match, False otherwise.
    """
    if result1 is None or result2 is None:
        return False
    
    # If the lengths are different, they don't match
    if len(result1) != len(result2):
        return False
    
    # Compare each row
    for row1, row2 in zip(result1, result2):
        if row1 != row2:
            return False
    
    return True

# Function to evaluate queries
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
            
            # Execute both queries
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
                    "Expected_Results": test_results,
                    "Actual_Results": output_results
                })
    
    # Calculate accuracy
    accuracy = (matches / total * 100) if total > 0 else 0
    
    return accuracy, errors, matches, total

# Main function
def main():
    test_file = "nl_test.json"  # Replace with your input file path
    output_file = "output_sql_generation_task.json"  # Replace with your output file path
    
    # Evaluate queries
    accuracy, errors, matches, total = evaluate_queries(test_file, output_file)
    
    # Print results
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
            
            # Show results if present
            if 'Expected_Results' in error:
                print(f"\nExpected results:")
                print(json.dumps(error['Expected_Results'], indent=2, cls=CustomEncoder))
            if 'Actual_Results' in error:
                print(f"\nActual results:")
                print(json.dumps(error['Actual_Results'], indent=2, cls=CustomEncoder))
            
            print("=" * 80)

    # Save results to a file
    with open("evaluation_results.json", "w") as f:
        json.dump({
            "accuracy": accuracy,
            "total": total,
            "matches": matches,
            "mismatches": total - matches,
            "errors": errors
        }, f, indent=2, cls=CustomEncoder)
    
    print(f"\nDetailed results saved to evaluation_results.json")

if __name__ == "__main__":
    main()