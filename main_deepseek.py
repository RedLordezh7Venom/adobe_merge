import os
import json
import time
from tenacity import retry, stop_after_attempt, wait_exponential
import psycopg2
from dotenv import load_dotenv
from groq import Groq

# Load environment variables
load_dotenv()

class SQLAgent:
    def __init__(self):
        self.groq_client = Groq(api_key=os.getenv("GROQ_KEY"))
        self.db_config = {
            "dbname": os.getenv("DB_NAME", "postgres"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", "root"),
            "host": os.getenv("DB_HOST", "localhost"),
            "port": os.getenv("DB_PORT", "5432")
        }
        self.schema = self._get_detailed_schema()
        self.total_tokens = 0

    def _get_detailed_schema(self):
        """Extract complete schema including enums and constraints"""
        schema = {
            "tables": {},
            "enums": {},
            "relationships": []
        }

        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()

            # Get enums first
            cur.execute("""
                SELECT t.typname, e.enumlabel
                FROM pg_type t
                JOIN pg_enum e ON t.oid = e.enumtypid
                ORDER BY t.typname, e.enumsortorder;
            """)
            for enum_name, value in cur.fetchall():
                if enum_name not in schema["enums"]:
                    schema["enums"][enum_name] = []
                schema["enums"][enum_name].append(value)

            # Get table structure with constraints
            cur.execute("""
                SELECT 
                    c.table_name,
                    c.column_name,
                    c.data_type,
                    c.is_nullable,
                    c.column_default,
                    tc.constraint_type
                FROM information_schema.columns c
                LEFT JOIN information_schema.constraint_column_usage ccu
                    ON c.table_name = ccu.table_name AND c.column_name = ccu.column_name
                LEFT JOIN information_schema.table_constraints tc
                    ON ccu.constraint_name = tc.constraint_name
                WHERE c.table_schema = 'public'
                ORDER BY c.table_name, c.ordinal_position;
            """)

            current_table = None
            for row in cur.fetchall():
                table_name = row[0]
                if table_name != current_table:
                    schema["tables"][table_name] = []
                    current_table = table_name
                
                column_info = {
                    "name": row[1],
                    "type": row[2],
                    "nullable": row[3] == 'YES',
                    "default": row[4],
                    "constraints": []
                }
                
                if row[5]:
                    column_info["constraints"].append(row[5])
                schema["tables"][table_name].append(column_info)

            # Get foreign keys using pg_catalog
            cur.execute("""
                SELECT
                    conname,
                    conrelid::regclass AS source_table,
                    a.attname AS source_column,
                    confrelid::regclass AS target_table,
                    af.attname AS target_column
                FROM pg_constraint AS c
                JOIN pg_attribute AS a ON a.attnum = ANY(c.conkey) AND a.attrelid = c.conrelid
                JOIN pg_attribute AS af ON af.attnum = ANY(c.confkey) AND af.attrelid = c.confrelid
                WHERE contype = 'f';
            """)
            
            for fk in cur.fetchall():
                schema["relationships"].append({
                    "name": fk[0],
                    "source_table": fk[1],
                    "source_column": fk[2],
                    "target_table": fk[3],
                    "target_column": fk[4]
                })

        except Exception as e:
            print(f"Schema extraction failed: {str(e)}")
        finally:
            conn.close()
            
        return schema

    def _format_schema_prompt(self):
        """Create detailed schema description with enums and constraints"""
        prompt = "Database Schema Description:\n"
        
        # Add enums
        if self.schema["enums"]:
            prompt += "\nEnumerated Types:"
            for enum, values in self.schema["enums"].items():
                prompt += f"\n- {enum}: {', '.join(values)}"

        # Add tables
        prompt += "\n\nTables:"
        for table, columns in self.schema["tables"].items():
            prompt += f"\n\nTable '{table}':"
            for col in columns:
                desc = f"- {col['name']} ({col['type']})"
                if not col["nullable"]:
                    desc += " NOT NULL"
                if col["default"]:
                    desc += f" DEFAULT {col['default']}"
                if col["constraints"]:
                    desc += f" [{', '.join(col['constraints'])}]"
                prompt += "\n" + desc

        # Add relationships
        if self.schema["relationships"]:
            prompt += "\n\nTable Relationships:"
            for rel in self.schema["relationships"]:
                prompt += (
                    f"\n- {rel['source_table']}.{rel['source_column']} "
                    f"→ {rel['target_table']}.{rel['target_column']} "
                    f"(Foreign Key: {rel['name']})"
                )
        
        return prompt

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _query_llm(self, prompt, task_type):
        """Execute LLM query with retry logic"""
        system_msg = {
            "generate": "You are a SQL expert. Generate correct PostgreSQL queries using the schema below. Return only SQL code.",
            "correct": "You are a SQL expert. Correct the SQL query using the schema below. Return only corrected SQL."
        }[task_type]

        response = self.groq_client.chat.completions.create(
            model="mixtral-8x7b-32768",
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=500
        )
        
        self.total_tokens += response.usage.completion_tokens
        return response.choices[0].message.content.strip()

    def generate_sql(self, nl_query):
        """Generate SQL from natural language query"""
        prompt = f"{self._format_schema_prompt()}\n\nUser Request: {nl_query}\nSQL Query:"
        response = self._query_llm(prompt, "generate")
        return self._clean_sql(response)

    def correct_sql(self, bad_sql):
        """Correct invalid SQL query"""
        prompt = f"{self._format_schema_prompt()}\n\nInvalid SQL:\n{bad_sql}\nCorrect SQL:"
        response = self._query_llm(prompt, "correct")
        return self._clean_sql(response)

    def _clean_sql(self, sql_str):
        """Extract SQL code from possible markdown formatting"""
        if '```sql' in sql_str:
            return sql_str.split('```sql')[1].split('```')[0].strip()
        if '```' in sql_str:
            return sql_str.split('```')[1].strip()
        return sql_str

def process_queries():
    agent = SQLAgent()
    
    # Process natural language queries
    with open('nl_test.csv', 'r') as f:
        nl_queries = json.load(f)
    
    nl_results = []
    for query in nl_queries:
        try:
            generated_sql = agent.generate_sql(query['nl_query'])
            nl_results.append({
                "id": query['id'],
                "generated_sql": generated_sql,
                "original_query": query['nl_query']
            })
        except Exception as e:
            nl_results.append({
                "id": query['id'],
                "error": str(e),
                "generated_sql": "/* Error generating query */"
            })

    # Process SQL corrections
    with open('incorrect_sql_test.csv', 'r') as f:
        bad_queries = json.load(f)
    
    corrected_results = []
    for query in bad_queries:
        try:
            corrected_sql = agent.correct_sql(query['incorrect_sql'])
            corrected_results.append({
                "id": query['id'],
                "corrected_sql": corrected_sql,
                "original_sql": query['incorrect_sql']
            })
        except Exception as e:
            corrected_results.append({
                "id": query['id'],
                "error": str(e),
                "corrected_sql": "/* Error correcting query */"
            })

    # Save results with metadata
    result_payload = {
        "metadata": {
            "schema_version": "hackathon_database_iitd.sql",
            "processing_time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "token_usage": agent.total_tokens,
            "tables_processed": list(agent.schema["tables"].keys())
        },
        "nl_results": nl_results,
        "corrected_results": corrected_results
    }

    with open('results.json', 'w') as f:
        json.dump(result_payload, f, indent=2)
    
    return result_payload

if __name__ == "__main__":
    start_time = time.time()
    results = process_queries()
    duration = time.time() - start_time
    
    print(f"Processed {len(results['nl_results'])} NL queries")
    print(f"Corrected {len(results['corrected_results'])} SQL queries")
    print(f"Total tokens used: {results['metadata']['token_usage']}")
    print(f"Execution time: {duration:.2f} seconds")
    print(f"Tables considered: {', '.join(results['metadata']['tables_processed'])}")