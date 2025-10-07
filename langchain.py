import os
from langchain.chat_models import ChatOpenAI
from langchain.chains import SQLDatabaseChain
from langchain.agents import initialize_agent, Tool
from langchain.agents import AgentType
from langchain.agents import AgentExecutor
from langchain.tools import SQLDatabase

# Set up environment variable for OpenAI API key
os.environ["OPENAI_API_KEY"] = "<your_openai_api_key>"

# 1. Initialize the OpenAI LLM (e.g., GPT-3, GPT-4)
llm = ChatOpenAI(temperature=0)  # Set temperature to 0 for more deterministic responses

# 2. Set up your database (SQL)
# Assuming you are using SQLite as an example. You can replace it with any SQL database.
from langchain.agents.tools import SQLDatabase
from langchain.sql_database import SQLDatabase

# Connect to your database. Replace with your actual DB path and connection string
db = SQLDatabase.from_uri("sqlite:///path_to_your_database.db")

# 3. Set up a SQLDatabaseChain for interaction
sql_database_chain = SQLDatabaseChain(llm=llm, database=db)

# 4. Initialize an agent that uses the SQLDatabaseChain
tools = [
    Tool(
        name="SQLDatabase",
        func=sql_database_chain.run,
        description="Use this tool to query the database"
    )
]

# 5. Initialize the AgentExecutor with the LLM
agent_executor = AgentExecutor(
    tools=tools,
    agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,  # Zero-shot agent type
    llm=llm
)
# Example natural language input
query = "Show me the total sales from the 'orders' table for 2024"

# Run the agent to convert the query to SQL and get the result
result = agent_executor.run(query)

# Print the result
print(result)
