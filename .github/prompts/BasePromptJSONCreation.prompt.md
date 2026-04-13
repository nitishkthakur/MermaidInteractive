---
agent: agent
---
# Goal
Build a prompt which takes as input a SQL Query/Collection of SQL Stored Procs / Informatica code / Ab-Initio mp graph files and write a json file(or multiple files) which captures lineage. Then, Write code that converts the JSON to an excel artifact(multi-sheet if requird) and a mermaid diagram. Plan on what attributes the JSON must have, elements to the prompt, structure of the excel file, etc.

# JSON Structure
The JSON Structure must be such that: it can be conveniently converted to both - the excel artifact and the mermaid programatically. THe JSON must clearly capture:
1. ALL THE INPUT TABLES/SCHEMAS, OUTPUT TABLES/SCHEMAS
2. Refer to the columns as table.column to show the user which table the column belongs to
3. The JSON MUST show how a column undergoes transformations to become the target column(s) - as one input column might feed into multiple output columns and vice versa. Think - that the user asks the following question - what transformations does this input column undergo and which columns does it end up calculating at the end of the pipeline
4. Account for the fact that there might be multiple stored procs per file or multiple CTEs or LONG Ab-Initio files. 

# Agent Harness
The Agent harness is deep_agent. Write the code and create an env file. I will fill in my openrouter keys there. Also let me specify the LLM in the env file. 

# Things you must output
1. Prompt which produces the JSON(s) having this comprehensive level of information and a structure that checks the resulting JSON against the Uploaded file multiple times. I need to capture everything.
2. An Excel Artifact showing all this info
3. Code that takes the JSON and converts it to a detailed mermaid flow.

# Test
1. Test extensively - create Long (1000, 2000 line SQLs, with conmplicated logic, CTEs, Joins, 20+ CTEs, including stored procs (10+), Ab-Initio file, Pandas ETL steps, Informatica files to test on and check the comprehensiveness. )

Plan, Iterate and validate your results. Start by initializing the process - and let me enter the env file crds so that you can validate and iterate. 

Implement everything in /Users/nitish/Documents/Github/MermaidInteractive/DataPipelineToMermaid Folder. Plan first.
