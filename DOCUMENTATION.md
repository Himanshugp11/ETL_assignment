1. Build Requirements
   
To build and run this tool, the following must be installed on the host system:

Python 3.10+ (Developed and tested using Python 3.14).

Docker Desktop: Required to host the LocalStack (SQS) and PostgreSQL containers.

Pip: Python package manager for installing dependencies.

LocalStack Account: For accessing the LocalStack Web Dashboard at app.localstack.cloud and publish the message in SQS.

==========================================================================

Dependencies
The tool requires the following Python libraries:

boto3: AWS SDK for Python to interact with SQS.

psycopg2-binary: PostgreSQL adapter for Python database connectivity.

===========================================================================
2. Configuration & Environment Setup
Step 1: Virtual Environment
It is recommended to use the provided virtual environment to ensure dependency isolation.

PowerShell
# Activate the existing environment
.\virtulenv\Scripts\activate

# Install required packages
pip install boto3 psycopg2-binary
Step 2: Infrastructure Setup (Docker & Cloud)
LocalStack (SQS Service):
The infrastructure uses LocalStack to emulate AWS SQS. You can monitor the queue status via the LocalStack Cloud Dashboard.

PowerShell
docker run -d --name localstack-main -p 4566:4566 -p 4510-4560:4510-4560 localstack/localstack
PostgreSQL Database:
A local PostgreSQL instance is used for data persistence.

PowerShell
docker run -d --name local-datastore -e POSTGRES_USER=admin -e POSTGRES_PASSWORD=****** -e POSTGRES_DB=**** -p 5432:5432 postgres:16-alpine

==========================================================================

3. How to Run the Tool
Step 1: Data Generation
Since the external message-generator binary was unavailable, a Python-based generator is used to simulate the interview data (including malformed, route-based, and location-based messages).

PowerShell
python src/mock_generator.py

Step 2: Event Processing
Run the main processing script to consume events from SQS, transform them, and persist them to the database.

PowerShell
python src/final_main.py

==========================================================================

4. Usage & Parameters
The tool operates as a headless CLI utility designed for automated ETL tasks.

Connection Defaults: The tool automatically routes to localhost:4566 (SQS) and localhost:5432 (PostgreSQL).

Idempotency: On execution, the tool automatically checks for the existence of the trip_events table and creates it if missing.

Queue Cleanup: In accordance with the requirements, the tool utilizes a "Delete-on-Success" pattern, ensuring the SQS queue is completely empty after the final event is persisted.

=========================================================================

5. Challenges & Solutions
Missing Message Generator Binary
Challenge: The expected windows.exe generator was not present in the local directory.

Solution: Analyzed the provided Go source code (message-generator.go) and re-implemented the logic in a Python mock script to ensure the data ingestion pipeline could be tested end-to-end.

Structural Heterogeneity
Challenge: Events arrived with inconsistent schemas (some using route lists with string dates, others using locations lists with Unix timestamps).

Solution: Developed a polymorphic transformation layer that identifies the schema type and normalizes all fields into a single, standard JSON output.

Database Bootstrapping & Connectivity
Challenge: Initial connection attempts failed with Connection refused because the Docker container was "Up" but the database engine was still initializing.

Solution: Integrated error handling and connectivity verification to ensure the database was ready to accept TCP/IP connections before the processor began execution.

SDK Authentication
Challenge: boto3 requires credentials even for local mock services.

Solution: Configured the client with "dummy" AWS credentials to satisfy the SDK's internal signing requirements.
