# Gemini Project

Gemini is an automatic randomized testing suite for 
the Scylla database. It operates by generating random mutations (INSERT, UPDATE, DELETE) and 
verifying them (SELECT) using the CQL protocol across two clusters: 
a system under test (SUT) and a test oracle.

## Project Structure

## Language & Framework
- **Language**: Go 1.24
- **Architecture**: CLI application with modular package structure
- **Database**: Scylla/Cassandra using CQL protocol
- **Testing**: Randomized testing with statistical distributions
