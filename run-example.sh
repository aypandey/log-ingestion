#!/bin/bash

# Log Ingestion System - Quick Start Script

echo "=== Log Ingestion System - Quick Start ==="
echo ""

# Check if Maven is installed
if ! command -v mvn &> /dev/null; then
    echo "Error: Maven is not installed. Please install Maven first."
    exit 1
fi

# Check Java version
echo "Checking Java version..."
java -version

echo ""
echo "Building the project..."
mvn clean compile

if [ $? -ne 0 ]; then
    echo "Build failed! Please check the errors above."
    exit 1
fi

echo ""
echo "Build successful!"
echo ""
echo "Starting the Log Ingestion System example..."
echo "=============================================="
echo ""

# Run the example
mvn exec:java -Dexec.mainClass="com.logingestion.Example"
