#!/bin/bash

echo "Starting backend services..."
python create_table.py

echo "Starting ETL process..."
python etl/main.py

echo "Creating a view..."
python create_view.py
