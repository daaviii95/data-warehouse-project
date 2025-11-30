#!/usr/bin/env python3
"""
Setup Airflow Connection for ShopZada PostgreSQL Database
Run this script to create the Airflow connection programmatically

Note: This script should be run within the Airflow environment
"""

import os
import sys

# Try to import Airflow components
try:
    from airflow.models import Connection
    from airflow import settings
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    print("⚠️  Airflow not available. Connection will need to be created manually in Airflow UI.")
    print("   Go to Admin → Connections → Add new connection:")
    print("   - Conn Id: shopzada_postgres")
    print("   - Conn Type: Postgres")
    print("   - Host: shopzada-db")
    print("   - Schema: shopzada")
    print("   - Login: postgres")
    print("   - Password: postgres")
    print("   - Port: 5432")
    sys.exit(0)

# Database connection details
conn_id = 'shopzada_postgres'
conn_type = 'postgres'
host = os.getenv('POSTGRES_HOST', 'shopzada-db')
schema = os.getenv('POSTGRES_DB', 'shopzada')
login = os.getenv('POSTGRES_USER', 'postgres')
password = os.getenv('POSTGRES_PASSWORD', 'postgres')
port = int(os.getenv('POSTGRES_PORT', '5432'))

if AIRFLOW_AVAILABLE:
    # Create connection object
    conn = Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        host=host,
        schema=schema,
        login=login,
        password=password,
        port=port
    )

    # Add connection to Airflow
    try:
        session = settings.Session()
        existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()

        if existing_conn:
            print(f"Connection {conn_id} already exists. Updating...")
            existing_conn.conn_type = conn_type
            existing_conn.host = host
            existing_conn.schema = schema
            existing_conn.login = login
            existing_conn.password = password
            existing_conn.port = port
        else:
            print(f"Creating new connection {conn_id}...")
            session.add(conn)

        session.commit()
        session.close()

        print(f"✅ Connection '{conn_id}' configured successfully!")
        print(f"   Host: {host}:{port}")
        print(f"   Database: {schema}")
    except Exception as e:
        print(f"❌ Error creating connection: {e}")
        print("   Please create connection manually in Airflow UI (Admin → Connections)")
        sys.exit(1)
