#!/usr/bin/env python3
# db_operations.py
# EPF - MDE Data Models - Lab 1
# Robust DBMS for Transactional Processing - Python Script

import mariadb
import time
import random
import threading
import zipfile
import os
from datetime import datetime

# Constants
DB_CONFIG = {
    'user': 'root',
    'password': 'myPas$',
    'host': 'localhost',
    'port': 3306,
    'database': 'flight_reservation'
}

DUMP_FILE = 'flight_database_dump.sql'
COMPRESSED_DUMP = 'flight_database_dump.zip'
MAX_RETRIES = 3
RETRY_DELAY = 2

def get_db_connection():
    """Establish database connection with retry logic"""
    attempts = 0
    while attempts < MAX_RETRIES:
        try:
            conn = mariadb.connect(**DB_CONFIG)
            return conn
        except mariadb.Error as e:
            print(f"Connection attempt {attempts + 1} failed: {e}")
            time.sleep(RETRY_DELAY)
            attempts += 1
    raise Exception("Failed to connect to database after multiple attempts")

def import_dump(dump_file_path, new_db_name=None):
    """
    Import SQL dump file into MariaDB
    
    Args:
        dump_file_path (str): Path to SQL dump file
        new_db_name (str): Optional new database name
        
    Returns:
        tuple: (success: bool, message: str)
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Create new database if specified
        if new_db_name:
            cur.execute(f"CREATE DATABASE IF NOT EXISTS {new_db_name}")
            cur.execute(f"USE {new_db_name}")
        
        # Read and execute dump file
        with open(dump_file_path, 'r', encoding='utf-8') as f:
            sql_commands = f.read().split(';')
            
            for command in sql_commands:
                if command.strip():
                    try:
                        cur.execute(command)
                    except mariadb.Error as e:
                        print(f"Warning: {e} - Continuing with next command")
            
        conn.commit()
        return True, "Dump imported successfully"
        
    except Exception as e:
        return False, f"Error importing dump: {e}"
    finally:
        if 'conn' in locals():
            conn.close()

def export_dump(output_file_path, db_name=None):
    """
    Export database to SQL dump file
    
    Args:
        output_file_path (str): Path for output SQL file
        db_name (str): Database to export (defaults to configured DB)
        
    Returns:
        tuple: (success: bool, message: str)
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        if db_name:
            cur.execute(f"USE {db_name}")
        
        # Get all tables
        cur.execute("SHOW TABLES")
        tables = [table[0] for table in cur.fetchall()]
        
        with open(output_file_path, 'w', encoding='utf-8') as f:
            # Write header
            f.write(f"-- Database dump generated on {datetime.now()}\n")
            f.write(f"-- Database: {db_name or DB_CONFIG['database']}\n\n")
            
            # Write table structure and data
            for table in tables:
                # Table structure
                f.write(f"\n-- Table structure for table `{table}`\n")
                f.write(f"DROP TABLE IF EXISTS `{table}`;\n")
                cur.execute(f"SHOW CREATE TABLE `{table}`")
                create_table = cur.fetchone()[1]
                f.write(f"{create_table};\n\n")
                
                # Table data
                f.write(f"-- Dumping data for table `{table}`\n")
                cur.execute(f"SELECT * FROM `{table}`")
                rows = cur.fetchall()
                
                if rows:
                    columns = [desc[0] for desc in cur.description]
                    f.write(f"INSERT INTO `{table}` (`{'`,`'.join(columns)}`) VALUES\n")
                    
                    for i, row in enumerate(rows):
                        values = []
                        for value in row:
                            if value is None:
                                values.append("NULL")
                            elif isinstance(value, (int, float)):
                                values.append(str(value))
                            else:
                                values.append(f"'{str(value).replace("'", "''")}'")
                        
                        f.write(f"({','.join(values)})")
                        f.write(";\n" if i == len(rows)-1 else ",\n")
        
        return True, "Dump exported successfully"
        
    except Exception as e:
        return False, f"Error exporting dump: {e}"
    finally:
        if 'conn' in locals():
            conn.close()

def compress_file(input_path, output_path):
    """Compress file using ZIP"""
    try:
        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(input_path, os.path.basename(input_path))
        return True, "File compressed successfully"
    except Exception as e:
        return False, f"Error compressing file: {e}"

def decompress_file(input_path, output_dir):
    """Decompress ZIP file"""
    try:
        with zipfile.ZipFile(input_path, 'r') as zipf:
            zipf.extractall(output_dir)
        return True, "File decompressed successfully"
    except Exception as e:
        return False, f"Error decompressing file: {e}"

def check_data_consistency():
    """Run data consistency checks"""
    results = {}
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Check 1: Duplicate seat assignments
        start_time = time.time()
        cur.execute("""
            SELECT f.flight_id, r.seat, COUNT(*) as duplicates
            FROM Flight f
            JOIN Booking b ON f.flight_id = b.flight_id
            JOIN Reserve r ON b.booking_id = r.booking_id
            GROUP BY f.flight_id, r.seat
            HAVING duplicates > 1
        """)
        duplicate_seats = cur.fetchall()
        results['duplicate_seats'] = {
            'count': len(duplicate_seats),
            'time': time.time() - start_time,
            'data': duplicate_seats
        }
        
        # Check 2: Overlapping flights for passengers
        start_time = time.time()
        cur.execute("""
            [Implementation of overlapping flight check]
        """)
        overlapping_flights = cur.fetchall()
        results['overlapping_flights'] = {
            'count': len(overlapping_flights),
            'time': time.time() - start_time,
            'data': overlapping_flights
        }
        
        # Add additional consistency checks here...
        
        return True, "Consistency checks completed", results
        
    except Exception as e:
        return False, f"Error during consistency checks: {e}", {}
    finally:
        if 'conn' in locals():
            conn.close()

def benchmark_import_export(iterations=5):
    """Benchmark import/export operations"""
    results = []
    
    for i in range(1, iterations + 1):
        # Import
        start_time = time.time()
        success, message = import_dump(DUMP_FILE, f"benchmark_db_{i}")
        import_time = time.time() - start_time
        
        if not success:
            results.append({
                'iteration': i,
                'success': False,
                'error': message,
                'import_time': None,
                'export_time': None
            })
            continue
        
        # Export
        export_file = f"benchmark_export_{i}.sql"
        start_time = time.time()
        success, message = export_dump(export_file, f"benchmark_db_{i}")
        export_time = time.time() - start_time
        
        results.append({
            'iteration': i,
            'success': success,
            'error': message if not success else None,
            'import_time': import_time,
            'export_time': export_time if success else None,
            'export_file': export_file if success else None
        })
    
    return results

def threaded_import(dump_file_path, db_name_prefix, num_threads=4):
    """Multi-threaded import of SQL dump"""
    # Split dump file into chunks
    chunks = split_dump_file(dump_file_path, num_threads)
    
    threads = []
    results = []
    
    for i, chunk_file in enumerate(chunks):
        db_name = f"{db_name_prefix}_thread_{i}"
        t = threading.Thread(
            target=import_chunk,
            args=(chunk_file, db_name, results)
        )
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
    
    # Clean up chunk files
    for chunk_file in chunks:
        try:
            os.remove(chunk_file)
        except:
            pass
    
    return results

def split_dump_file(dump_file_path, num_chunks):
    """Split SQL dump file into chunks for multi-threaded import"""
    # Implementation would go here
    # Returns list of chunk file paths
    return [dump_file_path] * num_chunks  # Placeholder

def import_chunk(chunk_file, db_name, results):
    """Thread worker for importing a chunk"""
    try:
        start_time = time.time()
        success, message = import_dump(chunk_file, db_name)
        elapsed = time.time() - start_time
        
        results.append({
            'db_name': db_name,
            'success': success,
            'message': message,
            'time': elapsed
        })
    except Exception as e:
        results.append({
            'db_name': db_name,
            'success': False,
            'message': str(e),
            'time': None
        })

if __name__ == "__main__":
    print("Database Operations Script - EPF Lab 1")
    
    # Example usage
    print("\n1. Importing database dump...")
    success, message = import_dump(DUMP_FILE)
    print(f"Result: {success}, Message: {message}")
    
    print("\n2. Running data consistency checks...")
    success, message, results = check_data_consistency()
    print(f"Result: {success}, Message: {message}")
    if success:
        print(f"Duplicate seats found: {results['duplicate_seats']['count']}")
    
    print("\n3. Benchmarking import/export...")
    benchmark_results = benchmark_import_export(3)
    for result in benchmark_results:
        print(f"Iteration {result['iteration']}: "
              f"Import {result['import_time']:.2f}s, "
              f"Export {result.get('export_time', 'N/A'):.2f}s")
    
    print("\nScript completed.")