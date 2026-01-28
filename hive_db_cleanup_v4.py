#!/usr/bin/env python3
"""
Hive Database Cleanup Script
Modified for Python 3.5+ Compatibility and Ozone External Table Purging.
Version 4: Added skipTrash flag, DB prefix filtering, and removed interactive prompts
"""

import os
import sys
import argparse
import logging
import subprocess
import json
from datetime import datetime
from typing import List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

class HiveDatabaseCleaner:
    def __init__(self, config_file: str):
        self.config = self.load_config(config_file)
        self.setup_logging()
        self.beeline_url = None
        
    def load_config(self, config_file: str) -> Dict[str, str]:
        """Load configuration from INI file"""
        config = {}
        try:
            with open(config_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip().strip('"')
                        
                        # Parse boolean values
                        if value.lower() in ['true', 'yes', '1']:
                            value = 'true'
                        elif value.lower() in ['false', 'no', '0']:
                            value = 'false'
                        
                        config[key] = value
            return config
        except Exception as e:
            print("ERROR: Failed to load config file: {}".format(e))
            sys.exit(1)
    
    def setup_logging(self):
        """Setup logging configuration"""
        log_dir = "/tmp/hive_db_cleanup/{}".format(datetime.now().strftime('%Y-%m-%d'))
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        log_file = os.path.join(log_dir, "db_cleanup_{}.log".format(datetime.now().strftime('%H%M%S')))
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("Log file: {}".format(log_file))
    
    def get_config_bool(self, key: str, default: bool = False) -> bool:
        """Get boolean value from config with default"""
        value = self.config.get(key)
        if value is None:
            return default
        return value.lower() == 'true'
    
    def kinit(self) -> bool:
        """Perform Kerberos authentication"""
        keytab_path = self.config.get('KEYTAB_PATH')
        principal = self.config.get('PRINCIPAL')
        
        if not keytab_path or not principal:
            self.logger.error("KEYTAB_PATH and PRINCIPAL must be configured")
            return False
        
        if not os.path.exists(keytab_path):
            self.logger.error("Keytab file not found: {}".format(keytab_path))
            return False
        
        try:
            subprocess.check_output(
                ['kinit', '-kt', keytab_path, principal],
                stderr=subprocess.STDOUT
            )
            self.logger.info("Kerberos authentication successful for {}".format(principal))
            return True
        except subprocess.CalledProcessError as e:
            self.logger.error("kinit failed: {}".format(e))
            return False
        except FileNotFoundError:
            self.logger.error("kinit command not found. Please install Kerberos client.")
            return False
    
    def build_beeline_url(self) -> str:
        """Build beeline connection URL from config"""
        host = self.config.get('HIVE_HOST')
        port = self.config.get('HIVE_PORT', '10000')
        database = self.config.get('HIVE_DATABASE', 'default')
        principal = self.config.get('HIVE_PRINCIPAL', 'hive/_HOST@REALM.COM')
        truststore = self.config.get('TRUSTSTORE_PATH')
        truststore_password = self.config.get('TRUSTSTORE_PASSWORD')
        
        if not host:
            self.logger.error("HIVE_HOST must be configured")
            return None
        
        url = "jdbc:hive2://{}:{}/{}".format(host, port, database)
        url += ";principal={}".format(principal)
        
        if truststore and truststore_password:
            url += ";ssl=true;sslTrustStore={}".format(truststore)
            url += ";trustStorePassword={}".format(truststore_password)
            url += ";trustStoreType=jks"
        
        return url
    
    def execute_beeline_query(self, query: str, silent: bool = False) -> Tuple[bool, str]:
        """Execute query using beeline (Compatible with Python 3.5+)"""
        if not self.beeline_url:
            self.logger.error("Beeline URL not initialized")
            return False, ""
        
        if not silent:
            self.logger.debug("Executing query: {}".format(query))
        
        cmd = [
            'beeline', '-u', self.beeline_url, '-e', query,
            '--silent=true', '--showHeader=false', '--outputformat=tsv2'
        ]
        
        try:
            # Compatible with Python 3.5+
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                timeout=600
            )
            
            if result.returncode == 0:
                return True, result.stdout.strip()
            else:
                if not silent:
                    self.logger.error("Query failed: {}".format(query))
                    self.logger.error("Return code: {}".format(result.returncode))
                    self.logger.error("Stdout: {}".format(result.stdout))
                    self.logger.error("Stderr: {}".format(result.stderr))
                return False, result.stderr.strip()
        except subprocess.TimeoutExpired:
            self.logger.error("Query timeout: {}".format(query))
            return False, "Query timeout"
        except Exception as e:
            self.logger.error("Error executing query: {}".format(e))
            return False, str(e)
    
    def test_connection(self) -> bool:
        """Test beeline connection"""
        self.logger.info("Testing beeline connection...")
        query = "SHOW DATABASES"
        success, output = self.execute_beeline_query(query, silent=False)
        
        if success:
            self.logger.info("Connection test successful")
            self.logger.info("Available databases: {}".format(output[:200]))
            return True
        else:
            self.logger.error("Connection test failed")
            self.logger.error("Output: {}".format(output))
            return False
    
    def database_exists(self, database: str) -> bool:
        """Check if database exists"""
        query = "SHOW DATABASES"
        success, output = self.execute_beeline_query(query, silent=True)
        
        if not success:
            self.logger.error("Failed to list databases")
            return False
        
        if not output:
            return False
        
        # Parse beeline table output to extract database names
        databases = []
        for line in output.split('\n'):
            line = line.strip()
            # Skip empty lines and table borders
            if not line or line.startswith('+') or line.startswith('|--'):
                continue
            
            # Extract database name from table format: | database_name |
            if line.startswith('|') and line.endswith('|'):
                db = line.strip('|').strip()
                # Skip header row
                if db and db.lower() not in ['database_name', 'tab_name']:
                    databases.append(db)
        
        self.logger.debug("Available databases: {}".format(databases))
        exists = database in databases
        self.logger.debug("Database '{}' exists: {}".format(database, exists))
        
        return exists
    
    def get_tables_in_database(self, database: str) -> List[str]:
        """Get all tables in database"""
        query = "SHOW TABLES IN {}".format(database)
        success, output = self.execute_beeline_query(query)
        
        if not success:
            self.logger.error("Failed to get tables for database: {}".format(database))
            return []
        
        if not output:
            return []
        
        tables = []
        for line in output.split('\n'):
            line = line.strip()
            
            # Skip empty lines and table borders
            if not line or line.startswith('+') or line.startswith('|--'):
                continue
            
            # Extract table name from table format: | tab_name |
            if line.startswith('|') and line.endswith('|'):
                table = line.strip('|').strip()
                # Skip header row
                if table and table.lower() not in ['tab_name', 'tablename']:
                    tables.append(table)
        
        self.logger.debug("Found {} tables in {}: {}".format(len(tables), database, tables))
        return tables
    
    def delete_ozone_data_directly(self, location: str, skip_trash: bool = True) -> bool:
        """Delete Ozone data directly using ozone fs command"""
        if not location or not location.startswith(('ofs://', 'o3fs://')):
            return False
        
        try:
            self.logger.info("Deleting Ozone data: {} (skipTrash={})".format(location, skip_trash))
            
            # Build command based on skipTrash flag
            if skip_trash:
                cmd = ['ozone', 'fs', '-rm', '-r', '-skipTrash', location]
            else:
                cmd = ['ozone', 'fs', '-rm', '-r', location]
            
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                timeout=300
            )
            
            if result.returncode == 0:
                self.logger.info("Successfully deleted Ozone data: {}".format(location))
                return True
            else:
                # It's OK if the path doesn't exist (might be already deleted)
                if 'No such file or directory' in result.stderr or 'does not exist' in result.stderr:
                    self.logger.info("Ozone path doesn't exist (already deleted?): {}".format(location))
                    return True
                else:
                    self.logger.error("Failed to delete Ozone data: {}".format(location))
                    self.logger.error("Error: {}".format(result.stderr))
                    return False
        except subprocess.TimeoutExpired:
            self.logger.error("Timeout deleting Ozone data: {}".format(location))
            return False
        except Exception as e:
            self.logger.error("Error deleting Ozone data {}: {}".format(location, e))
            return False
    
    def get_table_info(self, database: str, table: str) -> Dict:
        """Get table information including storage type"""
        info = {
            'database': database,
            'table': table,
            'is_partitioned': False,
            'partition_count': 0,
            'location': None,
            'is_ozone': False,
            'is_external': False
        }
        
        # Get table metadata
        query = "DESCRIBE FORMATTED {}.{}".format(database, table)
        success, output = self.execute_beeline_query(query, silent=True)
        
        if success and output:
            for line in output.split('\n'):
                line = line.strip()
                
                # Check location - handle table format with pipes
                if 'Location:' in line or 'location:' in line.lower():
                    # Remove pipes and extract location
                    parts = line.split('|')
                    for part in parts:
                        part = part.strip()
                        if part and 'location:' not in part.lower() and part.upper() != 'NULL':
                            # This should be the actual location
                            if part.startswith(('ofs://', 'o3fs://', 'hdfs://', '/')):
                                loc = part.strip()
                                info['location'] = loc
                                # Check if it's Ozone
                                if loc.startswith(('ofs://', 'o3fs://')):
                                    info['is_ozone'] = True
                                break
                
                # Check table type - handle table format with pipes
                if 'Table Type:' in line or 'table type:' in line.lower():
                    if 'EXTERNAL' in line.upper():
                        info['is_external'] = True
        
        # Check partitions
        query = "SHOW PARTITIONS {}.{}".format(database, table)
        success, output = self.execute_beeline_query(query, silent=True)
        if success and output and 'not partitioned' not in output.lower():
            # Parse partition output - skip table borders and headers
            partitions = []
            for line in output.split('\n'):
                line = line.strip()
                # Skip empty lines, borders, and headers
                if not line or line.startswith('+') or line.startswith('|--'):
                    continue
                # Extract partition from table format
                if line.startswith('|') and line.endswith('|'):
                    partition = line.strip('|').strip()
                    # Skip header row
                    if partition and partition.lower() not in ['partition', 'part_name']:
                        partitions.append(partition)
            
            if partitions:
                info['is_partitioned'] = True
                info['partition_count'] = len(partitions)
        
        return info
    
    def drop_table(self, database: str, table: str, 
                   delete_ozone_data: bool = True, skip_trash: bool = True) -> bool:
        """Drop a single table (Ozone-focused)"""
        info = self.get_table_info(database, table)
        
        # For Ozone tables with data deletion enabled
        if info['is_ozone'] and delete_ozone_data and info['location']:
            # Delete Ozone data BEFORE dropping the table
            # because external.table.purge doesn't work properly in Ozone
            self.logger.info("Deleting Ozone data before dropping table: {}.{}".format(database, table))
            
            # Delete the actual data using ozone fs command with skipTrash flag
            if not self.delete_ozone_data_directly(info['location'], skip_trash):
                self.logger.warning("Failed to delete Ozone data, but continuing with table drop")
        
        # Drop the table (metadata)
        query = "DROP TABLE IF EXISTS {}.{}".format(database, table)
        
        self.logger.info("Dropping table: {}.{} (Ozone: {})".format(
            database, table, 'Yes' if info['is_ozone'] else 'No'
        ))
        success, _ = self.execute_beeline_query(query)
        
        if success:
            self.logger.info("Successfully dropped: {}.{}".format(database, table))
        else:
            self.logger.error("Failed to drop: {}.{}".format(database, table))
        
        # Small delay to prevent overwhelming metastore
        time.sleep(0.1)
        return success

    def drop_tables_parallel(self, database: str, tables: List[str], max_workers: int = 5, 
                             delete_ozone_data: bool = True,
                             skip_trash: bool = True) -> Dict[str, int]:
        """Drop multiple tables in parallel"""
        results = {'success': 0, 'failed': 0}
        total = len(tables)
        
        if total == 0:
            return results
        
        self.logger.info("Dropping {} tables from {} using {} workers".format(total, database, max_workers))
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_table = {
                executor.submit(self.drop_table, database, t, delete_ozone_data, skip_trash): t 
                for t in tables
            }
            
            completed = 0
            for future in as_completed(future_to_table):
                table = future_to_table[future]
                completed += 1
                
                try:
                    if future.result():
                        results['success'] += 1
                    else:
                        results['failed'] += 1
                except Exception as e:
                    self.logger.error("Exception dropping {}: {}".format(table, e))
                    results['failed'] += 1
                
                # Progress updates
                if completed % 10 == 0 or completed == total:
                    progress_pct = (completed * 100) // total
                    self.logger.info("Progress: {}/{} ({}%) - Success: {}, Failed: {}".format(
                        completed, total, progress_pct, results['success'], results['failed']
                    ))
        
        return results

    def drop_database(self, database: str) -> bool:
        """Drop the database"""
        query = "DROP DATABASE IF EXISTS {}".format(database)
        self.logger.info("Dropping database: {}".format(database))
        success, _ = self.execute_beeline_query(query)
        
        if success:
            self.logger.info("Successfully dropped database: {}".format(database))
        else:
            self.logger.error("Failed to drop database: {}".format(database))
        
        return success

    def analyze_database(self, database: str) -> Dict:
        """Analyze database to get statistics"""
        self.logger.info("Analyzing database: {}".format(database))
        
        if not self.database_exists(database):
            self.logger.warning("Database does not exist: {}".format(database))
            return None
        
        tables = self.get_tables_in_database(database)
        
        analysis = {
            'database': database,
            'total_tables': len(tables),
            'partitioned_tables': 0,
            'non_partitioned_tables': 0,
            'total_partitions': 0,
            'ozone_tables': 0,
            'hdfs_tables': 0,
            'external_tables': 0,
            'managed_tables': 0,
            'tables': []
        }
        
        self.logger.info("Found {} tables in {}".format(len(tables), database))
        
        for i, table in enumerate(tables, 1):
            info = self.get_table_info(database, table)
            analysis['tables'].append(info)
            
            if info['is_partitioned']:
                analysis['partitioned_tables'] += 1
                analysis['total_partitions'] += info['partition_count']
            else:
                analysis['non_partitioned_tables'] += 1
            
            if info['is_ozone']:
                analysis['ozone_tables'] += 1
            else:
                analysis['hdfs_tables'] += 1
            
            if info['is_external']:
                analysis['external_tables'] += 1
            else:
                analysis['managed_tables'] += 1
            
            if i % 50 == 0 or i == len(tables):
                self.logger.info("Analyzed {}/{} tables...".format(i, len(tables)))
        
        return analysis

    def cleanup_database(self, database: str, dry_run: bool = False, max_workers: int = 5, 
                         delete_ozone_data: bool = True,
                         skip_trash: bool = True) -> bool:
        """Cleanup a database by dropping all tables then the database"""
        self.logger.info("="*80)
        self.logger.info("Starting cleanup for database: {}".format(database))
        self.logger.info("Dry run: {}".format(dry_run))
        self.logger.info("Max workers: {}".format(max_workers))
        self.logger.info("Delete Ozone data: {}".format(delete_ozone_data))
        self.logger.info("Skip Trash (Ozone): {}".format(skip_trash))
        self.logger.info("="*80)
        
        if not self.database_exists(database):
            self.logger.warning("Database not found: {}".format(database))
            return False
        
        tables = self.get_tables_in_database(database)
        
        if not tables:
            self.logger.info("Database {} is empty".format(database))
            if not dry_run:
                return self.drop_database(database)
            return True
        
        self.logger.info("Found {} tables in {}".format(len(tables), database))
        
        if dry_run:
            self.logger.info("DRY RUN MODE - Analyzing tables...")
            
            ozone_count = 0
            non_ozone_count = 0
            external_count = 0
            managed_count = 0
            
            for i, table in enumerate(tables, 1):
                info = self.get_table_info(database, table)
                if info['is_ozone']:
                    ozone_count += 1
                else:
                    non_ozone_count += 1
                
                if info['is_external']:
                    external_count += 1
                else:
                    managed_count += 1
                
                if i % 50 == 0:
                    self.logger.info("Analyzed {}/{} tables...".format(i, len(tables)))
            
            self.logger.info("\nStorage breakdown:")
            self.logger.info("  Ozone tables: {}".format(ozone_count))
            self.logger.info("  Non-Ozone tables: {}".format(non_ozone_count))
            self.logger.info("  External tables: {} (metadata only)".format(external_count))
            self.logger.info("  Managed tables: {} (will delete data)".format(managed_count))
            return True
        
        # Drop all tables
        results = self.drop_tables_parallel(database, tables, max_workers, 
                                           delete_ozone_data, skip_trash)
        
        self.logger.info("Table deletion results:")
        self.logger.info("  Success: {}".format(results['success']))
        self.logger.info("  Failed: {}".format(results['failed']))
        
        if results['failed'] > 0:
            self.logger.warning("{} tables failed to drop".format(results['failed']))
            return False
        
        # Drop the database
        return self.drop_database(database)

    def cleanup_ozone_trash(self, fid_paths: List[str] = None, buckets: List[str] = None) -> Dict[str, bool]:
        """
        Clean up Ozone .Trash directories independently.
        
        This function searches and deletes .Trash directories in specified FID paths.
        It can be run independently without deleting any databases.
        
        Args:
            fid_paths: List of FID paths to check (e.g., ['fid1', 'fid2', 'fid3'])
                      If None, uses default ['fid2']
            buckets: List of bucket names to check (e.g., ['raw', 'managed', 'work'])
                     If None, uses default ['raw', 'managed', 'work']
        
        Returns:
            Dict mapping trash paths to success/failure status
        
        Example:
            cleanup_ozone_trash(['fid1', 'fid2'], ['raw', 'managed'])
            # Checks:
            # - ofs://ozone.../fid1/raw/.Trash/
            # - ofs://ozone.../fid1/managed/.Trash/
            # - ofs://ozone.../fid2/raw/.Trash/
            # - ofs://ozone.../fid2/managed/.Trash/
        """
        
        results = {}
        
        # Get Ozone service ID from config
        ozone_service_id = self.config.get('OZONE_SERVICE_ID', 'ozone1756774157')
        
        # Use defaults if not provided
        if fid_paths is None:
            fid_paths = ['fid2']
        if buckets is None:
            buckets = ['raw', 'managed', 'work']
        
        self.logger.info("="*80)
        self.logger.info("CLEANING UP OZONE .TRASH DIRECTORIES")
        self.logger.info("="*80)
        self.logger.info("Ozone Service ID: {}".format(ozone_service_id))
        self.logger.info("FID Paths: {}".format(', '.join(fid_paths)))
        self.logger.info("Buckets: {}".format(', '.join(buckets)))
        self.logger.info("="*80)
        
        # Build trash path patterns for all combinations
        total_checked = 0
        total_found = 0
        total_deleted = 0
        total_failed = 0
        total_size_bytes = 0
        
        for fid in fid_paths:
            for bucket in buckets:
                # Pattern: ofs://<service>/<fid>/<bucket>/.Trash/
                trash_base_path = 'ofs://{}/{}/{}/.Trash'.format(ozone_service_id, fid, bucket)
                total_checked += 1
                
                self.logger.info("\n[{}/{}] Checking: {}".format(
                    total_checked, len(fid_paths) * len(buckets), trash_base_path
                ))
                
                try:
                    # Check if .Trash directory exists
                    check_cmd = ['ozone', 'fs', '-ls', trash_base_path]
                    check_result = subprocess.run(
                        check_cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        universal_newlines=True,
                        timeout=60
                    )
                    
                    if check_result.returncode == 0 and check_result.stdout.strip():
                        # .Trash directory exists, check what's inside
                        lines = check_result.stdout.strip().split('\n')
                        
                        # Filter out header lines and parse entries
                        trash_items = []
                        for line in lines:
                            if line and not line.startswith('Found'):
                                # Parse: drwxrwxrwx   - hive hive   0 2026-01-06 14:00 ofs://.../.Trash/hive
                                parts = line.split()
                                if len(parts) >= 8:
                                    trash_items.append(parts[-1])  # Last part is the path
                        
                        if not trash_items:
                            self.logger.info("  - .Trash exists but is empty (no subdirectories)")
                            continue
                        
                        # Trash has contents
                        total_found += 1
                        self.logger.info("  ✓ Trash found with {} subdirectories:".format(len(trash_items)))
                        for item in trash_items[:5]:  # Show first 5
                            item_name = item.split('/')[-1]
                            self.logger.info("    - {}".format(item_name))
                        if len(trash_items) > 5:
                            self.logger.info("    ... and {} more".format(len(trash_items) - 5))
                        
                        # Get size estimate using du
                        self.logger.info("  → Calculating size...")
                        du_cmd = ['ozone', 'fs', '-du', '-s', trash_base_path]
                        du_result = subprocess.run(
                            du_cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            universal_newlines=True,
                            timeout=60
                        )
                        
                        if du_result.returncode == 0 and du_result.stdout.strip():
                            # Parse: "12345678  ofs://..."
                            size_str = du_result.stdout.strip().split()[0]
                            size_bytes = int(size_str)
                            total_size_bytes += size_bytes
                            size_gb = size_bytes / (1024**3)
                            self.logger.info("  → Size: {:.2f} GB ({} bytes)".format(size_gb, size_bytes))
                        
                        # Delete the entire .Trash directory with all contents
                        self.logger.info("  → Deleting...")
                        delete_cmd = ['ozone', 'fs', '-rm', '-r', '-skipTrash', trash_base_path]
                        delete_result = subprocess.run(
                            delete_cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            universal_newlines=True,
                            timeout=600  # Increased timeout for large directories
                        )
                        
                        if delete_result.returncode == 0:
                            self.logger.info("  ✓ Successfully deleted: {}".format(trash_base_path))
                            results[trash_base_path] = True
                            total_deleted += 1
                        else:
                            self.logger.error("  ✗ Failed to delete: {}".format(trash_base_path))
                            self.logger.error("  Error: {}".format(delete_result.stderr))
                            results[trash_base_path] = False
                            total_failed += 1
                    else:
                        # .Trash directory doesn't exist or is not accessible
                        if 'No such file or directory' in check_result.stderr:
                            self.logger.info("  - No .Trash directory found (never created)")
                        else:
                            self.logger.info("  - No trash found (clean)")
                        
                except subprocess.TimeoutExpired:
                    self.logger.error("  ✗ Timeout checking/deleting: {}".format(trash_base_path))
                    results[trash_base_path] = False
                    total_failed += 1
                except Exception as e:
                    self.logger.error("  ✗ Error: {}".format(e))
                    results[trash_base_path] = False
                    total_failed += 1
        
        # Summary
        self.logger.info("\n" + "="*80)
        self.logger.info("TRASH CLEANUP SUMMARY")
        self.logger.info("="*80)
        self.logger.info("Paths checked: {}".format(total_checked))
        self.logger.info("Trash directories found: {}".format(total_found))
        self.logger.info("Successfully deleted: {}".format(total_deleted))
        self.logger.info("Failed: {}".format(total_failed))
        if total_size_bytes > 0:
            total_size_gb = total_size_bytes / (1024**3)
            self.logger.info("Total space reclaimed: {:.2f} GB ({} bytes)".format(
                total_size_gb, total_size_bytes
            ))
        self.logger.info("="*80)
        
        return results

    def cleanup_multiple_databases(self, databases: List[str], dry_run: bool = False,
                                   max_workers: int = 5,
                                   delete_ozone_data: bool = True,
                                   skip_trash: bool = True) -> Dict[str, bool]:
        """Cleanup multiple databases"""
        results = {}
        total = len(databases)

        self.logger.info("Starting cleanup of {} databases".format(total))

        for i, database in enumerate(databases, 1):
            self.logger.info("\n" + "="*80)
            self.logger.info("Processing database {}/{}: {}".format(i, total, database))
            self.logger.info("="*80 + "\n")

            success = self.cleanup_database(database, dry_run, max_workers, 
                                           delete_ozone_data, skip_trash)
            results[database] = success

            if success:
                self.logger.info("Successfully cleaned up: {}".format(database))
            else:
                self.logger.error("Failed to cleanup: {}".format(database))

        return results


def main():
    # ============================================================================
    # STEP 1: INITIALIZE ARGUMENT PARSER
    # ============================================================================
    parser = argparse.ArgumentParser(
        description='Hive Database Cleanup Script - Efficiently delete databases with thousands of tables',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all available databases
  python3 hive_db_cleanup_v4.py --config config.ini --list-databases
  
  # List all tables in a specific database
  python3 hive_db_cleanup_v4.py --config config.ini --list-tables "oz_hdfs_db4"
  
  # Dry run to analyze what will be deleted (ALWAYS DO THIS FIRST!)
  python3 hive_db_cleanup_v4.py --config config.ini --databases "oz_db1,oz_db2" --dry-run
  
  # Analyze a database to see storage types
  python3 hive_db_cleanup_v4.py --config config.ini --analyze "oz_hdfs_db5"
  
  # Delete Ozone databases with skipTrash (permanent deletion, default)
  python3 hive_db_cleanup_v4.py --config config.ini --databases "oz_db1,oz_db2" --ozone-db-prefix "oz_"
  
  # Delete Ozone data but send to trash (not using skipTrash)
  python3 hive_db_cleanup_v4.py --config config.ini --databases "oz_db1,oz_db2" --no-skip-trash
  # Note: Data goes to .Trash - run --cleanup-trash-only afterward to clean it
  
  # Two-step workflow: Delete with trash, then clean trash independently
  # Step 1: Delete databases (data goes to trash)
  python3 hive_db_cleanup_v4.py --config config.ini --databases "oz_db1" --no-skip-trash
  # Step 2: Clean trash independently when ready
  python3 hive_db_cleanup_v4.py --config config.ini --cleanup-trash-only --fid-paths "fid2"
  
  # Delete only databases with specific prefix (others will be skipped)
  python3 hive_db_cleanup_v4.py --config config.ini --databases "oz_db1,test_db2,oz_db3" --ozone-db-prefix "oz_"
  # Result: Only oz_db1 and oz_db3 will be processed, test_db2 will be skipped
  
  # Delete metadata but PRESERVE Ozone data
  python3 hive_db_cleanup_v4.py --config config.ini --databases "oz_db1" --preserve-ozone-data
  
  # Delete databases from file with prefix filtering
  python3 hive_db_cleanup_v4.py --config config.ini --databases-file /tmp/dbs.txt --ozone-db-prefix "oz_"
  
  # Delete with custom parallelism (faster for large databases)
  python3 hive_db_cleanup_v4.py --config config.ini --databases "oz_db1,oz_db2" --workers 10
  
  # Run unattended with all settings from config file
  python3 hive_db_cleanup_v4.py --config config.ini --databases-file /tmp/dbs.txt
  
  # ===== INDEPENDENT TRASH CLEANUP MODE =====
  # Clean up .Trash directories only (no database deletion)
  python3 hive_db_cleanup_v4.py --config config.ini --cleanup-trash-only
  
  # Clean up trash for specific FID paths
  python3 hive_db_cleanup_v4.py --config config.ini --cleanup-trash-only --fid-paths "fid1,fid2,fid3"
  
  # Clean up trash for all FIDs in config
  # Add to config.ini: FID_PATHS=fid1,fid2,fid3
  python3 hive_db_cleanup_v4.py --config config.ini --cleanup-trash-only

Config file format (config.ini):
  # Kerberos authentication
  KEYTAB_PATH=/path/to/hive.keytab
  PRINCIPAL=hive/host@REALM.COM
  
  # Hive connection
  HIVE_HOST=vvs-baseiq-1.vpc.cloudera.com
  HIVE_PORT=10000
  HIVE_DATABASE=default
  HIVE_PRINCIPAL=hive/_HOST@VPC.CLOUDERA.COM
  
  # SSL/TLS settings
  TRUSTSTORE_PATH=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks
  TRUSTSTORE_PASSWORD=password
  
  # Ozone configuration
  OZONE_SERVICE_ID=ozone1756774157
  
  # FID paths for trash cleanup (comma-separated)
  FID_PATHS=fid1,fid2,fid3
  
  # Cleanup behavior (optional - can also be set via command-line)
  # Command-line arguments override config file values
  OZONE_DB_PREFIX=oz_           # Only process databases with this prefix
  SKIP_TRASH=true               # Use skipTrash for Ozone (true/false)
  FORCE=false                   # Skip confirmation prompts (true/false)
  DELETE_OZONE_DATA=true        # Delete Ozone data for managed tables (true/false)

Note: Command-line arguments always take precedence over config file settings.
      This script is optimized for Ozone database cleanup.
        """
    )
    
    # ============================================================================
    # STEP 2: DEFINE ALL COMMAND-LINE ARGUMENTS
    # ============================================================================
    
    # Required argument
    parser.add_argument('--config', required=True, help='Path to config file')
    
    # Database selection
    parser.add_argument('--databases', help='Comma-separated list of databases to delete')
    parser.add_argument('--databases-file', help='File containing database names')
    
    # NEW: Database prefix filtering
    parser.add_argument('--ozone-db-prefix', 
                       help='Only process databases starting with this prefix (others will be skipped)')
    
    # Information/query modes
    parser.add_argument('--list-databases', action='store_true',
                       help='List all available databases and exit')
    parser.add_argument('--list-tables', help='List all tables in a database and exit')
    parser.add_argument('--analyze', help='Analyze a database without deleting')
    
    # Execution options
    parser.add_argument('--dry-run', action='store_true', 
                       help='Analyze what would be deleted without actually deleting')
    parser.add_argument('--workers', type=int, default=5,
                       help='Number of parallel workers (default: 5)')
    
    # Data deletion control
    parser.add_argument('--preserve-ozone-data', action='store_true',
                       help='Preserve Ozone data when dropping managed tables')
    
    # NEW: skipTrash control for Ozone
    parser.add_argument('--no-skip-trash', action='store_true',
                       help='Do NOT use skipTrash when deleting Ozone data (data goes to .Trash)')
    
    # NEW: FID paths for trash cleanup
    parser.add_argument('--fid-paths', 
                       help='Comma-separated list of FID paths for trash cleanup (e.g., fid1,fid2,fid3)')
    parser.add_argument('--cleanup-trash-only', action='store_true',
                       help='Only cleanup .Trash directories (no database deletion)')
    
    # Advanced override
    parser.add_argument('--beeline-url', help='Override beeline URL from config')
    
    # NEW: Force mode - no confirmation prompts
    parser.add_argument('--force', action='store_true',
                       help='Skip all confirmation prompts (use with caution!)')
    
    # ============================================================================
    # STEP 3: PARSE COMMAND-LINE ARGUMENTS
    # ============================================================================
    args = parser.parse_args()
    
    # ============================================================================
    # STEP 3.5: HANDLE CLEANUP-TRASH-ONLY MODE (INDEPENDENT OPERATION)
    # ============================================================================
    if args.cleanup_trash_only:
        print("\n" + "="*80)
        print("OZONE TRASH CLEANUP ONLY MODE")
        print("="*80)
        
        # Create cleaner instance just for config
        cleaner = HiveDatabaseCleaner(args.config)
        
        # Parse FID paths
        fid_paths = None
        if args.fid_paths:
            fid_paths = [f.strip() for f in args.fid_paths.split(',') if f.strip()]
            print("FID Paths: {}".format(', '.join(fid_paths)))
        else:
            print("FID Paths: Using default (fid2)")
        
        print("="*80)
        
        # Run trash cleanup independently
        results = cleaner.cleanup_ozone_trash(fid_paths=fid_paths)
        
        # Print summary
        success_count = sum(1 for v in results.values() if v)
        failed_count = len(results) - success_count
        
        print("\n" + "="*80)
        print("CLEANUP SUMMARY")
        print("="*80)
        print("Trash paths processed: {}".format(len(results)))
        print("Successfully deleted: {}".format(success_count))
        print("Failed: {}".format(failed_count))
        print("="*80)
        
        sys.exit(0 if failed_count == 0 else 1)
    
    # ============================================================================
    # STEP 4: CREATE CLEANER INSTANCE AND LOAD CONFIG (Normal database cleanup mode)
    # ============================================================================
    cleaner = HiveDatabaseCleaner(args.config)
    
    # ============================================================================
    # STEP 4.5: MERGE CONFIG FILE AND COMMAND-LINE ARGUMENTS
    # Command-line args take precedence over config file
    # ============================================================================
    
    # skipTrash: Default is True (use skipTrash), unless explicitly disabled
    # Priority: 1) --no-skip-trash flag, 2) SKIP_TRASH config, 3) default True
    if args.no_skip_trash:
        skip_trash = False
        cleaner.logger.info("skipTrash: disabled (from command line)")
    else:
        skip_trash = cleaner.get_config_bool('SKIP_TRASH', True)
        source = "config file" if 'SKIP_TRASH' in cleaner.config else "default"
        cleaner.logger.info("skipTrash: {} (from {})".format(skip_trash, source))
    
    # Ozone DB prefix filtering
    # Priority: 1) --ozone-db-prefix arg, 2) OZONE_DB_PREFIX config, 3) None
    ozone_db_prefix = args.ozone_db_prefix
    if ozone_db_prefix is None:
        ozone_db_prefix = cleaner.config.get('OZONE_DB_PREFIX')
        if ozone_db_prefix:
            cleaner.logger.info("Ozone DB prefix: '{}' (from config file)".format(ozone_db_prefix))
    else:
        cleaner.logger.info("Ozone DB prefix: '{}' (from command line)".format(ozone_db_prefix))
    
    # Force mode (skip confirmation)
    # Priority: 1) --force flag, 2) FORCE config, 3) default False
    if args.force:
        force_mode = True
        cleaner.logger.info("Force mode: enabled (from command line)")
    else:
        force_mode = cleaner.get_config_bool('FORCE', False)
        source = "config file" if 'FORCE' in cleaner.config else "default"
        cleaner.logger.info("Force mode: {} (from {})".format(force_mode, source))
    
    # Preserve Ozone data
    # Priority: 1) --preserve-ozone-data flag, 2) DELETE_OZONE_DATA config, 3) default True
    if args.preserve_ozone_data:
        delete_ozone_data = False
        cleaner.logger.info("Delete Ozone data: disabled (from command line)")
    else:
        delete_ozone_data = cleaner.get_config_bool('DELETE_OZONE_DATA', True)
        source = "config file" if 'DELETE_OZONE_DATA' in cleaner.config else "default"
        cleaner.logger.info("Delete Ozone data: {} (from {})".format(delete_ozone_data, source))
    
    # ============================================================================
    # STEP 5: KERBEROS AUTHENTICATION
    # ============================================================================
    if not cleaner.kinit():
        cleaner.logger.error("Kerberos authentication failed")
        sys.exit(1)
    
    # ============================================================================
    # STEP 6: BUILD BEELINE CONNECTION URL
    # ============================================================================
    if args.beeline_url:
        cleaner.beeline_url = args.beeline_url
        cleaner.logger.info("Using provided beeline URL")
    else:
        cleaner.beeline_url = cleaner.build_beeline_url()
        if not cleaner.beeline_url:
            cleaner.logger.error("Failed to build beeline URL")
            sys.exit(1)
    
    cleaner.logger.info("Beeline URL configured")
    
    # ============================================================================
    # STEP 7: TEST CONNECTION
    # ============================================================================
    if not cleaner.test_connection():
        cleaner.logger.error("Beeline connection test failed")
        print("\nERROR: Cannot connect to Hive. Please check:")
        print("  1. HIVE_HOST is correct")
        print("  2. HIVE_PORT is correct (default: 10000)")
        print("  3. Kerberos ticket is valid (run 'klist')")
        print("  4. SSL truststore path and password are correct")
        print("\nTry running this beeline command manually to test:")
        print("beeline -u \"{}\" -e \"SHOW DATABASES\"".format(cleaner.beeline_url))
        sys.exit(1)
    
    # ============================================================================
    # STEP 8: HANDLE INFORMATION MODES
    # ============================================================================
    
    # MODE 1: List all databases
    if args.list_databases:
        cleaner.logger.info("Listing all databases...")
        query = "SHOW DATABASES"
        success, output = cleaner.execute_beeline_query(query, silent=False)
        
        if success and output:
            print("\n" + "="*80)
            print("AVAILABLE DATABASES")
            print("="*80)
            databases = []
            
            for line in output.split('\n'):
                line = line.strip()
                if not line or line.startswith('+') or line.startswith('|--'):
                    continue
                
                if line.startswith('|') and line.endswith('|'):
                    db = line.strip('|').strip()
                    if db and db.lower() not in ['database_name', 'tab_name']:
                        databases.append(db)
            
            if databases:
                for i, db in enumerate(databases, 1):
                    print("  {}. {}".format(i, db))
                print("\nTotal: {} databases".format(len(databases)))
                
                list_file = "/tmp/hive_databases_list.txt"
                with open(list_file, 'w') as f:
                    for db in databases:
                        f.write(db + '\n')
                print("\nDatabase list saved to: {}".format(list_file))
            else:
                print("No databases found")
            print("="*80)
        else:
            print("Failed to list databases")
        
        sys.exit(0)
    
    # MODE 2: List tables in a specific database
    if args.list_tables:
        database = args.list_tables
        cleaner.logger.info("Listing all tables in database: {}".format(database))
        
        if not cleaner.database_exists(database):
            print("ERROR: Database '{}' does not exist".format(database))
            sys.exit(1)
        
        tables = cleaner.get_tables_in_database(database)
        
        print("\n" + "="*80)
        print("TABLES IN DATABASE: {}".format(database))
        print("="*80)
        
        if tables:
            print("\nFound {} tables:".format(len(tables)))
            for i, table in enumerate(tables, 1):
                print("  {}. {}".format(i, table))
            
            list_file = "/tmp/hive_tables_{}.txt".format(database)
            with open(list_file, 'w') as f:
                for table in tables:
                    f.write(table + '\n')
            print("\nTable list saved to: {}".format(list_file))
            
            print("\nGetting detailed information (this may take a moment)...")
            print("-" * 80)
            
            for i, table in enumerate(tables, 1):
                info = cleaner.get_table_info(database, table)
                print("\n{}. Table: {}".format(i, table))
                print("   Location: {}".format(info['location'] if info['location'] else 'N/A'))
                print("   Storage: {}".format('Ozone' if info['is_ozone'] else 'HDFS'))
                print("   Type: {}".format('EXTERNAL' if info['is_external'] else 'MANAGED'))
                print("   Partitioned: {}".format('Yes' if info['is_partitioned'] else 'No'))
                if info['is_partitioned']:
                    print("   Partitions: {}".format(info['partition_count']))
        else:
            print("\nNo tables found in database '{}'".format(database))
        
        print("\n" + "="*80)
        sys.exit(0)
    
    # MODE 3: Analyze a database
    if args.analyze:
        cleaner.logger.info("Analyzing database: {}".format(args.analyze))
        analysis = cleaner.analyze_database(args.analyze)
        
        if analysis:
            print("\n" + "="*80)
            print("DATABASE ANALYSIS: {}".format(args.analyze))
            print("="*80)
            print("Total tables: {}".format(analysis['total_tables']))
            print("Partitioned tables: {}".format(analysis['partitioned_tables']))
            print("Non-partitioned tables: {}".format(analysis['non_partitioned_tables']))
            print("Total partitions: {}".format(analysis['total_partitions']))
            print("\nStorage Type:")
            print("  Ozone-backed tables: {}".format(analysis['ozone_tables']))
            print("  HDFS-backed tables: {}".format(analysis['hdfs_tables']))
            print("\nTable Type:")
            print("  External tables: {} (metadata only)".format(analysis['external_tables']))
            print("  Managed tables: {} (data will be deleted)".format(analysis['managed_tables']))
            print("="*80 + "\n")
            
            output_file = "/tmp/hive_db_cleanup/analysis_{}_{}.json".format(
                args.analyze, datetime.now().strftime('%Y%m%d_%H%M%S')
            )
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            with open(output_file, 'w') as f:
                json.dump(analysis, f, indent=2)
            print("Detailed analysis saved to: {}".format(output_file))
        
        sys.exit(0)
    
    # ============================================================================
    # STEP 9: PARSE DATABASES TO DELETE
    # ============================================================================
    dbs_to_process = []
    if args.databases:
        dbs_to_process = [d.strip() for d in args.databases.split(',') if d.strip()]
    elif args.databases_file:
        try:
            with open(args.databases_file, 'r') as f:
                content = f.read().strip()
                
                if ',' in content:
                    dbs_to_process = [d.strip() for d in content.split(',') if d.strip()]
                else:
                    dbs_to_process = [
                        line.strip() 
                        for line in content.split('\n') 
                        if line.strip() and not line.strip().startswith('#')
                    ]
        except Exception as e:
            cleaner.logger.error("Failed to read databases file: {}".format(e))
            sys.exit(1)
    else:
        parser.error("Either --databases or --databases-file must be specified")
    
    if not dbs_to_process:
        cleaner.logger.error("No databases specified")
        sys.exit(1)
    
    # ============================================================================
    # STEP 10: APPLY PREFIX FILTERING (NEW FEATURE)
    # ============================================================================
    if ozone_db_prefix:
        original_count = len(dbs_to_process)
        filtered_dbs = [db for db in dbs_to_process if db.startswith(ozone_db_prefix)]
        skipped_dbs = [db for db in dbs_to_process if not db.startswith(ozone_db_prefix)]
        
        cleaner.logger.info("="*80)
        cleaner.logger.info("APPLYING DATABASE PREFIX FILTER: '{}'".format(ozone_db_prefix))
        cleaner.logger.info("="*80)
        cleaner.logger.info("Original database count: {}".format(original_count))
        cleaner.logger.info("Databases matching prefix: {}".format(len(filtered_dbs)))
        cleaner.logger.info("Databases skipped: {}".format(len(skipped_dbs)))
        
        if skipped_dbs:
            cleaner.logger.info("\nSkipped databases (no prefix match):")
            for db in skipped_dbs:
                cleaner.logger.info("  - {}".format(db))
        
        if filtered_dbs:
            cleaner.logger.info("\nDatabases to process:")
            for db in filtered_dbs:
                cleaner.logger.info("  - {}".format(db))
        
        cleaner.logger.info("="*80)
        
        dbs_to_process = filtered_dbs
        
        if not dbs_to_process:
            cleaner.logger.warning("No databases match the prefix filter. Exiting.")
            sys.exit(0)
    
    cleaner.logger.info("Final databases to process: {}".format(', '.join(dbs_to_process)))
    
    # ============================================================================
    # STEP 11: CONFIRMATION (unless force_mode or --dry-run)
    # ============================================================================
    if not args.dry_run and not force_mode:
        print("\n" + "="*80)
        print("WARNING: This will permanently delete the following Ozone databases:")
        for db in dbs_to_process:
            print("  - {}".format(db))
        print("="*80)
        
        print("\nData Deletion Policy:")
        print("  Ozone managed tables: {}".format(
            'Data WILL BE DELETED' if delete_ozone_data else 'Data will be PRESERVED'
        ))
        print("  Ozone skipTrash: {}".format(
            'YES (permanent deletion)' if skip_trash else 'NO (goes to .Trash)'
        ))
        print("  External tables: Metadata only (data always preserved)")
        print("="*80)
        
        try:
            response = input("\nType 'yes' to proceed or 'no' to cancel: ")
        except KeyboardInterrupt:
            print("\nOperation cancelled.")
            sys.exit(0)
        
        if response.lower() not in ['yes', 'y']:
            cleaner.logger.info("Cleanup cancelled by user")
            print("Cleanup cancelled.")
            sys.exit(0)
    
    # ============================================================================
    # STEP 12: EXECUTE CLEANUP (NO MORE PROMPTS AFTER THIS)
    # ============================================================================
    cleaner.logger.info("="*80)
    cleaner.logger.info("STARTING OZONE DATABASE CLEANUP")
    cleaner.logger.info("="*80)
    cleaner.logger.info("Delete Ozone data: {}".format(delete_ozone_data))
    cleaner.logger.info("Skip Trash (Ozone): {}".format(skip_trash))
    cleaner.logger.info("Force mode: {}".format(force_mode))
    cleaner.logger.info("="*80)
    
    results = cleaner.cleanup_multiple_databases(
        dbs_to_process, 
        dry_run=args.dry_run,
        max_workers=args.workers,
        delete_ozone_data=delete_ozone_data,
        skip_trash=skip_trash
    )
    
    # Note: Trash cleanup is now independent - use --cleanup-trash-only mode
    # to clean up .Trash directories separately
    if not skip_trash and not args.dry_run:
        cleaner.logger.info("\n" + "="*80)
        cleaner.logger.info("NOTE: Data was sent to .Trash directories")
        cleaner.logger.info("To clean up trash, run:")
        cleaner.logger.info("  python3 {} --config {} --cleanup-trash-only --fid-paths <fids>".format(
            sys.argv[0], args.config
        ))
        cleaner.logger.info("="*80)
    
    # ============================================================================
    # STEP 13: PRINT SUMMARY
    # ============================================================================
    print("\n" + "="*80)
    print("CLEANUP SUMMARY")
    print("="*80)
    
    success_count = sum(1 for v in results.values() if v)
    failed_count = len(results) - success_count
    
    print("Total databases processed: {}".format(len(results)))
    print("Successfully cleaned: {}".format(success_count))
    print("Failed: {}".format(failed_count))
    
    if failed_count > 0:
        print("\nFailed databases:")
        for db, success in results.items():
            if not success:
                print("  - {}".format(db))
    
    print("="*80 + "\n")
    
    sys.exit(0 if failed_count == 0 else 1)


if __name__ == "__main__":
    main()
