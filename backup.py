import argparse
import os
import subprocess
import sys
import time
from datetime import datetime

import boto3
import schedule

import logging

class DatabaseManager:
    def __init__(self):
        # Load environment variables
        self.db_container_name = os.getenv("DB_CONTAINER_NAME")
        self.db_name = os.getenv("DB_NAME")
        self.db_user = os.getenv("DB_USER")
        self.db_password = os.getenv("DB_PASSWORD")
        self.aws_access_key = os.getenv("AWS_ACCESS_KEY")
        self.aws_secret_key = os.getenv("AWS_SECRET_KEY")
        self.s3_bucket = os.getenv("S3_BUCKET")
        self.s3_endpoint_url = os.getenv("S3_ENDPOINT_URL")

        # Create a boto3 session
        self.boto3_session = boto3.Session(
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
        )

        # Create an S3 resource
        self.s3 = self.boto3_session.resource("s3", endpoint_url=self.s3_endpoint_url)

        # Set up logging
        logging.basicConfig(filename='backup.log', level=logging.INFO)

    def backup(self):
        """
        Creates a database backup and uploads it to S3.
        """
        # Get the current timestamp
        timestamp = datetime.now().strftime("%Y_%m_%d_%H%M%S")

        # Set the dumpfile name
        dumpfile = f"{self.db_name}_{timestamp}.sql"

        # Run pg_dump to create a backup file
        subprocess.call(
            [
                "pg_dump",
                "-Fp",
                "--no-acl",
                "--no-owner",
                "-h",
                "db",
                "-U",
                self.db_user,
                "-d",
                self.db_name,
                "-f",
                dumpfile,
            ],
            env={"PGPASSWORD": self.db_password},
        )

        # Upload the backup file to S3
        self.s3.meta.client.upload_file(dumpfile, self.s3_bucket, dumpfile)

        # Remove the backup file from the local filesystem
        os.remove(dumpfile)

        # Log the backup operation
        logging.info(f"Backup created and uploaded to S3 at {datetime.now()}")

    def restore(self, filename):
        """
        Restores a database from a backup file.
        """
        # Download the backup file from S3
        self.s3.meta.client.download_file(self.s3_bucket, filename, filename)

        # Run psql to restore the database from the backup file
        subprocess.call(
            [
                "psql",
                "-h",
                "db",
                "-U",
                self.db_user,
                "-d",
                self.db_name,
                "-f",
                filename,
            ],
            env={"PGPASSWORD": self.db_password},
        )

        # Remove the backup file from the local filesystem
        os.remove(filename)

        # Log the restore operation
        logging.info(f"Database restored from backup at {datetime.now()}")


if __name__ == "__main__":
    # Create a DatabaseManager object
    database_manager = DatabaseManager()

    # Create an argument parser
    parser = argparse.ArgumentParser(
        description="Manage database backup and restore operations."
    )

    # Create subparsers for the backup, restore, and schedule commands
    subparsers = parser.add_subparsers(dest="command", required=True)

    backup_parser = subparsers.add_parser("backup", help="Create a database backup.")
    restore_parser = subparsers.add_parser(
        "restore", help="Restore a database from a backup."
    )
    restore_parser.add_argument(
        "filename", type=str, help="The filename of the SQL file to restore."
    )
    schedule_parser = subparsers.add_parser(
        "schedule", help="Schedule database backups every hour."
    )

    # Parse the command line arguments
    args = parser.parse_args()

    # Execute the appropriate command
    if args.command == "backup":
        database_manager.backup()

    elif args.command == "restore":
        database_manager.restore(args.filename)

    elif args.command == "schedule":
        schedule.every().hour.do(database_manager.backup)
        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except (SystemExit, KeyboardInterrupt):
            sys.exit(0)
