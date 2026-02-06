import json
import os
import hashlib
import boto3
from botocore.exceptions import ClientError

class StateManager:
    def __init__(self, table_name=None, region_name="us-east-1"):
        self.table_name = table_name or os.getenv("DYNAMODB_TABLE")
        self.region_name = region_name or os.getenv("AWS_REGION", "us-east-1")
        self.local_file = "processed_urls.json"
        
        if self.table_name:
            self.dynamodb = boto3.resource('dynamodb', region_name=self.region_name)
            self.table = self.dynamodb.Table(self.table_name)
            print(f"StateManager using DynamoDB table: {self.table_name}")
        else:
            self.table = None
            self.processed = self._load_local_state()
            print("StateManager using local file.")

    def _load_local_state(self):
        if os.path.exists(self.local_file):
            with open(self.local_file, 'r') as f:
                try:
                    return set(json.load(f))
                except json.JSONDecodeError:
                    return set()
        return set()

    def _save_local_state(self):
        with open(self.local_file, 'w') as f:
            json.dump(list(self.processed), f)

    def is_processed(self, url):
        if self.table:
            try:
                # Use URL hash as key to avoid invalid characters in PK
                url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()
                response = self.table.get_item(Key={'url_hash': url_hash})
                return 'Item' in response
            except ClientError as e:
                print(f"DynamoDB Error checking state: {e}")
                return False
        else:
            return url in self.processed

    def mark_processed(self, url):
        if self.table:
            try:
                url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()
                self.table.put_item(
                    Item={
                        'url_hash': url_hash,
                        'url': url,
                        'timestamp': os.popen('date /t').read().strip() # Simple timestamp, safer to use datetime
                    }
                )
            except ClientError as e:
                print(f"DynamoDB Error saving state: {e}")
        else:
            self.processed.add(url)
            self._save_local_state()

