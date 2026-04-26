import decimal
import json
import logging
from typing import Any, Dict, List
from boto3.dynamodb.types import TypeDeserializer

import boto3
from botocore.exceptions import ClientError
from IPython.display import HTML

class DynamoDBManager:
    def __init__(self, region="us-east-1"):
        self.dynamodb = boto3.client("dynamodb", region_name=region)

    def create_tables_from_json(self, json_file):
        import json

        try:
            with open(json_file, "r") as f:
                config = json.load(f)

            for table in config["tables"]:
                print(f"Creating table: {table['table_name']}")

                self.dynamodb.create_table(
                    TableName=table["table_name"],
                    KeySchema=table["kwargs"]["KeySchema"],
                    AttributeDefinitions=table["kwargs"]["AttributeDefinitions"],
                    ProvisionedThroughput=table["kwargs"]["ProvisionedThroughput"]
                )

                waiter = self.dynamodb.get_waiter("table_exists")
                waiter.wait(TableName=table["table_name"])

                print(f"Table created: {table['table_name']}")

        except ClientError as e:
            print("Error:", e)

    def list_tables(self):
        try:
            response = self.dynamodb.list_tables()
            for name in response["TableNames"]:
                print(name)
        except ClientError as e:
            print("Error:", e)

    def delete_table(self, table_name):
        try:
            self.dynamodb.delete_table(TableName=table_name)
            print(f"Deleted table: {table_name}")
        except ClientError as e:
            print("Error:", e)


    def load_data_from_json(self,config_file):
       # try:
            
            

            with open(config_file, "r") as f:
                config = json.load(f)
            

            for table_config in config["tables"]:
                table_name = table_config["table_name"]
                file_path = table_config["file"]

                print(f"\nLoading data into {table_name} from {file_path}")
                with open(file_path, "r") as json_file:
                    items = json.load(json_file)
                    item=items[table_name.split('-')[-1]]
                    #print(f'{item}')
                
                    self.dynamodb.put_item(TableName=table_name, Item=item[0]['PutRequest']['Item'])
                   # print(f"{item[0]['PutRequest']['Item']} in {table_name}")

          

       # except ClientError as e:
        #    print("AWS Error:", e)
       # except Exception as e:
         #   print("Error:", e)



    def batch_write_item_db(self,config_file):
       # try:
            
            

            with open(config_file, "r") as f:
                config = json.load(f)
           

            for table_config in config["tables"]:
                table_name = table_config["table_name"]
                file_path = table_config["file"]

                print(f"\nLoading data into {table_name} from {file_path}")
                with open(file_path, "r") as json_file:
                    items = json.load(json_file)
                   
                
                    self.dynamodb.batch_write_item(RequestItems=items)
                    print(f"{items} in {table_name}")





                

       # except ClientError as e:
        #    print("AWS Error:", e)
       # except Exception as e:
         #   print("Error:", e)  
         # 
         # 
    


    def scan_table(self, table_name):
        deserializer = TypeDeserializer()
        items = []
        last_evaluated_key = None

        while True:
            if last_evaluated_key:
                response = self.dynamodb.scan(
                    TableName=table_name,
                    ExclusiveStartKey=last_evaluated_key
                )
            else:
                response = self.dynamodb.scan(TableName=table_name)

            batch = response.get("Items", [])

            for item in batch:
                items.append({
                    k: deserializer.deserialize(v)
                    for k, v in item.items()
                })

            last_evaluated_key = response.get("LastEvaluatedKey")

            if not last_evaluated_key:
                break
        print(f'{items}')
        return items
    

    def get_item_db(self,table_name, key: Dict[str, Any], **kwargs):
    

        try:
            ### START CODE HERE ### (~ 1 line of code)
            response = self.dynamodb.get_item(TableName=table_name, Key=key, **kwargs)
            ### END CODE HERE ###
            
        except ClientError as e:
            error = e.response.get("Error", {})
            logging.error(
                f"Failed to query DynamoDB. Error: {error.get('Message')}"
            )
            response = {}
        print(f'{response}')
        return response


