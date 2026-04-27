from main import  DynamoDBManager
import argparse
import json

def main():
    parser = argparse.ArgumentParser(description="dynamo")

    subparsers = parser.add_subparsers(dest="dynamo_command")



# create tables from JSON
    create_table = subparsers.add_parser("create-tables")
    create_table.add_argument("json_file")

    # list tables
    list_tables = subparsers.add_parser("list-tables")

    # delete table
    delete_table = subparsers.add_parser("delete-table")
    delete_table.add_argument("table_name")

    #load data item by item
    load_data = subparsers.add_parser("load-data")
    load_data.add_argument("config_file")

    #load batch data 
    load_batch= subparsers.add_parser("load-batch")
    load_batch.add_argument("config_file")

    scan_table= subparsers.add_parser("scan-table")
    scan_table.add_argument("table_name")

    get_item= subparsers.add_parser("get-item")
    get_item.add_argument("table_name")
    get_item.add_argument("key")

    quary_table= subparsers.add_parser("query-table")
    quary_table.add_argument("table_name")
    quary_table.add_argument("kwargs")

    update_item= subparsers.add_parser("update-item")
    update_item.add_argument("table_name")
    update_item.add_argument("key")
    update_item.add_argument("kwargs")

    delete_item= subparsers.add_parser("delete-item")
    delete_item.add_argument("table_name")
    delete_item.add_argument("key")

    transact_write = subparsers.add_parser("transact-write")
    transact_write .add_argument("transaction_items")
    transact_write .add_argument("kwargs")
   




    args = parser.parse_args()
    dynamo_manager = DynamoDBManager()

    if args.dynamo_command == "create-tables":
        dynamo_manager.create_tables_from_json(args.json_file)

    elif args.dynamo_command == "list-tables":
        dynamo_manager.list_tables()

    elif args.dynamo_command == "delete-table":
        dynamo_manager.delete_table(args.table_name)   
        
    elif args.dynamo_command == "load-data":
        dynamo_manager.load_data_from_json(args.config_file)

    elif args.dynamo_command == "load-batch":
        dynamo_manager.batch_write_item_db(args.config_file)

    elif args.dynamo_command == "scan-table":
        dynamo_manager.scan_table(args.table_name)

    elif args.dynamo_command == "get-item":
        key_dict = json.loads(args.key)
        dynamo_manager.get_item_db(args.table_name,key_dict)

    
    elif args.dynamo_command == "query-table":
        kwargs_dict = json.loads(args.kwargs)
        dynamo_manager.query_db(args.table_name,**kwargs_dict)

    elif args.dynamo_command == "update-item":
        key_dict = json.loads(args.key)
        kwargs_dict = json.loads(args.kwargs)
        dynamo_manager.update_item_db(args.table_name,args.key_dict,**kwargs_dict)

    elif args.dynamo_command == "delete-item":
        key_dict = json.loads(args.key)
        dynamo_manager.update_item_db(args.table_name,args.key_dict)

    
    elif args.dynamo_command == "transact-write":
        transaction_items = json.loads(args.transaction_items)
        kwargs_dict = json.loads(args.kwargs)

        dynamo_manager.transact_write_items_db(transaction_items,**kwargs_dict)


if __name__ == "__main__":
    main()