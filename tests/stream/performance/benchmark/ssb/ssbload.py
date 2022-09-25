from ast import Pass
import os, sys, getopt, json, random, copy
from re import sub
import logging, logging.config
from clickhouse_driver import Client
from clickhouse_driver import errors
import csv
import datetime
import time
import requests
import multiprocessing as mp
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def row_reader(csv_file_path):
    print(f"row_reader: csv_file_path = {csv_file_path}")
    with open(csv_file_path) as csv_file:
        print(f"row_reader: csv_file = {csv_file}")
        for line in csv.reader(csv_file):
            yield line

def input_from_csv(
    proton_server,
    proton_server_native_port,
    stream_name,
    columns,
    csv_file_path,
    with_header = True,
    batch_size = 1000,
    interval=0
):
    print(f"input_from_csv: csv_file_path = {csv_file_path}, columns = {columns}, with_header = {with_header}, batch_size = {batch_size}")
    client = Client(host=proton_server, port=proton_server_native_port)
    table_columns = ""
    # for element in columns:
    #    table_columns = table_columns + element.get("name") + ","
    # table_columns_str = "(" + table_columns[: len(table_columns) - 1] + ")"
    
    if (columns is None and not with_header) or (columns is not None and with_header):
        print(f"columns conflicts with with_header = True or no columns and with_header = False")
        exit(1) 
    
    if columns is not None:
        for column in columns:
            table_columns = table_columns + column + ","
        table_columns_str = "(" + table_columns[: len(table_columns) - 1] + ")"         

    print(f"table_columns_str = {table_columns_str}")

    if os.path.exists(csv_file_path):
        batch_str = ""
        batch_count = 0
        i = 1
        j = 1 #batch count
        input_sql = ""
        row_strs = []
        print(f"with_header = {with_header}")
        with open(csv_file_path, newline='') as csv_file:
            for row in csv.reader(csv_file):
                row_copy = row[:len(row)-1]
                #print(f"row = {row}")
                #print(f"row_copy = {row_copy}")      
                row_str = " "
                if with_header == True and i == 0:
                    for field in row_copy:
                        table_columns = table_columns + field + ","
                    table_columns_str = "(" + table_columns[: len(table_columns) - 1] + ")"
                    i += 1
                else:
                    # for field in row_copy:
                    #     # print("input_walk_through: field:", field)
                    #     if isinstance(field, str):
                    #         field = field.replace("\\", "\\\\").replace("'","\\'")
                    #     row_str = (
                    #         row_str + "'" + str(field) + "'" + ","
                    #     )  # python client does not support "", so put ' here
                    # row_str = "(" + row_str[: len(row_str) - 1] + ")"
                    # row_strs.append(row_str)
                    # #print(f"row_strs = {row_strs}")
                    
                    if j <= batch_size:
                        for field in row_copy:
                            # print("input_walk_through: field:", field)
                            if isinstance(field, str):
                                field = field.replace("\\", "\\\\").replace("'","\\'")
                            row_str = (
                                row_str + "'" + str(field) + "'" + ","
                            )  # python client does not support "", so put ' here
                        row_str = "(" + row_str[: len(row_str) - 1] + ")"
                        row_strs.append(row_str)
                        #print(f"row_strs = {row_strs}")                        
                        j += 1
                    else:
                        input_str =''
                        for row_str in row_strs:
                            input_str += row_str 
                        input_sql = (
                            f"insert into {stream_name} {table_columns_str} values {input_str}"
                        )
                        #print(f"input_sql = {input_sql}")
                        input_result = client.execute(input_sql)
                        print(f"i = {i}")
                        print(f"inserted {len(row_strs)} lines.")
                        row_strs = []
                        print(f"row_copy = {row_copy}")
                        row_str = " "
                        for field in row_copy:
                            # print("input_walk_through: field:", field)
                            if isinstance(field, str):
                                field = field.replace("\\", "\\\\").replace("'","\\'")
                            row_str = (
                                row_str + "'" + str(field) + "'" + ","
                            )  # python client does not support "", so put ' here
                        print(f"row_str = {row_str}")
                        row_str = "(" + row_str[: len(row_str) - 1] + ")"
                        row_strs.append(row_str)
                        print(f"row_strs = {row_strs}")                        
                        time.sleep(interval)
                        j = 2
                    i += 1
            #print(f"i = {i}, batch_size = {batch_size}, i%batch_size = {i%batch_size}")
            if (with_header and (i-1)%batch_size != 0) or (not with_header and i%batch_size != 0):
                input_str =''
                for row_str in row_strs:
                    input_str += row_str 
                input_sql = (
                    f"insert into {stream_name} {table_columns_str} values {input_str}"
                )  
                #print("input_sql = {input_sql}")             
                input_result = client.execute(input_sql)
                print(f"inserted {len(row_strs)} lines.")
    else:
        print("csv file specificed does not exist")
        raise Exception("csv file specificed does not exist")




if __name__ == '__main__':
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], '', ["proton_server=", "proton_port=","csv=","stream_name=", "columns="])
    except(getopt.GetoptError) as error:
        print(f"command error: {error}")
        print(f"usage: python3 perf_run_git.py --rx_mode=no_pipe")
        sys.exit(1)
    print(f"opts = {opts}")
    proton_server = 'localhost'
    proton_port = '8463'
    csv_file_path = None
    stream_name = None
    columns = None 
    for name, value in opts:
        if name in ("--proton_server"):
            proton_server = str(value)
        
        if name in ("--proton_port"):
            proton_port = value
        if name in ("--csv"):
            csv_file_path = value
        if name in ("--stream_name"):
            stream_name = value
        if name in ('--columns'):
            columns = value

    print(f"csv = {csv_file_path}, stream_name = {stream_name}, columns = {columns}")

    if csv_file_path is None or len(csv_file_path) == 0:
        print("Lack of input of csv path, exit.")
        exit(1)
    if columns is None or len(columns) == 0:
        print("Lack of input of columns, exit.")
        exit(1)
    if stream_name is None or len(stream_name) == 0:
        exit(1)
        print("Lack of input of stream name, exit.")

    columns = columns.split(",")
    start = datetime.datetime.now()
    print(f"start data input at {str(start)}")
    input_from_csv(
        proton_server,
        proton_port,
        stream_name,
        columns,
        csv_file_path,
        with_header = False,
        batch_size = 10000,
        interval=0
    )
    end = datetime.datetime.now()
    print(f"Finished data input at {str(end)}")
    duration = end - start
    print(f"duration = {duration.total_seconds()} s")
    

