
import argparse, csv, os, json, time, subprocess
PROTON_PYTHON_DRIVER_NANME = "clickhouse-driver"
PROTON_PYTHON_DRIVER_FILE_NAME ="clickhouse-driver-0.2.4.tar.gz"

command = "pip3 list | grep clickhouse-driver"
ret = ret = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8", timeout=600)
if PROTON_PYTHON_DRIVER_NANME not in ret.stdout:
    command = "pip3 install ./" + PROTON_PYTHON_DRIVER_FILE_NAME
    ret = ret = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8", timeout=600)
    print(f"ret of subprocess.run({command}) = {ret}")
else:
    print(f"{PROTON_PYTHON_DRIVER_NANME} exists bypass s3 download and install")

from clickhouse_driver import Client


class JsonRawReader():
    def __init__(self,json_row_file):
        self._json_row_file = iter(json_row_file)
    def __iter__(self):
        return self
    def __next__(self):
        line = next(self._json_row_file, None)
        if line is None:
            raise StopIteration
        else:
            line = json.loads(line)
            line = json.dumps(line)
            #line = line.replace("\\", "\\\\").replace("'","\\'")
            return [line]


class Stream():
    def __init__(self, name, host=None, port=None, client=None, ddl_file=None, *columns):
        self._host = host
        self._port = port
        self._name = name
        self._ddl_file = ddl_file
        self._columns = columns

        self._columns_len = len(columns)
        self._client = client
        if client is None:
            try:
                self._client = Client(host, port=port)
            except Exception as error:
                print(f"Proton client error, exception = {error}")
        self.set_columns(*columns)
        if ddl_file is not None and os.path.exists(ddl_file):
            self.load_ddl_file(ddl_file) 

    @property
    def name(self):
        if self._name is not None:
            return self._name
        else:
            return None
    @property
    def columns(self):
        if self._columns is not None:
            return self._columns
        else:
            return None
    
    def set_columns(self, *columns):
        self._columns = columns
        self._columns_len = len(columns)
        if columns is not None:
            table_columns =''
            for column in columns:
                table_columns = table_columns + column + ","
            table_columns_str = "(" + table_columns[: len(table_columns) - 1] + ")"
            self._table_columns_str = table_columns_str        


    def ddl_file_read(self, ddl_file):
        ddls = []
        with open(ddl_file) as ddl_file:
            ddls = ddl_file.readlines()
        return ddls    


    def run_ddls(self, *ddls_array):
        if ddls_array is None:
            ddls_array = self._ddls
        try:
            for ddl in ddls_array:
                self._client.execute(ddl)
                time.sleep(1) #sleep 1 sec after execute one ddl
        except Exception as error:
            print(f"create stream exception, error = {error}")

    def load_ddl_file(self, ddl_file):
        print(f"ddl_file = {ddl_file}")
        ddls_array = self.ddl_file_read(ddl_file)
        print(f"ddls_array = {ddls_array}")
        self.run_ddls(*ddls_array)

    def row_str_from_array(self, row_array, *columns): #create row_str from an array of row e.g. [a,b,c]

        if len(columns) == 0:
            row_len = self._columns_len
        else:
            row_len = len(columns)
        row_str = ""
        row_copy = row_array[:row_len] #when the columns are defined, only mumber of fields matching number of columns
        for field in row_copy:
            # print("input_walk_through: field:", field)
            if isinstance(field, str):
                field = field.replace("\\", "\\\\").replace("'","\\'")
            row_str = (
                row_str + "'" + str(field) + "'" + ","
            )  # python client does not support "", so put ' here
        row_str = "(" + row_str[: len(row_str) - 1] + ")"
        #print(f"row_str = {row_str}")
        return row_str        

    def batch_str_from_array(self, rows_array, *columns): #create batch str from array of an array of row e.g. [[a,b,c],[c,d,e]]
        batch_str = ''
        for row_array in rows_array:
            row_str = self.row_str_from_array(row_array, *columns)
            batch_str += row_str
        return batch_str
    
    def load_line(self, input_str=None, *columns): 
        if input_str is None:
            print("No input values")
            return        
        if len(columns) == 0:
            table_columns_str = self._table_columns_str
        else:
            table_columns = ""
            for column in columns:
                table_columns = table_columns + column + ","
            table_columns_str = "(" + table_columns[: len(table_columns) - 1] + ")"    

        input_sql = (
            f"insert into {self._name} {table_columns_str} values {input_str}"
        )            
        try:
            self._client.execute(input_sql)
        except Exception as error:
            print(f"Proton sql = insert into {self._name} {table_columns_str} values exception, error = {error}")
    
    def load_lines_from_array(self, rows_array, *columns):
        if len(columns) == 0:
            columns = self._columns
        batch_str = self.batch_str_from_array(rows_array, *columns)
        try:
            res = self.load_line(batch_str, *columns)
            print(f"load {len(rows_array)} lines into stream {self._name}")
        except Exception as error:
            print(f"Proton input error, exception = {error}")


class FileLoader():
    def __init__(self, data_file_path, reader, stream, format): 
        self._data_file_path = data_file_path
        self._format = format
        self._columns = []
        self._stream = stream
        self._reader = reader
    @property
    def file_path(self):
        if self._file_path is not None:
            return self._file_path
        else:
            return None
    @property
    def format(self):
        if self._format is not None:
            return self._format
        else:
            return None
    
    def load(self, with_header = True, batch_size = 1000): #a csv reader or a generator to yeld line of file as an array, for example csv.reader
        print(f"interface for loading file, batch_size = {batch_size}")
        if os.path.exists(self._data_file_path):
            batch_str = ""
            batch_count = 0
            i = 0
            j = 0 #batch count
            input_sql = ""
            rows_array = []
            with open(self._data_file_path, newline='') as file:
                for row in self._reader(file):
                    if with_header == True and i == 0:
                        self._columns = row
                        i += 1
                    else:
                        rows_array.append(row)
                        i += 1
                        if j < batch_size:
                            j += 1
                        else:
                            self._stream.load_lines_from_array(rows_array, *self._columns)
                            rows_array = []
                            j = 0           
                if (with_header and (i-1)%batch_size != 0) or (not with_header and i%batch_size != 0):
                    self._stream.load_lines_from_array(rows_array, *self._columns)
                if with_header:
                    total_lines = i - 1
                else:
                    total_lines = i
                print(f"load total {total_lines} lines into stream {self._stream.name}")                           
          

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("data_file", help="the data file to be loaded, only csv is supported so far")
    parser.add_argument("-s", "--stream_name", required=True, help="strean name to load to")
    parser.add_argument("-i", "--host", help="proton host, optional, default:localhost", default = 'localhost')
    parser.add_argument("-p", "--port", help="proton native port, default:8463", default = 8463)
    parser.add_argument("-c", "--columns",  help="columns of the stream, optional when csv file has head, string with fields seperated by ',' e.g. id,name,city")
    #parser.add_argument("--columns", nargs='+',help="columns of the stream, optional when csv file has head")
    parser.add_argument( "-d", "--ddl_file", help = "stream ddl file for creating stream, optional")
    parser.add_argument("-b", "--batch_size", help = "batch_size of ingest data, default = 10000", default = 10000)
    parser.add_argument("-f", "--format", help = "data file format", default = 'CSVWithHeader') #CSVWithHeader, CSV
    args = parser.parse_args()
    print(f"args = {args}")

    if args.format not in ["CSVWithHeader", "JSONROWTXT"]:
        print(f"unknown format, exit.")
        exit(1)

    if args.columns is None and args.format != 'CSVWithHeader':
        print(f"Either specify columns or the format need to be with Header.")
        exit(1)

    if args.columns is None:
        columns = []
    else:
        columns = args.columns.split(",")  
    
    
    stream = Stream(args.stream_name, args.host, args.port, None, args.ddl_file, *columns)
    
    if args.format == 'CSVWithHeader':
        csv_reader = csv.reader
    elif args.format == 'JSONROWTXT':
        csv_reader = JsonRawReader
    floader = FileLoader(args.data_file, csv_reader, stream, args.format)
    if args.format == 'CSVWithHeader':
        floader.load(batch_size = int(args.batch_size))
    else:
        floader.load(with_header = False, batch_size = int(args.batch_size))
