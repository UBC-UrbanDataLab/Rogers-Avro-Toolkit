import glob,os,pprint,json,time,sys,avro,copy,multiprocessing
from avro.io import DatumWriter,DatumReader
from avro.datafile import DataFileWriter, DataFileReader
from fastavro import reader
from joblib import Parallel, delayed

start_time_main = time.time()

coord_types = {
    "connection_test.json":1,
    "network_throughput_record.json":1,
    "network_information_record.json":1,
    "voice_call.json":0,
    "latency_record.json":0
}

lower_mainland = {"left_lon":  -123.385173,
                  "right_lon": -122.441928,
                  "down_lat":   49.00000,
                  "up_lat":     49.453122,
                  "output_dir": "cleaned"
                  }


ubcv_campus = {"left_lon":   -123.264930,
                "right_lon": -123.226796,
                "down_lat":   49.241625,
                "up_lat":     49.274197,
                "output_dir": "ubcv"
                }

ubco_campus = {"left_lon":   -123.226796,
                "right_lon": -118.962901,
                "down_lat":   49.655003,
                "up_lat":     50.386085,
                "output_dir": "ubco"
                }

boundary = ubco_campus

def clean_avro(input_avro: str, desired_schema: str) -> None:
    """
    Cleans the avro file by copying the desired columns to a new avro file with 
    the prefix "cleaned_" to the "cleaned" directory.

    Args:
        avro_file: The avro file to extract the desired columns from.
        avro_file_type: The avro file type (connection_test,voice_call,etc..). 
        
    """
    desired_columns = extract_cols(desired_schema)

    with open(f"./schemas/{desired_schema}","r") as schema:
        schema_dict = json.load(schema)

    schema_parsed = avro.schema.parse(json.dumps(schema_dict))

    output_dir = boundary["output_dir"]

    with open(f"./{output_dir}/{output_dir}_{input_avro}","wb+") as out_file:
        writer = DataFileWriter(out_file,DatumWriter(),schema_parsed)
        with open(input_avro,"rb") as file:
            avro_reader = reader(file)
            counter,in_bounds = 0,0
            if coord_types[desired_schema]:
                lat_key,lon_key = "LocationLatitude","LocationLongitude"
            else:
                lat_key,lon_key = "Start_LocationLatitude","Start_LocationLongitude"
            
            for row in avro_reader:
                lat,lon = row[lat_key],row[lon_key]
                if is_lower_mainland(lat,lon):
                    out_row = {val:row[val] for val in desired_columns}
                    writer.append(out_row)
                    in_bounds += 1
              
                counter += 1

        print(f"Processed {counter} rows in {file.name}, with {(in_bounds/counter)*100}% of the rows in {output_dir}.")


def extract_cols(json_schema_file: str) -> list:
    """
    Args:
        json_schema_file: The JSON file that corresponds to an avro schema to extract the column names from. 

    Returns:
        A list of the avro schema's column names.
    """
    with open(f"./schemas/{json_schema_file}","r") as schema:
        fields = json.load(schema)["fields"]

    return [val["name"] for val in fields]


def get_schema_from_avro(avro_file: str) -> dict:
    """
    Args:
        avro_file: The avro file to extract the schema from.

    Returns:
        A Python dictionary that corresponds to the schema of the input avro file. 
    """
    reader = avro.datafile.DataFileReader(open(avro_file,"rb"),avro.io.DatumReader())
    metadata = copy.deepcopy(reader.meta)
    schema_from_file = json.loads(metadata["avro.schema"])
    return schema_from_file   


def is_lower_mainland(lat: float,lon: float) -> bool:  
    """
    Args:
        lat: The latitude of a point.
        lon: The longitude of a point.
    
    Returns:
        If a point is in a rectangle that roughly encompasses the Lower Mainland. True if it is in, False otherwise. 

    """
    left_lon, right_lon = boundary["left_lon"], boundary["right_lon"]
    down_lat, up_lat    = boundary["down_lat"], boundary["up_lat"]
    
    return lat <= up_lat and lon <= right_lon and lat >= down_lat and lon >= left_lon

def start_clean(avro_file: str, avro_file_type: str) -> None:
    """
    A helper function to start the clean_avro function. 

    Args:
        avro_file: The avro file to extract the desired columns from.
        avro_file_type: The avro file type (connection_test,voice_call,etc..). 
    """

    print(f"Working on cleaning {avro_file} of type {avro_file_type}")
    clean_avro(avro_file,avro_file_type)


def main():

    avro_files = [avro_file for avro_file in glob.glob("deep_ubc_*.avro")]

    with open("./schemas/schema_mapping.json","r") as schema_map:
        schema_map = json.load(schema_map)
    
    num_threads = multiprocessing.cpu_count()
    Parallel(n_jobs=num_threads)(delayed(start_clean)(avro_file,schema_map[json.dumps(get_schema_from_avro(avro_file))].replace("_avro_schema","")) for avro_file in avro_files)
    

if __name__ == "__main__":
    main()
  
