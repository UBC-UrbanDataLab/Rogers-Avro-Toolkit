from rtree import index
from shapely.geometry import Polygon, Point
from fastavro import reader
import json,glob,os,copy,avro,multiprocessing
from multiprocessing import Pool
from dateutil import parser,tz

def get_containing_box(p):
    xcoords = [x for x, _ in p.exterior.coords]
    ycoords = [y for _, y in p.exterior.coords]
    box = (min(xcoords), min(ycoords), max(xcoords), max(ycoords))   
    return box

def build_rtree(polys):
    def generate_items():
        pindx = 0
        for pol in polys:
            box = get_containing_box(pol)
            yield (pindx, box,  pol)
            pindx += 1
    return index.Index(generate_items())

def get_intersection_func(rtree_index):
    MIN_SIZE = 0.0001
    def intersection_func(point):
        # Inflate the point, since the RTree expects boxes:
        pbox = (point[0]-MIN_SIZE, point[1]-MIN_SIZE, 
                 point[0]+MIN_SIZE, point[1]+MIN_SIZE)
        hits = rtree_index.intersection(pbox, objects='raw')
        #Filter false positives:
        result = [pol for pol in hits if pol.intersects(Point(point)) ]
        return result
    return intersection_func

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


with open("lower_mainland.geojson","r") as file:
    data = json.load(file)

features = data["features"]
polygons = []
poly_map = {}

manager = multiprocessing.Manager()
output = manager.dict()

for park in features:
    polys = park["geometry"]["coordinates"]
    park_type = park["geometry"]["type"]
    name = park["properties"]["UniquePark"]

    if park_type == "Polygon":
        for poly in polys:
            polygons.append(Polygon(poly))
            tuple_poly = tuple(map(tuple,poly))
            poly_map[tuple_poly] = name
    else:
        for poly_list in polys:
            for poly in poly_list:
                polygons.append(Polygon(poly))
                tuple_poly = tuple(map(tuple,poly))
                poly_map[tuple_poly] = name


   
my_rtree = build_rtree(polygons)
my_func = get_intersection_func(my_rtree)


coord_types = {
              "connection_test.json":           1,
              "network_information_record.json": 1,
              "network_throughput_record.json":  1,
              "latency_record.json":             0,
              "voice_call.json":                 0
              }

time_keys = {
            "connection_test.json":             "TestTimestamp",
            "network_information_record.json": "TestTimestamp",
            "network_throughput_record.json":  "TimestampBin",
            "latency_record.json":             "Start_Timestamp",
            "voice_call.json":                 "Start_TestTimestamp"
            }

avro_files = [avro_file for avro_file in glob.glob("*_deep_ubc_*.avro")]

with open("../avro_schemas/schema_mapping.json","r") as schema:
    schema_map = json.load(schema)

def count_parks_helper(avro_file):
    schema_string = json.dumps(get_schema_from_avro(avro_file))
    schema_json_file = schema_map[schema_string]
    
    print(f"Working on cleaning {avro_file} of type {schema_json_file}")

    coord_type = coord_types[schema_json_file]
    with open(avro_file,"rb") as input_file:
        avro_reader = reader(input_file)
        row_counter,park_counter = 0,0   
        if coord_type:
            lat_key = "LocationLatitude"
            lon_key = "LocationLongitude"
        else:
            lat_key = "Start_LocationLatitude"
            lon_key = "Start_LocationLongitude"
        
        time_key = time_keys[schema_json_file]
        
        to_zone = tz.gettz("America/Los_Angeles")
        for row in avro_reader:
            lon = row[lon_key]
            lat = row[lat_key]
            
            row_time = row[time_key]
            timestamp = parser.parse(row_time)
            pacific = timestamp.astimezone(to_zone).strftime('%Y-%m-%d')
            
            my_intersections = my_func((lon,lat))
            for pol in my_intersections:
                p = tuple(pol.exterior.coords[:])
                name = poly_map[p]
                if pacific not in output:
                    output[pacific] = manager.dict()
                    output[pacific][name] = 1
                elif name not in output[pacific]:
                    output[pacific][name] = 1
                else:
                    output[pacific][name] += 1

                park_counter += 1
                
            row_counter += 1

 
if __name__ == "__main__":
    
    pool = Pool(os.cpu_count())
    pool.map(count_parks_helper, avro_files)
    
    for val in output:
        output[val] = output[val].copy()

    to_output = json.loads(json.dumps(output.copy()))
    with open ("./park_stats_dates.json","w+") as outfile:
        json.dump(to_output,outfile,sort_keys=True)
                      
