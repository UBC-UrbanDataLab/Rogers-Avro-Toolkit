import json
import remove_rows_and_columns as util

#This script assumes that "remove_rows_and_columns.py" was run and the cleaned files are located in a directory named "cleaned".

schema_dict = {}

#connection test avro + reduced columns
ct_avro_schema = util.get_schema_from_avro("deep_ubc_01_08232021.avro")
ct_str = json.dumps(ct_avro_schema)
schema_dict[ct_str] = "connection_test_full_schema.json"

ct_schema_str = json.dumps(util.get_schema_from_avro("./cleaned/cleaned_deep_ubc_01_08232021.avro"))
schema_dict[ct_schema_str] = "connection_test.json_clean_schema"


#latency record avro + reduced columns
lr_avro_schema = util.get_schema_from_avro("deep_ubc_03_08232021.avro")
lr_str = json.dumps(lr_avro_schema)
schema_dict[lr_str] = "latency_record__full_schema.json"

lr_schema_str = json.dumps(util.get_schema_from_avro("./cleaned/cleaned_deep_ubc_03_08232021.avro"))
schema_dict[lr_schema_str] = "latency_record.json_clean_schema"



#network information record
nir_avro_schema = util.get_schema_from_avro("deep_ubc_05_08232021.avro")
nir_str = json.dumps(nir_avro_schema)
schema_dict[nir_str] = "network_information_record_full_schema.json"

nir_schema_str = json.dumps(util.get_schema_from_avro("./cleaned/cleaned_deep_ubc_05_08232021.avro"))
schema_dict[nir_schema_str] = "network_information_record_clean_schema.json"



#network throughput record
ntr_avro_schema = util.get_schema_from_avro("deep_ubc_20_08242021.avro")
ntr_str = json.dumps(ntr_avro_schema)
schema_dict[ntr_str] = "network_throughput_record_full_schema.json"

ntr_schema_str = json.dumps(util.get_schema_from_avro("./cleaned/cleaned_deep_ubc_20_08242021.avro"))
schema_dict[ntr_schema_str] = "network_throughput_record_clean_schema.json"


#voice call
vc_avro_schema = util.get_schema_from_avro("deep_ubc_22_08242021.avro")
vc_str = json.dumps(vc_avro_schema)
schema_dict[vc_str] = "voice_call_full_schema.json"

vc_schema_str = json.dumps(util.get_schema_from_avro("./cleaned/cleaned_deep_ubc_22_08242021.avro"))
schema_dict[vc_schema_str] = "voice_call_clean_schema.json"



with open("./schemas/schema_mapping.json","w+") as outfile:
    json.dump(schema_dict, outfile, indent=4)
