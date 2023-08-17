import json
from pprint import pprint

from dzops.src.dep.poc.librispeech.asr.asr_data_reader import ASRDataReader
from dzops.src.dep.Manager.DatasetMetadataManager import *
dataset_metadata_manager=DatasetMetadataManager()
from dzops.src.dep.config.Connection import *
connection = Connection()
conn=connection.get_connection()
class DatasetDataReaderManager:
    def store_data(self, dataset_name,dataset_details, output_location, schema_type, custom_schema=None):
        response = self.read_data(dataset_name,dataset_details, schema_type, custom_schema)
        conn=connection.get_connection()
    
        if response['status'] == "success":
            dataset_list = response['data']
            with open(output_location, 'w') as f:
                json.dump(dataset_list, f)
               # dataset_metadat_manager.insert_file_path(conn,str(output_location))
            return {'status': 'success', 'error': ''}
        else:
            return response

    def read_data(self,dataset_name,dataset_details, schema_type, custom_schema=None):
      #  dataset_details = datasetId
        if schema_type == "native":
            custom_schema = dataset_details['native_schema']
        elif schema_type == "common":
            custom_schema = dataset_details['common_schema']
        elif schema_type == "custom":
            pass
        else:
            return {'status': 'failed', 'error': 'invalid input', 'data': []}
        
        output_schema = self.get_output_schema(dataset_name,custom_schema)
        template_file_path = dataset_details['template_file_path']
        data_dir_path = dataset_details['data_dir_path']

        dataset = ASRDataReader().read_all_records(output_schema, data_dir_path, template_file_path)
        dataset_list = ASRDataReader().get_dataset_as_json(dataset)
        return {'status': 'success', 'error': '', 'data': dataset_list}

#    def get_output_schema(self, file_path):
 #       output_schema = json.load(open(file_path[0]))
  #      return output_schema['asr']
    def get_output_schema(self,dataset_name, file_path):

        dataset_detail=dataset_metadata_manager.get_dataset_metadata_by_id(dataset_name,conn)
        output_schema = json.load(open(file_path[0]))
    
        for dataset in dataset_detail:
        
            return output_schema[dataset['dataset_type']]
