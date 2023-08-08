from dzops.src.dep.Handler.DatasetHandler import *

import shutil
from typing import Optional
import typer

class udataset:
    def listCorpusNames(filter_value):
  #      typer.echo("load test")
#        print(filter_value)
        dataset_handler = DatasetHandler()
        response = dataset_handler.list_dataset_names(filter_value)
        #for row in response:
        #    print("Result :", row)


    def getCorpusMetadata(corpus_name):  # take one argument
        dataset_handler = DatasetHandler()
        row=dataset_handler.get_dataset_metadata(corpus_name)
        print(row)
        return row

    def getCorpusMetadatabytype(corpus_type: str):
        dataset_handler = DatasetHandler()
        row = dataset_handler.manager_get_metadata_type(corpus_type)
  #      print(row)
        return row
        

    def RDSConfig(host,dbname , user, password):
        dataset_handler = DatasetHandler()
        dataset_handler.RDSConfig(host = host , dbname = dbname , user = user , password = password)

    def dataset_custom_fields(corpusname , kv_pairs):
        dataset_handler = DatasetHandler()
        dataset_handler.dataset_custom_fields(corpusname , kv_pairs)
     
    def list_dataset(self):
        dataset_handler = DatasetHandler()
        dataset_handler.list_commits()

    def checkout(commitid):
        dataset_handler = DatasetHandler()
        dataset_handler.checkout(commitid)

    def delete_dataset(corpusname):
        dataset_handler = DatasetHandler()
        dataset_handler.delete_corpus(corpusname)

    def init(file,target):
        dataset_handler = DatasetHandler()
        dataset_properties = file
        dataset_handler.create_dataset(dataset_properties,target)
        print("Config written")


    def add(target: str):
        dataset_handler = DatasetHandler()
        dataset_handler.add_repo(target)
    
    def clone(args:str):
        dataset_handler = DatasetHandler()
        dataset_handler.clone_repo(args)


    def commit(message: str):
        dataset_handler = DatasetHandler()
        dataset_handler.commit_repo(message)


    def remote(name:str,data: str, gita: str):
        dataset_handler = DatasetHandler()
        dataset_handler.remote_repo(name,data,gita)


    def push(self):
        dataset_handler = DatasetHandler()
        dataset_handler.push_remote()


    def pull(audio):
        dataset_handler = DatasetHandler()
        dataset_handler.pull_repo(audio)


    # def update_corpus(self,file: str):
    #     corpus_handler = CorpusHandler()
    #     corpus_properties = json.load(open(file, 'r', encoding='utf-8'))
    #     str1 = corpus_handler.manager_update_corpus(corpus_properties)

    def datareader(corpus_details_dict, schema_type : Optional[str] =typer.Argument("common"),custom_schema:Optional[str] =typer.Argument(None)):
        dataset_handler = DatasetHandler()
        dataset_handler.datareader(corpus_details_dict, schema_type,custom_schema)
    
    def store_data(corpus_details_dict, output_loc, schema_type : Optional[str] =typer.Argument("common"), custom_schema:Optional[str] =typer.Argument(None) ):
        dataset_handler = DatasetHandler()
        dataset_handler.store_data(corpus_details_dict,output_loc,schema_type,custom_schema)

    def get_Counts(self):
        dataset_handler = DatasetHandler()
        return dataset_handler.get_Counts()

    def summary(self, column):
        dataset_handler = DatasetHandler()
        return dataset_handler.summary(column)

    def list_dataset(self,language , corpus_type ,  source_type):
        dataset_handler = DatasetHandler()
        return dataset_handler.list_dataset(language , corpus_type ,  source_type)
    

    def language(self):
        dataset_handler = DatasetHandler()
        return dataset_handler.language(conn)
    
    def source_type(self):
        dataset_handler = DatasetHandler()
        return dataset_handler.source_type(conn)
    
    def dataset_type(self):
        dataset_handler = DatasetHandler()
        return dataset_handler.dataset_type(conn)

    def search_dataset(self, corpus_name):
        dataset_handler = DatasetHandler()
        if dataset_handler.search_dataset(corpus_name) == 0:
            return 0
        else:
            return dataset_handler.search_dataset(corpus_name)

    def update_dataset(self, data):
        dataset_handler = DatasetHandler()
        if dataset_handler.update_dataset(data) == 1:
            return 1
        else:
            return 0

    def donut(self, column):
        dataset_handler = DatasetHandler()
        return dataset_handler.donut(column)

    def summary_custom(self, corpus_name):
        dataset_handler = DatasetHandler()
        return dataset_handler.summary_custom(corpus_name)

    def update_custom_field(self, data):
        dataset_handler = DatasetHandler()
        if dataset_handler.update_custom_field(data) == 1:
            return 1
        else:
            return 2

if __name__ == '__main__':
    udataset()
