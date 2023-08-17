from dzops.src.dep.Common.Constants import Constants
import json
from psycopg2.extras import RealDictCursor

    
class DatasetMetadataManager:
    def list_dataset_names1(self, filterValue, conn):
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            if not filterValue:
                cursor.execute(Constants.select_query)
                rows = cursor.fetchall()
                conn.commit()
                return rows
            else:
                
                mydict = json.loads(filterValue)
                if len(mydict) == 1:
                    cursor.execute(
                        Constants.select_query1 + list(mydict.keys())[0] + "= '" + list(mydict.values())[
                            0] + "'")
                    rows = cursor.fetchall()
                    conn.commit()
                    return rows
                elif len(mydict) == 2:
                    cursor.execute(
                        Constants.select_query1 + list(mydict.keys())[0] + "= '" + list(mydict.values())[
                            0] + "' AND " + list(mydict.keys())[1] + "= '" + list(mydict.values())[1] + "'")
                    rows = cursor.fetchall()
                    conn.commit()
                   # conn.close()
                    cursor.close()
                    return rows
                elif len(mydict) == 3:
                    cursor.execute(
                        Constants.select_query1 + list(mydict.keys())[0] + "= '" + list(mydict.values())[
                            0] + "' AND " + list(mydict.keys())[1] + "= '" + list(mydict.values())[1] + "' AND " +
                        list(mydict.keys())[
                            2] + "= '" + list(mydict.values())[2] + "'")
                    rows = cursor.fetchall()
                    conn.commit()
                    return rows
                elif len(mydict) == 4:
                    cursor.execute(
                        Constants.select_query1 + list(mydict.keys())[0] + "= '" + list(mydict.values())[
                            0] + "' AND " + list(mydict.keys())[1] + "= '" + list(mydict.values())[1] + "' AND " +
                        list(mydict.keys())[
                            2] + "= '" + list(mydict.values())[2] + "' AND " + list(mydict.keys())[
                            3] + "= '" + list(mydict.values())[3] + "'")
                    rows = cursor.fetchall()
                    conn.commit()
                    #conn.close()
                    cursor.close()
                    return rows
                elif len(mydict) == 5:
                    cursor.execute(
                        Constants.select_query1 + list(mydict.keys())[0] + "= '" + list(mydict.values())[
                            0] + "' AND " + list(mydict.keys())[1] + "= '" + list(mydict.values())[1] + "' AND " +
                        list(mydict.keys())[
                            2] + "= '" + list(mydict.values())[2] + "' AND " + list(mydict.keys())[
                            3] + "= '" + list(mydict.values())[3] + "' AND" + list(mydict.keys())[4] + "= '" +
                        list(mydict.values())[4] + "'")
                    rows = cursor.fetchall()
                    conn.commit()
                   # conn.close()
                    cursor.close()
                    return rows
                else:
                    return Constants.dataset_error
        except Exception as e:
            print(e)

    def get_dataset_metadata_by_id(self, dataset_id, conn):
        try:
            print(dataset_id)
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(
                Constants.query_metadta + dataset_id + "'")
            rows = cursor.fetchone()
            cursor.execute(Constants.query_metadta + dataset_id + "'")
            rows1 = cursor.fetchall()  # for dataset_custom_field
            return rows1
        except Exception as e:
            print(e)

    def get_dataset_metadata_by_type(self, dataset_type, conn):
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(Constants.metadata_select_query_type + dataset_type + "'")
            rows = cursor.fetchall()
            conn.commit()
            #conn.close()
            cursor.close()
      #      print(rows)
            return rows
        except Exception as e:
            print(e)
    
    def list_dataset_names(self, filterValue, conn):
        try:

            cursor = conn.cursor(cursor_factory=RealDictCursor)
            if not filterValue:
                cursor.execute(Constants.select_query)
                rows = cursor.fetchall()
                conn.commit()
                return rows
            else:
                mydict = self._filter(filterValue)
                # mydict = json.loads(filterValue)
                counter = len(mydict)

                while len(mydict) >= counter:
                    final_resp = []
                    for condition_list in mydict:
                        if len(condition_list) == 3:
                            cursor.execute(
                                "select * from dataset_metadata where dataset_type='" + condition_list[0] + "' AND " +
                                condition_list[1] + "='" + condition_list[2] + "'")
                            rows = cursor.fetchall()
                            final_resp.extend(rows)
                            conn.commit()
                            counter = counter - 1
                        else:
                            raise Exception("Please validate filter Condition!!")
                    # print(final_resp)
                    return final_resp

        except Exception as e:
            print(e)


    def create_dataset(self, json_loader, conn):
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(Constants.create_metadata_table)
            cursor.execute(Constants.create_custom_table)
            data = json_loader["dataset_name"], json_loader["dataset_type"], json_loader["language"], json_loader[
                "source_type"], \
                   json_loader["vendor"], json_loader["domain"],json_loader["description"],json_loader["lang_code"],json_loader["acquisition_date"], json_loader["migration_date"]

            cursor.execute(
                Constants.insert_query_metadata,
                data)
            cursor.execute(Constants.query_metadata + json_loader["dataset_name"] + "'")
            print("success")
            conn.commit()
            cursor.close()
            #conn.close()
        except Exception as e:
            print(e)

    def update_dataset(self,json_loader, conn):
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            dataset_name = json_loader["dataset_name"]
#           cursor.execute(Constants.query_metadata + json_loader["dataset_name"] + "'")
            query = f"select * from dataset_metadata where dataset_name='{dataset_name}'"
            cursor.execute(query)
            rows = cursor.fetchall()
#            print(len(rows))
            if len(rows) == 0:
                return 0
            else:
                for key, value in json_loader.items():
                    cursor = conn.cursor(cursor_factory=RealDictCursor)
                    query = f"UPDATE dataset_metadata SET {key}='{value}' where dataset_name ='{dataset_name}'"
                    cursor.execute(query)
                    conn.commit()
                return 1
        except Exception as e :
            print(e)

    def update_timestamp(self, conn, args):
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(Constants.update_ts_query, args)
        conn.commit()
       # conn.close()
        cursor.close()
    
    def update_dataset_remote(self,name,args1,args2,conn):
        cursor=conn.cursor()
        input_remote_tuple=(args2,args1,name)
        cursor.execute("update dataset_metadata set git_remote = %s, remote_location = %s where dataset_name=%s",input_remote_tuple)
        conn.commit()
      #  conn.close()
        cursor.close()
   
    def dataset_custom_fields(self ,datasetname,kv_pairs, conn):
        cur = conn.cursor()
        cur.execute("select dataset_id from dataset_metadata where dataset_name = %s",(datasetname,))
        rows = cur.fetchall()
        for i in rows:
            c = i[0]
        print(c)
        for key, value in kv_pairs.items():
           cur.execute("insert into dataset_custom_fields(dataset_id, field_name, field_value) values (%s , %s , %s)",(c,key,value))
           print(key,":",value,"\n")
           
        conn.commit()
        cur.close()
     #   conn.close()

    def delete_dataset(self,datasetname,conn):
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("DELETE FROM dataset_metadata WHERE dataset_name = %s",(datasetname,))
        print("Deleted dataset ",datasetname)
        conn.commit()
        cur.close()
      #  conn.close()
    
    def _filter(self, filterValue):
        filters = filterValue.split(",")
        resp = []
        for filter_con in filters:
            condition = filter_con.split(":")
            resp.append(condition)

        return resp

    ################### dataset API #############################

    def get_Counts(self, conn):
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(Constants.select_query2)
            count = cursor.fetchall()
            conn.commit()
            cursor.close()
            return count
        except Exception as e:
            print(e)

    def list_dataset(self, language , dataset_type , source_type , conn):
        try:
            lan = language 
            cor_type = dataset_type
            sor_type=source_type

            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            query= f"SELECT dataset_id, dataset_name, dataset_type, language, source_type, migration_date , lastupdated_ts , description, acquisition_date, (SELECT teamname FROM cfg_udops_teams_metadata tm WHERE tm.team_id IN ( SELECT team_id FROM cfg_udops_teams_acl cta WHERE cta.dataset_id = dataset_metadata.dataset_id )) AS teamname FROM dataset_metadata WHERE language IN (SELECT * FROM unnest(%s)) and dataset_type IN (SELECT * FROM unnest(%s)) and source_type IN (SELECT * FROM unnest(%s))"
            
            cursor.execute(query,(lan,cor_type,sor_type))
            rows = cursor.fetchall()
            conn.commit()
            cursor.close()
            if len(rows) == 0:
                return 0
            else:
                return rows
        except Exception as e:
            print(e)

    def language(self, conn):
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute( f"select DISTINCT language from dataset_metadata")
            rows = cursor.fetchall()
            conn.commit()
            cursor.close()
            return rows
        except Exception as e:
            print(e)

    def source_type(self, conn):
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute( f"select DISTINCT source_type from dataset_metadata")
            rows = cursor.fetchall()
            conn.commit()
            cursor.close()
            return rows
        except Exception as e:
            print(e)

    def dataset_type(self, conn):
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute( f"select DISTINCT dataset_type from dataset_metadata")
            rows = cursor.fetchall()
            conn.commit()
            cursor.close()
            return rows
        except Exception as e:
            print(e)

    def search_dataset(self, dataset_name, conn):
        try:
            if dataset_name=="":
               cursor = conn.cursor(cursor_factory=RealDictCursor)
               cursor.execute("SELECT dataset_id, dataset_name, dataset_type, language, source_type, (SELECT teamname FROM cfg_udops_teams_metadata tm WHERE tm.team_id IN (SELECT team_id FROM cfg_udops_teams_acl cta WHERE cta.dataset_id = dataset_metadata.dataset_id ) ) AS team_name FROM dataset_metadata")
               rows = cursor.fetchall()
               conn.commit()
               cursor.close()
               return rows
            else:
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                query=f"SELECT dataset_id, dataset_name, dataset_type, language, source_type,(SELECT teamname FROM cfg_udops_teams_metadata tm WHERE tm.team_id IN (SELECT team_id FROM cfg_udops_teams_acl cta WHERE cta.dataset_id = dataset_metadata.dataset_id ) ) AS team_name FROM dataset_metadata WHERE dataset_name ILIKE '%{dataset_name}%'"

#                cursor.execute(f"SELECT dataset_id, dataset_name, dataset_type, language, source_type,migration_date, lastupdated_ts, description, acquisition_date, (SELECT teamname FROM cfg_udops_teams_metadata tm WHERE tm.team_id IN (SELECT team_id FROM cfg_udops_teams_acl cta WHERE cta.dataset_id = dataset_metadata.dataset_id ) ) AS team_name FROM dataset_metadata where dataset_nam ='{dataset_name}'")
                cursor.execute(query)
                rows = cursor.fetchall()
                conn.commit()
                cursor.close()
                if rows==None:
                    return 0
                else:
                    return rows
        except Exception as e:
            return e

    def summary(self, conn, column):
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            col = column
            query1 = f"select DISTINCT {col} from dataset_metadata"
            # query = "select DISTINCT language from dataset_metadata"
            cursor.execute(query1)
            rows = cursor.fetchall()
            col_list = [dictionary[col] for dictionary in rows]
            dict = {}
            # query = " SELECT COUNT(*) FROM dataset_metadata WHERE vendor = %s "
            # print(query)
            for i in range(len(col_list)):
                data = col_list[i]
                query = f"SELECT COUNT(*) FROM dataset_metadata WHERE {col} = '{data}'"
                cursor.execute(query)
                # cursor.execute("SELECT COUNT(*) FROM dataset_metadata WHERE language =%s", (data,))
                result = cursor.fetchone()
                count = result['count']
                final_result = {data: count}
                dict.update(final_result)
            json_list = [{'key': k, 'value': v} for k, v in dict.items()]
            json_string = json.dumps(json_list)
            conn.commit()
            cursor.close()
            #   conn.close()
            return json_string
        except Exception as e:
            return e

    def donut(self, conn, column):
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            col = column
            query1 = f"select DISTINCT {col} from dataset_metadata"
            cursor.execute(query1)
            rows = cursor.fetchall()
            col_list = [dictionary[col] for dictionary in rows]
            value = []
            for i in range(len(col_list)):
                data = col_list[i]
                query = f"SELECT COUNT(*) FROM dataset_metadata WHERE {col} = '{data}'"
                cursor.execute(query)
                result = cursor.fetchone()
                count = result['count']
                value.append(count)
            conn.commit()
            cursor.close()
            # conn.close()
            return col_list, value
        except Exception as e:
            return e

    def summary_cutom(self, conn, dataset_name):
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            q = f"SELECT dataset_custom_fields.field_name, dataset_custom_fields.field_value FROM dataset_custom_fields JOIN dataset_metadata ON dataset_custom_fields.dataset_id= dataset_metadata.dataset_id WHERE dataset_metadata.dataset_name= '{dataset_name}'; "
            cursor.execute(q)
            rows = cursor.fetchall()
            dictionary = {}
            for row in rows:
                key = row['field_name']
                value = row['field_value']
                dictionary[key] = value
            json_list = [{'key': k, 'value': v} for k, v in dictionary.items()]
            json_string = json.dumps(json_list)
            conn.commit()
            cursor.close()
            return json_string
        except Exception as e:
            return e

    def update_custom_field(self, data, conn):
        try:
            for obj in data:
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                field_name = obj['field_name']
                field_value = obj['field_value']
                dataset_name = obj['dataset_name']
                query = f"select dataset_id from dataset_metadata where dataset_name ='{dataset_name}';"
                cursor.execute(query)
                rows = cursor.fetchone()
                c_id = rows['dataset_id']
                query_1 = f"UPDATE dataset_custom_fields SET field_value = " \
                          f"'{field_value}' where dataset_id = {c_id} AND field_name = '{field_name}';"
                cursor.execute(query_1)
                conn.commit()
                cursor.close()
            return 1
        except Exception as e:
            return e
