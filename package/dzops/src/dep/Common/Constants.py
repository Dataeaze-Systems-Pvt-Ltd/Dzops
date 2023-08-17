class Constants:
    # list queries
    select_query = "select * from public.dataset_metadata"
    select_query1 = "select * from dataset_metadata where "
    select_query2 = "select count(*) from dataset_metadata "
    # dataset_metadata query
    metadata_select_query = "select * from dataset_metadata where dataset_id='"
    metadata_select_query_type = "select * from dataset_metadata where dataset_type='"

    # Udops_Users
    Udops_users_insert = "insert into udops_users (user_name, first_name , last_name , email) values (%s,%s,%s,%s)"
    Udops_users_select = "select user_id from udops_users where user_name = '"
    Udops_User_list = "select * from udops_users"
    Udops_users_update = "Update udops_users set user_name = (%s), firstname = (%s), lastname = (%s), email = (%s) where user_name = '"

    # create dataset
    insert_query_metadata = "insert into dataset_metadata (dataset_name,dataset_type,language,source_type,vendor,domain,description,lang_code,acquisition_date, migration_date) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    insert_query_custom_field = "insert into dataset_custom_fields (dataset_id,field_name,field_value) values(%s,%s,%s)"
    select_query_create = "select * from dataset_metadata "
    query_metadata="select * from dataset_metadata where dataset_name='"
    query_dataset_type="select * from dataset_metadata where dataset_name='"
#   select_query_create = "select * from dataset_metadata "

    # Update dataset
    update_query = "UPDATE dataset_metadata SET source_type=%s,language=%s,customer_name=%s,data_domain_name= %s WHERE dataset_name=(%s) "
    dataset_error = "dataset Doesnt Exist"
    
    #Delete dataset
    delete_query = "DELETE from dataset_metadata where dataset_name = (%s) "
    
    # count dataset 
    
    # update_timestamp
    update_ts_query = "UPDATE dataset_metadata SET  lastUpdated_ts= timezone(INTERVAL '+00:00', now()) where dataset_name=(%s)"

    # Constants File
    file_location = "/DatabaseConnection.properties"

    # Create table
    create_custom_table = "create TABLE if not exists dataset_custom_fields( field_id serial,dataset_id int, field_name varchar(200) NOT NULL,field_value text DEFAULT NULL,creationTimestamp TIMESTAMP without time zone default timezone(INTERVAL '+00:00', now()),lastUpdated_ts TIMESTAMP without time zone default timezone(INTERVAL '+00:00', now()),primary key(field_id),CONSTRAINT dataset_custom_fields_fk_1 FOREIGN KEY (dataset_id) REFERENCES dataset_metadata(dataset_id) ON DELETE cascade)"
    create_metadata_table = "create table if not exists dataset_metadata(dataset_id serial,dataset_name varchar(200) not null UNIQUE,dataset_type varchar(200) not NULL, language varchar(200) not NULL,source_type varchar(200),customer_name varchar(200), data_domain_name varchar(200), creationTimestamp TIMESTAMP without time zone default timezone(INTERVAL '+00:00', now()),lastUpdated_ts TIMESTAMP without time zone default timezone(INTERVAL '+00:00', now()),primary key(dataset_id))"

    column_query= "SELECT array_agg(column_name) AS all_columns FROM information_schema.columns WHERE table_name = 'dataset_metadata';"
