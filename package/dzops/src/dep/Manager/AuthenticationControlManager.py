import requests
from psycopg2.extras import RealDictCursor

class udpos_authentication:
    def authenticate_user(self,ACCESS_TOKEN,conn):
        try:
            url = 'https://api.github.com/user'
            headers = {'Authorization': f'token {ACCESS_TOKEN}'}
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                username = response.json()['login']
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                query  = f"select user_id from udops_users where user_name = '{username}'"
                cursor.execute(query)
                rows = cursor.fetchone()
                user_id = rows['user_id']
                conn.commit()
                return user_id
            else:
                return 0 
        except Exception as e:
            print(e)

    def get_user_team(self, team_name ,conn):
        try:
            cursor = conn.cursor()
            query = f"select team_id from cfg_udops_teams_metadata where teamname = '{team_name}'"
            cursor.execute(query)
            rows = cursor.fetchone()
            if rows is not None:
               team_id = rows[0]
               conn.commit()
               return team_id
            else:
                return 0
        except Exception as e:
            print(e)

    def dataset_id(self,dataset_name,conn):
        try:
            cursor = conn.cursor()
            query = f"select dataset_id from dataset_metadata where dataset_name = '{dataset_name}'"
            cursor.execute(query)
            rows = cursor.fetchone()
            cid = rows[0]
            
            return cid
        except Exception as e:
            print(e)

    def default_access(self,dataset_id,user_id,conn):
        try:
            cursor = conn.cursor()
            
            query1 = f"select user_name from udops_users where user_id = {user_id};"
            cursor.execute(query1)
            rows = cursor.fetchone()
            
            username = rows[0]

            p = 'write'
            data = user_id,username,dataset_id,p
        
            query = f"insert into cfg_udops_acl (user_id,user_name,dataset_id,permission) values (%s,%s,%s,%s);"
            cursor.execute(query,data)
            conn.commit()
            cursor.close()
        except Exception as e:
            print(e)

    def Dataset_team_map(self,team_id , dataset_id,conn):
        try:
            cursor = conn.cursor()
            data = team_id,dataset_id
            query = f"insert into cfg_udops_teams_acl (team_id,dataset_id) values (%s,%s);"
            cursor.execute(query,data)
            conn.commit()
            cursor.close()
        except Exception as e:
            print(e)




