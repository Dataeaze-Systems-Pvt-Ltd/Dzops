from dzops.src.dep.udataset import * 
from dzops.src.dep.udataset import *
from dzops.src.dep.UserManagement import *
from dzops.src.dep.Manager.DatasetMetadataManager import *
from dzops.src.dep.Manager.UserManagementManager import *
from dzops.src.dep.Handler.UserManagementHandler import *
from dzops.src.dep.config.Connection import *
from dzops.src.dep.InputProperties import *
from rest_framework.decorators import api_view
from rest_framework.views import APIView
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import json
prop=properties()
connection = Connection()
conn = connection.get_connection()


#Create your views here.
################# -----------------------------------------------------##################
class get_udops_count(APIView):
    permission_classes=([IsAuthenticated])
    def get(self,request):
        if request.method == 'GET':
            re = udataset()
            response = re.get_Counts()
            response_data = {
                "status": "success",
                "data": response
            }
            return JsonResponse(response_data, safe=False)

class summary(APIView):
    permission_classes=([IsAuthenticated])
    def get(self,request):
        if request.method =='GET':
            data= json.loads(request.body)
            dataset = udataset()
            response=dataset.summary(data['column'])
            data = json.loads(response)
            return JsonResponse(data, safe=False)

class get_dataset_list(APIView):
    permission_classes=([IsAuthenticated])
    def post(self,request):
        if request.method == 'POST':
            data= json.loads(request.body)
            re = udataset()
            response = re.list_dataset(data["language"],data["dataset_type"],data["source_type"])
            
            if response==0:
                response_data = {
                "status": "success",
                "failure_error": " ",
                "data": "Data not Found"
                  }

            else:
                response_data = {
                "status": "success",
                "failure_error": " ",
                "data": response
                  }

            return JsonResponse(response_data, safe=False)
        
class language(APIView):
    permission_classes=([IsAuthenticated])
    def get(self,request):
        if request.method == 'GET':
            re = udataset()
            response = re.language()
            response_data = {
            "status": "success",
            "failure_error": " ",
            "data": response
            }
            return JsonResponse(response_data, safe=False)
        
class dataset_type(APIView):
    permission_classes=([IsAuthenticated])
    def get(self,request):
        if request.method == 'GET':
            re = udataset()
            response = re.dataset_type()
            response_data = {
            "status": "success",
            "failure_error": " ",
            "data": response
            }
            return JsonResponse(response_data, safe=False)

class source_type(APIView):
    permission_classes=([IsAuthenticated])
    def get(self,request):
        if request.method == 'GET':
            re = udataset()
            response = re.source_type()
            response_data = {
            "status": "success",
            "failure_error": " ",
            "data": response
            }
            return JsonResponse(response_data, safe=False)
        


class search_dataset(APIView):
    permission_classes=([IsAuthenticated])
    def post(self,request):
        if request.method == 'POST':
            data = json.loads(request.body)
            re = udataset()
            response = re.search_dataset(data['dataset_name'])
            if response==0:
                response_data = {
                    "status": "failure",
                    "failure_error": "dataset do not exits!!!",
                }
                return JsonResponse(response_data, safe=False)
            else:
                response_data = {
                "status": "success",
                "data": response
                }
                return JsonResponse(response_data, safe=False)

class upsert(APIView):
    permission_classes=([IsAuthenticated])
    def put(self,request):
        if request.method=='PUT':
            try:

                data= json.loads(request.body)
                dataset = udataset()
                if dataset.update_dataset(data)==0 :
                    return JsonResponse({"status":"failure","failure_error":"dataset doesn't exist"},safe=False)

                else:
                    return JsonResponse({"status":"success"},safe=False)
            except Exception as e:
                raise e


class donut(APIView):
    permission_classes=([IsAuthenticated])
    def get(self,request):
        if request.method =='GET':
            #data= json.loads(request.body)
            data = ['language','dataset_type','source_type','vendor','domain']
            dataset = udataset()
            const_data = []
            i =0
            for i in range(len(data)):
                dataset_property= data[i]
                response=dataset.donut(dataset_property)
                key = response[0]
                value = response[1]
                _data = {'name': f'Per {dataset_property}','labels':key,'dataset': [{'label': ' ','data':f'{value}' }]}
                const_data.append(_data)
                i = i +1
            return JsonResponse(const_data,safe=False)

class summary_custom(APIView):
    permission_classes=([IsAuthenticated])
    def post(self,request):
        if request.method =='POST':
            data= json.loads(request.body)
            dataset = udataset()
            response=dataset.summary_custom(data["dataset_name"])
        # print(response)
            data = json.loads(response)
            return JsonResponse(data, safe=False)


class update_custom_field(APIView):
    permission_classes=([IsAuthenticated])
    def post(self,request):
        if request.method == 'POST':
            data = json.loads(request.body)
            dataset = udataset()
            response = dataset.update_custom_field(data)
            if response ==1:
                return JsonResponse({"status": "updated successfully"}, safe=False)
            else:
                return JsonResponse({"status": "failed"}, safe=False)

###################### User Management API #####################################

class list_user(APIView):
    permission_classes=([IsAuthenticated])
    def get(self,request):
        if request.method=='GET':
            user = UserManagement()
            response=user.get_user_list()
            response_data = {
            "status":"success",
            "data":response
            }
            return JsonResponse(response_data, safe=False)



class upsert_user(APIView):
    permission_classes=([IsAuthenticated])
    def post(self,request):
        if request.method == 'POST':
            data = json.loads(request.body)
            dataset = UserManagement()
            response = dataset.update_user(data["firstname"],data["lastname"],data["email"],data["existing_user_name"],data["new_user_name"])
            if response==1:
                return JsonResponse({"status": "updated successfully !!!"}, safe=False)
            else:
                return JsonResponse({"status": "Existing Username is not present!!!"}, safe=False)



class team_list(APIView):
    permission_classes=([IsAuthenticated])
    def get(self,request):
        if request.method=='GET':
            user = UserManagement()
            response=user.get_team_list()
            response_data = {
            "status":"success",
            "data":response
            }
            return JsonResponse(response_data, safe=False)

class team_upsert(APIView):
    permission_classes=([IsAuthenticated])
    def post(self,request):
        if request.method == 'POST':
            data = json.loads(request.body)
            dataset = UserManagement()
            response = dataset.update_team(data["permanent_access_token"],data["tenant_id"],data["admin_user_name"],data["s3_base_path"],data["existing_teamname"],data["new_teamname"])
            response_data = {
            "status":"success",
            "data":response
            }
            return JsonResponse(response_data, safe=False)

class add_users_team(APIView):
    permission_classes=([IsAuthenticated])
    def post(self,request):
        if request.method == 'POST':
            data = json.loads(request.body)
            dataset = UserManagement()
            response = dataset.add_users_team(data["user_name"],data["teamname"])
            response_data = {
            "status":"success",
            "data":response
            }
            return JsonResponse(response_data, safe=False)

class remove_users_team(APIView):
    permission_classes=([IsAuthenticated])
    def post(self,request):
        if request.method == 'POST':
            data = json.loads(request.body)
            dataset = UserManagement()
            response = dataset.delete_user(data["user_name"],data["teamname"])
            if response==1:
                return JsonResponse({"status": "Data Deleted Successfully !!!"}, safe=False)
            else:
                return JsonResponse({"status": "Teamname is not valid!!!!!"}, safe=False)

class grant_dataset(APIView):
    permission_classes=([IsAuthenticated])
    def post(self,request):
        if request.method=='POST':
            data = json.loads(request.body)
            dataset = UserManagement()
            response = dataset.grant_access_dataset(data["user_name"],data["dataset_name"],data["permission"])
            if response==1:
                return JsonResponse({"status": "Permission granted successfully for user."}, safe=False)
            else:
                return JsonResponse({"status": "failed"}, safe=False)


class remove_user_dataset(APIView):
    permission_classes=([IsAuthenticated])
    def post(self,request):
        if request.method == 'POST':
            data = json.loads(request.body)
            dataset = UserManagement()
            response = dataset.remove_access_dataset(data["user_name"],data["dataset_name"],data["permission"])
            if response==1:
                return JsonResponse({"status": "Permission Deleted Successfully !!!"}, safe=False)
            else:
                return JsonResponse({"status": "failed"}, safe=False)


class grant_dataset_list_write(APIView):
    permission_classes=([IsAuthenticated])
    def post(self,request):
        if request.method=='POST':
            data = json.loads(request.body)
            dataset = UserManagement()
            response = dataset.access_dataset_list_write(data["dataset_name"])
            json_string = json.dumps(response)
            data = json.loads(json_string)
            response_data = {
            "status": "success",
            "data": data
            }
            return JsonResponse(response_data, safe=False)

class grant_dataset_list_read(APIView):
    permission_classes=([IsAuthenticated])
    def post(self,request):
        if request.method=='POST':
            data = json.loads(request.body)
            dataset = UserManagement()
            response = dataset.access_dataset_list_read(data["dataset_name"])
            json_string = json.dumps(response)
            data = json.loads(json_string)
            response_data = {
            "status": "success",
            "data": data
            }
            return JsonResponse(response_data, safe=False)


class get_list_teams_read(APIView):
    permission_classes=([IsAuthenticated])
    def post(self,request):
        if request.method=='POST':
            data = json.loads(request.body)
            dataset = UserManagement()
            response = dataset.get_list_teams_read(data["user_name"])
            json_string = json.dumps(response)
            data = json.loads(json_string)
            response_data = {
            "status": "success",
            "data": data
            }
            return JsonResponse(response_data, safe=False)


class get_list_teams_write(APIView):
    permission_classes=([IsAuthenticated])
    def post(self,request):
        if request.method=='POST':
            data = json.loads(request.body)
            dataset = UserManagement()
            response = dataset.get_list_teams_write(data["user_name"])
            json_string = json.dumps(response)
            data = json.loads(json_string)
            response_data = {
            "status": "success",
            "data": data
            }
            return JsonResponse(response_data, safe=False)


class grant_team_pemission_read(APIView):
    permission_classes=([IsAuthenticated])
    def post(self,request):
        if request.method == 'POST':
            data = json.loads(request.body)
            dataset = UserManagement()
            response = dataset.grant_team_pemission_read(data["user_name"],data["teamname"])
            if response==1:
                return JsonResponse({"status": "Permission Granted Successfully !!!"}, safe=False)
            elif response==2:
                return JsonResponse({"status": "No team found with the teamname !!!"}, safe=False)
            elif response==3:
                return JsonResponse({"status": "The user does not have access to the team !!!"}, safe=False)
            elif response==4:
                return JsonResponse({"status": "Invalid teamname !!!"}, safe=False)
            else:
                return JsonResponse({"status": "failed"}, safe=False)

class grant_team_pemission_write(APIView):
    permission_classes=([IsAuthenticated])
    def post(self,request):
        if request.method == 'POST':
            data = json.loads(request.body)
            dataset = UserManagement()
            response = dataset.grant_team_pemission_write(data["user_name"],data["teamname"])
            if response==1:
                return JsonResponse({"status": "Permission Granted Successfully !!!"}, safe=False)
            elif response==2:
                return JsonResponse({"status": "No team found with the teamname !!!"}, safe=False)
            elif response==3:
                return JsonResponse({"status": "The user does not have access to the team !!!"}, safe=False)
            elif response==4:
                return JsonResponse({"status": "Invalid teamname !!!"}, safe=False)
            else:
                return JsonResponse({"status": "failed"}, safe=False)

class existing_users(APIView):
    permission_classes=([IsAuthenticated])
    def post(self,request):
        if request.method=='POST':
            data = json.loads(request.body)
            dataset = UserManagement()
            response = dataset.existing_users(data["teamname"])
            json_string = json.dumps(response)
            data = json.loads(json_string)
            response_data = {
            "status": "success",
            "data": data
            }
            return JsonResponse(response_data, safe=False)

class not_existing_users(APIView):
    permission_classes=([IsAuthenticated])
    def post(self,request):
        if request.method=='POST':
            data = json.loads(request.body)
            dataset = UserManagement()
            response = dataset.not_existing_users(data["teamname"])
            json_string = json.dumps(response)
            data = json.loads(json_string)
            response_data = {
            "status": "success",
            "data": data
            }
            return JsonResponse(response_data, safe=False)

class add_team(APIView):
    permission_classes=([IsAuthenticated])
    def post(self,request):
        if request.method == 'POST':
            data = json.loads(request.body)
            dataset = UserManagement()
            response = dataset.add_team(data["permanent_access_token"],data["tenant_id"],data["admin_user_name"],data["s3_base_path"],data["teamname"])
            response_data = {
            "status":"success",
            "data":response
            }
            return JsonResponse(response_data, safe=False)

class add_user(APIView):
    permission_classes=([IsAuthenticated])
    def post(self,request):
        if request.method == 'POST':
            data = json.loads(request.body)
            dataset = UserManagement()
            response = dataset.add_user(data["user_name"],data["firstname"],data["lastname"],data["email"])
            response_data = {
            "status":"success",
            "data":response
            }
            return JsonResponse(response_data, safe=False)

class get_team_list_search(APIView):
    permission_classes=([IsAuthenticated])
    def post(self,request):
        if request.method == 'POST':
            data = json.loads(request.body)
            dataset = UserManagement()
            response = dataset.get_team_list_search(data["teamname_substring"])
            response_data = {
            "status":"success",
            "data":response
            }
            return JsonResponse(response_data, safe=False)

class list_user_search(APIView):
    permission_classes=([IsAuthenticated])
    def post(self,request):
        if request.method == 'POST':
            data = json.loads(request.body)
            dataset = UserManagement()
            response = dataset.list_user_search(data["user_name_substring"])
            response_data = {
            "status":"success",
            "data":response
            }
            return JsonResponse(response_data, safe=False)

@csrf_exempt
def user_status(request):
    if request.method == 'POST':
        data = json.loads(request.body)
        git_username = data['git_username']
        token = data['token']
        dataset = UserManagement()
        response = dataset.user_status(git_username, token)
        if response == 0:
            response_data = {
                "User_role": "User Not exist",
            }
        else:
            val = response[0]
            user_data = response[1]
            if val == 1:
                response_data = {
                    "User_role": "normal user",
                    "user_data":user_data
                }
            else:
                response_data = {
                    "User_role": "Admin user",
                    "user_data": user_data
                }
        return JsonResponse(response_data, safe=False)
