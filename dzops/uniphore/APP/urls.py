
from django.urls import path,include
from . import views
from rest_framework_simplejwt import views as jwt_views

urlpatterns = [
    path('login',jwt_views.TokenObtainPairView.as_view(),name='login'),
    path('udops/dataset/count/', views.get_udops_count.as_view()),
    path('udops/dataset/summary/',views.summary.as_view()),
    path('udops/dataset/list/',views.get_dataset_list.as_view()),
    path('udops/dataset/search/',views.search_dataset.as_view()),
    path('udops/dataset/upsert/', views.upsert.as_view()),
    path('udops/dataset/donut/', views.donut.as_view()),
    path('udops/dataset/summary_custom/',views.summary_custom.as_view()),
    path('udops/dataset/update_custom_field/',views.update_custom_field.as_view()),
    path('udops/dataset/language/',views.language.as_view()),
    path('udops/dataset/source_type/',views.source_type.as_view()),
    path('udops/dataset/dataset_type/',views.dataset_type.as_view()),
    ### user management API ###
    path('udops/user/list/',views.list_user.as_view()),
    path('udops/user/upsert_user/',views.upsert_user.as_view()),
    path('udops/team/list/',views.team_list.as_view()),
    path('udops/team/upsert/',views.team_upsert.as_view()),
    path('udops/team/add_users_team/',views.add_users_team.as_view()),
    path('udops/team/remove_users/',views.remove_users_team.as_view()),
    path('udops/user/access_permission/',views.grant_corpus.as_view()),
    path('udops/user/remove_permission/',views.remove_user_corpus.as_view()),
    path('udops/user/corpus_access_list_write/',views.grant_corpus_list_write.as_view()),
    path('udops/user/corpus_access_list_read/',views.grant_corpus_list_read.as_view()),
    path('udops/user/list_teams_read/',views.get_list_teams_read.as_view()),
    path('udops/user/list_teams_write/',views.get_list_teams_write.as_view()),
    path('udops/user/team_permission_read/',views.grant_team_pemission_read.as_view()),
    path('udops/user/team_permission_write/',views.grant_team_pemission_write.as_view()),
    path('udops/team/existing_users/',views.existing_users.as_view()),
    path('udops/team/not_existing_users/',views.not_existing_users.as_view()),
    path('udops/team/add_team/',views.add_team.as_view()),
    path('udops/user/add_user/',views.add_user.as_view()),
    path('udops/team/list_search_team/',views.get_team_list_search.as_view()),
    path('udops/user/list_search_user/',views.list_user_search.as_view()),
    path('udops/user/user_status/', views.user_status),
    ]
