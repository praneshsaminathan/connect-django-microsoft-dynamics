import sys
import json
import logging
import requests
from django.conf import settings

from custom_sql import ExecuteRawSQL

logger = logging.getLogger('crm')


class CRM:
    crm_web_api = settings.CRM_WEB_API
    token_end_point = 'https://login.microsoftonline.com/' + settings.CRM_TENANT_ID + '/oauth2/token'
    token_post = {
        'client_id': settings.CRM_CLIENT_ID,
        'resource': settings.CRM_RESOURCE_URI,
        'username': settings.CRM_USERNAME,
        'password': settings.CRM_PASSWORD,
        'grant_type': 'password'
    }

    def __init__(self):
        self.sql = ExecuteRawSQL()
        self.token = self.__get_token()
        self.headers = {
            'Authorization': 'Bearer ' + self.token,
            'OData-MaxVersion': '4.0',
            'OData-Version': '4.0',
            'Accept': 'application/json',
            'Content-Type': 'application/json; charset=utf-8',
            'Prefer': 'odata.maxpagesize=500',
            'Prefer': 'odata.include-annotations=OData.Community.Display.V1.FormattedValue'
        }

    def __get_token(self):
        token_response = requests.post(self.token_end_point, data=self.token_post)
        try:
            access_token = token_response.json()['access_token']
        except KeyError:
            access_token = None
            logger.warning('Could not get access token')

        return access_token

    def __get_query(self, entity, filters=None):
        """
        Build dynamic query

        :param entity: Should be CRM entity plural name
        :param filters: This should be filter query string. Eg. '?$filter=new_portidnumber eq 0001 &$top=1'
        Ref: https://docs.microsoft.com/en-us/dynamics365/customer-engagement/developer/webapi/query-data-web-api
        :return: Constructed query
        """
        __base_query = "{0}/{1}".format(self.crm_web_api, entity)
        if filters:
            return "{0}{1}".format(__base_query, filters)

        return __base_query

    @staticmethod
    def __log_error(e):
        try:
            logger.error('Error on line {0}: Type {1}: Message: {2}'.format(
                sys.exc_info()[-1].tb_lineno),
                type(e).__name__,
                e
            )
        except:
            logger.error(str(e))

    def get(self, entity, filters=None):
        try:
            crm_data = dict()
            get_res = requests.get(self.__get_query(entity, filters), headers=self.headers)
            get_result = get_res.json()
            if get_result['value']:
                crm_data = get_result['value']
        except KeyError:
            logger.error('Error was occurred while executing {}'.format(self.__get_query(entity, filters)))
        except Exception as e:
            self.__log_error(e)

        return crm_data

    def create(self, entity, data):
        resp = None
        try:
            resp = requests.post(self.__get_query(entity), data=json.dumps(data), headers=self.headers)
        except Exception as e:
            self.__log_error(e)
            logger.error('Error occurred while creating record: {}'.format(str(data)))

        return resp

    def update(self, entity, uid, data):
        resp = None
        try:
            resp = requests.patch(
                self.__get_query(entity, "({})".format(uid)),
                data=json.dumps(data), headers=self.headers
            )
        except Exception as e:
            self.__log_error(e)
            logger.error('Error occurred while updating record: {}'.format(str(data)))

        return resp

    def get_lookup_field_value(self, entity, return_field, where_clause=None):
        """
        This method is used for reduce network latency of CRM get.
        :param entity: Table Name
        :param return_field:
        :param where_clause: Where query
        :return: value
        """
        resp = None
        try:
            query = "SELECT TOP (1) * FROM dyn.{}".format(entity)
            if where_clause:
                query += " WHERE {}".format(where_clause)
            value = self.sql.fetchone_dict(query)
            resp = value[0].get(return_field, None) if value else None
        except KeyError:
            logger.error('Error was occurred while executing sql query: {}'.format(query))
        except Exception as e:
            self.__log_error(e)

        return resp
