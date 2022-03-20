from requests                                 import Session
from requests                                 import Response
import os
import logging
import json
from apache_ranger.utils                      import *
LOG = logging.getLogger(__name__)

class RangerClientHttp:
    def __init__(self, url, auth):
        self.url          = url
        self.session      = Session()
        self.session.auth = auth


    def call_api(self, api, query_params=None, request_data=None):
        ret    = None
        params = { 'headers': { 'Accept': api.consumes, 'Content-type': api.produces } }

        if query_params:
            params['params'] = query_params

        if request_data:
            params['data'] = json.dumps(request_data)

        path = os.path.join(self.url, api.path)
        path = self.url+api.path

        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("------------------------------------------------------")
            LOG.debug("Call         : %s %s", api.method, path)
            LOG.debug("Content-type : %s", api.consumes)
            LOG.debug("Accept       : %s", api.produces)

        response = None

        if api.method == HttpMethod.GET:
            response = self.session.get(path, **params)
            print(response)
        elif api.method == HttpMethod.POST:
            response = self.session.post(path, **params)
        elif api.method == HttpMethod.PUT:
            response = self.session.put(path, **params)
        elif api.method == HttpMethod.DELETE:
            response = self.session.delete(path, **params)

        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("HTTP Status: %s", response.status_code if response else "None")

        if response is None:
            ret = None
        elif response.status_code == api.expected_status:
            try:
                if response.status_code == HTTPStatus.NO_CONTENT or response.content is None:
                    ret = None
                else:
                    if LOG.isEnabledFor(logging.DEBUG):
                        LOG.debug("<== __call_api(%s, %s, %s), result=%s", vars(api), params, request_data, response)

                        LOG.debug(response.json())

                    ret = response.json()
            except Exception as e:
                print(e)

                LOG.exception("Exception occurred while parsing response with msg: %s", e)

                raise RangerServiceException(api, response)
        elif response.status_code == HTTPStatus.SERVICE_UNAVAILABLE:
            LOG.error("Ranger admin unavailable. HTTP Status: %s", HTTPStatus.SERVICE_UNAVAILABLE)

            ret = None
        elif response.status_code == HTTPStatus.NOT_FOUND:
            LOG.error("Not found. HTTP Status: %s", HTTPStatus.NOT_FOUND)

            ret = None
        else:
            raise RangerServiceException(api, response)

        return ret
