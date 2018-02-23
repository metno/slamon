""" Helper library to get status from and update statuspage.io
"""
# pylint: disable=C0301, C0111

__author__ = "Christian Skarby"
__version__ = "0.2.0"
__license__ = "GPLv2+"


import json
import time
import threading

import requests
from logzero import logger

class Incident:
    # pylint: disable=R0903
    def __init__(self, incident_id, title):
        self.id = incident_id  # pylint: disable=C0103
        self.title = title
        self.affected_components = []

class StatusPage:
    API_LOCK = threading.Semaphore()

    def __init__(self, page_id='4b9b31tffb5x', apikey=None, dryrun=False):
        self.__headers = {}
        if apikey:
            self.__headers['Authorization'] = 'OAuth {}'.format(apikey)
        self.__page_id = page_id
        self.__components = {}
        self.__update_local_component_status()
        self.__active_incidents = {}
        self.__update_local_incident_status()
        self.__dryrun = dryrun

    def __call(self, url, method='get', body='', timeout=10):
        with self.API_LOCK:
            time.sleep(1)  # statuspage.io has a limit of one request per second - enforce compliant behavior by waiting 1 second before each request
            response = getattr(requests, method)(url, data=body, headers=self.__headers, timeout=timeout)
            response.raise_for_status()
            result = response.json()
            if 'error' in result:
                raise RuntimeError("Failed to call {}, received {}".format(url, response.text))
            return result

    def __update_local_component_status(self):
        data = self.__call('https://{}.statuspage.io/api/v2/components.json'.format(self.__page_id))
        components = data.get("components", None)
        if components:
            for component in components:
                self.__components[component['id']] = component
                logger.debug('%s (%s) has status %s', component['name'], component['id'], component['status'])

    def __update_local_incident_status(self):
        active_incidents = {}
        data = self.__call('https://{}.statuspage.io/api/v2/incidents/unresolved.json'.format(self.__page_id))

        def iterate(data, key):
            """ helper function
            """
            subdata = data.get(key, None)
            if subdata:
                for item in subdata:
                    yield item

        for incident in iterate(data, "incidents"):
            obj = Incident(incident['id'], incident['name'])
            for update in iterate(incident, "incident_updates"):
                for component in iterate(update, "affected_components"):
                    if not component['code'] in obj.affected_components:
                        obj.affected_components.append(component['code'])
            active_incidents[obj.id] = obj

        self.__active_incidents = active_incidents

    def get_incidents_for_component(self, component_id):
        for _, incident in self.__active_incidents.items():
            if component_id in incident.affected_components:
                yield incident

    def status(self, component_id, new_status=None):
        if new_status:
            self.__set_status(component_id, new_status)
            self.__update_local_component_status()
        return self.__get_status(component_id)

    def __get_status(self, component_id):
        component = self.__components.get(component_id, None)
        if component:
            return component.get('status', None)
        return None

    def __set_status(self, component_id, new_status):
        assert new_status in ('operational', 'degraded_performance', 'partial_outage', 'major_outage')
        if self.__dryrun:
            logger.info("DRYRUN: Would set component %s to %s", component_id, new_status)
            return
        try:
            self.__call(
                'https://api.statuspage.io/v1/pages/{}/components/{}.json'.format(self.__page_id, component_id),
                method='patch',
                body=json.dumps({'component': {'status': new_status}}),
            )
        except:
            pass

    def create_incident(self, component_id, title, message, impact=None):
        """ Open remote incident
        """
        payload = {
            'name': title,
            'status': 'investigating',  # ('investigating', 'identified', 'monitoring', 'resolved')
            'message': message,
            'component_ids': [component_id],
        }
        if impact:
            assert impact in ('none', 'minor', 'major')
            payload['impact_override'] = impact
        body = json.dumps({"incident": payload})

        if self.__dryrun:
            logger.info("Would create incident: %s", body)
            logger.info("Dryrun - not creating incident on statuspage.io...")
            return
        logger.debug("Creating incident: %s", body)

        self.__call(
            'https://api.statuspage.io/v1/pages/{}/incidents.json'.format(self.__page_id),
            method='post',
            body=body,
        )
        self.__update_local_incident_status()

    def update_incident(self, incident_id, status=None, message=None, impact=None):
        """ update remote incident
        """
        payload = {}
        if status is not None:
            assert status in ('investigating', 'identified', 'monitoring', 'resolved')
            payload['status'] = status
        if message is not None:
            payload['message'] = message
        if impact:
            assert impact in ('none', 'minor', 'major')
            payload['impact_override'] = impact
        if not payload:
            return

        if self.__dryrun:
            logger.info("Dryrun - not writing update to statuspage.io...")
            logger.info("Would update incident %s: %s", incident_id, payload)
            return
        logger.debug("Updating incident %s: %s", incident_id, payload)

        self.__call(
            'https://api.statuspage.io/v1/pages/{}/incidents/{}.json'.format(self.__page_id, incident_id),
            method='patch',
            body=json.dumps({'incident': payload}),
        )

    def resolve_incident(self, incident_id):
        """ resolve remote incident
        """
        self.update_incident(incident_id, status='resolved', message='Our monitoring system has detected that this incident has been resolved.')
