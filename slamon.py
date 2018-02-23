#!/usr/bin/env python3
""" Monitoring timeliness of products published to thredds.met.no.

    Copyright (C) 2018 MET Norway, https://met.no

    This program is free software; you can redistribute it and/or
    modify it under the terms of the GNU General Public License
    as published by the Free Software Foundation; either version 2
    of the License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program; if not, write to the Free Software
    Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
"""
# pylint: disable=C0301, E0611

__author__ = "Christian Skarby"
__version__ = "0.2.0"
__license__ = "GPLv2+"

import datetime
import threading

from logzero import logger, loglevel

from slamon.thredds import ApplicationNode
from slamon.modelrun import MEPSdet, MEPSdetpp, MEPSens, AAdet, AAdetpp
from slamon.statuspage import StatusPage


def fetch_nodes(nodes):
    """ Update node objects with model run lists from thredds
    """
    threads = [threading.Thread(target=node.fetch) for node in nodes]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

def _bulletin(dct, key):
    """ helper method to extract .bulletin or return None
    """
    obj = dct.get(key, None)
    if obj:
        return obj.bulletin
    return None

def extract_most_recent_bulletin(models, nodes):
    """ Get the most recent bulletin that all nodes can agree on for each model
    """
    logger.debug('Deciding on most recent bulletin that all nodes can agree on for each model')
    most_recent_bulletin = {}
    for model in models:
        for node in nodes:
            kept_bulletin = _bulletin(most_recent_bulletin, model)
            candidate_bulletin = _bulletin(node.result, model)
            if candidate_bulletin:
                # as we have collected the most recent bulletin for each model from each node
                # what is left is to keep the least recent bulletin for each model
                # this is worst case of what a visitor sees (the most recent model run published on all nodes)
                if kept_bulletin is None or candidate_bulletin < kept_bulletin:
                    most_recent_bulletin[model] = node.result[model]  # store obj, not just datetime
        logger.debug('Most recent bulletin for %s is %s', model.NAME, _bulletin(most_recent_bulletin, model))
    return most_recent_bulletin

def resolve(statuspage, model, component_status, open_incident):
    """ Ensure that statuspage has healty component status and that any open
        incident is resolved.
    """
    if not statuspage.status(model.STATUSPAGE_ID) == component_status:
        statuspage.status(model.STATUSPAGE_ID, component_status)
    if open_incident:
        statuspage.resolve_incident(open_incident.id)

def main(args):
    """ Main entry point
    """
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    models = (MEPSdet, MEPSdetpp, MEPSens, AAdet, AAdetpp)

    nodes = []
    for node in args.config.get('nodes', []):
        nodes.append(ApplicationNode(models=models, **node))

    fetch_nodes(nodes)
    most_recent_bulletin = extract_most_recent_bulletin(models, nodes)

    statuspage = StatusPage(dryrun=args.dryrun, **args.config.get('statuspage', []))
    for model in most_recent_bulletin:
        title = '{} production'.format(model.NAME)
        required_model = model.required(now)

        open_incident = False
        for incident in statuspage.get_incidents_for_component(model.STATUSPAGE_ID):
            if incident.title == title:
                open_incident = incident
                logger.debug("Found open indicent %s for %s", incident.id, model.NAME)
                break

        component_status = 'operational'
        if most_recent_bulletin[model].bulletin >= required_model.bulletin:
            resolve(statuspage, model, component_status, open_incident)
        else:
            impact = 'none'
            message = "{} results from model run based on analysis of {} is not yet published. Normally we would expect it by now. Please use earlier forecast.".format(model.NAME, model.bulletin.strftime('%Y-%m-%d %H %Z'))
            if required_model.is_delayed(now) or required_model.prev().bulletin > most_recent_bulletin[model].bulletin:
                component_status = 'degraded_performance'
                impact = 'minor'
                message = "{} results from model run based on analysis of {} is not yet published. This is a significant delay and we are sorry about any inconvenience. Please use earlier forecast.".format(model.NAME, model.bulletin.strftime('%Y-%m-%d %H %Z'))

            if open_incident:
                if not statuspage.status(model.STATUSPAGE_ID) == component_status:
                    statuspage.status(model.STATUSPAGE_ID, component_status)
                    statuspage.update_incident(open_incident.id, message, impact=impact)
            else:
                statuspage.status(model.STATUSPAGE_ID, component_status)
                statuspage.create_incident(model.STATUSPAGE_ID, title, message, impact=impact)

if __name__ == "__main__":
    import argparse
    import json
    import logging

    PARSER = argparse.ArgumentParser()
    PARSER.add_argument("-n", "--dry-run", action="store_true", default=False, dest="dryrun")
    PARSER.add_argument("-v", "--verbose", action="count", default=0, help="Verbosity (-v, -vv, etc)")
    PARSER.add_argument("--version", action="version", version="%(prog)s (version {version})".format(version=__version__))
    PARSER.add_argument("config_file", help="JSON Configuration file")

    ARGS = PARSER.parse_args()
    if ARGS.verbose == 0:
        loglevel(logging.INFO)
    else:
        loglevel(logging.DEBUG)

    ARGS.config = json.load(open(ARGS.config_file))

    main(ARGS)
