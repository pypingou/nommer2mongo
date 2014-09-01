#-*- coding: utf-8 -*-

"""
 (c) 2014 - Copyright Red Hat Inc

 Authors:
   Ralph Bean <rbean@redhat.com>
   Pierre-Yves Chibon <pingou@pingoured.fr>

This script queries every single message stored in datagrepper and load them
into MongoDB.

"""

import argparse
import logging
import requests
import json

import fedmsg.config
import pymongo

try:
    # Python2.7 and later
    from logging.config import dictConfig
except ImportError:
    # For Python2.6, we rely on a third party module.
    from logutils.dictconfig import dictConfig


log = logging.getLogger("nommer2mongo")


def __get_messages(datagrepper_url, msg_id=None):
    """ Retrieves messages from datagrepper. """

    rows_per_page = 20

    def _load_page(page):
        param = {
            'order': 'desc',
            'page': page,
            'rows_per_page': rows_per_page,
            'meta': ['usernames', 'packages'],
        }

        print 'querying %s' % datagrepper_url + 'raw/'
        response = requests.get(datagrepper_url + 'raw/', params=param)
        return json.loads(response.text)

    if msg_id:
        param = {
            'id': msg_id,
        }

        response = requests.get(datagrepper_url + 'id/', params=param)
        data = json.loads(response.text)

        yield data

    else:
        # Make an initial query just to get the number of pages
        data = _load_page(page=1)
        pages = data['pages']

        for page in range(1, pages+1):
            print '%s / %s' % ( page, pages + 1)
            log.info("Requesting page %i of %i from datagrepper" %
                     (page, pages))
            data = _load_page(page)
            for message in data['raw_messages']:
                yield message
            break


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", dest="msg_id", default=None,
                      help="Process the specified message")
    parser.add_argument("--force", dest="force", default=False,
                      action="store_true",
                      help="Force processing the sources even if the database"
                           "already knows it")

    return parser.parse_args()


def main():
    opts = parse_args()

    config = fedmsg.config.load_config()
    config.update({
        'name': 'relay_inbound',
        'active': True,
    })

    dictConfig(config.get('logging', {'version': 1}))
    log.info("Starting loading mongodb")

    fedmsg.init(**config)

    session = pymongo.MongoClient('localhost', 27017)

    datagrepper_url = 'https://apps.fedoraproject.org/datagrepper/'
    messages = __get_messages(datagrepper_url, opts.msg_id)
    for message in messages:
        db.insert(message)

if __name__ == '__main__':
    main()
