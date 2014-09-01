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

import bson.errors
import pymongo
import pymongo.errors


log = logging.getLogger("nommer2mongo")


def __get_messages(datagrepper_url, msg_id=None):
    """ Retrieves messages from datagrepper. """

    rows_per_page = 100

    def _load_page(page):
        param = {
            'order': 'desc',
            'page': page,
            'rows_per_page': rows_per_page,
            'meta': ['usernames', 'packages'],
        }

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

    log.info("Starting loading mongodb")

    session = pymongo.MongoClient('localhost', 27017)
    db = session.fedmsg
    dbmsg = db.messages
    dbmsg.ensure_index(
        [("msg_id", pymongo.ASCENDING)],
        unique=True, background=True)

    datagrepper_url = 'https://apps.fedoraproject.org/datagrepper/'
    cnt = 0
    messages = __get_messages(datagrepper_url, opts.msg_id)
    failed = []
    for message in messages:

        try:
            dbmsg.insert(message)
        except pymongo.errors.DuplicateKeyError, err:
            print err
            failed.append((message['msg_id'], err.message))
        except bson.errors.InvalidDocument, err:
            print err
            print 'message: %s' % message['msg_id']
            failed.append((message['msg_id'], err.message))

        if (cnt % 100) == 0
            dmsg.save()

        cnt += 1
    print '%s messages processed' % cnt
    print '%s messages failed' % len(failed)

    with open('failed_messages', 'w') as stream:
        for msgid in failed:
            stream.write(' '.join(msgid) + '\n')


if __name__ == '__main__':
    main()
