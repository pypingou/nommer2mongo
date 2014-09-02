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
logging.basicConfig()
log.setLevel(logging.INFO)


def __insert_messages(dbmsg):
    """ Retrieves messages from datagrepper. """

    datagrepper_url = 'https://apps.fedoraproject.org/datagrepper/'
    rows_per_page = 100

    def _load_page(page):
        param = {
            'page': page,
            'rows_per_page': rows_per_page,
            'meta': ['usernames', 'packages'],
        }

        response = requests.get(datagrepper_url + 'raw/', params=param)
        return json.loads(response.text)

    cnt = 0
    failed = []

    # Make an initial query just to get the number of pages
    log.info('Requesting the first page to get the total number of pages')
    data = _load_page(page=1)
    pages = data['pages']

    for page in range(1, pages+1):
        log.info("Requesting page %i of %i from datagrepper" %
                 (page, pages))
        data = _load_page(page)
        for message in data['raw_messages']:
            try:
                dbmsg.insert(message)
            except pymongo.errors.DuplicateKeyError, err:
                print err
                failed.append((message['msg_id'], err.message))
            except bson.errors.InvalidDocument, err:
                print 'message: %s' % message['msg_id']
                print err
                failed.append((message['msg_id'], err.message))
            cnt += 1

    print '%s messages processed' % cnt
    print '%s messages failed' % len(failed)
    return failed


def main():

    log.info("Starting loading mongodb")

    session = pymongo.MongoClient('localhost', 27017)
    db = session.fedmsg
    dbmsg = db.messages
    dbmsg.ensure_index(
        [("msg_id", pymongo.ASCENDING)],
        unique=True, background=True)

    failed = __insert_messages(dbmsg)

    with open('failed_messages', 'w') as stream:
        for msgid in failed:
            stream.write(' '.join(msgid) + '\n')


if __name__ == '__main__':
    main()
