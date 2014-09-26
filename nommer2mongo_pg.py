#-*- coding: utf-8 -*-

"""
 (c) 2014 - Copyright Red Hat Inc

 Authors:
   Ralph Bean <rbean@redhat.com>
   Pierre-Yves Chibon <pingou@pingoured.fr>

This script queries every single message stored in postgresql and load them
into MongoDB.

"""

import argparse
import datetime
import logging
import json
import uuid

import bson.errors
import pymongo
import pymongo.errors

import fedmsg
import fedmsg.meta

import datanommer.models

from math import ceil

from sqlalchemy import create_engine
from sqlalchemy.orm import (
    sessionmaker,
    scoped_session,
)

config = fedmsg.config.load_config()
fedmsg.meta.make_processors(**config)

log = logging.getLogger("nommer2mongo_pg")
logging.basicConfig()
log.setLevel(logging.INFO)

URL = 'postgres://user:pass@localhost/db_name'
LIMIT = 1000

SESSION = scoped_session(sessionmaker())

def __insert_messages(dbmsg):
    """ Retrieves messages from datagrepper. """

    import datanommer.models
    engine = create_engine(URL)

    SESSION.configure(bind=engine)

    query = SESSION.query(
        datanommer.models.Message
    ).order_by(
        datanommer.models.Message.timestamp.asc()
    )

    total = query.count()
    total_page = int(ceil(total / float(LIMIT)))

    query = query.limit(LIMIT)

    log.info("%s messages to process", total)

    cnt = 0
    failed = []
    for page in range(total_page + 1):
        log.info('page %s/%s', page, total_page +1)
        for msg in query.offset(page * LIMIT).all():
            message = msg.__json__()
            message['users'] = list(
                fedmsg.meta.msg2usernames(message))
            message['packages'] = list(
                fedmsg.meta.msg2packages(message))
            if 'meta' in message:
                del(message['meta'])

            if not message['msg_id']:
                date = datetime.datetime.utcfromtimestamp(message['timestamp'])
                message['msg_id'] = str(date.year) + '-' + str(uuid.uuid4())

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
    db = session.fedmsg2
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
