# -*- coding: utf-8 -*-

"""pytest configuration"""


import pytest
from tile_processor import db

#-------------------------------------------------------------------- testing DB
@pytest.fixture(scope="session")
def bag3d_db(request):
    dbs = db.DB(dbname='bag3d_db', host='localhost', port=5590,
                user='bag3d_tester', password='bag3d_test')
    yield dbs
    dbs.close()
