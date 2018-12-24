'''
Demo of bulk Postgres insertions using JSON unrolling.
    
    pipenv run python test.py slow
    slow: 8.231123685836792 second(s)
    
    pipenv run python test.py fast
    fast: 0.07080864906311035 second(s)

100x speedup when inserting 10,000 rows
'''

import sqlalchemy
import sys
import contextlib
import time
import names
import random
from psycopg2.extras import Json


@contextlib.contextmanager
def timer(name="duration"):
    'Utility function for timing execution'
    start=time.time()
    yield
    duration=time.time()-start
    print("{0}: {1} second(s)".format(name,duration))
    
    
ROWCOUNT=10000

# assumes existence of locally hosted postgres database called 'bulktest', with owner 'bulktest', password 'password'
engine=sqlalchemy.create_engine("postgresql+psycopg2://bulktest:password@localhost/bulktest")

def setup():
    sql='''
        create table test(id int primary key, firstname text, lastname text, age int);
    '''
    engine.execute(sql)
    
    
def sample_data():
    return [
        dict(
            id=id,
            firstname=names.get_first_name(),
            lastname=names.get_last_name(),
            age=random.randint(1,100)
        )
        for id in range(ROWCOUNT)
    ]
def reset():
    engine.execute('delete from test')
def slow():
    reset()
    rows=sample_data()
    
    INSERT = """
        INSERT INTO test (id, firstname,lastname,age) 
             VALUES (%(id)s, %(firstname)s, %(lastname)s, %(age)s)
    """
    with timer('slow'):
        with engine.connect() as connection:
            for row in rows:
                connection.execute(INSERT,row)
            
 
def fast():
    reset()
    rows=sample_data()
    INSERT = """
        INSERT INTO test (id, firstname,lastname,age) 
            SELECT 
                (el->>'id')::int,
                el->>'firstname',
                el->>'lastname',
                (el->>'age')::int
              FROM (
                    SELECT jsonb_array_elements(%(data)s) el
              ) a;
    """
    with timer('fast'):
        with engine.connect() as connection:
            connection.execute(INSERT,data=Json(rows))
        
if __name__=='__main__':
    
    if(sys.argv[1]=='slow'):
        slow()
    if(sys.argv[1]=='fast'):
        fast()
    if(sys.argv[1]=='setup'):
        setup()
    