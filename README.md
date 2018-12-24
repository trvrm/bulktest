# bulktest


Demo of bulk Postgres insertions using JSON unrolling.
    
    pipenv run python test.py slow
    slow: 8.231123685836792 second(s)
    
    pipenv run python test.py fast
    fast: 0.07080864906311035 second(s)

100x speedup when inserting 10,000 rows
