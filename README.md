# tftosql

Convert text-fabric-data to sqlite for rapid querying. You will need `text-fabric` and `text-fabric-data`. You can use `pip install` to get the former, the latter can be cloned from <https://github.com/ETCBC/text-fabric-data> (make sure that it shares a parent directory with `tftosql`).

To generate the actual sqlite3 file:

    python3 main.py

To make api_search.py work, you need the index (which adds a lot of bulk to the file)

    python3 generate_range_wid_index.py


If all has gone well, you should now be able to run searches on the data that generate results remarkably quickly:

    python3 api_search.py

Using the search term that finds sentences with ראה

    search_data = {
	    "query":[{"lex":"R>H["}],
	    "search_range":"sentence",
    }

The module returns 1280 results in fraction of a second:

    t=0
    RESULTS: 1280
    time to complete: 0.21850655297748744
