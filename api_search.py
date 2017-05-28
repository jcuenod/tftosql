import sqlite3

dbfile = "textfabric.sqlite"
db = sqlite3.connect(dbfile)
db.row_factory = sqlite3.Row

search_data = {
	"query":[{"lex":"R>H["}],
	"search_range":"sentence",
}

### CORE FUNCTIONS
book_to_index = {
	"Genesis": 1,
	"Exodus": 2,
	"Leviticus": 3,
	"Numbers": 4,
	"Deuteronomy": 5,
	"Joshua": 6,
	"Judges": 7,
	"Ruth": 8,
	"1_Samuel": 9,
	"2_Samuel": 10,
	"1_Kings": 11,
	"2_Kings": 12,
	"1_Chronicles": 13,
	"2_Chronicles": 14,
	"Ezra": 15,
	"Nehemiah": 16,
	"Esther": 17,
	"Job": 18,
	"Psalms": 19,
	"Proverbs": 20,
	"Ecclesiastes": 21,
	"Song_of_songs": 22,
	"Isaiah": 23,
	"Jeremiah": 24,
	"Lamentations": 25,
	"Ezekiel": 26,
	"Daniel": 27,
	"Hosea": 28,
	"Joel": 29,
	"Amos": 30,
	"Obadiah": 31,
	"Jonah": 32,
	"Micah": 33,
	"Nahum": 34,
	"Habakkuk": 35,
	"Zephaniah": 36,
	"Haggai": 37,
	"Zechariah": 38,
	"Malachi": 39,
}
def keyFromValue(dictionary, value):
	return list(dictionary.keys())[list(dictionary.values()).index(value)]
def indexToPassage(rid):
	verse_number = rid % 1000
	chapter_number = int(((rid - verse_number) % 10000000) / 1000)
	book_index = int((rid - chapter_number - verse_number) / 10000000)
	return (keyFromValue(book_to_index, book_index), chapter_number, verse_number)



# ACTUAL API
def api_search():
	query = search_data["query"]
	search_range = search_data["search_range"]
	search_filter = None if "search_filter" not in search_data else search_data["search_filter"]

	results = []
	results_to_exclude = []
	for q in query:
		invert = False
		if "invert" in q:
			invert = q["invert"] == "t"
			del q["invert"]
		requirementArray = list(map(lambda k: '`{}`="{}"'.format(k, q[k]), [ k for k in q ]))
		filter_requirement = " AND `TreeData`.`book` IN ('" + "','".join(map(lambda s: str(book_to_index[s]), search_filter)) + "')" if search_filter is not None else ""
		sql_search_query = "SELECT `" + search_range + "` FROM `TreeData`, `WordData` WHERE `WordData`.`wid` = `TreeData`.`wid` AND " + " AND ".join(requirementArray) + filter_requirement
		db_cursor = db.execute(sql_search_query)
		temp_results = list(map(lambda x: x[search_range], db_cursor.fetchall()))
		if invert:
			results_to_exclude += temp_results
		else:
			results.append(temp_results)

	intersection_to_filter = set(results[0]).intersection(*results[1:])
	intersected_set = list(filter(lambda x: x not in results_to_exclude, intersection_to_filter))
	print("RESULTS:", len(intersected_set))

	sql_get_matched_words = "SELECT `g_word_utf8`, `trailer_utf8`, `range_node`, `rid` FROM `WordData`, `TreeData` INNER JOIN `RangeNodeIndex` ON `WordData`.`wid` == `RangeNodeIndex`.`wid` AND `RangeNodeIndex`.`range_node` IN ({range_nodes}) WHERE `WordData`.`wid`=`TreeData`.`wid` ORDER BY `RangeNodeIndex`.`range_node`".format(range_nodes=",".join(map(str, intersected_set)))
	db_cursor = db.execute(sql_get_matched_words)
	rows = db_cursor.fetchall()
	rid_to_index = {}
	verse_range_per_range_match = []
	for row in rows:
		if row['range_node'] not in rid_to_index:
			rid_to_index[row['range_node']] = len(verse_range_per_range_match)
			verse_range_per_range_match.append({
					"node_matched":	row["range_node"],
					"rids":			[row["rid"]],
					"verse_text":	""
				})
		if row["rid"] not in verse_range_per_range_match[ rid_to_index[row['range_node']] ]["rids"]:
			verse_range_per_range_match[ rid_to_index[row['range_node']] ]["rids"].append(row["rid"])
		if row["g_word_utf8"]:
			verse_range_per_range_match[ rid_to_index[row['range_node']] ]['verse_text'] += row["g_word_utf8"]
		if row["trailer_utf8"]:
			verse_range_per_range_match[ rid_to_index[row['range_node']] ]['verse_text'] += row["trailer_utf8"]

	for i, vrprm in enumerate(verse_range_per_range_match):
		minV = min(vrprm["rids"])
		maxV = max(vrprm["rids"])
		reference = "{} {}:{}".format(*indexToPassage(minV))
		if minV != maxV:
			reference += "-" + str(indexToPassage(maxV)[2])
		verse_range_per_range_match[i]["order_key"] = minV
		verse_range_per_range_match[i]["reference"] = reference

	if True:
		for v in sorted(verse_range_per_range_match, key=lambda k: k['order_key']) :
			print(v["reference"],"\n",v["verse_text"],"\n")


import timeit

start_time = timeit.default_timer()
print("t=0")
api_search()
print("time to complete:",timeit.default_timer()-start_time)
