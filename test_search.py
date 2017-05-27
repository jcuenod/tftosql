import sqlite3
dbfile = "textfabric.sqlite"
db = sqlite3.connect(dbfile)

# search_data = {"query":[{"lex":"ZNH[","vs":"qal"},{"lex":"<L"}],"search_range":"clause"}
# search_data = {"query":[{"lex":"XZQ[","vs":"qal"},{"lex":"B"}],"search_range":"sentence"}
# search_data = {"query":[{"lex":"R>H[","vs":"qal"},{"gn":"f"}],"search_range":"clause"}
# search_data = {"query":[{"lex":"R>H[","vs":"qal"},{"gn":"f"},{"lex":"H"}],"search_range":"clause"}
# search_data = {"query":[{"gn":"f"},{"lex":"H"},{"vs":"qal","voc_utf8":"חזק"}],"search_range":"clause"}
search_data = {"query":[{"tricons":"משׁה"},{"lex":">MR["},{"voc_utf8":"אֶל","invert":"t"}],"search_range":"clause"}

query = search_data["query"]
search_range = search_data["search_range"]

word_groups_to_exclude = []
word_group_with_match = [[] for i in range(len(query))]
found_words = []

results = []
results_to_exclude = []
for q in query:
	invert = False
	if "invert" in q:
		invert = q["invert"] == "t"
		del q["invert"]
	query_keys = [ k for k in q ]
	requirementArray = list(map(lambda k: '`{}`="{}"'.format(k, q[k]), query_keys))
	print(requirementArray)
	sql_query = "SELECT `" + search_range + "` FROM `TreeData`, `WordData` WHERE `WordData`.`wid` = `TreeData`.`wid` AND " + " AND ".join(requirementArray)
	print("QUERY:",sql_query)
	db_cursor = db.execute(sql_query)
	temp_results = list(map(lambda x: x[0], db_cursor.fetchall()))
	if invert:
		results_to_exclude += temp_results
	else:
		results.append(temp_results)
	print("RESULTS:", len(temp_results))

# intersected_set = list(set.intersection(*map(set, results)))
intersection_to_filter = set(results[0]).intersection(*results[1:])
intersected_set = list(filter(lambda x: x not in results_to_exclude, intersection_to_filter))
# print(r)
print("\nMATCHES:", len(intersected_set))


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
	chapter_number = int(((rid - verse_number) % 1000000) / 1000)
	book_index = int((rid - chapter_number - verse_number) / 1000000)
	return (keyFromValue(book_to_index, book_index), chapter_number, verse_number)

sql_query = "SELECT DISTINCT `rid`, `{}` FROM `TreeData` WHERE `{}` IN ({})".format(search_range, search_range, ",".join(map(str, intersected_set)))
db_cursor = db.execute(sql_query)
rid_and_range_matches = db_cursor.fetchall()

def wordsForOtypeIn(otype, nodes):
	sql_query = "SELECT `rid`, `g_word_utf8`, `trailer_utf8` FROM `WordData`, `TreeData` WHERE `TreeData`.`wid` = `WordData`.`wid` AND `TreeData`.`{range}` IN ({range_matches}) ORDER BY `WordData`.`wid`".format(range=otype, range_matches=",".join(map(str, nodes)))
	print(sql_query)
	db_cursor = db.execute(sql_query)
	return db_cursor.fetchall()

currentRid = 0
if True: # Print results
	for row in wordsForOtypeIn(search_range, map(lambda x: x[1], rid_and_range_matches)):
		if currentRid != row[0]:
			currentRid = row[0]
			print("\n\n", indexToPassage(currentRid))
		if row[1]:
			print(row[1], end="")
		if row[2]:
			print(row[2], end="")

print("\ndone")

# for n in {all_words}:
# 	inverted_search_done = False
# 	regular_search_done = False
# 	for q_index, q in enumerate(query):
# 		query_inverted = "invert" in q
# 		if (inverted_search_done and query_inverted) or (regular_search_done and not query_inverted):
# 			continue
# 		if test_node_with_query(n, q):
# 			search_range_node = L.u(n, otype=search_range)[0]
# 			word_group_with_match[q_index].append(search_range_node)
# 			found_words.append({
# 				"search_range_node": search_range_node,
# 				"word_node": n
# 			})
# 			if query_inverted:
# 				inverted_search_done = True
# 			else:
# 				regular_search_done = True
# 			if regular_search_done and inverted_search_done:
# 				break

# words_groups_to_intersect = []
# words_groups_to_filter = []
# for q_index, q in enumerate(query):
# 	if "invert" in q and q["invert"] == "t":
# 		words_groups_to_filter += word_group_with_match[q_index]
# 	else:
# 		words_groups_to_intersect.append(word_group_with_match[q_index])


# intersection_to_filter = list(set.intersection(*map(set, words_groups_to_intersect)))
# intersection = list(filter(lambda x: x not in words_groups_to_filter, intersection_to_filter))
# print (str(len(intersection)) + " results")

# # Truncate array if too long
# truncated = False
# if len(intersection) > 1000:
# 	intersection = intersection[:500]
# 	print ("Abbreviating to just 500 elements")
# 	truncated = True


# retval = []
# for r in intersection:
# 	found_word_nodes = list(map(lambda x : x["word_node"], filter(lambda x : x["search_range_node"] == r, found_words)))
# 	clause_text = get_highlighted_words_nodes_of_verse_range_from_node(r, found_word_nodes)
# 	retval.append({
# 		"passage": passage_abbreviation(r),
# 		"node": r,
# 		"clause": clause_text,
# 	})

# retval_sorted = sorted(retval, key=lambda r: sortKey(r["node"]))

# response.content_type = 'application/json'
# return json.dumps({
# 	"truncated": truncated,
# 	"search_results": retval_sorted
# })