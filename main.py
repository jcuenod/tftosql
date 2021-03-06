import sqlite3
from tf.fabric import Fabric

### NB Todo: don't assume that mapping dict to keys/values will produce the same order (assumption runs throughout)

TF = Fabric(locations='../text-fabric-data', modules='hebrew/etcbc4c')
api = TF.load('''
	book chapter verse
	sp nu gn ps vt vs st
	otype
	det
	g_word_utf8 trailer_utf8
	lex_utf8 lex voc_utf8
	g_prs_utf8 g_uvf_utf8
	prs_gn prs_nu prs_ps g_cons_utf8
	gloss sdbh lxxlexeme
	accent accent_quality
	tab typ
''')
api.makeAvailableIn(globals())

output_filename = "textfabric.sqlite"


### HELPERS ###

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
def passageToIndex(passage):
	if passage[0] not in book_to_index:
		print(passage)
		print(passage[0])
		raise IndexError('Try using the right kind of book names bro')
	return book_to_index[passage[0]] * 10000000 + int(passage[1]) * 1000 + int(passage[2])


### WORD DATA ###

def nullifyNaAndEmptyAndUnknown(list_to_reduce):
	templist = list_to_reduce
	keys_to_remove = set()
	for key, value in templist.items():
		if value == "NA" or value == "" or value == "unknown":
			keys_to_remove.add(key)
	for key in keys_to_remove:
		templist[key] = None
	return templist

def wordData(n):
	r = {
		"wid": n,
		"g_word_utf8": F.g_word_utf8.v(n),
		"trailer_utf8": F.trailer_utf8.v(n),
		"tricons": F.lex_utf8.v(n).replace('=', '').replace('/','').replace('[',''),
		"sdbh": F.sdbh.v(n),
		"lex": F.lex.v(n),
		"voc_utf8": F.voc_utf8.v(L.u(n, otype='lex')[0]),
		"lxxlexeme": F.lxxlexeme.v(n),
		"sp": F.sp.v(n),
		"ps": F.ps.v(n),
		"nu": F.nu.v(n),
		"gn": F.gn.v(n),
		"vt": F.vt.v(n), # vt = verbal tense
		"vs": F.vs.v(n), # vs = verbal stem
		"st": F.st.v(n), # construct/absolute/emphatic
		"is_definite": F.det.v(L.u(n, otype='phrase_atom')[0]),
		"g_prs_utf8": F.g_prs_utf8.v(n),
		"g_uvf_utf8": F.g_uvf_utf8.v(n),
		"g_cons_utf8": F.g_cons_utf8.v(n),
		"prs_nu": F.prs_nu.v(n),
		"prs_gn": F.prs_gn.v(n),
		"prs_ps": F.prs_ps.v(n),
		"accent": F.accent.v(n),
		"accent_quality": F.accent_quality.v(n),
		"has_suffix": "Yes" if F.g_prs_utf8.v(n) != "" else "No",
		"gloss": F.gloss.v(L.u(n, otype='lex')[0]),
	}
	return nullifyNaAndEmptyAndUnknown(r);

def treeData(n):
	section = T.sectionFromNode(n)
	r = {
		"wid": n,
		"phrase_atom": L.u(n, otype="phrase_atom")[0],
		"phrase": L.u(n, otype="phrase")[0],
		"clause_atom": L.u(n, otype="clause_atom")[0],
		"clause": L.u(n, otype="clause")[0],
		"sentence": L.u(n, otype="sentence")[0],
		"rid": passageToIndex(section),
		"book": book_to_index[section[0]]
	}
	return r



### GATHER DATA ###

print("\nGathering node data:")

word_rows = []
tree_rows = []
for n in F.otype.s('word'):
	word_rows.append(wordData(n))
	tree_rows.append(treeData(n))
	if len(word_rows) % 50000 == 0:
		print(" |", len(word_rows), "rows processed")
	# if len(word_rows) > 10:
	# 	break

print(" |", len(word_rows), " rows processed")
print(" - DONE\n")



### EXPORT ###

db = sqlite3.connect(output_filename)

def dictToColumnData(d, primary_key, nullable):
	def extra(key, value):
		if type(value) is int:
			typeinfo = "INT"
		else:
			typeinfo = "TEXT"
		if not nullable:
			typeinfo = typeinfo + " NOT NULL"
		typeinfo = typeinfo + " PRIMARY KEY" if key == primary_key else typeinfo
		return typeinfo
	return map(lambda k: k + " " + extra(k, d[k]), d.keys())

def createTable(table_name, sample_row, primary_key, nullable=True):
	print("Creating table:", table_name)
	sqlCreateTable = '''
DROP TABLE IF EXISTS {0};
CREATE TABLE {0} (
  {1}
);
'''.format(table_name, ",\n  ".join(dictToColumnData(sample_row, primary_key, nullable)))
	print(sqlCreateTable)
	db.executescript(sqlCreateTable)
createTable("WordData", word_rows[0], 'wid')
createTable("TreeData", tree_rows[0], 'wid', False)


print("Writing to sqlite:", output_filename)

def insertData(table, rows):
	data = {
		"table_name": table,
		"column_names": ",".join( map(lambda k: "`" + k + "`", rows[0].keys()) ),
		"question_marks": ",".join(["?"] * len(rows[0].keys()))
	}
	sql_query = "INSERT INTO `{table_name}` ({column_names}) VALUES ({question_marks})".format(**data)
	print(" - writing ", table)
	db.executemany(sql_query, [tuple(d.values()) for d in rows])
	db.commit()

insertData("WordData", word_rows)
insertData("TreeData", tree_rows)


db.close()