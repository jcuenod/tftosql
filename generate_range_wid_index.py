import sqlite3

output_filename = "textfabric.sqlite"
db = sqlite3.connect(output_filename)
db.row_factory = sqlite3.Row
db_cursor = db.cursor()

ranges = ["phrase", "clause", "sentence"]
# ranges = ["sentence"]

# nodes = []

sqlCreateTable = """
DROP TABLE IF EXISTS `RangeNodeIndex`;
CREATE TABLE `RangeNodeIndex` (
  `range_node` INT NOT NULL,
  `wid` INT NOT NULL,
  PRIMARY KEY (`range_node`, `wid`)
);
"""
db.executescript(sqlCreateTable)



for range_name in ranges:
	print("Busy with", range_name)
# 	sql_insert = """
# 	INSERT INTO `RangeNodeIndex` (`range_node`, `wid`)
# 	SELECT `RangeData`.`range_node`, `TreeData`.`wid`
# 		FROM `TreeData`, (
# 				SELECT `{range}` AS `range_node`, min(`rid`) AS `min_rid`, max(`rid`) AS `max_rid`
# 					FROM `TreeData`
# 					GROUP BY `{range}`
# 			) AS `RangeData`
# 		WHERE   `TreeData`.`rid` >= `RangeData`.`min_rid`
# 			AND `TreeData`.`rid` <= `RangeData`.`max_rid`
# 	""".format(range=range_name)
# 	db.executescript(sql_insert)



# db.close()

## THIRD ATTEMPT

	sql_query = "SELECT DISTINCT `rid`, `{range}` as `range_node` FROM `TreeData`".format(range=range_name)
	db_cursor = db.execute(sql_query)
	distinct_rid_range = db_cursor.fetchall()
	range_node_data = {}
	for row in distinct_rid_range:
		if row['range_node'] not in range_node_data:
			range_node_data[row['range_node']] = {
				"rids": [row['rid']]
			}
		else:
			range_node_data[row['range_node']]["rids"].append(row['rid'])

	rid_to_wid = "SELECT `wid`, `rid` FROM `TreeData`"
	db_cursor = db.execute(rid_to_wid)
	rid_wid = db_cursor.fetchall()
	print("  - now building set of executemanys")
	# rid_wid_map = [(row["range_node"], row["wid"]) for row in rid_wid]

	from functools import reduce
	def build_rid_to_wid_list(carry, row):
		if row["rid"] not in carry:
			carry[row["rid"]] = [row["wid"]]
		else:
			carry[row["rid"]].append(row["wid"])
		return carry
	rid_to_wid_list = reduce(build_rid_to_wid_list, rid_wid,{})


	insertion_list = []
	for i, key in enumerate(range_node_data.keys()):
		v = range_node_data[key]
		minV = min(v["rids"])
		maxV = max(v["rids"])
		# filtered_rid_wids = filter(lambda x: x["rid"] >= minV and x["rid"] <= maxV, rid_wid)
		# insertion_list += map(lambda x: {"wid": x["wid"], "range_node": key}, filtered_rid_wids)
		for rid in range(minV, maxV + 1):
			insertion_list += map(lambda x: (key, x), rid_to_wid_list[rid])
	sql_query = "INSERT INTO `RangeNodeIndex` (`range_node`, `wid`) VALUES (?, ?)"
	print("executing", len(insertion_list))
	db.executemany(sql_query, insertion_list)
	print("  - now committing")
	db.commit()


db.close()

## OLD CODE BELOW
# (new method above does not give output and is long running but plain sql... I don't know)





# for range_name in ranges:
# 	db_cursor.execute(sql_query)
# 	result = db_cursor.fetchall()

# 	nodes += result

# ## Create table
# def dictToColumnData(d, primary_key, nullable):
# 	def extra(key, value):
# 		if type(value) is int:
# 			typeinfo = "INT"
# 		else:
# 			typeinfo = "TEXT"
# 		if not nullable:
# 			typeinfo = typeinfo + " NOT NULL"
# 		typeinfo = typeinfo + " PRIMARY KEY" if key == primary_key else typeinfo
# 		return typeinfo
# 	return map(lambda k: k + " " + extra(k, d[k]), d.keys())

# def createTable(table_name, sample_row, primary_key, nullable=True):
# 	print("Creating table:", table_name)
# 	sqlCreateTable = '''
# DROP TABLE IF EXISTS {0};
# CREATE TABLE {0} (
#   {1}
# );
# '''.format(table_name, ",\n  ".join(dictToColumnData(sample_row, primary_key, nullable)))
# 	print(sqlCreateTable)
# 	db.executescript(sqlCreateTable)
# createTable("RangeNodeIndex", nodes[0], 'range_node', False)



# print("Writing to sqlite:", output_filename)

# def insertData(table, rows):
# 	data = {
# 		"table_name": table,
# 		"column_names": ",".join( map(lambda k: "`" + k + "`", rows[0].keys()) ),
# 		"question_marks": ",".join(["?"] * len(rows[0].keys()))
# 	}
# 	sql_query = "INSERT INTO `{table_name}` ({column_names}) VALUES ({question_marks})".format(**data)
# 	print(" - writing ", table)
# 	db.executemany(sql_query, [tuple(d) for d in rows])
# 	db.commit()

# insertData("RangeNodeIndex", nodes)


# db.close()