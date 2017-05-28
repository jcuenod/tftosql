import sqlite3

output_filename = "textfabric.sqlite"
db = sqlite3.connect(output_filename)
db.row_factory = sqlite3.Row
db_cursor = db.cursor()

ranges = ["phrase", "clause", "sentence"]

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

	print(" | building rid/range data")
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

	print(" | mapping wids to range_nodes")
	rid_to_wid = "SELECT `wid`, `rid` FROM `TreeData`"
	db_cursor = db.execute(rid_to_wid)
	rid_wid = db_cursor.fetchall()
	# rid_wid_map = [(row["range_node"], row["wid"]) for row in rid_wid]

	from functools import reduce
	def build_rid_to_wid_list(carry, row):
		if row["rid"] not in carry:
			carry[row["rid"]] = [row["wid"]]
		else:
			carry[row["rid"]].append(row["wid"])
		return carry
	rid_to_wid_list = reduce(build_rid_to_wid_list, rid_wid,{})

	print(" | building insertion list")
	insertion_list = []
	for i, key in enumerate(range_node_data.keys()):
		v = range_node_data[key]
		minV = min(v["rids"])
		maxV = max(v["rids"])
		for rid in range(minV, maxV + 1):
			insertion_list += map(lambda x: (key, x), rid_to_wid_list[rid])
	print(" | executing insertion.", len(insertion_list), "items")
	sql_query = "INSERT INTO `RangeNodeIndex` (`range_node`, `wid`) VALUES (?, ?)"
	db.executemany(sql_query, insertion_list)
	print(" | doing commit")
	db.commit()
	print(" - done\n")

db.close()
