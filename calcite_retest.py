import tempfile
import sys
import subprocess
import csv
import os
from config import *
calcite_config_dict = CALCITE_CONFIG

# test_sql_list = [
# # "SELECT name FROM Drinker WHERE NOT EXISTS (SELECT bar FROM Serves s, Likes WHERE drinker = name AND s.beer = Likes.beer AND NOT EXISTS (SELECT bar FROM Frequents WHERE drinker = name AND s.bar=bar));",
#  # "SELECT l.beer, s.bar FROM Likes l, Serves s WHERE l.drinker = 'Eve' AND l.beer = s.beer AND EXISTS(SELECT * FROM Serves WHERE beer = s.beer AND price < s.price);",
# "SELECT * FROM Serves s WHERE (SELECT COUNT(DISTINCT price) FROM Serves WHERE price < s.price) < 2;",
# # "(select bar from frequents) union all (select bar from serves)",
# "WITH rat0(a0) as (SELECT drinker from Frequents), rat1(a0, a1) as (SELECT rat0.a0, Likes.beer FROM rat0, Likes WHERE a0=Likes.Drinker) SELECT * FROM rat1;"
# # "SELECT drinker FROM frequents f join serves s on f.bar=s.bar;",
# # "SELECT l.beer, s.bar FROM Likes l, Serves s WHERE l.drinker = 'Eve' AND l.beer = s.beer AND s.price=(SELECT max(price) FROM Serves WHERE beer = s.beer)",
# # "SELECT name FROM drinker;",
# # "SELECT name FROM Drinker WHERE NOT EXISTS (SELECT s.bar FROM Serves s, Frequents f WHERE f.bar=s.bar AND name=f.drinker);",
# # "SELECT name FROM drinker WHERE NOT (name  LIKE 'Eve%');"
# ]

def getFile():
    data = []
    filePath = './Data'
    for root, dirs, files in os.walk(filePath, topdown=True):
        for name in files:
            path = os.path.join(root, name)
            if path.endswith((".sql", ".txt")):
                f = open(path, "r")
                content = f.read()
                parts = path.split('/')
                dirs = "/".join(parts[ : len(parts) - 2])
                fileName = parts[-1]
                data.append([path, dirs, fileName])
    return data

data = getFile()


for d in data:
    # Get the query
    readPath, savePath, fileName = d
    f = open(readPath, "r")
    sql = f.read()
        
    infile = tempfile.NamedTemporaryFile()
    # infile = open('test.txt', 'w')
    # print(sql)
    # print(sql.encode())
    infile.write(sql.encode())
    # infile.write(sql)
    infile.seek(0)
    # try:
    sp_output = subprocess.check_output(
        ['java', '-jar', calcite_config_dict['path'],
        '-a',
        '--db={}'.format(calcite_config_dict['db']),
        '--user={}'.format(calcite_config_dict['user']),
        '--pass={}'.format(calcite_config_dict['pass']),
        infile.name
        ],
        stderr=subprocess.STDOUT
    )
    if not os.path.exists(savePath):
        os.makedirs(savePath)
    with open(savePath + "/calcite_result_new.csv", 'a') as myfile:
                wr = csv.writer(myfile)
                wr.writerow([sql, sp_output.decode("utf-8")])
    infile.close()

    # print(sp_output)

