import pyproj
from functools import partial
import time
import psycopg2
#script transforms stations coordinates(latitude, longtitude) to aeqd flat plane

def get_record(fname):
    with open(fname) as f:
        content = f.readlines()
        content = [x.strip() for x in content]
    return content

lined_reports = get_record("C:\\Users\\Radoslaw\\Desktop\\20\\pol_stations.txt")

print(lined_reports[0])

for i, line in enumerate(lined_reports):
    lined_reports[i]= line.split()


parsed_stations = list()
for line in lined_reports:
    wmoind = line[0]
    name = line[2]
    lat_deg = int(line[4][0:2])
    lat_min = float(line[4][3:5]) / 60
    lon_deg = int(line[5][0:3])
    lon_min = float(line[5][4:6]) / 60
    lat_dec = lat_deg + lat_min
    lon_dec = lon_deg + lon_min
    print(wmoind)
    print(name)
    print(lat_dec)
    print(lon_dec)
    record = list()
    record.append(wmoind)
    record.append(name)
    record.append(lat_dec)
    record.append(lon_dec)
	# transform the given lat-long onto the flat AEQD plane
    WGS84 = pyproj.Proj(init='epsg:4326')	
    AEQD = pyproj.Proj(proj='aeqd', lat_0=52.3469, lon_0=19.0926, x_0=lon_dec, y_0=lat_dec)
    tx_lon, tx_lat = pyproj.transform(WGS84, AEQD, lon_dec, lat_dec)
    record.append(tx_lon)
    record.append(tx_lat)
    print(tx_lon)
    print(tx_lat)
    parsed_stations.append(record)
    #input("test")
print("--------")


sql = """INSERT INTO "places" VALUES(%s) RETURNING  id;"""
conn = None
wmoind = 1234

conn = psycopg2.connect(host="127.0.0.1" , port="2223", database="postgres", user="postgres", password="postgres")
cur = conn.cursor()
statement = "INSERT INTO places (wmoind, name, lat, lon, aeqd_x, aeqd_y) VALUES (%s, %s, %s, %s, %s, %s)"
#data = ("1","2")
#cur.execute(statement, data)
for record in parsed_stations:
    cur.execute(statement,(record[0], record[1], record[2], record[3], record[4], record[5]))
conn.commit()
cur.close()

input("post_sql")


