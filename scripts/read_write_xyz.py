import psycopg2
import os, glob
from collections import namedtuple
#get places from DB, in each file from radars(a lot) look for places(coords have been prepared to hit these in 
#radars report files) - for each hit get data for dbz(if >0) and write to DB
def get_places():
    conn = psycopg2.connect(host="127.0.0.1" , port="2223", database="postgres", user="postgres", password="postgres")
    cur = conn.cursor()
    statement = "SELECT * FROM places"
    cur.execute(statement)
    rows = cur.fetchall()
    # 0 id
    # 1 geom
    # 2 wmoind
    # 3 name
    # 4 lat
    # 5 lon
    # 6 aeqd_x
    # 7 aeqd_y
    return rows

def get_record(fname):
    with open(fname) as f:
        content = f.readlines()
        tupled_lines = dict()
        for line in content:
            str_line = str(line)
            xyz_delta = [x.strip() for x in str_line.split(',')]
            if xyz_delta[2] == "nan":
                xyz_delta[2] = 0
            temp_point = {((int(float(xyz_delta[0]))), int(float(xyz_delta[1]))): 'tuple as key'}
            temp_point[int(float(xyz_delta[0])), int(float(xyz_delta[1]))] = xyz_delta[2]
            tupled_lines.update(temp_point)
    return tupled_lines
    
def dbz_to_mm_h(dbz):
    if (dbz != "nan"):
        print("input for dbz check")
        print(dbz)
        math_dbz = float(10**(float(dbz)/10))
        math_dbz = float(math_dbz/200)
        math_dbz = pow(math_dbz,(0.625))
        print(math_dbz)
        #input("wait")
        return math_dbz

def save_dbz_and_mm_h(wmoind, date, precept_dbz, precept_mm):
    conn = psycopg2.connect(host="127.0.0.1" , port="2223", database="postgres", user="postgres", password="postgres")
    cur = conn.cursor()
    statement = """INSERT INTO radars (wmoind, date, precept_dbz, precept_mm) VALUES (%s, %s, %s, %s)"""
    data = (wmoind, date, precept_dbz, precept_mm)
    cur.execute(statement, data)
    conn.commit()
    cur.close()
        
rows = get_places()
#row[8] row[9]
path = 'F:\\magisterka\\poland\\'
print("done")

for filename in glob.glob(os.path.join(path, '*.csv')):
    year = filename[21:25]
    month = filename[25:27]
    day = filename[27:29]     
    hour = filename[29:31]
    minute = filename[31:33]
        
    timestamp = year + "-" + month + "-" + day + " " + hour + ":" + minute + ":00"
    print(timestamp)
    print(year,month,day,hour,minute)
        
    radar_data_in_tuples = get_record(filename)
        
    for stations in rows:
        if (int(stations[8]), int(stations[9])) in radar_data_in_tuples:
            dbz_z_data = radar_data_in_tuples[int(stations[8]), int(stations[9])]
            precept_mm = dbz_to_mm_h(dbz_z_data)
            print(dbz_z_data)
            if(dbz_z_data) > 0:
                print(dbz_to_mm_h(radar_data_in_tuples[int(stations[8]), int(stations[9])]))
                save_dbz_and_mm_h(stations[2], timestamp, dbz_z_data, precept_mm)
            else:
                save_dbz_and_mm_h(stations[2], timestamp, 0, 0)
