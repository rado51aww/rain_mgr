import csv
import math
import psycopg2
import time
#find nearest square with coordinates
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
    # 6 aeqd_xs
    # 7 aeqd_y
    # 8 aeqd_x_dp
    # 9 aeqd_y_dp
    return rows

def get_record(fname):
    with open(fname) as f:
        content = f.readlines()
        content = [x.strip() for x in content]
    return content

def update_coord(low_dim_x, low_dim_y, wmoind):
    conn = psycopg2.connect(host="127.0.0.1" , port="2223", database="postgres", user="postgres", password="postgres")
    cur = conn.cursor()
    statement = """UPDATE places SET aeqd_x_rounded = '%s', aeqd_y_rounded = '%s' WHERE wmoind = '%s';""" 
    data = (low_dim_x, low_dim_y, wmoind)
    cur.execute(statement, data)
    conn.commit()
    cur.close()

rows = get_places()

with open('2xyz.csv') as csv_file:
    csv_reader = list(csv.reader(csv_file, delimiter=','))
    line_count = 0
    for places in rows:
        low = 2000
        low_dim_x = 0
        low_dim_y = 0
        line_count = line_count + 1
        for row in csv_reader:
            dim_x = int(str(row[0]).split('.')[0])
            dim_y = int(str(row[1]).split('.')[0])
            distance = math.sqrt(((dim_x - int(places[6]))**2) + ((dim_y - int(places[7]))**2))
            if distance < low:
                low = distance
                low_dim_x = dim_x
                low_dim_y = dim_y
                print(distance)
                print(dim_x)
                print(dim_y)
                print(' ')
        line_count += 1
        update_coord(low_dim_x, low_dim_y, places[2])
        print("updated coord")
        print(places[2])
        print("-----------------")
    print(line_count)
        


