import psycopg2
import os, glob
from collections import namedtuple
import datetime
import time
#get reports from DB, check if there's a rain indication in report, check for what delta has it been reported, 
#get for t-delta data from radars(sum - data from radars have interval of 10minutes) and write it to db 

synop_hours =	{
  "1": "6",
  "2": "12",
  "3": "18",
  "4": "24",
  "5": "1",
  "6": "2",
  "7": "3",
  "8": "9",
  "9": "15",
}

def get_report():
    conn = psycopg2.connect(host="127.0.0.1" , port="2223", database="postgres", user="postgres", password="postgres")
    cur = conn.cursor()
    statement = "SELECT * FROM reports"
    cur.execute(statement)
    rows = cur.fetchall()
    return rows

def get_radar(wmoind, date_start, date_end):
    conn = psycopg2.connect(host="127.0.0.1" , port="2223", database="postgres", user="postgres", password="postgres")
    cur = conn.cursor()
    statement = """SELECT * FROM radars WHERE WMOIND=%s and date BETWEEN %s::timestamp AND %s::timestamp AND precept_dbz > 0"""
    data = (wmoind, date_start, date_end)
    cur.execute(statement, data)
    rows = cur.fetchall()
    return rows
        
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

def mm_h_to_dbz(mm_h):
    print("stub")
    
def save_sum_mm_into_reports(precept_radar_mm, id, wmoind):
    conn = psycopg2.connect(host="127.0.0.1" , port="2223", database="postgres", user="postgres", password="postgres")
    cur = conn.cursor()
    statement = """UPDATE reports SET precept_radar_mm=%s WHERE ID=%s AND WMOIND=%s"""
    data = (precept_radar_mm, id, wmoind)
    cur.execute(statement, data)
    conn.commit()
    cur.close()
        
rows = get_report()
#row[8] row[9]

    #timestamp = year + "-" + month + "-" + day + " " + hour + ":" + minute + ":00"

    #(76477, None, 2x'12100', '2015', '07', '06', None, '1', '1', '2', '9', '65', '7',
    #'21', '02', '0', '250', '0', '180', '0173', '0180', '3', '004', 23x'0', 24x'2', '', ''
    #, '7', '0', '3', '/', 31x'05', 32xNone, 33xNone, 34xNone, 35xNone)

for report in rows:
    if(report[23] > 0):
        #print(report)
        synop_mm = report[23]
        #synop_dbz = dbz_to_mm_h(synop_mm)
        
        diff = datetime.timedelta(seconds=0, minutes=0, hours=int(synop_hours[report[24]]), days=0)
        diff_hour_only = str(diff)[0:2]
        if diff_hour_only[1] == ":":
            diff_hour_only = str(diff)[0:1]
        
        date_end = datetime.datetime(int(report[3]), int(report[4]), int(report[31]), int(report[5]))
        date_start = date_end - diff
        #timestamp_start = report[3] + "-" + report[4] + "-" + report[31] + " " + diff_hour_only + ":" + "00" + ":00"
        #timestamp_end = report[3] + "-" + report[4] + "-" + report[31] + " " + report[5] + ":" + "00" + ":00"
        # datetime(year, month, day, hour, minute, second, micosecond)
        june = datetime.datetime(2015, 06, 30, 23, 59, 59, 0)
        if(date_start > june):
            radars_records = get_radar(report[2], date_start, date_end)
            sum_mm_radars = 0
            for records in radars_records:
               sum_mm_radars = sum_mm_radars +  records[5]
            save_sum_mm_into_reports(sum_mm_radars, report[0], report[2])
            print("date start: " + str(date_start))
            print("date_end: " + str(date_end))