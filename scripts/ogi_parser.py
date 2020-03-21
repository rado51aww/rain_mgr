import time
import psycopg2

#1. Put reports with data in text file to be parsed.
#2. Script will parse each line to get data and push it to the DB.

class Structie(object):
    def __init__(self,**kwargs):
        self.__dict__.update(kwargs)

class Report:
    wmoind = None
    year = None
    month = None
    day = None
    hour = None
    minute = None
    wmo_stat_numb = None
    
    day = None
    hour = None
    wind_type = None

    prec_indic = None
    stat_type = None
    cloud_base = None
    visibility = None
    
    total_cloud_cover = None
    wind_direction_10s_degree = None
    wind_speed = None

    temperature_sign = None
    temperature_celsius = None

    dewpoint_sign = None
    dewpoint_temperature_celsius = None

    station_pressure = None #in 0.1 mb

    sea_lvl_pressure = None #in 0.1 mb

    pressure_tendency_characteristic = None
    pressure_tendency_change = None #in 0.1 mb

    precpt_mm = None
    precpt_t = None

    present_weather = None
    past_weather = None

    cloud_amount = None
    low_cloud_type = None
    middle_cloud_type = None
    high_cloud_type = None

    time_hours = None
    time_minutes = None
    
    precpt_amount = None
    precpt_period = None
    
    precpt_24h = None
    
    rest = None

    def __init__(self, content):
        self.rest = content.split()

    def strip(self):
    #TODO CHANGE NUMBERS
        #print(len(self.rest))
        self.year = self.rest[0][:4]
        self.month = self.rest[0][4:6]
        self.day = self.rest[0][6:8]
        self.hour = self.rest[0][8:10]
        self.minute = self.rest[0][10:12]
        self.wind_type = self.rest[2][4:5]#1=m/s
        self.wmoind = self.rest[3]
        #self.wmo_stat_numb = self.rest[6]
        #self.day = self.rest[7][:2]
        #self.hour = self.rest[7][2:4]
        
        self.prec_indic = self.rest[4][0]#which section IMPORTANT
        self.stat_type = self.rest[4][1] # weather group
        self.cloud_base = self.rest[4][2:3] # lowest observed clouds
        self.visibility = self.rest[4][3:5] # horizontal visibility
    
        self.total_cloud_cover = self.rest[5][:1] # total cloud cover
        self.wind_direction_10s_degree = self.rest[5][1:3]#
        self.wind_speed = self.rest[5][3:5]#wind speed with prev given indicator
        
        if (self.rest[6][:2] == '00'): #optional
            print("tak")
            #process
            n_id = 7
        else:
            n_id = 6
        #air temperature
        if (n_id < len(self.rest) and self.rest[n_id][:1] == '1'):
            self.temperature_sign = self.rest[n_id][1:2]
            self.temperature_celsius = self.rest[n_id][2:5]
            n_id = n_id + 1
        #dew point temperature    
        if (n_id < len(self.rest) and self.rest[n_id][:1] == '2'):
            self.dewpoint_sign = self.rest[n_id][1:2]
            self.dewpoint_temperature_celsius = self.rest[n_id][2:5]
            n_id = n_id + 1
        #barometric pressure
        if (n_id < len(self.rest) and self.rest[n_id][:1] == '3'):
            if self.rest[n_id][4:5] == '/':
                self.station_pressure = self.rest[n_id][1:4]
                self.station_pressure = float(self.station_pressure) / 10
            else:
                self.station_pressure = self.rest[n_id][1:5]
            n_id = n_id + 1
            
        if (n_id < len(self.rest) and self.rest[n_id][:1] == '4'):
            if self.rest[n_id][4:5] == '/':
                self.sea_lvl_pressure = self.rest[n_id][1:4]
                self.sea_lvl_pressure = float(self.station_pressure) / 10
            elif int(self.rest[n_id][1:2]) > 0:
                print("not supported")
            else:
                self.sea_lvl_pressure = self.rest[n_id][1:5]
            n_id = n_id + 1

        if (n_id < len(self.rest) and self.rest[n_id][:1] == '5'):
            self.pressure_tendency_characteristic = self.rest[n_id][1:2]
            self.pressure_tendency_change = self.rest[n_id][2:5]
            n_id = n_id + 1
            
        if (n_id < len(self.rest) and self.rest[n_id][:1] == '6'):
            self.precpt_mm = self.rest[n_id][1:4]
            self.precpt_t = self.rest[n_id][4:5]
            if self.precpt_mm == "///":
                self.precpt_mm = 0
            else:
                self.precpt_mm = int(self.precpt_mm)
            if int(self.precpt_mm) == 990:
                self.precpt_mm = 0.5
            if float(self.precpt_mm) > 990:
                self.precpt_mm = float(self.precpt_mm) - 990
                self.precpt_mm = self.precpt_mm / 10
                print(self.precpt_mm)
            n_id = n_id + 1
            
        if (n_id < len(self.rest) and self.rest[n_id][:1] == '7'):
            self.present_weather = self.rest[n_id][1:3]
            self.past_weather = self.rest[n_id][3:5]
            n_id = n_id + 1

        if (n_id < len(self.rest) and self.rest[n_id][:1] == '8'):
            self.cloud_amount = self.rest[n_id][1:2]
            self.low_cloud_type = self.rest[n_id][2:3]
            self.middle_cloud_type = self.rest[n_id][3:4]
            self.high_cloud_type = self.rest[n_id][4:5]
            n_id = n_id + 1

        if (n_id < len(self.rest) and self.rest[n_id][:1] == '9'):
            self.time_hours = self.rest[n_id][1:3]
            self.time_minutes = self.rest[n_id][3:5]
            n_id = n_id + 1
        
    #if (n_id < len(self.rest) and self.rest[n_id][:3] == '333'): #climatologic data - adjust the scope due to documentation mismatch
    #        n_id = n_id + 1
    #    while (n_id < len(self.rest) and self.rest[n_id][:1] =! '6'):
    #    n_id = n_id + 1
    #    if (n_id < len(self.rest) and self.rest[n_id][:1] == '6'):
    #        self.precpt_amount = self.rest[n_id][1:4]
    #        self.precpt_period = self.rest[n_id][4:5]
    #        n_id = n_id + 1
    #    if (n_id < len(self.rest) and self.rest[n_id][:1] == '7'):
    #        self.precpt_24h = self.rest[n_id][1:5]
                
def get_record(fname):
    with open(fname) as f:
        content = f.readlines()
        content = [x.strip() for x in content]
        #content = [x.replace(',', ' ') for x in content]#obsolete in new report file ranging in full month
    return content

lined_reports = get_record("C:\\Users\\radoslaw\\Desktop\\reports_july_full_poland_modified.txt")

print(lined_reports[0])
parsed_reports = list()
for line in lined_reports:
    parsed_report = Report(line)
    parsed_report.strip()
    parsed_reports.append(parsed_report)
print("--------")




for att in dir(parsed_reports[0]):
         print (att, getattr(parsed_reports[0],att))
conn = psycopg2.connect(host="127.0.0.1" , port="2223", database="postgres", user="postgres", password="postgres")
cur = conn.cursor()
statement = """INSERT INTO reports (wmoind, year, month, day, hour, wind_type, prec_indic, stat_type, close, visibility, total_cloud_cover, wind_direction_10s_degree, wind_speed, temperature_sign, 
                temperature_celsius, dewpoint_sign, dewpoint_temperature_celsius, station_pressure, sea_lvl_pressure, pressure_tendency_characteristic, pressure_tendency_change, 
                precept_mm, precept_t, present_weather, past_weather, cloud_amount, low_cloud_type, middle_cloud_type, high_cloud_type) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ,%s ,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

for parsed_report in parsed_reports:               
    data = (parsed_report.wmoind, parsed_report.year, parsed_report.month, parsed_report.day, parsed_report.hour,
                  parsed_report.wind_type,
                  parsed_report.prec_indic,  parsed_report.stat_type ,  parsed_report.cloud_base ,  parsed_report.visibility ,
                   parsed_report.total_cloud_cover ,  parsed_report.wind_direction_10s_degree ,  parsed_report.wind_speed ,
                   parsed_report.temperature_sign ,  parsed_report.temperature_celsius ,  parsed_report.dewpoint_sign ,
                   parsed_report.dewpoint_temperature_celsius ,
                   parsed_report.station_pressure ,  parsed_report.sea_lvl_pressure ,  parsed_report.pressure_tendency_characteristic ,
                   parsed_report.pressure_tendency_change ,  parsed_report.precpt_mm ,  parsed_report.precpt_t ,  parsed_report.present_weather ,
                   parsed_report.past_weather ,  parsed_report.cloud_amount ,  parsed_report.low_cloud_type ,  parsed_report.middle_cloud_type ,
                   parsed_report.high_cloud_type)
    cur.execute(statement, data)
    #id = cur.fetchone()[0]
    #print("executed")
    conn.commit()
cur.close()