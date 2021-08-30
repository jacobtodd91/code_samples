#!/usr/bin/env python
# coding: utf-8

from datetime import datetime
import requests, json, pyodbc as db, sys

#Get today's date
currentDateTime = datetime.today().strftime('%Y%m%d')

#Open the JSON configuration file
f = open('common.json')
config = json.load(f)
print(config['sql']['connectionString'])

#Set variables
agolUrl = config['agolUrl']
tokenUrl = config['tokenUrl']
#null = 'NULL'

class ArcGISOnlineHandler(object):
    def __init__(self, config):
        #AGOL variables
        self.config = config
        self.tokenUrl = self.config['tokenUrl']
        self.client_id = self.config['client_id']
        self.client_secret = self.config['client_secret']
        self.grant_type = self.config['grant_type']
        self.services = self.config['services']
        self.token = self.generateToken()

        #SQL variables
        self.database = config['sql']['database']
        print(self.database)
        #sys.exit()
    #Generate a token method
    def generateToken(self):
        params = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'grant_type': self.grant_type
        }

        request = requests.post(self.tokenUrl, params, verify=False)
        data = request.json()

        return(data['access_token'])

    #Get layer name method
    def getLayerInfo(self):
        params = {
            'f': 'json',
            'token': self.token
        }

        request = requests.post(service, params, verify=False)
        data = json.loads(request.text)

        fields = []
        for field in data['fields']:
            # if(bool(field['domain'])):
            #     print(field['name'], field['type'], field['domain'])
            # else:
            #     print(field['name'], field['type'])
            for key, value in field.items():
                if(value == "esriFieldTypeDate"):
                    fields.append(field['name'])

        return {
            'name': data['name'],
            'fields': fields
        }

    #Return AGOL data method
    def getData(self):      
        #Parameters used to all of the feature service data
        params = {
            'where': '1=1'
            ,'outFields': '*'
            ,'geometryType': 'esriGeometryEnvelope'
            ,'returnGeometry': 'true'
            ,'returnExceededLimitFeatures': 'true'
            ,'sqlFormat': 'standard'
            ,'f': 'json'
            ,'token': self.token
        }

        request = requests.post(service + '/query', params, verify=False)
        data = json.loads(request.text)

        if(len(data['features']) > 0):
            return data['features']
        else:
            return None

    #Truncate records method
    def truncateRecords(self, cursor, connection, table):
        db = self.database
        table = f'{db}.{table}'
        query = "TRUNCATE TABLE {table}".format(table=table)

        try:
            cursor.execute(query)
            connection.commit()
        except:
            pass

    #Convert from milliseconds to time datetime
    def msToDate(self, ms):
        s = ms/1000
        date = datetime.fromtimestamp(s).strftime('%Y-%m-%d %H:%M:%S')
        
        return date
    
    #Insert records method
    def insertRecords(self, cursor, connection, table, columns, values):
        val = None
        db = self.database
        table = f'{db}.{table}'
        #query = "INSERT INTO {table} {columns} VALUES {values};".format(table=table,columns=columns,values=values)
        query = "INSERT INTO {table} VALUES {values};".format(table=table,values=values)
        #sys.exit()
        cursor.execute(query.format(val))
        connection.commit()
    
if __name__ == "__main__":

    #Make a database connection
    con = db.connect(config['sql']['connectionString'])
    cur = con.cursor()

    agolHandler = ArcGISOnlineHandler(config)

    for service in config['services']:
        layerInfo = agolHandler.getLayerInfo()
        layerName = layerInfo['name']
        dateFields = layerInfo['fields']
        data = agolHandler.getData()
        #sys.exit()

        #Truncate table
        agolHandler.truncateRecords(cur, con, layerName)

        keys = []
        values = []
        for row in data:
            rowValues = []
            for key, value in row['attributes'].items():
                keys.append(key)
                if(value == None):
                    value = ''
                if(key not in dateFields):
                    rowValues.append(value)
                else:
                    dateTimeVals = agolHandler.msToDate(value)
                    rowValues.append(dateTimeVals)
            rowValues[21] = str(rowValues[21])
            columns = tuple(set(keys))
            valueString = (tuple(rowValues))
            
            agolHandler.insertRecords(cur, con, layerName, columns, valueString)

    cur.close()
    con.close()
    