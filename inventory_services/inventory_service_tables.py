import sys, requests, json, os, logging, pyodbc as db, pandas as pd, csv
#from arcgis.gis import GIS
from datetime import datetime
import getpass
from pathlib import Path

#Get current working directories
currentDirectory = os.path.dirname(os.path.abspath(__file__))

#Open and read config.json
f = open(os.path.join(currentDirectory,'config.json'))
config = json.load(f)

table=config['sql']['table']

class ArcGISEnterpriseHandler(object):
    def __init__(self, config):

        #instance variables derived from config.json
        self.config = config
        self.connectionString = self.config['sql']['connectionString']
        self.enterpriseInfo = self.getEnterpriseInfo()
        self.entServer = self.enterpriseInfo['serverUrl']
        self.portalWebAdaptor = self.hasPortal()
        self.agsWebAdaptor = config['environments'][self.entServer]['arcgisServer']['url']
        self.token = self.getToken()
        self.folders = self.getFolders()
        self.services = self.getServices()
        self.database = config['sql']['database']

    def getEnterpriseInfo(self):
        #Provide the instance name of the ArcGIS Enterprise environment
        entServer = input("Server url: ")

        return {
            "serverUrl": entServer
        }

    def hasPortal(self):
        #Detects whether or not the environment has an ArcGIS Portal.
        #This information is derived from config.json
        portalUrl = 0
    
        for x in config['environments'][self.entServer].keys():
            if(x == 'portal'):
                portalUrl = config['environments'][self.entServer]['portal']['url']
        
        return portalUrl

    def getToken(self):
        #Generates a token based on whether or not the environment contains a ArcGIS Portal or not
        if(self.portalWebAdaptor):
            enterpriseAuth = config['environments'][self.entServer][self.portalWebAdaptor]
        else:
            enterpriseAuth = config['environments'][self.entServer]['arcgisServer']

        serverParams = {
            "serverTokenUrl": None,
            "tokenParams": {
                'username': enterpriseAuth['username'],
                'password': enterpriseAuth['password'],
                'f': 'json'
            }
        }

        if(self.portalWebAdaptor == "portal"):
            print("Get portal token")
            serverParams['serverTokenUrl'] = f'https://{self.entServer}/{self.portalWebAdaptor}/sharing/rest/generateToken'
            serverParams['tokenParams']['ip'] = None
            serverParams['tokenParams']['client'] = 'referer'
            serverParams['tokenParams']['referer'] = f'https://{self.entServer}/{self.portalWebAdaptor}'
            serverParams['tokenParams']['expiration'] = 60
        else:
            print("Get ArcGIS Server token")
            serverParams['serverTokenUrl'] = f'https://{self.entServer}/{self.agsWebAdaptor}/admin/generateToken'
            serverParams['tokenParams']['client'] = 'requestip'

        #Make a POST request to retrieve a response that contains a token
        tokenResponse = requests.post(serverParams['serverTokenUrl'],serverParams['tokenParams'])
        print(tokenResponse)
        #Parse the returned json response and get the token
        parsedResponse = json.loads(tokenResponse.text)

        if "token" not in parsedResponse:
            print(parsedResponse['error'])
            sys.exit()
        else:
            print(parsedResponse["token"])
            return parsedResponse["token"]

    def getFolders(self):
        #Get all of the folders in the ArcGIS for Server environment
        serverUrl = f'https://{self.entServer}/{self.agsWebAdaptor}/admin/services'

        #print(serverUrl)

        params = {
            'token': self.token,
            'f': 'json'
        }

        jsonRequest = requests.post(serverUrl, params)
        #print(jsonRequest)
        jsonResponse = json.loads(jsonRequest.text)
        #print(jsonResponse)

        return jsonResponse['folders']

    def getServices(self):
        #Get all of the services that exist in each folder
        services = []
        for folder in self.folders:
            #print("Folder: {}".format(folder))
            serverUrl = 'https://' + self.entServer + '/' + self.agsWebAdaptor + '/rest/services'
            serverAdminUrl = 'https://' + self.entServer + '/' + self.agsWebAdaptor + '/admin/services'
            folderUrl = '{}/{}'.format(serverAdminUrl, folder)

            params = {
                'token': self.token,
                'f': 'json'
            }

            try:
                jsonRequest = requests.post(folderUrl, params)
                jsonResponse = json.loads(jsonRequest.text)

                for service in jsonResponse['services']:
                    if folder != 'System' and folder != 'Utilities':
                        serviceDict = {}
                        serviceDict['Instance'] = self.entServer
                        serviceDict['ServiceFolder'] = folder
                        serviceDict['ServiceName'] = service['serviceName']
                        serviceDict['ServiceType'] = service['type']
                        serviceDict['ServiceUrl'] = "{}/{}/{}/{}".format(serverUrl, folder, service['serviceName'], service['type'])
                        serviceDict['ItemInfoUrl'] = f'{serverAdminUrl}/{folder}/{service["serviceName"]}.{service["type"]}/iteminfo'
                        serviceDict['ServiceManifestUrl'] = "{}/{}/{}.{}/iteminfo/manifest/manifest.json".format(serverAdminUrl, folder, service['serviceName'], service['type'])

                        services.append(serviceDict)
            except:
                pass
                
        serviceDataFrame = pd.DataFrame.from_dict(services)

        return serviceDataFrame

    def getServiceInfo(self, service):
        #Return service information
        itemInfoUrl = service['ItemInfoUrl']

        params = {
            'token': self.token,
            'f': 'json'
        }

        request = requests.post(itemInfoUrl, params)
        data = json.loads(request.text)

        if('guid' in data):
            return {
                'guid': data['guid'],
            }            

    def getServiceManifest(self, service):
        #Return service manifest
        datasets = []
        params = {
                'f': 'json',
                'token': self.token
            }

        try:
            jsonRequest = requests.post(service['ServiceManifestUrl'], params)
            jsonResponse = json.loads(jsonRequest.text)

            for database in jsonResponse['databases']:
                #print("Database: {}".format(database))
                for dataset in database['datasets']:
                    #print(" - Dataset: {}".format(dataset))
                    datasetDict = {}
                    datasetDict['datasetName'] = dataset['onServerName']
                    datasetDict['serviceName'] = service['ServiceName']

                    serverConString = database['onServerConnectionString']
                    conStringList = serverConString.split(';')

                    for item in conStringList:
                        key = item.split('=')[0]
                        value = item.split('=')[1]

                        datasetDict[key]=value

                    datasets.append(datasetDict)

        except:
            pass

        datasetDataFrame = pd.DataFrame.from_dict(datasets)

        return datasetDataFrame

    def getRecordCount(self, cursor, connection, table):
        #Get the number of records of each unique instance entry in the database table
        db = self.database
        table = f'{db}.dbo.{table}'
        query = f"SELECT COUNT(Instance) FROM {table} WHERE Instance='{self.entServer}'"
        cursor.execute(query)
        count = cursor.fetchone()

        return count[0]

    def deleteRecords(self, cursor, connection, table):
        #Delete records from the database table where the instance name is equal to 'x'
        db = self.database
        table = f'{db}.dbo.{table}'
        query = f"DELETE FROM {table} WHERE Instance= '{self.entServer}'"
        cursor.execute(query)
        connection.commit()

    def insertRecords(self, cursor, connection, table, columns, values):
        #Insert records into database table
        db = self.database
        table = f'{db}.dbo.{table}'
        query = "INSERT INTO {table} {columns} VALUES {values};".format(table=table,columns=columns,values=values)
        print(query)

        cursor.execute(query)
        connection.commit()
    
    
if __name__ == "__main__":
    arcGisHandler = ArcGISEnterpriseHandler(config)
    
    con = db.connect(arcGisHandler.connectionString)
    cur = con.cursor()

    #Return all of the services
    services = arcGisHandler.getServices()

    #Delete records corresponding the queried ArcGIS Server Instance
    recordCount = arcGisHandler.getRecordCount(cur, con, table)
    if(recordCount > 0):
        arcGisHandler.deleteRecords(cur, con, table)

    datasetList = []
    for s, service in services.iterrows(): 
        try:
            serviceInfo = arcGisHandler.getServiceInfo(service)
            serviceManifest = arcGisHandler.getServiceManifest(service)
            for d, dataset in serviceManifest.iterrows():

                datasetDict = {}
                datasetDict['ServiceGuid'] = serviceInfo['guid']
                datasetDict['Instance'] = service['Instance']
                datasetDict['Name'] = str(dataset['datasetName']).lower()
                datasetDict['DatabaseName'] = str(dataset['DATABASE']).lower()
                datasetDict['DbClient'] = dataset['DBCLIENT']
                datasetDict['DbConnectionProperties'] = dataset['DB_CONNECTION_PROPERTIES']
                datasetDict['ServerName'] = str(dataset['SERVER']).lower()
                datasetDict['ServiceManifestUrl'] = service['ServiceManifestUrl']

                datasetList.append(datasetDict)
        except:
            pass

        columnString = ','.join(list(datasetDict.keys()))
        columns = f'({columnString})'
        valueString = str(tuple(datasetDict.values())).strip('[]')

        try:
            arcGisHandler.insertRecords(cur, con, table, columns, valueString)
        except:
            pass

    cur.close()
    con.close()