import sys, requests, json, os, logging, time, urllib
from datetime import date
from pathlib import Path

#Set variables
commonConfigFile = sys.argv[1]
serviceConfigFile = sys.argv[2]
instance = sys.argv[3]

#Get current datetime as time stamp
today = date.today().strftime("%Y%m%d")

#Setup logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s:%(levelname)s:%(message)s')

#Get current working directories
currentDirectory = os.path.dirname(os.path.abspath(__file__))
parentDirectory = os.path.dirname(currentDirectory)

#Open and read config.json and service_config.json
f = open(os.path.join(currentDirectory, commonConfigFile))
s = open(os.path.join(currentDirectory, serviceConfigFile))

config = json.load(f)
serviceConfig = json.load(s)

class ArcGISEnterpriseHandler(object):
    def __init__(self, config, instance):
        self.config = config
        self.entServer = instance
        self.portalWebAdaptor = self.hasPortal()
        self.agsWebAdaptor = config['environments'][self.entServer]['arcgisServer']['url']
        self.token = self.getToken()

    def hasPortal(self):
        portalUrl = 0
    
        for x in config['environments'][self.entServer].keys():
            if(x == 'portal'):
                portalUrl = config['environments'][self.entServer]['portal']['url']
        
        return portalUrl

    def getToken(self):

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

    def getStatus(self, service, logger):
        requestUrl = f'https://{self.entServer}/arcgis/admin/services/{service["folder"]}/{service["name"]}.{service["type"]}/status'

        params = {
            'f': 'json',
            'token': self.token
        }

        request = requests.post(requestUrl, params)
        response = request.json()
        print(response)

        status = "STOPPED"
        if(response['configuredState'] == 'STARTED' and response['realTimeState'] == 'STARTED'):
            status = "STARTED"
            logger.info(f"Service: {service['name']}; STATUS: {status}")
        else:
            logger.warning(f"Service: {service['name']}; STATUS: {status}")

        return status

    def queryService(self, service, address):
        serviceFolder = service["folder"]
        serviceName = service["name"]
        serviceType = service["type"]

        singleLineInputAddress = f'{address["Street"]} {address["City"]} {address["State"]}'
        urlEncodedAddress = urllib.parse.quote_plus(singleLineInputAddress)

        # requestUrl = f'https://{self.entServer}/arcgis/rest/services/{serviceFolder}/{serviceName}/{serviceType}/findAddressCandidates?outFields=*&Single%20Line%20Input=1245+Allegiance+Drive&f=pjson&outSR=4326'

        requestUrl = f'https://{self.entServer}/arcgis/rest/services/{serviceFolder}/{serviceName}/{serviceType}/findAddressCandidates?outFields=*&Single%20Line%20Input={urlEncodedAddress}&f=pjson&outSR=4326'

        logger.info(f"Service: {service['name']}; MESSAGE: Querying {singleLineInputAddress}")

        request = requests.get(requestUrl)
        response = request.json()

        candidates = response['candidates']

        return len(candidates)

    def stopService(self, service, logger):
        requestUrl = f'https://{self.entServer}/arcgis/admin/services/{service["folder"]}/{service["name"]}.{service["type"]}/stop'

        params = {
            'f': 'json',
            'token': self.token
        }

        request = requests.post(requestUrl, params)
        response = request.json()

        if(response['status'] == 'success'):
            logger.warning(f"Service: {service['name']}; MESSAGE: Service has been stopped")
        else:
            logger.error(f"Service: {service['name']}; MESSAGE: Service could be stopped")

    def startService(self, service, logger):
        requestUrl = f'https://{self.entServer}/arcgis/admin/services/{service["folder"]}/{service["name"]}.{service["type"]}/start'

        params = {
            'f': 'json',
            'token': self.token
        }

        request = requests.post(requestUrl, params)
        response = request.json()

        if(response['status'] == 'success'):
            logger.info(f"Service: {service['name']}; MESSAGE: Service has been started")
        else:
            logger.error(f"Service: {service['name']}; MESSAGE: Service could be started")

def restartService(service, logger):
    arcGisHandler.stopService(service, logger)
    time.sleep(30)
    arcGisHandler.startService(service, logger)

    #TEST
    # if("Test" in service['name']):
    #     print("Test Service Found")
    #     arcGisHandler.stopService(service)
    #     time.sleep(30)
    #     arcGisHandler.startService(service)

def setupLogger(logger_name, log_file, level=logging.INFO):
    l = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(asctime)s | %(levelname)s | Line No %(lineno)s | %(message)s')
    fileHandler = logging.FileHandler(log_file, mode='a')
    fileHandler.setFormatter(formatter)
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)

    l.setLevel(level)
    l.addHandler(fileHandler)
    l.addHandler(streamHandler)

if __name__ == "__main__":
    setupLogger("services", f"logs/services_{today}.log")
    logger = logging.getLogger("services")

    arcGisHandler = ArcGISEnterpriseHandler(config, instance)
    for service in serviceConfig['services']:
        #Get the status of the service
        serviceStatus = arcGisHandler.getStatus(service, logger)
        if(serviceStatus == "STARTED"):
            logger.info(f"Service: {service['name']}; STATUS: {serviceStatus}")
            hasCandidates = True
            for address in serviceConfig['addresses']:
                #Attempt to query the service 5 times
                for i in range(5):
                    #Query the service to return the number of candidates
                    returnedCandidates = arcGisHandler.queryService(service, address)
                    if(returnedCandidates < 1):
                        hasCandidates = False
                        logger.error(f"Service: {service['name']}; MESSAGE: No candidates returned")
                        try:
                            restartService(service, logger)
                            break
                        except:
                            logger.error(f"Service: {service['name']}; MESSAGE: Stopping the script")
                            break
                if hasCandidates == False:
                    break
        else:
            #Start the service if it is not currently running
            arcGisHandler.startService(service, logger)
            



