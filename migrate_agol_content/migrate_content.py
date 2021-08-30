from arcgis import GIS
from configparser import ConfigParser
from datetime import datetime
import csv, pandas as pd, sys, requests, json, os, pyodbc as db

#Get current and parent working directories
currentDirectory = os.path.dirname(os.path.abspath(__file__))
parentDirectory = os.path.abspath(os.path.join(currentDirectory, os.pardir))
baseDirectory = r"<path>"

#Read main-config.json
f = open(os.path.join(currentDirectory,'config.json'))
config = json.load(f)

#ArcGIS Online Information
orgId = config['accounts']['agol']['orgid']
baseURL = config['accounts']['agol']['url']
agol_username = config['accounts']['agol']['username']
agol_password = config['accounts']['agol']['password']
gis = GIS(baseURL, agol_username, agol_password)

#Database connections and queries
sdw_con=config['dataType']['SQL']['database']['<dbName>']['connectionString']
sdw_getUsers=config['dataType']['SQL']['database']['<dbName>']['queries']['getUsers']
sdw_markAsMigrated=config['dataType']['SQL']['database']['<dbName>']['queries']['updateSingleUser']

#SQL Connection information
def getConnectionInfo(server, database):
    global connectionString
    connectionString = 'DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + server + ';Trusted_Connection=yes;DATABASE=' + database
    return connectionString

#User input
def getUsers(cursor):
    userInput = input("Who would you like to run this migration against? ")
    if(userInput == 'all'):
        agolUsers = GetData(cursor, user = None)
    elif(userInput == 'exit'):
        print("Stopping script...")
        sys.exit()
    else:
        agolUsers = GetData(cursor, user = userInput)

    return agolUsers

def continueScript(con):
    answer = input("\nContinue? (y/n): ")

    if(answer.lower() == 'y' and answer.lower() != 'n'):
        pass
    elif(answer.lower() == 'n'):
        print("Stopping script...")
        con.close()
        sys.exit()
    elif(answer.lower() != 'n' or answer.lower() != 'y'):
        print("You entered an invalid choice. This process has been terminated.")
        con.close()
        sys.exit()
 
#Generate token
def GenerateToken(baseURL_):

    #Set variables
    generateTokenURL = baseURL_ + '/sharing/rest/generateToken'
 
    #Generate token parameters
    generateTokenParams = {
        'username': agol_username,
        'password': agol_password,
        'referer': baseURL_,
        'f': 'json',
    }

    #Make a POST request to retrieve a response that contains a token
    jsonRequest = requests.post(generateTokenURL, generateTokenParams)
    #Parse the returned json response and get the token
    parsedResponse = json.loads(jsonRequest.text)
    token = parsedResponse["token"]

    return token

#User related functions
def GetData(conStr, query):
    userInput = input('Who would you like to run the script against (upn, not_migrated)?: ')

    con = db.connect(conStr)
    cur = con.cursor()

    global results

    results = []
    if(userInput != 'upn' and userInput != 'not_migrated'):
        print("You entered an incorrect option. Stopping the job.")
        sys.exit()
    else:
        if(userInput == 'upn'):
            upn = input("Enter the user's new UPN that you want to migrate: ")
            query = query + " WHERE UPN = '" + upn + "' AND AGOL_Migrated = 0 AND AGOL_BackedUp = 1"
        elif(userInput == 'not_migrated'):
            #query = query + " WHERE AGOL_Migrated = 0 AND AGOL_BackedUp = 1"
            query = query + " WHERE AGOL_Migrated = 0"

        for row in cur.execute(query):
            results.append(row)
            #print(row)
    
    print("- Found {} non-migrated users.\n- Would you like to continue?".format(len(results)))
    continueScript(con)

    #sys.exit()
    return results

    cur.close()
    con.close()

    del con, cur

def ReadTable(table):
    fieldList = arcpy.ListFields(table)

    fieldString = ""

    for field in fieldList:
        fieldString += "{},".format(field.name)

    fieldString = fieldString.rstrip(',')

    fieldNameList = fieldString.split(',')

    df = pd.DataFrame(arcpy.da.FeatureClassToNumPyArray(
        in_table = table,
        field_names = fieldNameList,
        skip_nulls = False,
        null_value = None
    ))

    return df

def ReadTable_CSV(*args, user = None):
    global data
    path = config['csv']['path']
    df = pd.read_csv(path)
    df.insert(0, 'Index', range(1, 1 + len(df)))
    
    if(user):
        data =  df.query('UPN == "{}"'.format(user))
    else:
        data = df
        

    return data.to_numpy()

def UserDoesExist(username_):
    doesExist = 0
    if(gis.users.get(username_)):
        doesExist = 1
    else:
        doesExist = 0
    return doesExist

def GetNewUserInformation(row_, oldUser_):  

    index1 = row_[2].find('.')
    index2 = row_[2].find('@')
    firstName = row_[2][:index1].strip()
    lastName = row_[2][index1 + 1:index2].strip()

    index = row[1].find(',')
    firstName = row[1][index + 2:]
    lastName = row[1][:index]

    newUserInfo_ = {
        'username': row_[2]
        ,'password': 'None'
        ,'firstName': firstName
        ,'lastName': lastName
        ,'email': row_[2]
        ,'role': oldUser_['role']
        ,'roleId': oldUser_['roleId']
        ,'provider': oldUser_['provider']
        ,'idp_username': row_[2]
        ,'level': oldUser_['level']
        ,'groups': oldUser_['groups']
    }

    return newUserInfo_

def CreateNewUser(oldUser_, newUserInfo_):
    #Create new user account
    newUser_ = gis.users.create(
        username = newUserInfo_['username']
        ,password = 'None'
        ,firstname = newUserInfo_['firstName']
        ,lastname = newUserInfo_['lastName']
        ,email = newUserInfo_['email']
        ,role = 'org_user'
        ,provider = newUserInfo_['provider']
        ,idp_username = newUserInfo_['idp_username']
        ,level = int(newUserInfo_['level'])
    )

    if(newUser_):
        #Set the role of the new user based on the original user
        role = newUserInfo_['roleId']
        #print(role)
        newUser_.update_role(role)
        print("- User role updated.")
        #Allocate credits to the new user
        assignedCredits = oldUser_.assignedCredits
        gis.admin.credits.allocate(newUser_, assignedCredits)
        print("- User credits allocated.")
        #Enable esri access
        newUser_.esri_access = True

    return newUser_

def MarkUserAsMigrated(cursor_, connection_, updateField_, upn_):
    updateQuery = sdw_markAsMigrated.format(updateField_, upn_)

    cursor_.execute(updateQuery)

    connection_.commit()

def RecreateFolderStructure(oldUser_, newUsername_):
    #Create the same folder structure in the new user's content as the old user
    print("- Folder information:")
    folders = oldUser_.folders
    for folder in folders:
        gis.content.create_folder(folder['title'], newUsername_)
        print("  - {}".format(folder))

#Licensing function
def GetPurchases(baseUrl_, token_):
    url = '{}/sharing/rest/portals/self/purchases'.format(baseUrl_)
    params = {
        'token': token_,
        'f': 'json'
    }

    request = requests.post(url, params)
    response = json.loads(request.text)

    return response['purchases']

def GetListings(token_, purchases_):
    listings = {}
    for purchase in purchases_:
        for key, value in purchase.items():
            if type(value) is dict:
                for key2, value2 in value.items():
                    if(key2 == 'title'):
                        listings[value2] = value['itemId']
    return listings

def GetUserEntitlements(baseUrl_, listings_, username_, token_):
    userEntitlementsDict = {}
    for key, value in listings_.items():
        listingUrl = '{}/sharing/rest/content/listings/{}/userEntitlements/{}'.format(baseUrl_, value, username_)

        params = {
            'token': token_
            ,'mylistings': False
            ,'f': 'json'
            ,'referer':'https://<organization>.maps.arcgis.com'
        }

        #Make a POST request to retrieve a response that contains a token
        jsonRequest = requests.post(listingUrl, params)
        #Parse the returned json response and get the token
        userEntitlements = json.loads(jsonRequest.text)

        for key2, value2 in userEntitlements.items():
            if type(value2) is dict:
                for key3, value3 in value2.items():
                    if key3 != 'code' and key3 == 'entitlements':
                        if type(value3) is list:
                            entitlementDict = {'itemId': value, key3: value3}
                            userEntitlementsDict[key] = entitlementDict

    return userEntitlementsDict

def SetUserEntitlements(baseUrl_, entitlements_, username_, token_):
    for key, value in entitlements_.items():
        #Provision entitlement url
        url = '{}/sharing/rest/content/listings/{}/provisionUserEntitlements'.format(baseUrl_, value['itemId'])
        
        #Create list then append entitlement items as a formatted string to the list
        entitlementList = []
        for item in value['entitlements']:
            itemString = "'{}'".format(item)
            entitlementList.append(itemString)

        #Create a string from the user entitlement items
        entitlementString = "[{}]".format(','.join(entitlementList))

        #Format an entitlement string that uses username and the entitlement string
        userEntitlements = '{"users":["' + username_ + '"],"entitlements": ' + entitlementString + '}'
        
        params = {
            'token': token_
            ,'suppressCustomerEmail': True
            ,'userEntitlements': userEntitlements
            ,'f': 'json'
        }

        #Make a POST request to retrieve a response that contains a token
        jsonRequest = requests.post(url, params)
        #Parse the returned json response and get the token
        response = json.loads(jsonRequest.text)

        print('- User entitlements ({}) set for {}'.format(entitlementString, username_))

#Group functions
def GetAllOrgGroups(baseUrl, orgId, token):
    groups = []
    url = baseUrl + "/sharing/rest/community/groups"
    num = 100
    start = 1

    for i in range(start, 1000, 100):
        params = {
            'q': orgId,
            'num': 100,
            'start': i,
            'sortField': "title",
            'f': "json",
            'token': token
        }

        request = requests.post(url, params)
        response = json.loads(request.text)

        for grp in response['results']:
            grpDict = {'index': i, 'id': grp['id'], 'title': grp['title']}
            #print(grpDict)
            groups.append(grp['id'])
            i+=1
        
    return groups
## NOT USED ##
def GetUserGroups(user1_, username1_):
    usergroups = user1_['groups']
    userOwnedGroups = []
    userMembershipGroups = []
    for group in usergroups:
        grp = gis.groups.get(group['id'])
        if(grp.owner.lower() == username1_.lower()):
            userOwnedGroups.append(group)
        else:
            userMembershipGroups.append(group)

    groupDict = {
        'membership': userMembershipGroups,
        'ownership': userOwnedGroups
    }

    return groupDict

def GetGroupInformation(baseURL_, groupId_, token_):
    url = baseURL_ + '/sharing/rest/community/groups/{}'.format(groupId_)

    params = {
        'token': token_,
        'f': 'json'
    }

    jsonRequest = requests.post(url,data=params)
    parsedResponse = json.loads(jsonRequest.text)

    return parsedResponse

def GetItemSharingGroups(baseUrl_, item_, token_):
    groupIdLst = []

    baseURL = baseUrl_ + '/sharing/rest/'

    params = {
        'token': token_,
        'f': 'json'
    }

    itemURL = baseURL + 'content/items/{}/groups'.format(item_['id'])
    
    jsonRequest = requests.post(itemURL,data=params)
    parsedResponse = json.loads(jsonRequest.text)

    for key, value in parsedResponse.items():
        if(value):
            for val in value:
                print("- {} is shared to: {}".format(item_['title'], val['title']))
                groupIdLst.append(val['id'])

    if(len(groupIdLst) < 1):
        print("- This item does not belong to any groups.")
    groupIdString = ','.join(groupIdLst)
    return groupIdString

def AddUserToGroup(baseUrl_, groupId_, user_, token_):
    params = {
        'token': token_,
        'f': 'json',
        'users': user_['username']
    }

    url = baseUrl_ + '/sharing/rest/community/groups/{}/addUsers'.format(groupId_)
    print("- Group URL: {}".format(url))
    jsonRequest = requests.post(url,data=params)
    response = json.loads(jsonRequest.text)

    return response

def AssignGroupOwnership(user1_, username1_, username2_, orgGroups):
    usergroups = user1_['groups']
    for group in usergroups:
        grp = gis.groups.get(group['id'])
        if(group['id'] in orgGroups):
            if(grp.owner.lower() == username1_.lower()):
                try:
                    grp.reassign_to(username2_)
                except:
                    print("Something went wrong...")
        else:
            print("Group {} is not a part of the Charlotte Organization and will be skipped.".format(group['id']))

def AssignGroupOwnership_OLD(user1_, username1_, username2_, orgGroups):
    usergroups = user1_['groups']
    for group in usergroups:
        #grp = gis.groups.get(group['id'])
        if(group['id'] in orgGroups):
            if(group['owner'].lower() == username1_.lower()):
                try:
                    group['id'].reassign_to(username2_)
                    print("- {} reassigned to {}".format(group['id'], username2_))
                    pass
                except:
                    print("- Something went wrong.")
        else:
            print("Group {} is not a part of the Charlotte Organization and will be skipped.".format(group['id']))

def AssignGroupOwnership_RESTAPI(baseURL_, user1_, username1_, username2_):
    usergroups = user1_['groups']
    for group in usergroups:
        #Get group owner
        groupInfo = GetGroupInformation(baseURL_, group['id'], token)
        print(groupInfo)
        #Check to see if the group owner is the same as username1
        #
        if(grpOwner.lower() == username1_.lower()):
            AddUserToGroup(group['id'], user2_, token_)
            print("{} reassigned to {}".format(group['id'], username2_))

def AssignGroupMembership(user1_, user2_, orgGroups):
    usergroups = user1_['groups']

    for grp in usergroups:
        AddUserToGroup(baseURL, grp['id'], user2_, token)

def AssignGroupMembership_(user1_, user2_, token_):
    usergroups = user1_['groups']
    for group in usergroups:
        AddUserToGroup(group['id'], user2_, token_)

def RemoveGroupMembership(user1_, username1_, baseURL_, token_):
    removeFromGroupParams = {
        'f': 'json',
        'users': username1_,
        'token': token_
    }

    usergroups = user1_['groups']
    
    for group in usergroups:
        grp = gis.groups.get(group['id'])
        removeFromGroupURL = '{}/sharing/rest/community/groups/{}/removeUsers'.format(baseURL_, grp['id'])
        requests.post(removeFromGroupURL, data = removeFromGroupParams)

        print("- " + username1_ + " removed from " + grp['title'])

#Content functions
def GetUserContent(user1_):
    usercontent = user1_.items()

    return usercontent

def ReassignUserContent(username1_, username2_, token_):
    oldUser = gis.users.get(username1_)
    newUser = gis.users.get(username2_)

    oldUserContent = oldUser.items()
    newUserContent = newUser.items()

    #Get the old user's content in their root folder
    print("- Root folder content:\n")
    for item in oldUserContent:
        print("- " + item['title'], '({})'.format(item['type']))
        #Get a comma separated list of group ids
        groupIdString = GetItemSharingGroups(baseURL, item, token_)

        try:
            if(item):
                print('- Attempting to unshare item: {}'.format(item['id']))
                item.unshare(groupIdString)
                print('- Item unshared.')
            else:
                print("- item does not exist")

            if(item):
                print('- Attempting to reassign item: {}'.format(item['id']))
                item.reassign_to(username2_)
                print('- Item reassigned.')
            else:
                print("- item does not exist")

            if(item):
                print('- Attempting to share {} back to it\'s original group(s)'.format(item['id']))
                sharingParams = SetSharingParameters(item['access'], groupIdString)
                ShareItem(baseURL, username2_, item['id'], sharingParams, token_)
                print("- Content reshared.")
            else:
                print("- item does not exist")
        except:
            print("- Item is not in this folder. Moving to the next item.")
            pass
        
        print('\n')

    #Get the old user's folders
    oldUserFolders = oldUser.folders
    for folder in oldUserFolders:
        #Get the old user's content in each folder
        folderItems = oldUser.items(folder=folder['title'])
        print("- {} folder content:\n".format(folder['title']))

        for item in folderItems:
            print("- " + item['title'], '({})'.format(item['type']))
            #Get a comma separated list of group ids
            groupIdString = GetItemSharingGroups(baseURL, item, token_)
            try:
                if(item):
                    print('- Attempting to unshare item: {}'.format(item['id']))
                    item.unshare(groupIdString)
                    print('- Item unshared.')
                else:
                    print("- item does not exist")

                if(item):
                    print('- Attempting to reassign item: {}'.format(item['id']))
                    item.reassign_to(username2_, target_folder=folder['title'])
                    print('- Item reassigned...')
                else:
                    print("- item does not exist")

                if(item):
                    print('- Attempting to share {} back to it\'s original group(s)'.format(item['id']))
                    sharingParams = SetSharingParameters(item['access'], groupIdString)
                    ShareItem(baseURL, username2_, item['id'], sharingParams, token_)
                    print("- Content reshared.")
                else:
                    print("- item does not exist")
            except:
                print("- Item is not in this folder. Moving to the next item.")
                pass
            
            print('\n')
        
        print('\n') 

def UnshareItemToGroup(user1_, username1_, itemId_, groupId_):
    usergroups = user1_['groups']
    usercontent = user1_.items()
    for item in usercontent:
        groupSharing = item.shared_with['groups']
        groupList = [gis.groups.get(grp['id']) for grp in groupSharing]

        if(item.id == itemId_):
            try:
                item.unshare(groupList)
                print("- Successfully unshared item: {} from {}".format(itemId_, groupId_))
            except:
                print("- Something went wrong.")

def SetSharingParameters(itemAccess_, groups_):
    sharingParams = {}

    if(itemAccess_ == 'public'):
        sharingParams['everyone'] = True
        sharingParams['org'] = False
    else:
        sharingParams['everyone'] = False

    if(itemAccess_ == 'org'):
        sharingParams['org'] = True
        sharingParams['everyone'] = False
    else:
        sharingParams['org'] = False

    if(groups_):
        sharingParams['groups'] = groups_
    else:
        sharingParams['groups'] = False

    return sharingParams

def ShareItem(baseUrl_, username_, itemId_, params_, token_):
    #import modules
    import requests, json

    #Set variables
    itemURL = baseUrl_ + '/sharing/rest/content/items/{}/share'.format(itemId_)

    #Add to share item parameters
    params_['token'] = token_
    params_['f'] = 'json'
    params_['confirmItemControl'] = True

    #Make a POST request to share the specifed items
    shareItemRequest =  requests.post(itemURL, data = params_)  

    #print("Item {} shared to group(s): {}...".format(itemId_, groupId_))

#########################
## Establish variables ##
#########################

con = db.connect(sdw_con)
cur = con.cursor()

agol_users = GetData(sdw_con, sdw_getUsers)
print("- Generating a list of users to be migrated.")
print("- There are {} users that will be migrated.".format(len(agol_users)))

oldUser = None
newUser = None
oldUsername = None
newUsername = None

token = GenerateToken(baseURL)
print("- Generated token: {}".format(token))

organizationGroups = GetAllOrgGroups(baseURL, orgId, token)

#sys.exit()

##################################################
## City of Charlotte AGOL licensing information ##
##################################################

purchases = GetPurchases(baseURL, token)
print("- Obtained ArcGIS Online Purchases")

listings = GetListings(token, purchases)
print("- Obtained ArcGIS Online Listings: {}".format(listings))

####################################################
## Loop through the user returned user dictionary ##
####################################################

print("\n======================================================================\n" + 
       "Beginning to migrate user's groups, folders, content, and entitlements\n" +
       "======================================================================")

for row in agol_users:
    title = row[1]
    upn = row[2]
    old_upn = row[3]
    domain = row[4]

    oldUsername = None
    oldUser = None
    newUser = None
    oldUsername = old_upn
    oldUser = gis.users.get(oldUsername)

    newUserInfo = GetNewUserInformation(row, oldUser)

    #############################################################
    ## Precursor variables to backup the oldUser's information ##
    #############################################################

    # Get groups that the user is involved with
    #userGroups = GetUserGroups(oldUser, oldUsername)
    userGroups = oldUser['groups']
    # Get user content
    userContent = GetUserContent(oldUser)

    ######################################
    ## Backup the oldUser's information ##
    ######################################

    ### There will be a series of functions that writes the user's information to CSV files

    ##################################################################################
    ## (1.) Check to see if the new user already exists                             ##
    ## (2.) Create the new user if the user does not already exist                  ##
    ## (3.) Assign the new user to the correct role                                 ##
    ## (4.) Assign the new user with the same amount of credits as the old user     ##
    ## (5.) Remove licenses from the old user                                       ##
    ##################################################################################

    if(UserDoesExist(newUserInfo['username']) == 0):
        try:
            CreateNewUser(oldUser, newUserInfo)
            print("- Account has been created for {}".format(newUserInfo['username']))
        except Exception:
            print("- Something went wrong while trying to create {}'s account.\n".format(newUserInfo['username']))
            print("- Unexpected Error: " , sys.exc_info()[0])
            raise
    else:
        print("- Account has already been created for {}, moving to the next step.\n".format(newUserInfo['username']))

    print("- Setting {} entitlements.".format(newUserInfo['username']))
    #continueScript(con)
    ####################################################################################
    ## (6.) Check to see if the new user already exists                               ##
    ## (7.) Get user entitlements                                                     ##
    ## (8.) Set user entitlements                                                     ##
    ####################################################################################

    if(UserDoesExist(newUserInfo['username']) == 1):
        #Get user entitlements
        userEntitlements = GetUserEntitlements(baseURL, listings, oldUser['username'], token)

        try:
            #Set user entitlements
            SetUserEntitlements(baseURL,userEntitlements, newUserInfo['username'], token)
            print('- Entitlements set for {}\n.'.format(newUserInfo['username']))
        except Exception:
            print("- Failed to set user entitlements for {}\n.".format(newUserInfo['username']))
            print("- Unexpected Error: " , sys.exc_info()[0])
            raise
    
    print("- Migrating {} content.".format(newUserInfo['username']))
    #continueScript(con)
    ####################################################################################
    ## (9.) Unshare user content                                                      ##
    ## (10.) Transfer content ownership from the old user to the new user             ##
    ## (11.) Reshare content back to it's original groups                             ##
    ## (12.) Recreate the new user's content folder structure to match the old user's ##
    ## (13.) Assign the new user's content to the appropriate folders                 ##
    ## (14.) Remove the old user's account                                            ##
    ####################################################################################

    #Verify that the new user was created successfully
    if(UserDoesExist(newUserInfo['username']) == 1):
        # Create the new user's folder stucture
        # Assign the new user's content the appropriate folder
        try:
            RecreateFolderStructure(oldUser, newUserInfo['username'])
        except Exception:
            print("- Failed to either create the {}'s folder structure or assign the content to the appropriate folder.")
            print("- Unexpected Error: " , sys.exc_info()[0])
            raise

        # Unshare user content
        # Transfer content ownership
        # Reshare content back to original groups
        try:
            ReassignUserContent(oldUsername, newUserInfo['username'], token)
        except Exception:
            print("- Failed to reassign {} content to {}.".format(oldUser['username'], newUserInfo['username']))
            print("- Unexpected Error: " , sys.exc_info()[0])
            raise
    
    print("- Migrating {} groups.".format(newUserInfo['username']))
    #continueScript(con)
    ##############################################################################
    ## (15.) Check to see if the new user account was created                   ##
    ## (16.) Add the new user to the same groups that the old user is a part of ##
    ## (17.) Transfer group ownership from the old user to the new user         ##
    ## (18.) Remove the the old user from all groups                            ##
    ## (19.) Update the user migration status to true                           ##
    ##############################################################################

    #Verify that the new user was created successfully
    if(UserDoesExist(newUserInfo['username']) == 1):
        #Add the new user to the same groups as the old user
        try:
            #print(newUserInfo['username'])
            AssignGroupMembership(oldUser, newUserInfo, organizationGroups)
        except Exception:
            print("- Something went wrong when adding the new user to groups.")
            print("- Unexpected Error: " , sys.exc_info()[0])
            raise
        
        #Transfer group ownership from the old user to the new user
        try:
            AssignGroupOwnership(oldUser, oldUser['username'], newUserInfo['username'], organizationGroups)
        except Exception:
            print("- Something went wrong when transferring group ownership from the old user to the new user.")
            print("- Unexpected Error: " , sys.exc_info()[0])
            raise

        # #Remove old user from groups
        # try:
        #     RemoveGroupMembership(oldUser, oldUser['username'], baseURL, token)
        # except Exception:
        #     print("Could not remove {} from groups...".format(oldUser))
        #     print("Unexpected Error: " , sys.exc_info()[0])
        #     raise

        #Update user status to reflect that the user has been migrated
        try:
            MarkUserAsMigrated(cur,con, "AGOL_Migrated", upn)
            print("\n- {} has been successfully migrated.\n".format(upn))
        except Exception:
            print("- Something went wrong when setting the user migration status to true.")
            print("- Unexpected Error: " , sys.exc_info()[0])
            raise
        
cur.close()
con.close()

del con, cur

    

                
