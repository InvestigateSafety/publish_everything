from time import strptime, strftime, mktime, gmtime
import csv,os,requests,json,re
from python_postgres import *
from datetime import datetime, timezone

class CINSERTDB():

    def __init__(self):
        self.postgres = POSTGRESAPI()
        self.debug = True

    def writing_csv(self,data,csv_filename):

        myFile = open(csv_filename, 'w')
        with myFile:
            writer = csv.writer(myFile)
            writer.writerows(data)

        return csv_filename

    def reading_csv(self,csv_filename):
        f = open(csv_filename,'r')
        csv_data = []
        reader = csv.reader(f)
        for row in reader:
            csv_data.append(row)
         
        f.close()
        return csv_data 

    def write_json_file(self,data,filename):
        with open(filename, 'w') as outfile:
            json.dump(data, outfile,indent=4)

    def read_json_file(self,filename):
        data = {}
        with open(filename) as json_data:
            data = json.load(json_data)
        return data

    def is_file_exist(self,filename):
        if os.path.exists(filename):
            return True
        else:
            return False

    def splitting_names(self,name):
        newname=''
        for i in range(len(name)):
            if name[i].isupper():
                newname += '_' + name[i].lower()
            else:
                newname += name[i]
        return newname

    def preparing_data(self,visits_data):
        config = self.read_json_file('./conf.json')
        if self.postgres.connect_db(config['dbname'],config['user'],config['password'],config['host']):

            if self.debug:
                print("*"*50)
                print("Total Data to Insert: ", len(visits_data))

            for i in range(len(visits_data)):
                try:
                    if self.debug:
                        print("Current Data Processing: ", i , ' / ', len(visits_data))
                    print(json.dumps(visits_data[i],indent=4))
                    ip_whois = ''
                    org = ''
                    org_domain = ''
                    is_ip_exists = None

                    ip = visits_data[i]['visitIp']
                    is_ip_exists = self.postgres.is_ip_present(ip)

                    if is_ip_exists is None:
                        if self.debug:
                            print("[WARNING] Ip not present in DB so doing whois: ",ip)

                        try:
                            ip_whois = os.popen('whois %s'%(ip)).read()
                        except:
                            ip_whois = ''
                        try:
                            org = re.search('([Oo]rganization|org\-name|descr):\s+(?P<org>.*)', ip_whois).group('org')
                        except:
                            org = ''
                        try:
                            org_domain = re.search("@(?P<domain>[\-\w.]+)", ip_whois).group('domain')
                        except:
                            org_domain = ''

                        print(org , ' : ', org_domain, ' : ' , '.gov' in ip_whois)
                    else:
                        pass
                        if self.debug:
                            print("Ip information present in DB: ",is_ip_exists)
                        org = is_ip_exists[2]
                        gov_org  = is_ip_exists[3]
                        org_domain = is_ip_exists[4]

                    action_details_id = 0
                    if self.debug:
                        print("Total Actions: ",len(visits_data[i]['actionDetails']) )

                    for action in visits_data[i]['actionDetails']:

                        action_details_id = action_details_id + 1
                        print()
                        print("Current Action Processing: " ,action_details_id , ' / ' , len(visits_data[i]['actionDetails']))

                        idSite =   int(visits_data[i].get("idSite",'0'))
                        idVisit =   int(visits_data[i].get("idVisit",'0'))
                        visitIp =   visits_data[i].get("visitIp",'')
                        visitorId =   visits_data[i].get("visitorId",'')
                        fingerprint =   visits_data[i].get("fingerprint",'')

                        action_type =   action.get("type",'')
                        url =   action.get("url",'')

                        pageTitle = ''
                        if 'pageTitle' in action:
                            if  action["pageTitle"] is not None:
                                pageTitle =   action.get("pageTitle",'')

                        pageIdAction =   int(action.get("pageIdAction",'0'))
                        idpageview =   action.get("idpageview",'')
                        action_serverTimePretty =   action["serverTimePretty"]
                        pageId =   int(action.get("pageId",'0'))
                        action_timeSpent =   action.get("timeSpent",'')
                        timeSpentPretty =   action.get("timeSpentPretty",'')
                        generationTimeMilliseconds =   int(action.get("generationTimeMilliseconds",'0'))

                        generationTime =   action.get("generationTime",'')
                        interactionPosition = 0

                        if 'interactionPosition' in action:
                            if  action["interactionPosition"] is not None:
                                interactionPosition =   int(action.get("interactionPosition",'0'))

                            

                        title =   action.get("title",'')
                        subtitle =   action.get("subtitle",'').replace('"','').replace("'",'')
                        #print(subtitle)
                        icon =   action.get("icon",'')
                        iconSVG =   action.get("iconSVG",'')

                        action_timestamp =   action.get("timestamp",'')
                        action_timestamp = datetime.fromtimestamp(action_timestamp, timezone.utc)

                        unique_index = visits_data[i].get("visitorId",'') + '_' + str(action.get("timestamp",''))
                        goalConversions = int(visits_data[i].get("goalConversions",'0'))
                        siteCurrency =  visits_data[i].get("siteCurrency",'')
                        siteCurrencySymbol =  visits_data[i].get("siteCurrencySymbol",'')
                        serverDate =  visits_data[i].get("serverDate",'')

                        visitServerHour = int(visits_data[i].get("visitServerHour",'0'))
                        lastActionTimestamp = int(visits_data[i].get("lastActionTimestamp",'0'))
                        lastActionDateTime = visits_data[i].get("lastActionDateTime",'')
                        siteName  = visits_data[i].get("siteName",'')

                        serverTimestamp   = visits_data[i].get("serverTimestamp",'')
                        serverTimestamp = datetime.fromtimestamp(serverTimestamp, timezone.utc)

                        firstActionTimestamp    = visits_data[i].get("firstActionTimestamp",'')
                        firstActionTimestamp = datetime.fromtimestamp(firstActionTimestamp, timezone.utc)

                        serverTimePretty   = visits_data[i].get("serverTimePretty",'')
                        serverDatePretty = visits_data[i].get("serverDatePretty",'')
                        serverDatePrettyFirstAction  = visits_data[i].get("serverDatePrettyFirstAction",'')
                        serverTimePrettyFirstAction  = visits_data[i].get("serverTimePrettyFirstAction",'')
                        userId  = visits_data[i].get("userId",'')
                        visitorType = visits_data[i].get("visitorType",'')
                        visitorTypeIcon  = visits_data[i].get("visitorTypeIcon",'')
                        visitConverted = int(visits_data[i].get("visitConverted",'0'))
                        visitConvertedIcon = visits_data[i].get("visitConvertedIcon",'')
                        visitCount = int(visits_data[i].get("visitCount",'0'))
                        visitEcommerceStatus = visits_data[i].get("visitEcommerceStatus",'')
                        visitEcommerceStatusIcon= visits_data[i].get("visitEcommerceStatusIcon",'')
                        daysSinceFirstVisit= int(visits_data[i].get("daysSinceFirstVisit",'0'))
                        daysSinceLastEcommerceOrder = int(visits_data[i].get("daysSinceLastEcommerceOrder",'0'))
                        visitDuration = int(visits_data[i].get("visitDuration",'0'))
                        visitDurationPretty = visits_data[i].get("visitDurationPretty",'')
                        searches= int(visits_data[i].get("searches",'0'))
                        actions= int(visits_data[i].get("actions",'0'))
                        interactions= int(visits_data[i].get("interactions",'0'))
                        referrerType= visits_data[i].get("referrerType",'')
                        referrerTypeName= visits_data[i].get("referrerTypeName",'')
                        referrerName= visits_data[i].get("referrerName",'')
                        referrerKeyword= visits_data[i].get("referrerKeyword",'')
                        referrerKeywordPosition = visits_data[i].get("referrerKeywordPosition",'')
                        referrerUrl= visits_data[i].get("referrerUrl",'')
                        referrerSearchEngineUrl= visits_data[i].get("referrerSearchEngineUrl",'')
                        referrerSearchEngineIcon= visits_data[i].get("referrerSearchEngineIcon",'')
                        referrerSocialNetworkUrl= visits_data[i].get("referrerSocialNetworkUrl",'')
                        referrerSocialNetworkIcon = visits_data[i].get("referrerSocialNetworkIcon",'')
                        languageCode= visits_data[i].get("languageCode",'')
                        language= visits_data[i].get("language",'')
                        deviceType= visits_data[i].get("deviceType",'')
                        deviceTypeIcon= visits_data[i].get("deviceTypeIcon",'')
                        deviceBrand= visits_data[i].get("deviceBrand",'')
                        deviceModel= visits_data[i].get("deviceModel",'')
                        operatingSystem= visits_data[i].get("operatingSystem",'')
                        operatingSystemName= visits_data[i].get("operatingSystemName",'')
                        operatingSystemIcon= visits_data[i].get("operatingSystemIcon",'')
                        operatingSystemCode= visits_data[i].get("operatingSystemCode",'')
                        operatingSystemVersion= visits_data[i].get("operatingSystemVersion",'')
                        browserFamily= visits_data[i].get("browserFamily",'')
                        browserFamilyDescription= visits_data[i].get("browserFamilyDescription",'')
                        browser= visits_data[i].get("browser",'')
                        browserName= visits_data[i].get("browserName",'')
                        browserIcon= visits_data[i].get("browserIcon",'')
                        browserCode= visits_data[i].get("browserCode",'')
                        browserVersion= visits_data[i].get("browserVersion",'')
                        totalEcommerceRevenue = visits_data[i].get("totalEcommerceRevenue",'')
                        totalEcommerceConversions= int(visits_data[i].get("totalEcommerceConversions",'0'))
                        totalEcommerceItems= int(visits_data[i].get("totalEcommerceItems",'0'))
                        totalAbandonedCartsRevenue= visits_data[i].get("totalAbandonedCartsRevenue",'')
                        totalAbandonedCarts= int(visits_data[i].get("totalAbandonedCarts",'0'))
                        totalAbandonedCartsItems= int(visits_data[i].get("totalAbandonedCartsItems",'0'))
                        events = int(visits_data[i].get("events",'0'))
                        continent= visits_data[i].get("continent",'')
                        continentCode= visits_data[i].get("continentCode",'')
                        country= visits_data[i].get("country",'')
                        countryCode= visits_data[i].get("countryCode",'')
                        countryFlag= visits_data[i].get("countryFlag",'')
                        region= visits_data[i].get("region",'')
                        regionCode= visits_data[i].get("regionCode",'')
                        city= visits_data[i].get("city",'')
                        location= visits_data[i].get("location",'')
                        latitude= visits_data[i].get("latitude",'')
                        longitude= visits_data[i].get("longitude",'')
                        visitLocalTime= visits_data[i].get("visitLocalTime",'')
                        visitLocalHour= int(visits_data[i].get("visitLocalHour",'0'))
                        daysSinceLastVisit= int(visits_data[i].get("daysSinceLastVisit",'0'))
                        customVariables= visits_data[i].get("customVariables",'')
                        resolution= visits_data[i].get("resolution",'')
                        plugins= visits_data[i].get("plugins",'')
                        if visits_data[i]['pluginsIcons'] is not None:
                            if len(visits_data[i].get('pluginsIcons','')) > 0:
                                pluginsIcons_pluginIcon= visits_data[i].get('pluginsIcons','')[0].get("pluginIcon",'')
                                pluginsIcons_pluginName = visits_data[i].get('pluginsIcons','')[0].get("pluginName",'')
                            else:
                                pluginsIcons_pluginIcon = ''
                                pluginsIcons_pluginName = ''
                        else:
                            pluginsIcons_pluginIcon = ''
                            pluginsIcons_pluginName = ''
                        experiments = visits_data[i].get("experiments",'')
                        formConversions = int(visits_data[i].get("formConversions",'0'))
                        sessionReplayUrl= visits_data[i].get("sessionReplayUrl",'')
                        campaignId= visits_data[i].get("campaignId",'')
                        campaignContent= visits_data[i].get("campaignContent",'')
                        campaignKeyword= visits_data[i].get("campaignKeyword",'')
                        campaignMedium= visits_data[i].get("campaignMedium",'')
                        campaignName= visits_data[i].get("campaignName",'')
                        campaignSource= visits_data[i].get("campaignSource",'')
                        gov_org = str(int('.gov' in ip_whois))

                        is_data_present = self.postgres.is_data_present(unique_index)
                        if is_data_present is None: 
                            if self.debug:
                                print("Inserting New Data in DB: ", unique_index)
                            self.postgres.insert_data(idSite, idVisit, visitIp ,visitorId ,fingerprint ,action_type ,url ,pageTitle ,
                                                pageIdAction ,idpageview ,action_serverTimePretty ,pageId ,action_timeSpent ,timeSpentPretty ,
                                                generationTimeMilliseconds, generationTime ,interactionPosition ,title ,subtitle ,icon ,
                                                iconSVG ,action_timestamp ,unique_index,goalConversions ,siteCurrency ,siteCurrencySymbol ,
                                                serverDate , visitServerHour ,lastActionTimestamp ,lastActionDateTime, siteName ,serverTimestamp,
                                                firstActionTimestamp ,serverTimePretty ,serverDatePretty , serverDatePrettyFirstAction ,serverTimePrettyFirstAction ,userId ,visitorType ,
                                                visitorTypeIcon ,visitConverted ,visitConvertedIcon ,visitCount ,visitEcommerceStatus ,visitEcommerceStatusIcon ,daysSinceFirstVisit ,
                                                daysSinceLastEcommerceOrder ,visitDuration ,visitDurationPretty ,searches ,actions ,interactions ,referrerType ,referrerTypeName ,
                                                referrerName ,referrerKeyword ,referrerKeywordPosition ,referrerUrl ,referrerSearchEngineUrl ,referrerSearchEngineIcon ,referrerSocialNetworkUrl ,
                                                referrerSocialNetworkIcon ,languageCode ,language ,deviceType ,deviceTypeIcon ,deviceBrand ,deviceModel ,operatingSystem ,operatingSystemName
                                                ,operatingSystemIcon ,operatingSystemCode ,operatingSystemVersion ,browserFamily , browserFamilyDescription ,browser, browserName
                                                ,browserIcon ,browserCode ,browserVersion ,totalEcommerceRevenue ,totalEcommerceConversions , totalEcommerceItems ,totalAbandonedCartsRevenue
                                                ,totalAbandonedCarts ,totalAbandonedCartsItems ,events ,continent,continentCode, country,countryCode
                                                ,countryFlag ,region,regionCode ,city , location, latitude,longitude,visitLocalTime, visitLocalHour ,daysSinceLastVisit ,customVariables
                                                ,resolution ,plugins ,pluginsIcons_pluginIcon,pluginsIcons_pluginName ,experiments ,formConversions
                                                ,sessionReplayUrl , campaignId ,campaignContent ,campaignKeyword ,campaignMedium ,campaignName ,campaignSource ,org,gov_org,
                                                org_domain,action_details_id)
                            
                            self.postgres.commit_api()
                        else:
                            pass
                            if self.debug:
                                print("[WARNING]Data already present in DB: ", unique_index)
                except:
                    print("[ERROR] caught an error and skipping current action")
                print("*"*50)
                print()
            self.postgres.close()

    def csv_sql(self,visits_data):

        if self.debug:
            print("*"*50)
            print("Total Data to Insert: ", len(visits_data))
            sub_list = ['idSite','idVisit','visitIp','visitorId','fingerprint','action_type','url','pageTitle','pageIdAction',
                        'idPageView','action_serverTimePretty','pageId','timeSpent','timeSpentPretty','generationTimeMilliseconds',
                        'generationTime','interactionPosition','title', 'subtitle','icon','icon_svg','timestamp','unique_index','goalConversions','siteCurrency','siteCurrencySymbol','serverDate',
                        'visitServerHour', 'lastActionTimestamp','lastActionDateTime','siteName','serverTimestamp','firstActionTimestamp','serverTimePretty',
                        'serverDatePretty','serverDatePrettyFirstAction', 'serverTimePrettyFirstAction','userId','visitorType','visitorTypeIcon','visitConverted','visitConvertedIcon',
                        'visitCount','visitEcommerceStatus','visitEcommerceStatusIcon', 'daysSinceFirstVisit','daysSinceLastEcommerceOrder','visitDuration','visitDurationPretty','searches','actions',
                        'interactions','referrerType','referrerTypeName', 'referrerName','referrerKeyword','referrerKeywordPosition','referrerUrl','referrerSearchEngineUrl',
                        'referrerSearchEngineIcon','referrerSocialNetworkUrl','referrerSocialNetworkIcon','languageCode', 'language','deviceType','deviceTypeIcon','deviceBrand', 'deviceModel','operatingSystem','operatingSystemName','operatingSystemIcon','operatingSystemCode',
                        'operatingSystemVersion', 'browserFamily','browserFamilyDescription', 'browser','browserName','browserIcon','browserCode','browserVersion','totalEcommerceRevenue','totalEcommerceConversions',
                        'totalEcommerceItems', 'totalAbandonedCartsRevenue','totalAbandonedCarts','totalAbandonedCartsItems','events','continent','continent_code','country','country_code','countryFlag',
                        'region','regionCode', 'city','location','latitude','longitude','visitLocalTime','visitLocalHour','daysSinceLastVisit','customVariables','resolution',
                        'plugins','pluginsIcons_pluginIcon','pluginsIcons_pluginName', 'experiments','formConversions', 'sessionReplayUrl',
                        'campaignId', 'campaignContent', 'campaignKeyword', 'campaignMedium', 'campaignName', 'campaignSource', 'org',
                        'gov_org','org_domain','action_details_id']
            print('here: ',len(sub_list))
            new_sub_list = []
            for index in range(len(sub_list)):
                new_sub_list.append(self.splitting_names(sub_list[index]))
            list_csv = [[]]
            list_csv.append(new_sub_list)

        for i in range(1):#(len(visits_data)):
            if self.debug:
                print("Current Data Processing: ", i , ' / ', len(visits_data))
            #print(json.dumps(visits_data[i],indent=4))
            ip_whois = ''
            org = ''
            org_domain = ''
            is_ip_exists = None

            ip = visits_data[i]['visitIp']

            if is_ip_exists is None:
                if self.debug:
                    print("[WARNING] Ip not present in DB so doing whois: ",ip)

                try:
                    ip_whois = os.popen('whois %s'%(ip)).read()
                except:
                    ip_whois = ''
                try:
                    org = re.search('([Oo]rganization|org\-name|descr):\s+(?P<org>.*)', ip_whois).group('org')
                except:
                    org = ''
                try:
                    org_domain = re.search("@(?P<domain>[\-\w.]+)", ip_whois).group('domain')
                except:
                    org_domain = ''

                print(org , ' : ', org_domain, ' : ' , '.gov' in ip_whois)
            else:
                pass
                if self.debug:
                    print("Ip information present in DB: ",is_ip_exists)
                    org = is_ip_exists[2]
                    gov_org  = is_ip_exists[3]
                    org_domain = is_ip_exists[4]

            action_details_id = 0
            if self.debug:
                print("Total Actions: ",len(visits_data[i]['actionDetails']) )

            for action in visits_data[i]['actionDetails']:

                action_details_id = action_details_id + 1
                print()
                print("Current Action Processing: " ,action_details_id , ' / ' , len(visits_data[i]['actionDetails']))

                sub_list = []
                sub_list.append(visits_data[i].get("idSite",''))
                sub_list.append(visits_data[i].get("idVisit",''))
                sub_list.append(visits_data[i].get("visitIp",''))
                sub_list.append(visits_data[i].get("visitorId",''))
                sub_list.append(visits_data[i].get("fingerprint",''))
                sub_list.append(action.get("type",''))
                sub_list.append(action.get("url",''))
                sub_list.append(action.get("pageTitle",''))
                sub_list.append(action.get("pageIdAction",''))
                sub_list.append(action.get("idpageview",''))
                sub_list.append(action["serverTimePretty"])
                sub_list.append(action.get("pageId",''))
                sub_list.append(action.get("timeSpent",''))
                sub_list.append(action.get("timeSpentPretty",''))
                sub_list.append(action.get("generationTimeMilliseconds",''))
                sub_list.append(action.get("generationTime",''))
                sub_list.append(action.get("interactionPosition",''))
                sub_list.append(action.get("title",''))
                sub_list.append(action.get("subtitle",'').replace('"','').replace("'",''))
                sub_list.append(action.get("icon",''))
                sub_list.append(action.get("iconSVG",''))
                sub_list.append(action.get("timestamp",''))
                sub_list.append(visits_data[i].get("visitorId",'') + '_' + str(action.get("timestamp",'')))
                sub_list.append(visits_data[i].get("goalConversions",''))
                sub_list.append(visits_data[i].get("siteCurrency",''))
                sub_list.append(visits_data[i].get("siteCurrencySymbol",''))
                sub_list.append(visits_data[i].get("serverDate",''))
                sub_list.append(visits_data[i].get("visitServerHour",''))
                sub_list.append(visits_data[i].get("lastActionTimestamp",''))
                sub_list.append(visits_data[i].get("lastActionDateTime",''))
                sub_list.append(visits_data[i].get("siteName",''))
                sub_list.append(visits_data[i].get("serverTimestamp",''))
                sub_list.append(visits_data[i].get("firstActionTimestamp",''))
                sub_list.append(visits_data[i].get("serverTimePretty",''))
                sub_list.append(visits_data[i].get("serverDatePretty",''))
                sub_list.append(visits_data[i].get("serverDatePrettyFirstAction",''))
                sub_list.append(visits_data[i].get("serverTimePrettyFirstAction",''))
                sub_list.append(visits_data[i].get("userId",''))
                sub_list.append(visits_data[i].get("visitorType",''))
                sub_list.append(visits_data[i].get("visitorTypeIcon",''))
                sub_list.append(visits_data[i].get("visitConverted",''))
                sub_list.append(visits_data[i].get("visitConvertedIcon",''))
                sub_list.append(visits_data[i].get("visitCount",''))
                sub_list.append(visits_data[i].get("visitEcommerceStatus",''))
                sub_list.append(visits_data[i].get("visitEcommerceStatusIcon",''))
                sub_list.append(visits_data[i].get("daysSinceFirstVisit",''))
                sub_list.append(visits_data[i].get("daysSinceLastEcommerceOrder",''))
                sub_list.append(visits_data[i].get("visitDuration",''))
                sub_list.append(visits_data[i].get("visitDurationPretty",''))
                sub_list.append(visits_data[i].get("searches",''))
                sub_list.append(visits_data[i].get("actions",''))
                sub_list.append(visits_data[i].get("interactions",''))
                sub_list.append(visits_data[i].get("referrerType",''))
                sub_list.append(visits_data[i].get("referrerTypeName",''))
                sub_list.append(visits_data[i].get("referrerName",''))
                sub_list.append(visits_data[i].get("referrerKeyword",''))
                sub_list.append(visits_data[i].get("referrerKeywordPosition",''))
                sub_list.append(visits_data[i].get("referrerUrl",''))
                sub_list.append(visits_data[i].get("referrerSearchEngineUrl",''))
                sub_list.append(visits_data[i].get("referrerSearchEngineIcon",''))
                sub_list.append(visits_data[i].get("referrerSocialNetworkUrl",''))
                sub_list.append(visits_data[i].get("referrerSocialNetworkIcon",''))
                sub_list.append(visits_data[i].get("languageCode",''))
                sub_list.append(visits_data[i].get("language",''))
                sub_list.append(visits_data[i].get("deviceType",''))
                sub_list.append(visits_data[i].get("deviceTypeIcon",''))
                sub_list.append(visits_data[i].get("deviceBrand",''))
                sub_list.append(visits_data[i].get("deviceModel",''))
                sub_list.append(visits_data[i].get("operatingSystem",''))
                sub_list.append(visits_data[i].get("operatingSystemName",''))
                sub_list.append(visits_data[i].get("operatingSystemIcon",''))
                sub_list.append(visits_data[i].get("operatingSystemCode",''))
                sub_list.append(visits_data[i].get("operatingSystemVersion",''))
                sub_list.append(visits_data[i].get("browserFamily",''))
                sub_list.append(visits_data[i].get("browserFamilyDescription",''))
                sub_list.append(visits_data[i].get("browser",''))
                sub_list.append(visits_data[i].get("browserName",''))
                sub_list.append(visits_data[i].get("browserIcon",''))
                sub_list.append(visits_data[i].get("browserCode",''))
                sub_list.append(visits_data[i].get("browserVersion",''))
                sub_list.append(visits_data[i].get("totalEcommerceRevenue",''))
                sub_list.append(visits_data[i].get("totalEcommerceConversions",''))
                sub_list.append(visits_data[i].get("totalEcommerceItems",''))
                sub_list.append(visits_data[i].get("totalAbandonedCartsRevenue",''))
                sub_list.append(visits_data[i].get("totalAbandonedCarts",''))
                sub_list.append(visits_data[i].get("totalAbandonedCartsItems",''))
                sub_list.append(visits_data[i].get("events",''))
                sub_list.append(visits_data[i].get("continent",''))
                sub_list.append(visits_data[i].get("continentCode",''))
                sub_list.append(visits_data[i].get("country",''))
                sub_list.append(visits_data[i].get("countryCode",''))
                sub_list.append(visits_data[i].get("countryFlag",''))
                sub_list.append(visits_data[i].get("region",''))
                sub_list.append(visits_data[i].get("regionCode",''))
                sub_list.append(visits_data[i].get("city",''))
                sub_list.append(visits_data[i].get("location",''))
                sub_list.append(visits_data[i].get("latitude",''))
                sub_list.append(visits_data[i].get("longitude",''))
                sub_list.append(visits_data[i].get("visitLocalTime",''))
                sub_list.append(visits_data[i].get("visitLocalHour",''))
                sub_list.append(visits_data[i].get("daysSinceLastVisit",''))
                sub_list.append(visits_data[i].get("customVariables",''))
                sub_list.append(visits_data[i].get("resolution",''))
                sub_list.append(visits_data[i].get("plugins",''))
                if visits_data[i]['pluginsIcons'] is not None:
                    if len(visits_data[i].get('pluginsIcons','')) > 0:
                        pluginsIcons_pluginIcon =  visits_data[i].get('pluginsIcons','')[0].get("pluginIcon",'')
                        pluginsIcons_pluginName = visits_data[i].get('pluginsIcons','')[0].get("pluginName",'')                              
                    else:
                        pluginsIcons_pluginIcon = ''                            
                        pluginsIcons_pluginName = ''                           
                else:
                    pluginsIcons_pluginIcon = ''                                 
                    pluginsIcons_pluginName  = ''  
                sub_list.append(pluginsIcons_pluginName)
                sub_list.append(pluginsIcons_pluginIcon)                              
                sub_list.append(visits_data[i].get("experiments",''))
                sub_list.append(visits_data[i].get("formConversions",''))
                sub_list.append(visits_data[i].get("sessionReplayUrl",''))
                sub_list.append(visits_data[i].get("campaignId",''))
                sub_list.append(visits_data[i].get("campaignContent",''))
                sub_list.append(visits_data[i].get("campaignKeyword",''))
                sub_list.append(visits_data[i].get("campaignMedium",''))
                sub_list.append(visits_data[i].get("campaignName",''))
                sub_list.append(visits_data[i].get("campaignSource",''))
                sub_list.append(org)
                sub_list.append('.gov' in ip_whois)
                sub_list.append(org_domain)
                sub_list.append('1')

                list_csv.append(sub_list)
                print(len(sub_list))

        self.writing_csv(list_csv,"for_sql.csv")

    def write_log(self):
        config = self.read_json_file('./conf.json')
        auth_token = config['TOKENAPI']
        while 1:
            visits = requests.get('https://investigatesafety.matomo.cloud/?module=API&method=Live.getLastVisitsDetails&idSite=1&filter_limit=10000&date=today&period=month&format=JSON&token_auth='+auth_token).json()
            if self.debug:
                print('Current URL: https://investigatesafety.matomo.cloud/?module=API&method=Live.getLastVisitsDetails&idSite=1&filter_limit=-1&date=today&period=month&format=JSON&token_auth='+auth_token)
            #print(json.dumps(visits,indent=4))
            #write_json_file(visits,'visits.json')
            self.preparing_data(visits)
            time.sleep(10)
            #self.csv_sql(visits)
            #break

        

if __name__ == "__main__":
    db_handle = CINSERTDB()
    db_handle.write_log()
    #print(db_handle.splitting_names("workINGNow"))