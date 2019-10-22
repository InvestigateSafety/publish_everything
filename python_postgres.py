import psycopg2
from psycopg2 import sql
from datetime import datetime
import json

class POSTGRESAPI:

	
	def __init__(self):
		conn = ''
		cur = ''

	def connect_db(self,dbname,user,password,host):

		if len(dbname) < 1  or len(user) < 1 or len(host) < 1 or len(password) < 1:
			print("Please check DB credentials")
			return False

		connect_str = ('dbname={} user={} host={} password={}'.format(dbname,user,host,password))
		print ("Connecting to database: {}".format( connect_str))

		self.conn = psycopg2.connect(connect_str)
		self.cur = self.conn.cursor()
		return True

	def commit_api(self):
		print("Committing the changes.....")
		pass
		self.conn.commit()

	def is_ip_present(self,ip):
		command = "SELECT id,visit_ip,org,gov_org,org_domain from visits_to_investigatesafety1 where visit_ip like '{}';".format(ip)
		#print(command)
		self.cur.execute(command)
		isIPExists = self.cur.fetchone()
		# while isIPExists is not None:
		# 	print('here: ',isIPExists)
		# 	isIPExists= self.cur.fetchone()

		return isIPExists

	def is_data_present(self,unique_index):
		command = "SELECT id from visits_to_investigatesafety1 where unique_index like '{}';".format(unique_index)
		#print(command)
		self.cur.execute(command)
		isIPExists = self.cur.fetchone()

		return isIPExists

	def insert_data(self,idSite, idVisit, visitIp ,visitorId ,fingerprint ,action_type ,url ,pageTitle ,
                    pageIdAction ,idpageview ,action_serverTimePretty ,pageId ,timeSpent ,timeSpentPretty ,
                    generationTimeMilliseconds, generationTime ,interactionPosition ,title ,subtitle ,icon ,
                    iconSVG ,action_timestamp ,unique_index,goalConversions ,siteCurrency ,siteCurrencySymbol ,
                    serverDate , visitServerHour ,lastActionTimestamp ,lastActionDateTime, siteName ,serverTimestamp,
                    firstActionTimestamp ,serverTimePretty ,serverDatePretty , serverDatePrettyFirstAction ,serverTimePrettyFirstAction ,userId ,visitorType ,
                    visitorTypeIcon ,visitConverted ,visitConvertedIcon ,visitCount ,visitEcommerceStatus ,visitEcommerceStatusIcon ,daysSinceFirstVisit ,
                    daysSinceLastEcommerceOrder ,visitDuration ,visitDurationPretty ,searches ,actions ,interactions ,referrerType ,referrerTypeName ,
                    referrerName ,referrerKeyword ,referrerKeywordPosition ,referrerUrl ,referrerSearchEngineUrl ,referrerSearchEngineIcon ,referrerSocialNetworkUrl ,
                    referrerSocialNetworkIcon ,languageCode ,language ,deviceType ,deviceTypeIcon ,deviceBrand ,deviceModel ,operatingSystem ,operatingSystemName
                    ,operatingSystemIcon ,operatingSystemCode ,operatingSystemVersion ,browserFamily ,
                    browserFamilyDescription ,browser, browserName,browserIcon ,browserCode ,browserVersion ,totalEcommerceRevenue ,totalEcommerceConversions ,
                    totalEcommerceItems ,totalAbandonedCartsRevenue ,totalAbandonedCarts ,totalAbandonedCartsItems ,events ,continent,continentCode, country,countryCode
                    ,countryFlag ,region,regionCode ,city , location, latitude,longitude,visitLocalTime,
                    visitLocalHour ,daysSinceLastVisit ,customVariables ,resolution ,plugins ,pluginsIcons_pluginIcon,pluginsIcons_pluginName ,experiments ,formConversions
                    ,sessionReplayUrl , campaignId ,campaignContent ,campaignKeyword ,campaignMedium ,campaignName ,campaignSource ,org,gov_org,
                    org_domain,action_details_id):

		#command = "INSERT INTO visits_to_investigatesafety (idsite, idvisit, visitip ,visitorid ,fingerprint ,action_type ,action_url ,action_pagetitle ,action_pageidaction ,action_idpageview ,action_servertimepretty ,action_pageid ,action_timespent ,action_timespentpretty ,action_generationtimemilliseconds ,action_generationtime ,action_interactionposition,action_title ,action_subtitle ,action_icon ,action_iconsvg ,action_timestamp ,goalconversions ,sitecurrency ,sitecurrencysymbol ,serverdate ,visitserverhour ,lastactiontimestamp ,lastactiondatetime ,sitename ,servertimestamp ,firstactiontimestamp ,servertimepretty ,serverdatepretty ,serverdateprettyfirstaction ,servertimeprettyfirstaction ,userid ,visitortype ,visitortypeicon ,visitconverted ,visitconvertedicon ,visitcount ,visitecommercestatus ,visitecommercestatusicon ,dayssincefirstvisit ,dayssincelastecommerceorder ,visitduration ,visitdurationpretty ,searches ,actions ,interactions ,referrertype ,referrertypename ,referrername ,referrerkeyword ,referrerkeywordposition ,referrerurl ,referrersearchengineurl ,referrersearchengineicon ,referrersocialnetworkurl ,referrersocialnetworkicon ,languagecode ,language ,devicetype ,devicetypeicon ,devicebrand ,devicemodel ,operatingsystem ,operatingsystemname ,operatingsystemicon ,operatingsystemcode ,operatingsystemversion ,browserfamily ,browserfamilydescription ,browser ,browsericon ,browsercode ,browserversion ,totalecommercerevenue ,totalecommerceconversions ,totalecommerceitems ,totalabandonedcartsrevenue ,totalabandonedcarts ,totalabandonedcartsitems ,events ,country ,countryflag ,region ,city ,visitlocalhour ,dayssincelastvisit ,customvariables ,resolution ,plugins ,pluginsicons_pluginname ,experiments ,formconversions ,sessionreplayurl ,campaignid ,campaigncontent ,campaignkeyword ,campaignmedium ,campaignname ,campaignsource ,org,gov_org,org_domain,action_details_id,unique_index) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');" % (idSite, idVisit, visitIp ,visitorId ,fingerprint ,action_type ,url ,pageTitle ,pageIdAction ,idpageview ,action_serverTimePretty ,pageId ,timeSpent ,timeSpentPretty ,generationTimeMilliseconds ,generationTime ,interactionPosition ,title ,subtitle ,icon ,iconSVG ,timestamp_string ,goalConversions ,siteCurrency ,siteCurrencySymbol ,serverDate ,visitServerHour ,lastActionTimestamp ,lastActionDateTime ,siteName ,serverTimestamp ,firstActionTimestamp ,serverTimePretty ,serverDatePretty ,serverDatePrettyFirstAction ,serverTimePrettyFirstAction ,userId ,visitorType ,visitorTypeIcon ,visitConverted ,visitConvertedIcon ,visitCount ,visitEcommerceStatus ,visitEcommerceStatusIcon ,daysSinceFirstVisit ,daysSinceLastEcommerceOrder ,visitDuration ,visitDurationPretty ,searches ,actions ,interactions ,referrerType ,referrerTypeName ,referrerName ,referrerKeyword ,referrerKeywordPosition ,referrerUrl ,referrerSearchEngineUrl ,referrerSearchEngineIcon ,referrerSocialNetworkUrl ,referrerSocialNetworkIcon ,languageCode ,language ,deviceType ,deviceTypeIcon ,deviceBrand ,deviceModel ,operatingSystem ,operatingSystemName ,operatingSystemIcon ,operatingSystemCode ,operatingSystemVersion ,browserFamily ,browserFamilyDescription ,browser ,browserIcon ,browserCode ,browserVersion ,totalEcommerceRevenue ,totalEcommerceConversions ,totalEcommerceItems ,totalAbandonedCartsRevenue ,totalAbandonedCarts ,totalAbandonedCartsItems ,events ,country ,countryFlag ,region ,city ,visitLocalHour ,daysSinceLastVisit ,customVariables ,resolution ,plugins ,pluginsIcons_pluginName ,experiments ,formConversions ,sessionReplayUrl ,campaignId ,campaignContent ,campaignKeyword ,campaignMedium ,campaignName ,campaignSource ,org,gov_org,org_domain,action_details_id,unique_index,)
		command = "INSERT INTO visits_to_investigatesafety1 (id_site, id_visit, visit_ip,visitor_id, fingerprint,action_type,url, page_title,page_id_action,id_page_view,action_server_time_pretty, page_id,time_spent,time_spent_pretty,generation_time_milliseconds, generation_time,interaction_position,title,subtitle,icon,icon_svg,timestamp,unique_index,goal_conversions,site_currency,site_currency_symbol,server_date,visit_server_hour,last_action_timestamp,last_action_date_time,site_name,server_timestamp,first_action_timestamp,server_time_pretty,server_date_pretty,server_date_pretty_first_action,server_time_pretty_first_action,user_id,visitor_type,visitor_type_icon,visit_converted,visit_converted_icon,visit_count,visit_ecommerce_status,visit_ecommerce_status_icon,days_since_first_visit,days_since_last_ecommerce_order,visit_duration,visit_duration_pretty,searches,actions,interactions,referrer_type,referrer_type_name,referrer_name,referrer_keyword,referrer_keyword_position,referrer_url,referrer_search_engine_url,referrer_search_engine_icon,referrer_social_network_url,referrer_social_network_icon,language_code,language,device_type,device_type_icon,device_brand,device_model,operating_system,operating_system_name,operating_system_icon,operating_system_code,operating_system_version,browser_family,browser_family_description,browser,browser_name,browser_icon,browser_code,browser_version,total_ecommerce_revenue,total_ecommerce_conversions,total_ecommerce_items,total_abandoned_carts_revenue,total_abandoned_carts,total_abandoned_carts_items,events,continent,continent_code,country,country_code,country_flag,region,region_code,city,location,latitude,longitude,visit_local_time,visit_local_hour,days_since_last_visit,custom_variables,resolution,plugins,plugins_icons_plugin_icon,plugins_icons_plugin_name,experiments,form_conversions,session_replay_url,campaign_id,campaign_content,campaign_keyword,campaign_medium,campaign_name,campaign_source,org,gov_org,org_domain,action_details_id) VALUES ('%d', '%d', '%s', '%s', '%s', '%s', '%s', '%s', '%d', '%s', '%s', '%d', '%s', '%s', '%d', '%s', '%d', '%s', '%s', '%s', '%s', '%s', '%s', '%d', '%s', '%s', '%s', '%d', '%d', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%d', '%s', '%d', '%s', '%s', '%d', '%d', '%d', '%s', '%d', '%d', '%d', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%d', '%d', '%s', '%d', '%d', '%d', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%d', '%d', '%s', '%s', '%s', '%s', '%s', '%s', '%d', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%d');" % (idSite, idVisit, visitIp ,visitorId ,fingerprint ,action_type ,url ,pageTitle , pageIdAction ,idpageview ,action_serverTimePretty ,pageId ,timeSpent ,timeSpentPretty , generationTimeMilliseconds, generationTime ,interactionPosition ,title ,subtitle ,icon , iconSVG ,action_timestamp ,unique_index,goalConversions ,siteCurrency ,siteCurrencySymbol , serverDate , visitServerHour ,lastActionTimestamp ,lastActionDateTime, siteName ,serverTimestamp, firstActionTimestamp ,serverTimePretty ,serverDatePretty , serverDatePrettyFirstAction ,serverTimePrettyFirstAction ,userId ,visitorType , visitorTypeIcon ,visitConverted ,visitConvertedIcon ,visitCount ,visitEcommerceStatus ,visitEcommerceStatusIcon ,daysSinceFirstVisit , daysSinceLastEcommerceOrder ,visitDuration ,visitDurationPretty ,searches ,actions ,interactions ,referrerType ,referrerTypeName , referrerName ,referrerKeyword ,referrerKeywordPosition ,referrerUrl ,referrerSearchEngineUrl ,referrerSearchEngineIcon ,referrerSocialNetworkUrl , referrerSocialNetworkIcon ,languageCode ,language ,deviceType ,deviceTypeIcon ,deviceBrand ,deviceModel ,operatingSystem ,operatingSystemName ,operatingSystemIcon ,operatingSystemCode ,operatingSystemVersion ,browserFamily , browserFamilyDescription ,browser, browserName,browserIcon ,browserCode ,browserVersion ,totalEcommerceRevenue ,totalEcommerceConversions , totalEcommerceItems ,totalAbandonedCartsRevenue ,totalAbandonedCarts ,totalAbandonedCartsItems ,events ,continent,continentCode, country,countryCode ,countryFlag ,region,regionCode ,city , location, latitude,longitude,visitLocalTime, visitLocalHour ,daysSinceLastVisit ,customVariables ,resolution ,plugins ,pluginsIcons_pluginIcon,pluginsIcons_pluginName ,experiments ,formConversions ,sessionReplayUrl , campaignId ,campaignContent ,campaignKeyword ,campaignMedium ,campaignName ,campaignSource ,org,gov_org, org_domain,action_details_id,)

		
		#print(command)
		self.cur.execute(command)

	def close(self):
		self.cur.close()
		self.conn.close()