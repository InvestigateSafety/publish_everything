import psycopg2
from psycopg2 import sql
from datetime import datetime
import json
from psycopg2.extras import json, Json

class POSTGRESAPI:

	
	def __init__(self):
		conn = ''
		cur = ''

	def connect_db(self,dbname,user,password,host):

		if len(dbname) < 1  or len(user) < 1 or len(host) < 1 or len(password) < 1:
			print("Please check DB credentials")
			return False

		connect_str = ('dbname={} user={} host={} password={}'.format(dbname,user,host,password))
		print ("Connecting to Database.....")

		self.conn = psycopg2.connect(connect_str)
		self.cur = self.conn.cursor()
		return True

	def commit_api(self):
		print("Committing the changes.....")
		pass
		self.conn.commit()

	def is_data_present(self,call_id,session_id):
		command = "SELECT id from ringcentral_communication_log where call_id like '{}' and sessionid like '{}';".format(call_id,session_id)
		#print(command)
		self.cur.execute(command)
		isIPExists = self.cur.fetchone()

		return isIPExists

	def updating_json_data(self,call_id,session_id,json_data):
		command = "UPDATE ringcentral_communication_log set transcription_json = %s where call_id like '%s' and sessionid like '%s';" % (Json(json_data),call_id,session_id)
		#print(command)
		self.cur.execute(command)

		return True

	def insert_data(self,uri,call_id,sessionId,startTime,duration,call_type,direction,action,
					result,to_name,to_phoneNumber,to_extensionId,to_extensionNumber,to_location,
					from_name,from_phoneNumber,from_extensionId,from_extensionNumber,from_location,
					recording_uri,recording_id,recording_type,recording_contentUri,extension_uri,
					extension_id,reason,reasonDescription,transport,lastModifiedTime,billing_costIncluded,billing_costPurchased,
					legs_list, message_uri,message_id,message_type,master,transcription_json,notes):

	
		command = "INSERT INTO ringcentral_communication_log (uri,call_id,sessionid,starttime,duration,type,direction,action, result,to_name,to_phonenumber,to_extensionid,to_extensionnumber,to_location, from_name,from_phonenumber,from_extensionid,from_extensionnumber,from_location, recording_uri,recording_id,recording_type,recording_contenturi,extension_uri, extension_id,reason,reasondescription,transport,lastmodifiedtime,billing_costincluded,billing_costpurchased, legs_0_starttime,legs_0_duration,legs_0_type,legs_0_direction,legs_0_action,legs_0_result, legs_0_to_name,legs_0_to_phonenumber,legs_0_to_extensionid,legs_0_to_extensionnumber,legs_0_to_location, legs_0_from_name,legs_0_from_phonenumber,legs_0_from_extensionid,legs_0_from_extensionnumber,legs_0_from_location, legs_0_extension_uri,legs_0_extension_id,legs_0_reason,legs_0_reasondescription,legs_0_transport, legs_0_legtype,legs_0_master,legs_0_message_uri,legs_0_message_id,legs_0_message_type, legs_0_billing_costincluded,legs_0_billing_costpurchased, legs_1_starttime,legs_1_duration,legs_1_type,legs_1_direction,legs_1_action,legs_1_result, legs_1_to_name,legs_1_to_phonenumber,legs_1_to_extensionid,legs_1_to_extensionnumber,legs_1_to_location, legs_1_from_name,legs_1_from_phonenumber,legs_1_from_extensionid,legs_1_from_extensionnumber,legs_1_from_location, legs_1_extension_uri,legs_1_extension_id,legs_1_reason,legs_1_reasondescription,legs_1_transport, legs_1_legtype,legs_1_master,legs_1_message_uri,legs_1_message_id,legs_1_message_type, legs_1_billing_costincluded,legs_1_billing_costpurchased, message_uri,message_id,message_type,master,transcription_json,notes) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');" % (uri,call_id,sessionId,startTime,duration,call_type,direction,action, result,to_name,to_phoneNumber,to_extensionId,to_extensionNumber, to_location, from_name,from_phoneNumber,from_extensionId,from_extensionNumber,from_location, recording_uri,recording_id,recording_type, recording_contentUri,extension_uri, extension_id,reason,reasonDescription,transport,lastModifiedTime,billing_costIncluded, billing_costPurchased, legs_list[0] , legs_list[1] , legs_list[2] , legs_list[3] , legs_list[4] , legs_list[5] , legs_list[6] , legs_list[7] , legs_list[8] , legs_list[9] , legs_list[10] , legs_list[11] , legs_list[12] , legs_list[13] , legs_list[14] , legs_list[15] , legs_list[16] , legs_list[17] , legs_list[18] , legs_list[19] , legs_list[20] , legs_list[21] , legs_list[22] , legs_list[23] , legs_list[24] , legs_list[25] , legs_list[26] , legs_list[27] , legs_list[28] , legs_list[29] , legs_list[30] , legs_list[31] , legs_list[32] , legs_list[33] , legs_list[34] , legs_list[35] , legs_list[36] , legs_list[37] , legs_list[38] , legs_list[39] , legs_list[40] , legs_list[41] , legs_list[42] , legs_list[43] , legs_list[44] , legs_list[45] , legs_list[46] , legs_list[47] , legs_list[48] , legs_list[49] , legs_list[50] , legs_list[51] , legs_list[52] , legs_list[53] , legs_list[54] , legs_list[55] , message_uri,message_id,message_type,master,transcription_json,notes,)

		
		#print(command)
		self.cur.execute(command)

	def close(self):
		self.cur.close()
		self.conn.close()
