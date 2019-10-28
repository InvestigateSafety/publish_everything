import json,urllib.request,os
from ringcentral import SDK
from postgresql_interface import *


class OPENRC():
	def __init__(self):
		self.configuration_filename = './conf.json'
		self.rcsdk = ''
		self.platform = ''
		self.db_handle = CINSERTDB()

	def initialize(self):
		config = self.read_json_file(self.configuration_filename)
		RINGCENTRAL_CLIENTID = config['clientId']
		RINGCENTRAL_CLIENTSECRET = config['clientSecret']
		RINGCENTRAL_SERVER = 'https://platform.devtest.ringcentral.com'

		self.RINGCENTRAL_USERNAME = config['username']
		self.RINGCENTRAL_PASSWORD = config['rc_password']
		self.RINGCENTRAL_EXTENSION = config['extensionId']

		self.rcsdk = SDK(RINGCENTRAL_CLIENTID, RINGCENTRAL_CLIENTSECRET, RINGCENTRAL_SERVER)
		self.platform = self.rcsdk.platform()
	
		return True

	def checking_folder_existence(self,dest_dir):
		if not os.path.exists(dest_dir):
			os.mkdir(dest_dir)
			print("Directory " , dest_dir ,  " Created ")
		else:
			pass
			#print("Directory " , dest_dir ,  " Exists ")

		return dest_dir


	def read_json_file(self,filename):
	    data = {}
	    with open(filename) as json_data:
	        data = json.load(json_data)
	    return data

	def write_json_file(self,data,filename):

	    with open(filename, 'w') as outfile:
	        json.dump(data, outfile,indent=4)


	def login(self):
		try:
			print("Loging in.......")
			result = self.platform.login(self.RINGCENTRAL_USERNAME, self.RINGCENTRAL_EXTENSION, self.RINGCENTRAL_PASSWORD)
			#print(result.json_dict())
			#access_token =  result.json_dict()['access_token']
			return True
		except:
			print("LOGIN ERROR....")
			return False


	def get_call_logs(self,view='Detailed',with_rec=True):
		try:
			params = {
			    'view': view,
			    'withRecording': with_rec,
				'dateFrom': '2019-10-23'
			}
			#print(params)
			resp = self.platform.get('/restapi/v1.0/account/~/extension/~/call-log', params)
			data = resp.json_dict()
			return data
		except:
			return False

	def logout(self):
		try:
			return self.platform.logout()
		except:
			return False

	def refresh_token(self):
		try:
			return self.platform.refresh()
		except:
			return False

	def get_call_recording_metadata(self,resp):
		return resp.raw().info()

	def download_single_call_recording(self,recording_id):
		print("Checking recording calls...")
		audio_file_folder = './audio_files/'
		self.checking_folder_existence(audio_file_folder)
		if recording_id:
			resp = self.platform.get(f'/restapi/v1.0/account/~/recording/{recording_id}/content')
			metadata = self.get_call_recording_metadata(resp)
			file_name = audio_file_folder +  metadata.getlist('Content-Disposition')[0].split('=')[1]
			file_size = int(metadata.getheaders("Content-Length")[0])
			print ("Downloading: %s Bytes: %s" % (file_name, file_size))
			audio_file= open(file_name,"wb")
			audio_file.write(resp.body())
			audio_file.close()

	def download_all_call_recordings(self):
		data = self.get_call_logs()
		#print(json.dumps(data,indent=4))
		for record in range(len(data['records'])):
			recording_id = data['records'][record]['recording']['id']
			content_uri = data['records'][record]['recording']['contentUri']
			self.download_single_call_recording(recording_id)


	def interfaceing_database(self,data):
		self.db_handle.inserting_in_db(data['records'])

	def saving_to_csv(self,data):
		data_dir = self.db_handle.checking_folder_existence('./data/')
		csv_filename = data_dir + 'csv_file.csv'
		for record in data['records']:
			self.db_handle.json_to_csv(record,csv_filename)




if __name__ == "__main__":
	rec = OPENRC()
	if rec.initialize():
		rec.login()
		data = rec.get_call_logs(with_rec=False)
		if data:
			rec.saving_to_csv(data)
			rec.interfaceing_database(data)
			rec.download_all_call_recordings()
