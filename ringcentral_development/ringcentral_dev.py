import json,urllib.request,os,time
from ringcentral import SDK
from postgresql_interface import *
from aws_wasabi_interface import *
import urllib.request

class OPENRC():
	def __init__(self):
		self.configuration_filename = './conf.json'
		self.rcsdk = ''
		self.platform = ''
		self.db_handle = CINSERTDB()
		self.recording_filenames = {}
		self.audio_file_folder = './audio_files/'

	def initialize(self):
		if not self.is_file_exist(self.configuration_filename):
			print("[ERROR] configuration file not present")
			return False

		self.config = self.read_json_file(self.configuration_filename)

		#ringcentral credentials
		RINGCENTRAL_CLIENTID = self.config['clientId']
		RINGCENTRAL_CLIENTSECRET = self.config['clientSecret']
		RINGCENTRAL_SERVER = 'https://platform.devtest.ringcentral.com'

		self.RINGCENTRAL_USERNAME = self.config['username']
		self.RINGCENTRAL_PASSWORD = self.config['rc_password']
		self.RINGCENTRAL_EXTENSION = self.config['extensionId']

		#aws credentials
		self.AWS_ACCESS_KEY = self.config['AWS_ACCESS_KEY']
		self.AWS_SECRET_KEY = self.config['AWS_SECRET_KEY']

		#wasabi credentials
		self.WASABI_ACCESS_KEY = self.config['WASABI_ACCESS_KEY']
		self.WASABI_SECRET_KEY = self.config['WASABI_SECRET_KEY']

		self.rcsdk = SDK(RINGCENTRAL_CLIENTID, RINGCENTRAL_CLIENTSECRET, RINGCENTRAL_SERVER)
		self.platform = self.rcsdk.platform()
	
		return True

	def is_file_exist(self,filename):
		if os.path.exists(filename):
			return True
		else:
			return False

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


	def get_time_stamp(self):
		return time.strftime('%Y-%m-%d')

	def login(self):
		try:
			print("Signing in ringcentral account....")
			result = self.platform.login(self.RINGCENTRAL_USERNAME, self.RINGCENTRAL_EXTENSION, self.RINGCENTRAL_PASSWORD)
			#print(result.json_dict())
			#access_token =  result.json_dict()['access_token']
			return True
		except:
			print("[Error]Signing in error....")
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

		self.checking_folder_existence(self.audio_file_folder)
		if recording_id:
			resp = self.platform.get(f'/restapi/v1.0/account/~/recording/{recording_id}/content')
			metadata = self.get_call_recording_metadata(resp)
			file_name = self.audio_file_folder +  metadata.getlist('Content-Disposition')[0].split('=')[1]
			file_size = int(metadata.getheaders("Content-Length")[0])
			print ("Downloading: %s Bytes: %s" % (file_name, file_size))
			audio_file= open(file_name,"wb")
			audio_file.write(resp.body())
			audio_file.close()
			print ("Finished Downloading: %s Bytes: %s" % (file_name, file_size))
			print()
			return file_name 


	def download_all_call_recordings(self):
		data = self.get_call_logs()
		main_list = []
		#print(json.dumps(data,indent=4))
		for record in range(len(data['records'])):
			sub_dict = {}
			sub_dict['call_id'] = data['records'][record]['id']
			sub_dict['session_id'] = data['records'][record]['sessionId']
			recording_id = data['records'][record]['recording']['id']
			content_uri = data['records'][record]['recording']['contentUri']
			sub_dict['file_name'] = self.download_single_call_recording(recording_id)
			main_list.append(sub_dict)

		self.recording_filenames['data'] = main_list


	def interfaceing_database(self,data):
		self.db_handle.inserting_in_db(data['records'])

	def saving_to_csv(self,data):
		data_dir = self.db_handle.checking_folder_existence('./data/')
		csv_filename = data_dir + 'csv_file.csv'
		for record in data['records']:
			self.db_handle.json_to_csv(record,csv_filename)

	def do_transcribe(self):
		aws = CTRANSCRIBE()
		procesed_filename = 'transcibed.json'
		if self.is_file_exist(procesed_filename):
			already_transcribe_data = self.read_json_file(procesed_filename)
		else:
			already_transcribe_data = []

		for file in range(len(self.recording_filenames['data'])):

			if self.recording_filenames['data'][file]['file_name'] not in already_transcribe_data:

				if aws.initialize('aws',self.AWS_ACCESS_KEY,self.AWS_SECRET_KEY):
					pass

					if not aws.is_bucket_present(self.config['current_bucket_name']):
						print(self.config['current_bucket_name'] , " not present, so creating one.")
						aws.create_bucket(self.config['current_bucket_name'])

					contents = aws.listing_bucket_contents(self.config['current_bucket_name'])

					if self.recording_filenames['data'][file]['file_name'] not in contents:
						print("Uploading file: ", self.recording_filenames['data'][file]['file_name'], " on AWS-S3")
						aws.upload_file(self.recording_filenames['data'][file]['file_name'],self.config['current_bucket_name'],self.recording_filenames['data'][file]['file_name'].replace('./audio','audio'))
					else:
						print(self.recording_filenames['data'][file], " already present on AWS S3 bucket: ", self.config['current_bucket_name'])

					all_jobs = aws.list_jobs()
					current_job_name = "job_"
					job_names_list = []
					for job in all_jobs['TranscriptionJobSummaries']:
						if current_job_name in job['TranscriptionJobName']:
							try:
								job_names_list.append(int(job['TranscriptionJobName'].replace(current_job_name,'')))
							except:
								pass

					if len(job_names_list) > 0:
						current_job_name += str(max(job_names_list))
					else:
						current_job_name += '0'

					#for file in range(len(self.recording_filenames['data'])):

					job_uri = "https://" + self.config['current_bucket_name'] + ".s3.us-east-2.amazonaws.com/" +self.recording_filenames['data'][file]['file_name'].replace('./audio','audio')

					current_job_name =  "job_" + str(int(current_job_name.replace('job_','')) + 1)

					print("Current Job Name: ", current_job_name)
					print()
					response = aws.do_transcribe(current_job_name,job_uri)
					print("transcibe job finished for: ", current_job_name)
					print()
				
					print('Downloading output json file: ', self.recording_filenames['data'][file]['file_name'].replace('.mp3','.json'))
					urllib.request.urlretrieve(response['TranscriptionJob']['Transcript']['TranscriptFileUri'], self.recording_filenames['data'][file]['file_name'].replace('.mp3','.json'))

					json_data =self.read_json_file(self.recording_filenames['data'][file]['file_name'].replace('.mp3','.json'))

					print("Updating the json data in the Database with call_id: ", self.recording_filenames['data'][file]['call_id'], " and session_id: ", self.recording_filenames['data'][file]['session_id'])
					self.db_handle.updating_json_data(self.recording_filenames['data'][file]['call_id'], self.recording_filenames['data'][file]['session_id'], json_data)

					print("Deleting file from AWS server: ", self.recording_filenames['data'][file]['file_name'].replace('./audio','audio'))
					aws.deleting_file_from_aws(self.config['current_bucket_name'], self.recording_filenames['data'][file]['file_name'].replace('./audio','audio'))
					print()

				if aws.initialize('wasabi',self.WASABI_ACCESS_KEY,self.WASABI_SECRET_KEY):

					if not aws.is_bucket_present(self.config['current_bucket_name']):
						print(self.config['current_bucket_name'] , " not present, so creating one.")
						aws.create_bucket(self.config['current_bucket_name'])

					contents = aws.listing_bucket_contents(self.config['current_bucket_name'])

					if self.recording_filenames['data'][file]['file_name'] not in contents:
						print("Uploading file: ", self.recording_filenames['data'][file]['file_name'], " on WASABI SERVER")
						aws.upload_file(self.recording_filenames['data'][file]['file_name'],self.config['current_bucket_name'],self.recording_filenames['data'][file]['file_name'].replace('./audio','audio'))
					else:
						print(file, " already present on wasabit S3 bucket: ", self.config['current_bucket_name'])


					if self.is_file_exist(procesed_filename):
						already_transcribe_data_new = self.read_json_file(procesed_filename)
					else:
						already_transcribe_data_new = []
					already_transcribe_data_new.append(self.recording_filenames['data'][file]['file_name'])

					self.write_json_file(already_transcribe_data_new,procesed_filename)
			else:
				print("already transcrobed: ", self.recording_filenames['data'][file]['file_name'])
		
		self.db_handle.close_api()

if __name__ == "__main__":
	rec = OPENRC()
	while 1:
		if rec.initialize():		
			rec.login()
			data = rec.get_call_logs(with_rec=False)
			if data:
				rec.saving_to_csv(data)
				rec.interfaceing_database(data)
				rec.download_all_call_recordings()
				rec.do_transcribe()
		print("sleeping for 10 seconds")
		time.sleep(10)
			

