import logging
import boto3
from botocore.exceptions import ClientError
import os,sys,threading,time


class CTRANSCRIBE():

	def __init__(self):
		pass

	def initialize(self,server,access_key,access_secret):
		ACCESS_KEY = access_key
		SECRET_KEY = access_secret
		try:
			if 'aws' in server:
				self.s3_client = boto3.client('s3',region_name='us-east-2',aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
				self.transcribe = boto3.client('transcribe',region_name='us-east-2',aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
				print("Initialized AWS Server......")
				return True
			elif 'wasabi' in server:
				endpoint_url = 'https://s3.us-east-2.wasabisys.com'
				self.s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY,endpoint_url = endpoint_url)
				print("Initialzed WASABI Server.....")
				return True
			else:
				print("Unknown Server")
				return False
		except:
			return False


	def create_bucket(self,bucket_name, region='us-east-2'):
		try:
			location = {'LocationConstraint': region}
			self.s3_client.create_bucket(Bucket=bucket_name,CreateBucketConfiguration=location)
			print(bucket_name ," created successfully.")
		except ClientError as e:
			logging.error(e)
			return False
		
		return True

	def listing_buckets(self):
		response = self.s3_client.list_buckets()

		bucket_list = []
		for bucket in response['Buckets']:
		    #print(f'  {bucket["Name"]}')
		    #print(bucket)
		    bucket_list.append(bucket)
		return bucket_list

	def listing_bucket_contents(self,bucket_name):
		contents = []
		bucket_contents = self.s3_client.list_objects(Bucket=bucket_name)
		#print(bucket_contents)
		if 'Contents' in bucket_contents:
			for key in bucket_contents['Contents']:
				contents.append(key['Key'])

		return contents


	def upload_file(self,file_name, bucket_name, bucket_file_name=None):

	    if bucket_file_name is None:
	        bucket_file_name = file_name

	    try:
	        response = self.s3_client.upload_file(file_name, bucket_name, bucket_file_name)
	    except ClientError as e:
	        logging.error(e)
	        return False
	    return True

	def uploading_file_as_obj(self,file_name,bucket_name,bucket_file_name=None):

		if bucket_file_name is None:
			bucket_file_name = file_name

		with open(file_name, "rb") as f:
			self.s3_client.upload_fileobj(f, bucket_name, bucket_file_name)

	def download_file(self,file_name,bucket_name,bucket_file_name):
		self.s3_client.download_file(bucket_name, bucket_file_name, file_name)

	def downloading_file_as_obj(self,file_name,bucket_name,bucket_file_name):
		with open(file_name, 'wb') as f:
			self.s3_client.download_fileobj(bucket_name, bucket_file_name, f)

	def deleting_file_from_aws(self,bucket_name,file_name):
		print("Deleting ", file_name, " from bucket: ", bucket_name)
		self.s3_client.delete_object(Bucket=bucket_name, Key=file_name)

	def is_bucket_present(self,bucket_name):
		bucket_list = self.listing_buckets()

		for bucket in bucket_list:
			if bucket_name in bucket['Name']:
				return True
		return False

	def do_transcribe(self,job_name,job_uri):

		if not job_name and not job_uri:
			print( "Empty parameters")
			return False

		input_file_format = job_uri.split('.')
		input_file_format = input_file_format[len(input_file_format)-1]
		#language_code : 'en-US'|'es-US'|'en-AU'|'fr-CA'|'en-GB'|'de-DE'|'pt-BR'|'fr-FR'|'it-IT'|'ko-KR'|'es-ES'|'en-IN'|'hi-IN'|'ar-SA'|'ru-RU'|'zh-CN'
		self.transcribe.start_transcription_job(
		    TranscriptionJobName=job_name,
		    Media={'MediaFileUri': job_uri},
		    MediaFormat=input_file_format,
		    LanguageCode='en-US',
		    Settings={
		        'ShowSpeakerLabels': True,
		        'MaxSpeakerLabels': 2
		    }
		)
		while True:
		    status = self.transcribe.get_transcription_job(TranscriptionJobName=job_name)
		    if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
		        break
		    print("transcript not yet ready for job name: ", job_name)
		    time.sleep(5)

		return status

	def list_jobs(self,job_status="ALL"):
		current_jobs = {}
		avaialble_status = ['IN_PROGRESS',['FAILED'],['COMPLETED']]
		#job_status = IN_PROGRESS'|'FAILED'|'COMPLETED
		response = self.transcribe.list_transcription_jobs(
		    MaxResults=99
		)
		return response


if __name__ == "__main__":
	aws = CTRANSCRIBE()
	job_uri = "https://investigatesafetybusinesses.s3.us-east-2.amazonaws.com/audio_files/test3.mp3"
	aws.do_transcribe('testing7',job_uri)
	# bucket_list = aws.listing_buckets()
	# for bucket in bucket_list:
	# 	aws.listing_bucket_contents(bucket['Name'])