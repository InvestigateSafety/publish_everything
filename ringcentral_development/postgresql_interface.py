from time import strptime, strftime, mktime, gmtime
import csv,os,requests,json,re,time
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

    def checking_folder_existence(self,dest_dir):
        if not os.path.exists(dest_dir):
            os.mkdir(dest_dir)
            print("Directory " , dest_dir ,  " Created ")
        else:
            pass
            #print("Directory " , dest_dir ,  " Exists ")

        return dest_dir

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

    def json_to_csv(self,json_data,csv_filename):
        sub_list = []

        if 'uri' in json_data:
            sub_list.append(json_data['uri'] )
        else:
            sub_list.append('')

        if 'id' in json_data:
            sub_list.append(json_data['id'])
        else:
            sub_list.apend('')

        if 'sessionId' in json_data:
            sub_list.append(json_data['sessionId'])
        else:
            sub_list.append('')

        if 'startTime' in json_data:
            sub_list.append(json_data['startTime'])
        else:
            sub_list.append('')

        if 'duration' in json_data:
            sub_list.append(json_data['duration'])
        else:
            sub_list.append('')

        if 'type' in json_data:
            sub_list.append(json_data['type'])
        else:
            sub_list.append('')

        if 'direction' in json_data:
            sub_list.append(json_data['direction'])
        else:
            sub_list.append('')

        if 'action' in json_data:
            sub_list.append(json_data['action'])
        else:
            sub_list.append('')

        if 'result' in json_data:
            sub_list.append(json_data['result'])
        else:
            sub_list.append('')

        if 'to' in json_data:
            if 'name' in json_data['to']:
                sub_list.append(json_data['to']['name'])
            else:
                sub_list.append('')

            if 'phoneNumber' in json_data['to']:
                sub_list.append(json_data['to']['phoneNumber'])
            else:
                sub_list.append('')

            if 'extensionId' in json_data['to']:
                sub_list.append(json_data['to']['extensionId'])
            else:
                sub_list.append('')

            if 'extensionNumber' in json_data['to']:
                sub_list.append(json_data['to']['extensionNumber'])
            else:
                sub_list.append('')

            if 'location' in json_data['to']:
                sub_list.append(json_data['to']['location'])
            else:
                sub_list.append('')
        else:
            sub_list.append('')
            sub_list.append('')
            sub_list.append('')
            sub_list.append('')
            sub_list.append('')

        if 'from' in json_data:

            if 'name' in json_data['from']:
                sub_list.append(json_data['from']['name'])
            else:
                sub_list.append('')

            if 'phoneNumber' in json_data['from']:
                sub_list.append(json_data['from']['phoneNumber'])
            else:
                sub_list.append('')

            if 'extensionId' in json_data['from']:
                sub_list.append(json_data['from']['extensionId'])
            else:
                sub_list.append('')

            if 'extensionNumber' in json_data['from']:
                sub_list.append(json_data['from']['extensionNumber'])
            else:
                sub_list.append('')

            if 'location' in json_data['from']:
                sub_list.append(json_data['from']['location'])
            else:
                sub_list.append('')

        else:
            sub_list.append('')
            sub_list.append('')
            sub_list.append('')
            sub_list.append('')
            sub_list.append('')

        if 'recording' in json_data:
            if 'uri' in json_data['recording']:
                sub_list.append(json_data['recording']['uri'])
            else:
                sub_list.append('')

            if 'id' in json_data['recording']:
                sub_list.append(json_data['recording']['id'])
            else:
                sub_list.append('')

            if 'type' in json_data['recording']:
                sub_list.append(json_data['recording']['type'])
            else:
                sub_list.append('')

            if 'contentUri' in json_data['recording']:
                sub_list.append(json_data['recording']['contentUri'])
            else:
                sub_list.append('')
        else:
            sub_list.append('')
            sub_list.append('')
            sub_list.append('')
            sub_list.append('')

        if 'extension' in json_data:
            if 'uri' in json_data['extension']:
                sub_list.append(json_data['extension']['uri'])
            else:
                sub_list.append('')

            if 'id' in json_data['extension']: 
                sub_list.append(json_data['extension']['id'])
            else:
                sub_list.append('')
        else:
            sub_list.append('')
            sub_list.append('')

        if 'reason' in json_data:
            sub_list.append(json_data['reason'])
        else:
            sub_list.append('')

        if 'reasonDescription' in json_data:
            sub_list.append(json_data['reasonDescription'])
        else:
            sub_list.append('')

        if 'transport' in json_data:
            sub_list.append(json_data['transport'])
        else:
            sub_list.append('')

        if 'lastModifiedTime' in json_data:
            sub_list.append(json_data['lastModifiedTime'])
        else:
            sub_list.append('')

        if 'billing' in json_data:
            if 'costIncluded' in json_data['billing']:
                sub_list.append(json_data['billing']['costIncluded'])
            else:
                sub_list.append('')

            if 'costPurchased' in json_data['billing']:
                sub_list.append(json_data['billing']['costPurchased'])
            else:
                sub_list.append('')
        else:
            sub_list.append('')
            sub_list.append('')

        if 'legs' in json_data:
            for leg in range(len(json_data['legs'])):
            
                if len(json_data['legs']) > 0:
                    if 'startTime' in json_data['legs'][leg]:
                        sub_list.append(json_data['legs'][leg]['startTime'])
                    else:
                        sub_list.append('')

                    if 'duration' in json_data['legs'][leg]:
                        sub_list.append(json_data['legs'][leg]['duration'])
                    else:
                        sub_list.append('')

                    if 'type' in json_data['legs'][leg]:
                        sub_list.append(json_data['legs'][leg]['type'])
                    else:
                        sub_list.append('')

                    if 'direction' in json_data['legs'][leg]:
                        sub_list.append(json_data['legs'][leg]['direction'])
                    else:
                        sub_list.append('')

                    if 'action' in json_data['legs'][leg]:
                        sub_list.append(json_data['legs'][leg]['action'])
                    else:
                        sub_list.append('')

                    if 'result' in json_data['legs'][leg]:
                        sub_list.append(json_data['legs'][leg]['result'])
                    else:
                        sub_list.append('')

                    if 'to' in json_data['legs'][leg]:
                        if 'name' in json_data['legs'][leg]['to']:
                            sub_list.append(json_data['legs'][leg]['to']['name'])
                        else:
                            sub_list.append('')

                        if 'phoneNumber' in json_data['legs'][leg]['to']:
                            sub_list.append(json_data['legs'][leg]['to']['phoneNumber'])
                        else:
                            sub_list.append('')

                        if 'extensionId' in json_data['legs'][leg]['to']:
                            sub_list.append(json_data['legs'][leg]['to']['extensionId'])
                        else:
                            sub_list.append('')

                        if 'extensionNumber' in json_data['legs'][leg]['to']:
                            sub_list.append(json_data['legs'][leg]['to']['extensionNumber'])
                        else:
                            sub_list.append('')

                        if 'location' in json_data['legs'][leg]['to']:
                            sub_list.append(json_data['legs'][leg]['to']['location'])
                        else:
                            sub_list.append('')
                    else:
                        sub_list.append('')
                        sub_list.append('')
                        sub_list.append('')
                        sub_list.append('')
                        sub_list.append('')

                    if 'from' in json_data['legs'][leg]:
                        if 'name' in json_data['legs'][leg]['from']:  
                            sub_list.append(json_data['legs'][leg]['from']['name'])
                        else:
                            sub_list.append('')

                        if 'phoneNumber' in json_data['legs'][leg]['from']:
                            sub_list.append(json_data['legs'][leg]['from']['phoneNumber'])
                        else:
                            sub_list.append('')

                        if 'extensionId' in json_data['legs'][leg]['from']:
                            sub_list.append(json_data['legs'][leg]['from']['extensionId'])
                        else:
                            sub_list.append('') 

                        if 'extensionNumber' in json_data['legs'][leg]['from']:
                            sub_list.append(json_data['legs'][leg]['from']['extensionNumber'])
                        else:
                            sub_list.append('')


                        if 'location' in json_data['legs'][leg]['from']:
                            sub_list.append(json_data['legs'][leg]['from']['location'])
                        else:
                            sub_list.append('')

                    else:
                        sub_list.append('')
                        sub_list.append('')
                        sub_list.append('')
                        sub_list.append('')
                        sub_list.append('')                   
                    
                    if 'extension' in json_data['legs'][leg]['extension']:
                        if 'uri' in json_data['legs'][leg]['extension']:
                            sub_list.append(json_data['legs'][leg]['extension']['uri'])
                        else:
                            sub_list.append('')

                        if 'id' in json_data['legs'][leg]['extension']:
                            sub_list.append(json_data['legs'][leg]['extension']['id'])
                        else:
                            sub_list.append('')
                    else:
                        sub_list.append('')
                        sub_list.append('')

                    if 'reason' in json_data['legs'][leg]:
                        sub_list.append(json_data['legs'][leg]['reason'])
                    else:
                        sub_list.append('')

                    if 'reasonDescription' in json_data['legs'][leg]:
                        sub_list.append(json_data['legs'][leg]['reasonDescription'])
                    else:
                        sub_list.append('')

                    if 'transport' in json_data['legs'][leg]:
                        sub_list.append(json_data['legs'][leg]['transport'])
                    else:
                        sub_list.append('')

                    if 'legType' in json_data['legs'][leg]:
                        sub_list.append(json_data['legs'][leg]['legType'])
                    else:
                        sub_list.append('')

                    if 'master' in json_data['legs'][leg]:
                        sub_list.append(json_data['legs'][leg]['master'])
                    else:
                        sub_list.append('')

                    if 'message' in json_data['legs'][leg]:
                        if 'uri' in json_data['legs'][leg]['message']:
                            sub_list.append(json_data['legs'][leg]['message']['uri'])
                        else:
                            sub_list.append('')

                        if 'id' in json_data['legs'][leg]['message']:
                            sub_list.append(json_data['legs'][leg]['message']['id'])
                        else:
                            sub_list.append('')

                        if 'type' in json_data['legs'][leg]['message']:
                            sub_list.append(json_data['legs'][leg]['message']['type'])
                        else:
                            sub_list.append('')
                    else:
                        sub_list.append('')
                        sub_list.append('')
                        sub_list.append('')

                    if 'billing' in json_data['legs'][leg]:
                        if 'costIncluded' in json_data['legs'][leg]['billing']:
                            sub_list.append(json_data['legs'][leg]['billing']['costIncluded'])
                        else:
                            sub_list.append('')

                        if 'costPurchased' in json_data['legs'][leg]['billing']:
                            sub_list.append(json_data['legs'][leg]['billing']['costPurchased'])
                        else:
                            sub_list.append('')
                    else:
                        sub_list.append('')
                        sub_list.append('')

        if 'message' in json_data:
            if 'uri' in json_data['message']:
                sub_list.append(json_data['message']['uri'])
            else:
                sub_list.append('')

            if 'id' in json_data['message']:
                sub_list.append(json_data['message']['id'])
            else:
                sub_list.append('')

            if 'type' in json_data['message']:
                sub_list.append(json_data['message']['type'])
            else:
                sub_list.append('')
        else:
            sub_list.append('')
            sub_list.append('')
            sub_list.append('')

        if 'master' in json_data:
            sub_list.append(int(json_data['legs'][1]['master']))
        else:
            sub_list.append('0')

        sub_list.append('')
        sub_list.append('')

        if self.is_file_exist(csv_filename):
            csv_data = self.reading_csv(csv_filename)
        else:
            csv_data = [['uri','call_id','sessionId','startTime','duration','type','direction','action',
                        'result','to_name','to_phoneNumber','to_extensionId','to_extensionNumber','to_location',
                        'from_name','from_phoneNumber','from_extensionId','from_extensionNumber','from_location',
                        'recording_uri','recording_id','recording_type','recording_contentUri','extension_uri',
                        'extension_id','reason','reasonDescription','transport','lastModifiedTime','billing_costIncluded','billing_costPurchased',
                        'legs_0_startTime','legs_0_duration','legs_0_type','legs_0_direction','legs_0_action','legs_0_result',
                        'legs_0_to_name','legs_0_to_phoneNumber','legs_0_to_extensionId','legs_0_to_extensionNumber','legs_0_to_location',
                        'legs_0_from_name','legs_0_from_phoneNumber','legs_0_from_extensionId','legs_0_from_extensionNumber','legs_0_from_location',
                        'legs_0_extension_uri','legs_0_extension_id','legs_0_reason','legs_0_reasonDescription','legs_0_transport',
                        'legs_0_legType','legs_0_master','legs_0_message_uri','legs_0_message_id','legs_0_message_type',
                        'legs_0_billing_costIncluded','legs_0_billing_costPurchased',
                        'legs_1_startTime','legs_1_duration','legs_1_type','legs_1_direction','legs_1_action','legs_1_result',
                        'legs_1_to_name','legs_1_to_phoneNumber','legs_1_to_extensionId','legs_to_1_extensionNumber','legs_to_1_location',
                        'legs_1_from_name','legs_1_from_phoneNumber','legs_1_from_extensionId','legs_from_1_extensionNumber','legs_from_1_location',
                        'legs_1_extension_uri','legs_1_extension_id','legs_1_reason','legs_1_reasonDescription','legs_1_transport',
                        'legs_1_legType','legs_1_master','legs_1_message_uri','legs_1_message_id','legs_1_message_type',
                        'legs_1_billing_costIncluded','legs_1_billing_costPurchased',
                        'message_uri','message_id','message_type','master','transcription_json','notes']]
        csv_data.append(sub_list)
        self.writing_csv(csv_data,csv_filename)


    def saving_to_db(self,json_data):
        
        if 'uri' in json_data:
            uri = json_data['uri'] 
        else:
            uri = ''

        if 'id' in json_data:
            call_id = json_data['id']
        else:
            call_id = ''

        if 'sessionId' in json_data:
            sessionId = json_data['sessionId']
        else:
            sessionId = '0'

        if 'startTime' in json_data:
            startTime = json_data['startTime']
        else:
            startTime = ''

        if 'duration' in json_data:
            duration = int(json_data['duration'])
        else:
            duration = 0

        if 'type' in json_data:
            call_type = json_data['type']
        else:
            call_type = ''

        if 'direction' in json_data:
            direction = json_data['direction']
        else:
            direction = ''

        if 'action' in json_data:
            action = json_data['action']
        else:
            action = ''

        if 'result' in json_data:
            result = json_data['result']
        else:
            result = ''

        if 'to' in json_data:
            if 'name' in json_data['to']:
                to_name = json_data['to']['name']
            else:
                to_name = ''

            if 'phoneNumber' in json_data['to']:
                to_phoneNumber = json_data['to']['phoneNumber']
            else:
                to_phoneNumber = ''

            if 'extensionId' in json_data['to']:
                to_extensionId = json_data['to']['extensionId']
            else:
                to_extensionId = ''

            if 'extensionNumber' in json_data['to']:
                to_extensionNumber = json_data['to']['extensionNumber']
            else:
                to_extensionNumber = ''

            if 'location' in json_data['to']:
                to_location = json_data['to']['location']
            else:
                to_location = ''
        else:
            to_name = ''
            to_phoneNumber = ''
            to_extensionId = ''
            to_extensionNumber = ''
            to_location = ''

        if 'from' in json_data:

            if 'name' in json_data['from']:
                from_name = json_data['from']['name']
            else:
                from_name = ''

            if 'phoneNumber' in json_data['from']:
                from_phoneNumber = json_data['from']['phoneNumber']
            else:
                from_phoneNumber = ''

            if 'extensionId' in json_data['from']:
                from_extensionId = json_data['from']['extensionId']
            else:
                from_extensionId = ''

            if 'extensionNumber' in json_data['from']:
                from_extensionNumber = json_data['from']['extensionNumber']
            else:
                from_extensionNumber = ''

            if 'location' in json_data['from']:
                from_location = json_data['from']['location']
            else:
                from_location = ''

        else:
            from_name = ''
            from_phoneNumber = ''
            from_extensionId = ''
            from_extensionNumber = ''
            from_location = ''

        if 'recording' in json_data:
            if 'uri' in json_data['recording']:
                recording_uri = json_data['recording']['uri']
            else:
                recording_uri = ''

            if 'id' in json_data['recording']:
                recording_id = json_data['recording']['id']
            else:
                recording_id = '0'

            if 'type' in json_data['recording']:
                recording_type = json_data['recording']['type']
            else:
                recording_type = ''

            if 'contentUri' in json_data['recording']:
                recording_contentUri = json_data['recording']['contentUri']
            else:
                recording_contentUri = ''
        else:
            recording_uri = ''
            recording_id = 0
            recording_type = ''
            recording_contentUri = ''

        if 'extension' in json_data:
            if 'uri' in json_data['extension']:
                extension_uri = json_data['extension']['uri']
            else:
                extension_uri = ''

            if 'id' in json_data['extension']: 
                extension_id= json_data['extension']['id']
            else:
                extension_id = ''
        else:
            extension_uri = ''
            extension_id = ''

        if 'reason' in json_data:
            reason = json_data['reason']
        else:
            reason = ''

        if 'reasonDescription' in json_data:
            reasonDescription = json_data['reasonDescription']
        else:
            reasonDescription = ''

        if 'transport' in json_data:
            transport = json_data['transport']
        else:
            transport = ''

        if 'lastModifiedTime' in json_data:
            lastModifiedTime = json_data['lastModifiedTime']
        else:
            lastModifiedTime = ''

        if 'billing' in json_data:
            if 'costIncluded' in json_data['billing']:
                billing_costIncluded = json_data['billing']['costIncluded']
            else:
                billing_costIncluded = ''

            if 'costPurchased' in json_data['billing']:
                billing_costPurchased = json_data['billing']['costPurchased']
            else:
                billing_costPurchased = ''
        else:
            billing_costIncluded = ''
            billing_costPurchased = ''

        legs_list = []
        if 'legs' in json_data:
            for leg in range(len(json_data['legs'])):
            
                if len(json_data['legs']) > 0:
                    if 'startTime' in json_data['legs'][leg]:
                        legs_list.append(json_data['legs'][leg]['startTime'])
                    else:
                        legs_list.append('')

                    if 'duration' in json_data['legs'][leg]:
                        legs_list.append(int(json_data['legs'][leg]['duration']))
                    else:
                        legs_list.append(0)

                    if 'type' in json_data['legs'][leg]:
                        legs_list.append(json_data['legs'][leg]['type'])
                    else:
                        legs_list.append('')

                    if 'direction' in json_data['legs'][leg]:
                        legs_list.append(json_data['legs'][leg]['direction'])
                    else:
                        legs_list.append('')

                    if 'action' in json_data['legs'][leg]:
                        legs_list.append(json_data['legs'][leg]['action'])
                    else:
                        legs_list.append('')

                    if 'result' in json_data['legs'][leg]:
                        legs_list.append(json_data['legs'][leg]['result'])
                    else:
                        legs_list.append('')

                    if 'to' in json_data['legs'][leg]:
                        if 'name' in json_data['legs'][leg]['to']:
                            legs_list.append(json_data['legs'][leg]['to']['name'])
                        else:
                            legs_list.append('')

                        if 'phoneNumber' in json_data['legs'][leg]['to']:
                            legs_list.append(json_data['legs'][leg]['to']['phoneNumber'])
                        else:
                            legs_list.append('')

                        if 'extensionId' in json_data['legs'][leg]['to']:
                            legs_list.append(json_data['legs'][leg]['to']['extensionId'])
                        else:
                            legs_list.append('')

                        if 'extensionNumber' in json_data['legs'][leg]['to']:
                            legs_list.append(json_data['legs'][leg]['to']['extensionNumber'])
                        else:
                            legs_list.append('')

                        if 'location' in json_data['legs'][leg]['to']:
                            legs_list.append(json_data['legs'][leg]['to']['location'])
                        else:
                            legs_list.append('')
                    else:
                        legs_list.append('')
                        legs_list.append('')
                        legs_list.append('')
                        legs_list.append('')
                        legs_list.append('')

                    if 'from' in json_data['legs'][leg]:
                        if 'name' in json_data['legs'][leg]['from']:  
                            legs_list.append(json_data['legs'][leg]['from']['name'])
                        else:
                            legs_list.append('')

                        if 'phoneNumber' in json_data['legs'][leg]['from']:
                            legs_list.append(json_data['legs'][leg]['from']['phoneNumber'])
                        else:
                            legs_list.append('')

                        if 'extensionId' in json_data['legs'][leg]['from']:
                            legs_list.append(json_data['legs'][leg]['from']['extensionId'])
                        else:
                            legs_list.append('' )

                        if 'extensionNumber' in json_data['legs'][leg]['from']:
                            legs_list.append(json_data['legs'][leg]['from']['extensionNumber'])
                        else:
                            legs_list.append('')


                        if 'location' in json_data['legs'][leg]['from']:
                            legs_list.append(json_data['legs'][leg]['from']['location'])
                        else:
                            legs_list.append('')

                    else:
                        legs_list.append('')
                        legs_list.append('')
                        legs_list.append('')
                        legs_list.append('')
                        legs_list.append('')
                    
                    if 'extension' in json_data['legs'][leg]['extension']:
                        if 'uri' in json_data['legs'][leg]['extension']:
                            legs_list.append(json_data['legs'][leg]['extension']['uri'])
                        else:
                            legs_list.append('')

                        if 'id' in json_data['legs'][leg]['extension']:
                            legs_list.append(json_data['legs'][leg]['extension']['id'])
                        else:
                            legs_list.append('')
                    else:
                        legs_list.append('')
                        legs_list.append('')

                    if 'reason' in json_data['legs'][leg]:
                        legs_list.append(json_data['legs'][leg]['reason'])
                    else:
                        legs_list.append('')

                    if 'reasonDescription' in json_data['legs'][leg]:
                        legs_list.append(json_data['legs'][leg]['reasonDescription'])
                    else:
                        legs_list.append('')

                    if 'transport' in json_data['legs'][leg]:
                        legs_list.append(json_data['legs'][leg]['transport'])
                    else:
                        legs_list.append('')

                    if 'legType' in json_data['legs'][leg]:
                        legs_list.append(json_data['legs'][leg]['legType'])
                    else:
                        legs_list.append('')

                    if 'master' in json_data['legs'][leg]:
                        legs_list.append(json_data['legs'][leg]['master'])
                    else:
                        legs_list.append('')

                    if 'message' in json_data['legs'][leg]:
                        if 'uri' in json_data['legs'][leg]['message']:
                            legs_list.append(json_data['legs'][leg]['message']['uri'])
                        else:
                            legs_list.append('')

                        if 'id' in json_data['legs'][leg]['message']:
                            legs_list.append(json_data['legs'][leg]['message']['id'])
                        else:
                            legs_list.append('0')

                        if 'type' in json_data['legs'][leg]['message']:
                            legs_list.append(json_data['legs'][leg]['message']['type'])
                        else:
                            legs_list.append('')
                    else:
                        legs_list.append('')
                        legs_list.append('')
                        legs_list.append('')

                    if 'billing' in json_data['legs'][leg]:
                        if 'costIncluded' in json_data['legs'][leg]['billing']:
                            legs_list.append(json_data['legs'][leg]['billing']['costIncluded'])
                        else:
                            legs_list.append('')

                        if 'costPurchased' in json_data['legs'][leg]['billing']:
                            legs_list.append(json_data['legs'][leg]['billing']['costPurchased'])
                        else:
                            legs_list.append('')
                    else:
                        legs_list.append('')
                        legs_list.append('')
        if len(json_data['legs']) < 2:
            legs_list.append(json_data['legs'][0]['startTime'])
            legs_list.append(0)
            legs_list.append('')
            legs_list.append('')
            legs_list.append('')
            legs_list.append('')
            legs_list.append('')
            legs_list.append('')
            legs_list.append('')
            legs_list.append('')
            legs_list.append('')
            legs_list.append('')
            legs_list.append('')
            legs_list.append('')
            legs_list.append('')
            legs_list.append('')
            legs_list.append('')
            legs_list.append('')
            legs_list.append('')
            legs_list.append('')
            legs_list.append('')
            legs_list.append('')
            legs_list.append('')
            legs_list.append('')
            legs_list.append('')
            legs_list.append('')
            legs_list.append('')
            legs_list.append('')

        if 'message' in json_data:
            if 'uri' in json_data['message']:
                message_uri = json_data['message']['uri']
            else:
                message_uri = ''

            if 'id' in json_data['message']:
                message_id = json_data['message']['id']
            else:
                message_id = '0'

            if 'type' in json_data['message']:
                message_type = json_data['message']['type']
            else:
                message_type = ''
        else:
            message_uri = ''
            message_id = 0
            message_type = ''

        if 'master' in json_data:
            master = str(int(json_data['legs'][1]['master']))
        else:
            master = int(0)

        transcription_json = {}
        notes = ''


        if self.postgres.is_data_present(call_id,sessionId) is None:
            self.postgres.insert_data(uri,call_id,sessionId,startTime,duration,call_type,direction,action,
                        result,to_name,to_phoneNumber,to_extensionId,to_extensionNumber,to_location,
                        from_name,from_phoneNumber,from_extensionId,from_extensionNumber,from_location,
                        recording_uri,recording_id,recording_type,recording_contentUri,extension_uri,
                        extension_id,reason,reasonDescription,transport,lastModifiedTime,billing_costIncluded,billing_costPurchased,
                        legs_list, message_uri,message_id,message_type,master,transcription_json,notes)

            self.postgres.commit_api()
        else:
            print("Data already present in the DB")


    def inserting_in_db(self,records):
        config = self.read_json_file('./conf.json')
        connected_db = self.postgres.connect_db(config['dbname'],config['user'],config['password'],config['host'])
        if connected_db:
            for record in records:
                self.saving_to_db(record)
        self.postgres.close()



if __name__ =="__main__":
    db_handle = CINSERTDB()
    records = db_handle.read_json_file('./records.json')
    db_handle.inserting_in_db(records['records'])

        # data_dir = db_handle.checking_folder_existence('./data/')
        # csv_filename = data_dir + 'csv_file.csv'
        # db_handle.json_to_csv(record,csv_filename)
    #print(db_handle.splitting_names("workINGNow"))
