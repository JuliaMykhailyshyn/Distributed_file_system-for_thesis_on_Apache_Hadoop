import json
import os
import time
import requests
import base64
import ssl
import aiohttp
import asyncio
from filesplit.split import Split
import shutil
import csv

# Create an SSLContext with the desired SSL version
context = ssl.SSLContext(ssl.PROTOCOL_TLS)

# Disable SSL verification (not recommended in production)
context.verify_mode = ssl.CERT_NONE

# Create a session and set the SSL context
session = requests.Session()
session.verify = False  # Disable SSL verification
session.cert = None  # Disable SSL certificate

class Client():
    def __init__(self):
        self.management_node = '127.0.0.1:5010'
        self.size_of_piece = 10000#bytes
        self.slave_nodes_list = []
    def request_working_nodes(self):
        response = requests.get('http://' + self.management_node + '/get_working_nodes')
        print(response.text)
        if response.status_code != 200:
            print("ERROR")
        else:
            print("SUCCESS")

        self.slave_nodes_list = response.json()['slave_nodes_list']
        #print(response.text)
        return self.slave_nodes_list

    def split_and_send_the_file(self, file_directory):
        # i am sending the bytes data in a form of a string, because bytes variable is not json serializable
        name_of_file = file_directory.split('/')[-1]  # for some reason '\' didn't work
        count_of_slave_nodes = 0
        count_of_blocks = 0
        directory = "Temporary_folder"
        parent_dir = "D:/Користувачі/Distributed file system 2.0/client"
        path = os.path.join(parent_dir, directory)
        os.mkdir(path)
        split = Split(inputfile='Sample-Spreadsheet-100000-rows.csv', outputdir=directory + '/')
        split.bysize(size=self.size_of_piece, newline=True)

        files_ready_to_send = [f for f in os.listdir(directory)]
        print(files_ready_to_send)
        files_ready_to_send.remove("manifest")
        # got our ready files to send

        # have to sort it first
        files_ready_to_send = sorted(list(files_ready_to_send),
                                     key=lambda x: int(x[x.find('_') + 1:-4]))
        # ['Sample-Spreadsheet-100000-rows_1.csv', 'Sample-Spreadsheet-100000-rows_2.csv', 'Sample-Spreadsheet-100000-rows_3.csv',...]
        print(files_ready_to_send)

        for block_to_distribute in files_ready_to_send:
            file = open(directory + '/' + block_to_distribute, 'rb')

            my_block = file.read()

            name_of_block = name_of_file.replace('.csv', '') + "#fragment" + str(count_of_blocks) + '.csv'
            base64_encoded_data = base64.b64encode(my_block).decode('utf-8')

            # to_send = {'file_name': name_of_block, 'my_bytes_string_file': base64_encoded_data}

            if count_of_slave_nodes == len(self.slave_nodes_list):
                count_of_slave_nodes = 0

            response = requests.post('http://' + self.slave_nodes_list[count_of_slave_nodes] + '/post_trial',
                                     json=json.dumps(
                                         {'file_name': name_of_block, 'my_bytes_string_file': base64_encoded_data}))

            count_of_blocks += 1
            count_of_slave_nodes += 1

            file.close()

        shutil.rmtree(path)  #####
    def get_slave_nodes_list_for_the_file(self, file_directory):
        file_name = file_directory.split('/')[-1]
        response = requests.get('http://' + self.management_node + '/get_nodes_with_file',
                                json = json.dumps({'file_name': file_name}))
        return response.json()['slave_nodes_with_needed_file']

    def retrieve_the_file(self, file_name, list_of_nodes_to_retrieve_from):
        contents_table = {}#name and bytes_file pairs

        for slave_node in list_of_nodes_to_retrieve_from:
            response = requests.get('http://' + slave_node + '/node/get_file', json = json.dumps({'file_name': file_name}))
            contents_table.update(response.json()['dictionary_with_files'])
            
        contents_table = dict(sorted(contents_table.items(), 
                                     key = lambda x: int(x[0][x[0].find('#fragment') +9 : -4])   ))#!!
        
        print(str(contents_table.keys()))

        temporary_folder = 'Temporary_file'
        parent_dir = 'D:/Користувачі/Distributed file system 2.0/client'
        path = os.path.join(parent_dir, temporary_folder)
        os.mkdir(path)

        block_place = temporary_folder + '/'

        for key in contents_table:

            block_name = block_place + key
            file = open(block_name,'wb')
            received_base64_encoded_data = contents_table[key]
            received_bytes = base64.b64decode(received_base64_encoded_data.encode('utf-8'))
            file.write(received_bytes)
            file.close()

        # List of CSV file names to merge
        names_of_blocks = list(contents_table.keys())

        # Output file name
        output_file = 'retrieved_files/'+ file_name

        # Merge CSV files
        with open(output_file, 'w', newline='') as outfile:
            writer = csv.writer(outfile)
            for filename in names_of_blocks:
                with open(block_place+filename, 'r') as infile:
                    reader = csv.reader(infile)
                    for row in reader:
                        writer.writerow(row)

        shutil.rmtree(path)

        return True

                
    #def add_node(self, node_address):

    async def get_result(self, session, filename, mappy_encoded, reducepy_encoded, unique_job_name):
        async with session.get('http://' + self.management_node + '/mapreduce/launch',
                                json=json.dumps({'filename': filename, 'map_code': mappy_encoded,
                                      'reduce_code': reducepy_encoded, 'unique_job_name': unique_job_name})) as response:
            return await response.json()

    async def check_progress(self, session, unique_job_name):
        await asyncio.sleep(3)
        job_running = True
        while job_running:
            async with session.get('http://' + self.management_node + '/inform_client_of_job_status',
                                   json=json.dumps({'job_identifier': unique_job_name})) as response:
                check_the_progress = await response.json()
                if check_the_progress['process'] == "The job finished or never started":
                    print(check_the_progress['process'])
                    return "job finished (successfully), tracking finished"
                print(check_the_progress['process'] + '----' + str(check_the_progress['percentage_of_nodes_completed']))
                if check_the_progress['process'] == 'reduce' and check_the_progress['percentage_of_nodes_completed'] == 100:
                    job_running = False
            await asyncio.sleep(1)

    async def run_tasks(self, filename, mappy_encoded, reducepy_encoded, unique_job_name):
        async with aiohttp.ClientSession() as session:
            # Launch mapreduce process without awaiting completion
            launch_task = asyncio.create_task(
                self.get_result(session, filename, mappy_encoded, reducepy_encoded, unique_job_name))
            # Start checking progress immediately
            progress_task = asyncio.create_task(self.check_progress(session, unique_job_name))

            # Wait for both tasks to complete
            await asyncio.gather(launch_task, progress_task)
    def launch_mapreduce_process(self, filename, mappy, reducepy, unique_job_name):
        map_bytes = open(mappy, 'rb')
        reduce_bytes = open(reducepy, 'rb')
        mappy_encoded = base64.b64encode(map_bytes.read()).decode('utf-8')
        reducepy_encoded = base64.b64encode(reduce_bytes.read()).decode('utf-8')
        map_bytes.close()
        reduce_bytes.close()
        
        return asyncio.run(
            self.run_tasks(filename, mappy_encoded, reducepy_encoded, unique_job_name)
        )











        
