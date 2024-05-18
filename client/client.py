import json
import os
import requests
import base64
import ssl

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
        
        #i am sending the bytes data in a form of a string, because bytes variable is not json serializable

        name_of_file = file_directory.split('/')[-1]  # for some reason '\' didn't work
        count_of_slave_nodes = 0
        count_of_blocks = 0
        
        size = os.stat(file_directory).st_size #size in bytes
        file = open(file_directory, 'rb')
        while size >= self.size_of_piece:
            
            my_block = file.read(self.size_of_piece)
            size -= self.size_of_piece 

            name_of_block = name_of_file + "#fragment" + str(count_of_blocks)
            base64_encoded_data = base64.b64encode(my_block).decode('utf-8')
            print(base64_encoded_data)

            #to_send = {'file_name': name_of_block, 'my_bytes_string_file': base64_encoded_data}
            #sending_json = json.dumps(to_send)  # jsonify function is same!!!!!!!it makes a json object
            # json.dumps(--) makes a JSON STRING!!! so after we have to use JSON.LOADS()
            if count_of_slave_nodes == len(self.slave_nodes_list):
                count_of_slave_nodes = 0
            
            response = requests.post('http://' + self.slave_nodes_list[count_of_slave_nodes] + '/post_trial', 
                          json=json.dumps({'file_name': name_of_block, 'my_bytes_string_file': base64_encoded_data}))

            count_of_blocks += 1
            count_of_slave_nodes += 1

        if size != 0:
            my_block = file.read()#size

            name_of_block = name_of_file + "#fragment" + str(count_of_blocks)
            base64_encoded_data = base64.b64encode(my_block).decode('utf-8')

            if count_of_slave_nodes == len(self.slave_nodes_list):
                count_of_slave_nodes = 0

            response = requests.post('http://' + self.slave_nodes_list[count_of_slave_nodes] + '/post_trial',
                                     json=json.dumps(
                                         {'file_name': name_of_block, 'my_bytes_string_file': base64_encoded_data}))

        file.close()

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
                                     key = lambda x: int(x[0][x[0].find('#fragment') +9 :])   ))#careful###YEAP###MISTAKE THERE!!!!!!!
        print("ATTENTION!!! " + str(contents_table.keys()))
        file = open("retrieved_files/" + file_name,'wb')
        for key in contents_table:
            received_base64_encoded_data = contents_table[key]
            received_bytes = base64.b64decode(received_base64_encoded_data.encode('utf-8'))
            file.write(received_bytes)
        file.close()
            
        return True
                
    #def add_node(self, node_address):






        
