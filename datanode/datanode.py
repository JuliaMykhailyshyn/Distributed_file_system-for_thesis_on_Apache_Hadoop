import base64
from flask import jsonify

class DataNode():
    def __init__(self, address):
        self.address = address
        self.list_of_stored_filenames = []
        self.management_node = '127.0.0.1:5010'
    def get_address(self):
        return self.address

    def place_file(self, file_name, file_bytes_in_string_from_request_json):
        received_bytes = base64.b64decode(file_bytes_in_string_from_request_json.encode('utf-8'))
        with open('folder_with_files/' + file_name, 'wb') as file1:
            file1.write(received_bytes)

        self.list_of_stored_filenames.append(file_name)
        # sending the post request to the management node to make changes to the general table
        return "GREAT JOB"

    def assemble_file(self, file_name):

        dictionary_to_return = {}
        for piece_of_file in self.list_of_stored_filenames:
            if file_name in piece_of_file:
                file1 = open('folder_with_files/' + piece_of_file, 'rb')
                block_of_bytes = file1.read()
                base64_encoded_data = base64.b64encode(block_of_bytes).decode('utf-8')
                dictionary_to_return[piece_of_file] = base64_encoded_data
        return jsonify({'dictionary_with_files': dictionary_to_return})


