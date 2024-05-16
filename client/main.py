from client import Client
import requests, json

import os


client = Client()

#while True:
user_file = input("The directory of your file: ")
print(client.request_working_nodes())

client.split_and_send_the_file(user_file)  #json.dumps() - json file attached with request --- becomes like a text
#INTERVIEW - Copy.pptx


slave_nodes_list_to_retrieve_from = client.get_slave_nodes_list_for_the_file(user_file)
print(slave_nodes_list_to_retrieve_from)

retrieved_file = client.retrieve_the_file(user_file, slave_nodes_list_to_retrieve_from)