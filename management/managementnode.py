import json

from flask import jsonify, request
import csv
import requests

import threading
import time

class ManagementNode():
    def __init__(self, management_node_address: str):
        self.management_node_address = management_node_address
        self.control_table = {}

        with open('nodes_addresses.csv', 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            for line in csv_reader:
                for node in line:
                    if node != '':
                        self.control_table[node] = []
        print(self.control_table)

        self.status_track = {} # 'address' : 'status'
        self.jobs_running = {}# 'job' : ['process', 100, file_name on which the job is performed]

        #self.control_table = {'127.0.0.1:5021':
        #            []#які файли містить в собі кожна нода
                #'127.0.0.1:5022':
                #    []
        #}
# який сенс в тому, що клієнт може сам добавляти ноди???????? тобто це ніби ми(наша система/програма) надаємо йому сервіс
# обробити його дані на наших нодах і це все контролювати
# а в нього наприклад ще свої комп'ютери можуть бути? і він також хоче використовувати їх щоб обробляти свої дані?

        # request to working node- and it just tells us what changed and that's all( in slave's file system )
    def add_node(self, node_address):
        self.control_table[node_address] = []
    def get_slave_nodes(self):
        a = list(self.control_table.keys())
        to_json = {'slave_nodes_list': a}
        to_send = jsonify(to_json)
        print(str(to_send))
        return jsonify(to_json)

    def find_file(self, file_name):
        to_return_slave_nodes_list = []

        for key in self.control_table:
            for j in self.control_table[key]:
                if file_name.replace('.csv', '') in j and key not in to_return_slave_nodes_list:
                    to_return_slave_nodes_list.append(key)
                    break
        print(to_return_slave_nodes_list)
        return jsonify({"slave_nodes_with_needed_file": to_return_slave_nodes_list})

    def update_node_add_file(self, node_name, file_name):
        self.control_table[node_name].append(file_name)

# run a new  thread and use sockets for this communication



    def mapreduce_management(self, filename, mappy, reducepy, unique_job_name):
        list_of_slave_nodes = self.find_file(filename)

        #while True loop will halp me


        t1 = threading.Thread(target, args)
        t1.start()





    def map_launch(self, mappy, job_name):# + file_name
        self.jobs_running[job_name][0] = 'map'
        file_name = self.jobs_running[job_name][2]
        slave_nodes_working_on_given_file = requests.get(
            'http://' + self.management_node_address + '/get_nodes_with_file',
            json=json.dumps({'file_name': file_name}))
        nodes_doing_the_job = slave_nodes_working_on_given_file.json()[
            'slave_nodes_with_needed_file']

        for slave in nodes_doing_the_job:
            if self.status_track[str(slave)] not in ['map_processing', 'shuffle_processing', 'reduce_processing'] :
                response = requests.get('http://' + slave + '/map_launch',
                                        json=json.dumps({'file_name': file_name, 'map_py': mappy, 'job_name': job_name}))
                if response.status_code != 200:
                    return False

        return True #every node finished the job successfully

    def shuffle_launch(self, shuffle, job_name):
        file_name = self.jobs_running[job_name][2]
        slave_nodes_working_on_given_file = requests.get(
            'http://' + self.management_node_address + '/get_nodes_with_file',
            json=json.dumps({'file_name': file_name}))
        self.jobs_running[job_name][0] = 'shuffle'  ###щоб уникнути shuffle----100, бо від датаноди
        # відповідь з оновленим статусом чекати довше

        nodes_doing_the_job = slave_nodes_working_on_given_file.json()[
            'slave_nodes_with_needed_file']

        for slave in nodes_doing_the_job:
            if self.status_track[str(slave)] == 'map_finished':
                response = requests.get('http://' + slave + '/shuffle_launch',
                                        json=json.dumps(
                                            {'file_name': file_name, 'shuffle': shuffle, 'job_name': job_name}))
                if response.status_code != 200:
                    return False

        return True

    def reduce_launch(self, reduce_py, job_name):
        file_name = self.jobs_running[job_name][2]
        slave_nodes_working_on_given_file = requests.get(
            'http://' + self.management_node_address + '/get_nodes_with_file',
            json=json.dumps({'file_name': file_name}))
        self.jobs_running[job_name][0] = 'reduce'
        nodes_doing_the_job = slave_nodes_working_on_given_file.json()[
            'slave_nodes_with_needed_file']

        for slave in nodes_doing_the_job:
            if self.status_track[str(slave)] == 'shuffle_finished':
                response = requests.get('http://' + slave + '/reduce_launch',
                                        json=json.dumps(
                                            {'file_name': file_name, 'reduce_py': reduce_py, 'job_name': job_name}))
                if response.status_code != 200:
                    return False

        return True

    ### map_processing OR map_finished

    def percent_of_nodes_with_specific_status_for_the_job(self, unique_job_name, specific_status):

        file_name = self.jobs_running[unique_job_name][2]
        slave_nodes_working_on_given_file = requests.get('http://' + self.management_node_address + '/get_nodes_with_file',
                                                         json=json.dumps({'file_name': file_name}))
        nodes_doing_the_job = slave_nodes_working_on_given_file.json()[
            'slave_nodes_with_needed_file']
        print('nodes working' + str(nodes_doing_the_job))
        count_of_who_is_with_our_status = 0

        time.sleep(1) ## TO LOOK INTO THIS -- PROBABLY NOT NEEDED

        for slave in nodes_doing_the_job:
            if self.status_track[str(slave)] == specific_status:
                count_of_who_is_with_our_status += 1
        return (count_of_who_is_with_our_status / len(nodes_doing_the_job)) * 100


    def start_active_job_tracking(self, unique_job_name): #to_thread_continuous_updating
        #it can be 'map', 100 --> 'shuffle', 0       --- there will be nothing like "map_finished" or something

        to_continue_tracking = True
        while to_continue_tracking:
            try:
                check_if_the_job_is_running = self.jobs_running[unique_job_name]
                the_process_running_now = self.jobs_running[unique_job_name][0]
                status_to_check = the_process_running_now + '_finished'
                self.jobs_running[unique_job_name][1] = self.percent_of_nodes_with_specific_status_for_the_job(unique_job_name, status_to_check)

                #check_if_the_job_is_running = self.jobs_running[unique_job_name]
            except KeyError:
                to_continue_tracking = False
            except Exception as e:
                print(f"An error occured: {e}")
                to_continue_threading = False
        return 'job_tracking_finished'



        #stop when 100 is reached
        # or stop when our job will be deleted form JOBS_RUNNING list - so when it's finished
        # so it will be 'try catch' kind of scenario
        # залежно від того який статус нашої роботи - ми самі дивимось, що рахувати



