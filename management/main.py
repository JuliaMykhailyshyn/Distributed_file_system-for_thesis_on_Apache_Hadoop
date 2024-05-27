from flask import Flask, request
from managementnode import ManagementNode
import json
from flask_cors import CORS

import time
import requests
from flask import render_template, jsonify

import threading

control_node = ManagementNode('127.0.0.1:5010')

app = Flask('management_node')
CORS(app)  # Дозволяє CORS для всіх маршрутів.0.

@app.route('/get_working_nodes', methods = ["GET"])
def get_working_nodes():

    #return "<head><body><h1>" + str(to_send) + "</h1></body></head>"
    #return render_template('working_nodes.html', data=to_json)

    #to_json = {'slave_nodes_list': control_node.get_slave_nodes()}
    #to_send = jsonify(to_json)
    #print(str(to_send))
    #return json.dumps(to_json)
    return control_node.get_slave_nodes()

@app.route('/get_nodes_with_file', methods= ["GET"])
def get_nodes_with_file():
    #i have two options response.data OR response.json -- data is more simple
    received_json = json.loads(request.json)
    return control_node.find_file(received_json['file_name'])

@app.route('/update_add_file', methods = ["POST"])
def update_add_file():
    json_received = json.loads(request.json)
    control_node.update_node_add_file(json_received['slave_node_name'], json_received['file_name'])
    return 'Success'


@app.route('/heartbeat', methods = ["POST"])
def heartbeat():
    json_received = json.loads(request.json)
    if json_received['node_address'] in control_node.control_table.keys():
        control_node.status_track[json_received['node_address']] = json_received['status']
        print (control_node.status_track)
    return "success"


# 1.
# do I need to save those .py files for the job requested
# for us to run on the management node itseolf??
# maybe for safety reasons
# but I think we have it inside our huge request "/mapreduce/launch/"

@app.route('/mapreduce/launch', methods = ["GET"]) # my app is threaded so I should not worry about the down time I think
def launch_mapreduce():
    # we were given
    # json=json.dumps({'filename': filename, 'map_code': mappy_encoded,
    #                                       'reduce_code': reducepy_encoded, 'unique_job_name': unique_job_name})
    json_received = json.loads(request.json)
    unique_job_name = json_received['unique_job_name']
    file_name = json_received['filename']

    # 1. роботу яку нам дали - починаємо - вносимо її в словник jobs_running
    control_node.jobs_running[unique_job_name] = ['waiting_to_be_processed', '0', file_name]
    # process, percent, file_on_which_we_are_working

    # 2. просимо систематичне повідомлення від датанод - їх статус - чим зараз займаються
    list_of_working_nodes = requests.get('http://' + control_node.management_node_address + "/get_working_nodes").json()
    print(list_of_working_nodes)
    for slave in list_of_working_nodes['slave_nodes_list']:
        print(str(slave))
        requests.post('http://' + slave + '/turn_on_heartbeat_function',
                      json=json.dumps({'heartbeat': 'ON'}))


    time.sleep(3) # можливо не потрібно буде # need to wair until our slave nodes respond to this heartbeat request
    # 3. start tracking the process
    to_thread_continuous_updating = threading.Thread(target=control_node.start_active_job_tracking, args=(unique_job_name,))
    to_thread_continuous_updating.start()

    # 4. get the job going!!
    control_node.jobs_running[unique_job_name][0] = 'map'
    time.sleep(20)# current request is on pause, but the flask app is threaded !!
    control_node.jobs_running[unique_job_name][0] = 'shuffle'
    time.sleep(20)
    control_node.jobs_running[unique_job_name][0] = 'reduce'
    time.sleep(20)




    # DELETE the job FROM THE LIST JOBS_RUNNING
    if unique_job_name in control_node.jobs_running:
        del control_node.jobs_running[unique_job_name]

    # nodes don't have to update me on their status so ofter now
    for slave in list_of_working_nodes['slave_nodes_list']:     #можеш мене he апдейтити тепер
        requests.post('http://' + slave + '/turn_on_heartbeat_function',
                      json=json.dumps({'heartbeat': 'OFF'}))
    print('/mapreduce/launch SUCCESS')
    return jsonify({'hello': json_received['unique_job_name']})


@app.route('/inform_client_of_job_status', methods = ["GET"])
def inform_client_of_status():
    json_received = json.loads(request.json)
    job_identifier = json_received['job_identifier']
    if job_identifier not in control_node.jobs_running.keys():
        return jsonify({'process': "The job finished or never started"})
    to_return = {'process': 1, 'percentage_of_nodes_completed': 0}
    to_return['process'] = control_node.jobs_running[job_identifier][0]
    to_return['percentage_of_nodes_completed'] = control_node.jobs_running[job_identifier][1]

    return jsonify(to_return)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5010, threaded=True)  # Start the Flask server
    #threading- multiple requests to be processed concurrently

#app.run('localhost', 5010)


