from flask import Flask, request
from managementnode import ManagementNode
import json
from flask_cors import CORS

from flask import render_template, jsonify

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


#@app.


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5010)  # Start the Flask server
#app.run('localhost', 5010)


