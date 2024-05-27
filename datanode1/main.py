from flask import Flask, request, jsonify
from datanode import DataNode
from flask_cors import CORS
import json
import requests

import threading
import time

app = Flask('datanode')
CORS(app)  # Дозволяє CORS для всіх маршрутів

datanode = DataNode('127.0.0.1:5022')
datanode.inform_status()


# @app.post or @app.get also can be used instead of @app.route


@app.route('/post_trial', methods=['POST'])
def post_trial():
    json_to_post = request.json
    json_obj = json.loads(json_to_post)  # Assuming json_to_post is a JSON string
    print(str(json_obj))

    to_return = datanode.place_file(json_obj['file_name'], json_obj['my_bytes_string_file'])
    requests.post('http://' + datanode.management_node + '/update_add_file',
                  json=json.dumps({'slave_node_name': datanode.address,
                                   'file_name': json_obj['file_name']}))
    return to_return


# кожна датанода має сама мати собі список того що в себе зберегла
@app.route("/node/get_file", methods=['GET'])
def get_file():
    json_received = json.loads(request.json)
    return datanode.assemble_file(json_received['file_name'])


'''@app.route('/map_launch', methods = ['GET'])
def map_launch():
    return
@app.route('/shuffle_launch', methods = ['POST'])
def shuffle_launch():
    return

@app.route('/reduce_launch', methods = ['GET'])
def reduce_launch():
    return

'''


@app.route('/turn_on_heartbeat_function', methods=["POST"])
def turn_on_heartbeat_function():
    print("IT GOT HERE")
    received_json = json.loads(request.get_json())  #### = json.loads(request.json)
    if received_json['heartbeat'] == 'ON':
        datanode.heart_beat = 'ON'
    else:
        datanode.heart_beat = 'OFF'
    response = ''

    def function_to_thread():
        if datanode.heart_beat == 'ON':
            to_inform = True
        else:
            to_inform = False
        while to_inform:
            response = requests.post('http://' + datanode.management_node + '/heartbeat',
                                     json=json.dumps(
                                         {'node_address': datanode.address, 'status': datanode.node_status}))
            time.sleep(2)
            if datanode.heart_beat != 'ON':
                to_inform = False

    my_thread = threading.Thread(target=function_to_thread)  # passing an object of function! -- without calling
    my_thread.start()
    # not an important thread - if everything else is finished - you can quit the script
    # daemon thread!! (target=,daemon=True).start()

    # Once app.run() is called, it starts the server and
    # waits for requests, blocking any further code
    # execution within the if __name__ == "__main__" block.
    # It's common to have initialization code before app.run(),
    # but any code meant to execute after app.run() would not
    # run unless you terminate the server.
    return response


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5022)

# I need my management node to inform the client of the progress happening
# as the percentage of the execution that's finished




