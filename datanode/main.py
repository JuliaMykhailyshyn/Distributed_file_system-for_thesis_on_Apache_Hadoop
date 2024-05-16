from flask import Flask, request, jsonify
from datanode import DataNode
from flask_cors import CORS
import json
import requests


app = Flask('datanode')
CORS(app)  # Дозволяє CORS для всіх маршрутів

datanode = DataNode('127.0.0.1:5021')
#@app.post or @app.get also can be used instead of @app.route



@app.route('/post_trial', methods = ['POST'])###GREAT
def post_trial():
    json_to_post = request.json
    json_obj = json.loads(json_to_post)  # Assuming json_to_post is a JSON string
    print(str(json_obj))

    to_return = datanode.place_file(json_obj['file_name'], json_obj['my_bytes_string_file'])
    requests.post('http://' + datanode.management_node + '/update_add_file',
                  json=json.dumps({'slave_node_name': datanode.address,
                                   'file_name': json_obj['file_name']}))




#кожна датанода має сама мати собі список того що в себе зберегла
@app.route("/node/get_file", methods = ['GET'])
def get_file():
    json_received = json.loads(request.json)
    return datanode.assemble_file(json_received['file_name'])


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5021)


