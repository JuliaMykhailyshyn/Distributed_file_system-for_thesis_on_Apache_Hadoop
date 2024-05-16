from flask import jsonify

class ManagementNode():
    def __init__(self, management_node_address: str):
        self.management_node_address = management_node_address
        self.control_table = {'127.0.0.1:5021':
                    []#які файли містить в собі кожна нода
                #'127.0.0.1:5022':
                #    []

        }
        # request to working node- and it just tells us what changed and that's all
        # we need updates

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
                if file_name in j and key not in to_return_slave_nodes_list:#лишнє
                    to_return_slave_nodes_list.append(key)
                    break
        print(to_return_slave_nodes_list)
        return jsonify({"slave_nodes_with_needed_file": to_return_slave_nodes_list})

    def update_node_add_file(self, node_name, file_name):
        self.control_table[node_name].append(file_name)







