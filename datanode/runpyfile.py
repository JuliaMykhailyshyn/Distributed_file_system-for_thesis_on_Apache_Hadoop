import csv
import importlib
import sys
sys.path.append("my_folder")
# Let's say we have a variable that contains the name of the module we want to import
module_name = 'map_function'

# We can use importlib to import the module dynamically
module = importlib.import_module(module_name)

print(module.map_function('new.csv'))



