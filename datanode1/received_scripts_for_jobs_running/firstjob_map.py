import csv
def map_function(csv_reader):
    separated_values = []
    for line in csv_reader:
        print('line:' + str(line))
        for word in line:
            separated_values += word.split(',')
    # separated values DONE
    to_return = []
    for single in separated_values:
        if single != '':
            key_value_pair = (single, 1)
            to_return.append(key_value_pair)
            print('word: ' + single)

    return to_return


