def reduce_function(key_and_list_of_values):
    what_we_got = {'key':[1,2,1,1,1],}
    for key_word in key_and_list_of_values:
        sum = 0
        for single_value in key_and_list_of_values[key_word]:
            sum += single_value
        key_and_list_of_values[key_word] = sum
    return key_and_list_of_values #dictionary {'key': 6,}
        
