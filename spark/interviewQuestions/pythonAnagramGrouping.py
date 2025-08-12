# Anagram Grouping: Input = ['nat', 'tan', 'bat', 'ate', 'eat', 'tea'] and 


s= ['nat', 'tan', 'bat', 'ate', 'eat', 'tea']


dictionary = {}


for i in s:
    # print(i)
    sortedi = ''.join(sorted(i))
    if sortedi in dictionary.keys():
        dictionary[sortedi].append(i)
    else:
        dictionary[sortedi] = [i]


print([values for key,values in dictionary.items()])
#output = [['nat', 'tan'], ['bat'],['ate','eat','tea']]
