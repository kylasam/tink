import os
import matplotlib.pyplot as plt

contents = []
path_to_json = 'C:/Users/user/PycharmProjects/KafkaDataStreaming/data/ProcessedData/CustomerData_20191111'
json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]
print(json_files)
with open('output_file', 'w') as outfile:
    for fname in json_files:
        fname1=('../data/ProcessedData/CustomerData_20191111//' + fname)
        Uniue_elemnt = []
        COuntry_List=[]
        with open(fname1) as infile:
            for line in infile:
                values = (line.split (",")[0].split (":")[1].replace ('"', ''))
                Country=(line.split (",")[5].split (":")[1].replace ('"', ''))
                Uniue_elemnt.append (values)
                COuntry_List.append(Country)
                outfile.write(line)
        print("Unique id",len(set(Uniue_elemnt)))
        print ("Unique COuntry",len (set (COuntry_List)))
        print("COuntry list is ",COuntry_List)
        print("maximum occurences list:",max(COuntry_List,key=COuntry_List.count))
        print ("maximum occurences list:", min (COuntry_List, key=COuntry_List.count))
        plt.hist (COuntry_List)
        plt.show()


import re


with open('../data/ProcessedData/CustomerData_20191111//' + fname) as f:
    contents = f.read()
    Male_count = sum(1 for match in re.finditer(r"\bMale\b", contents))
    Female_count=sum(1 for match in re.finditer(r"\Female\b", contents))
print("Male COunt",Male_count)
print("Female COunt",Female_count)

