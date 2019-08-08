import sys
import json
import commands
###################################error message################################
def no_param_provided():
        ruler = "#################################"
        msg = ("\tNo paramiter provided\nPlease provide parameter based on the following operations\n"+
        "to print data set(s): 0\n"+
        "to write data set(s) in csv files:1\n ")
        print ruler
        print msg
####################################end of error msgs###############################
#Hdfs command to create csv file
def createCsvFileToDir(file_name):
        #create the file cmd
        create_cmd = 'hadoop fs -touchz /user/bdgatetl/.staging/dataDir/'+file_name
        #create_cmd = 'cat>'+file_name
        status,output = commands.getstatusoutput(create_cmd)
        return file_name
#write json object into csv file object
def write_to_csv_file(key, value):
        global file_name
        full_path = '/user/bdgatetl/.staging/dataDir/'+file_name
        #hadoop fs -appendToFile - /dir/hadoop/hello_world.txt
        cmd = "echo "+key+','+value+" | hadoop fs -appendToFile  - "+full_path
        status,output=commands.getstatusoutput(cmd)
#@abdullah write json Object into csv format
def iterable_csv_writer(di):
        global operation_flag
	for sk,sv in di.items():
		if sk=='eventType':
			pass
		elif type(sv)==dict:
			iterable_csv_writer(sv)
		else:
                        if operation_flag == 1:
                                write_to_csv_file(str(sk),str(sv))
                        else:
			        print ','.join([str(sk),str(sv)])

#function to start csv creation
def create_csv(record):
    #call reduce function
    #return reduces version of json object
    global rowNo
    global operation_flag
    print "row number:", rowNo
    #convert json to python
    json_obj = json.loads(record)
    #base dictionary
    d = {}
    #for every event type in list put them in list d
    for i in range(0, len(json_obj)):
            d[json_obj[i]["eventType"]] = json_obj[i]
    #for every key in dict d, create the csv format
    for k in d.keys():
            #print ":".join(["eventType", k])
            #header for csv file
            #if creating csv file write type
            #else just print types
            if operation_flag == 1:
                    write_to_csv_file("eventType",k)
            else:
                    print ','.join(["eventType", k])
            iterable_csv_writer(d[k])
    rowNo = rowNo +1
#main code
rowNo = 0
file_name = ""
#operation flag to either print or generate csv files 
# 1: for create scv files
# 0: for write data set to console
# **note** 
#generating files take time because it has to run mutiple jobs to write fields into csv
operation_flag = 2
if len(sys.argv) == 2 and int(sys.argv[1]) == 1:
        operation_flag = 1
elif len(sys.argv) == 2 and int(sys.argv[1]) == 0:
        operation_flag = 0
else:
        print "provide a flag 1 to create csv files, 0 to print sample sets"
if operation_flag != 2:
        for line in sys.stdin:
                if operation_flag == 1:
                        file_name = str(rowNo)+".csv"
                        #create csv file here
                        createCsvFileToDir(file_name)
                create_csv(line)
else:
        no_param_provided()

        
                