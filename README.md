# HiveJsonToCsv_-python-Hive-UDF-

# ##################### Goals #####################################
# Read Json string row Data from hive,                            #
# manipulate each Json Rows,                                      #
# foreach Json Rows Create a csv file or print to screen.         #
# ################################################################

# it takes extra params that represents:
# operation flag to either print or generate csv files 
# 1: for create CSV files
# 0: for write data set to console
# **note** generating files take time because it has to run mutiple jobs to write fields into csv 
## sample run to create CSV files.
# add the pyhton script
> add file ~python_script~;
> SELECT TRANSFORM(~column~) USING '~python_script 1' as (~column_alias~)from ~hive_table~;

## sample run to print output to screen.
# add the pyhton script
> add file ~python_script~;
> SELECT TRANSFORM(~column~) USING '~python_script 0' as (~column_alias~)from ~hive_table~;

# **note** run cmd from local of python script **note**

-Cheers!
