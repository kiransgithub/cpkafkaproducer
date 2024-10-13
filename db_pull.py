# Originating from testk_5_r0.py
import sys
import time
import datetime
import dbm
import re
import pyodbc
import json
import sql_pre_connection_setup as pre_odbc


def update_startPoint_jsonFile(startPoint_jsonFile_argument, update_value_argument):
    with open(startPoint_jsonFile_argument, "w") as startPoint_jsonFile_file:
        json.dump(update_value_argument, startPoint_jsonFile_file)


# print("Number of arguments at commandline: " + str(len(sys.argv)))
# SETUP time variables.
now_time_float = time.time()
now_time_with_milsec_int = int(now_time_float * 1000)

# This is where to set the Less-Than-Equal to value of the time-range in UnixTime in Milliseconds
# Setting it to Now - 30 seconds.
timestamp_endtime_inclusive = now_time_with_milsec_int - (30 * 1000)

now_time_YYYYMMDDTHHMMSS_zulu = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now_time_float))
# print("now_time_YYYYMMDDTHHMMSS_zulu value:" + now_time_YYYYMMDDTHHMMSS_zulu)

timestamp_endtime_inclusive_YYYYMMDDTHHMMSS_zulu = time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                                                 time.gmtime(timestamp_endtime_inclusive / 1000))
# END Setup time variables.

# Start PRE ODBC work
dsn_str_list = ["ms_sqlserver_yadw"]
if len(sys.argv) <= 5:
    print("TOO FEW ARGUMENTS.")
    pre_odbc.usageStatement_and_Exit(1, dsn_str_list)

pre_odbc.checking_DSN_pattern(sys.argv[1], dsn_str_list)
dsn = sys.argv[1]
sql_cmd = pre_odbc.get_sql_code_from_SQL_code_src_file(sys.argv[2])
checkPointMethod = pre_odbc.get_CheckpointMethod(sys.argv[3])
if checkPointMethod == 'TimeBased':
    startPoint_jsonFile = sys.argv[4]
    startingpoint = pre_odbc.get_startingPoint_from_startPoint_jsonFile(startPoint_jsonFile)
    dest_dir = pre_odbc.get_Output_Destination_Directory(sys.argv[5])
    if len(sys.argv) > 6:  # User has provided a Output_Filename_prefix at commandline when using TimeBased
        output_filename_prefix = sys.argv[6]
elif checkPointMethod == 'SequenceBased':
    sequenceBased_column = sys.argv[4]
    startPoint_jsonFile = sys.argv[5]
    startingpoint = pre_odbc.get_startingPoint_from_startPoint_jsonFile(startPoint_jsonFile)
    dest_dir = pre_odbc.get_Output_Destination_Directory(sys.argv[6])
    if len(sys.argv) > 7:  # User has provided a Output_Filename_prefix at commandline when using SequenceBased
        output_filename_prefix = sys.argv[7]
# if len(sys.argv) > 6: # User has provided a Output_Filename_prefix at commandline
#    output_filename_prefix = sys.argv[6]
# else:
#    output_filename_prefix = "SQL_output_" + dsn

TimeHasMillsecs = True

if checkPointMethod == 'TimeBased':
    if TimeHasMillsecs:
        timeInsecs = startingpoint / 1000
    else:
        timeInsecs = startingpoint
    startPoint_time_YYYYMMDDTHHMMSS_zulu = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(timeInsecs))
    output_filename = output_filename_prefix + "_" + startPoint_time_YYYYMMDDTHHMMSS_zulu + "_" + timestamp_endtime_inclusive_YYYYMMDDTHHMMSS_zulu + "_" + time.strftime(
        "%Y%m%d%H%M%SZ", time.gmtime(time_float)) + ".json"
elif checkPointMethod == 'SequenceBased':
    output_filename = output_filename_prefix + "_" + str(sequenceBased_column) + "_" + str(
        startingpoint) + "_" + time.strftime("%Y%m%d%H%M%SZ", time.gmtime(now_time_float)) + ".json"

if re.match(r'./$', dest_dir):
    output_path_n_filename = dest_dir + output_filename
else:
    output_path_n_filename = dest_dir + '/' + output_filename

# print("using DSN: " + dsn)
# print("using Checkpoint Method: " + checkPointMethod)
# print("using startingpoint: " + str(startingpoint))
# print("using Output_Filename_Prefix: " + output_filename_prefix)
# print("using output_filename: " + output_filename)
# print("using output_path_n_filename: " + output_path_n_filename)
### END PRE ODBC work ###

# dsn = ms_sqlserver_sem_mtr;
try:
    cnxn = pyodbc.connect('DSN=' + dsn)
    cursor = cnxn.cursor()
except pyodbc.Error as ex:
    msg = ex.args[1]
    if re.search('No Kerberos', msg):
        print('You must login using kinit before using this script.')
        exit(1)
    else:
        cnxn.close()
        raise

# sql_cmd = ""

# print("now_time_float: " + str(now_time_float))
# print("now_time_with_milsec_int: " + str(now_time_with_milsec_int))
# print("timestamp_endtime_inclusive: " + str(timestamp_endtime_inclusive))

if checkPointMethod == 'TimeBased':
    cursor.execute(sql_cmd, startingpoint, timestamp_endtime_inclusive)
elif checkPointMethod == 'SequenceBased':
    cursor.execute(sql_cmd, startingpoint)
    # print("SequenceBased column used: " + sequenceBased_column)
rows_returned_count = cursor.rowcount
row_accumulator = 0
# row = cursor.fetchone()
max_batch_size = 500
rows = cursor.fetchmany(max_batch_size)
fetch_batch_count = 1
# desc = row.cursor_description
# num_columns = len(rdesc)
output_path_n_filename_file = open(output_path_n_filename, "w")
while rows:
    # print("Fetching Batch number: " + str(fetch_batch_count))
    # print("Size of this Batch: " + str(len(rows)))
    iteration_count_within_batch = 0
    for row in rows:
        iteration_count_within_batch += 1
        # print("Iterating to Item: " + str(iteration_count_within_batch) + " within BATCH: " + str(fetch_batch_count))
        # jrow = json.dumps(row)
        rdesc = row.cursor_description
        num_columns = len(rdesc)
        row_dictionary_datastruct = {}
        # print(row)
        # print(rdesc)
        for column_position in range(num_columns):
            row_dictionary_datastruct[rdesc[column_position][0]] = row[column_position]
        # print(row_dictionary_datastruct)
        # print(json.dumps(row_dictionary_datastruct, default=str))
        json.dump(row_dictionary_datastruct, output_path_n_filename_file, default=str)
        output_path_n_filename_file.write(
            '\n')  # Add Newline character after each JSON object so it can be parsed by others.
    if checkPointMethod == 'SequenceBased':
        sequenceBased_column_temp_value = row_dictionary_datastruct[sequenceBased_column]
    row_accumulator += 1
    rows = cursor.fetchmany(max_batch_size)
    fetch_batch_count += 1
# print('success')
output_path_n_filename_file.close()
cnxn.close()
# print("Row Count: " + str(rows_returned_count))
print("Row count from accumulator: " + str(row_accumulator))
# print("now_time_YYYYMMDDTHHMMSS_gmt value:" + now_time_YYYYMMDDTHHMMSS_zulu)
# print("Using Output_Filename_Prefix: " + output_filename_prefix)
# print("Using output_filename: " + output_filename)
# print("Using output_path_n_filename: " + output_path_n_filename)
# print("OLD startingpoint value: " + str(startingpoint))
if checkPointMethod == 'TimeBased':
    update_startPoint_jsonFile(startPoint_jsonFile, timestamp_endtime_inclusive)
    # print("NEW startingpoint value on Next-RUN: " + str(timestamp_endtime_inclusive))
elif checkPointMethod == 'SequenceBased':
    update_startPoint_jsonFile(startPoint_jsonFile, sequenceBased_column_temp_value)
    print("NEW startingpoint value on Next-RUN: " + str(sequenceBased_column_temp_value))
