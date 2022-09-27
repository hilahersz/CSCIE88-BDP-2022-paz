# Print steps
import datetime

IS_DEMO = False
IS_VERBOSE = False

MACHINE_URL = "hadoop@ec2-3-144-155-69.us-east-2.compute.amazonaws.com"
QUERY = "q4"
START = "2022-09-05 19:00"
END = "2022-09-06 08:00"

hasher = datetime.datetime.now().microsecond
hasher = QUERY + "_" + str(hasher)
hasher = "q4_1545"
if IS_DEMO:
    demo = "_demo"
else:
    demo = ""

print(f"Commands for {QUERY}:")

print("*********Copy files:*********")
s = f"scp -i hw4.pem hw4/src/q4_mapper.py hw4/src/q2_reducer.py " \
    f"{MACHINE_URL}:~"
print(s)
print("")

# if IS_DEMO:
#     print("*********Copy Demo file:*********")
#     s = f"scp -i hw4.pem hw4/logs_demo/file-input1.csv " \
#         f"{MACHINE_URL}:~/logs"
#     print(s)
#     print("")
#
#     print("*********Copy Demo Out file:*********")
#     s = f"scp -i hw4.pem hw4/logs_demo_mapper/out.csv " \
#         f"{MACHINE_URL}:~/logs_demo_mapper"
#     print(s)
#     print("")
#
# if IS_VERBOSE:
#     print("*********Copy Whole Log Files*********")
#     for i in range(1,5):
#         s = f"scp -i hw4.pem hw4/logs/file-input{i}.csv "\
#             f"{MACHINE_URL}:~/all-logs"
#         print(s)
#     print("")

print("*********Connect through ssh:*********")
s = f"ssh -i hw4.pem {MACHINE_URL}"
print(s)
print("")

# if IS_DEMO:
#     print("Run demo mapper: ")
#     s = f"cat logs | python q2_mapper.py -qt {QUERY} | sort"
#     print(s)
#     print("")
#
#     print("Run demo reducer: ")
#     s = "cat logs_demo_mapper | python q2_reducer.py"
#     print(s)
#     print("")
#
#     print("Run Whole Process: ")
#     s = f"cat logs | python q2_mapper.py -qt {QUERY} | sort | python q2_reducer.py"
#     print(s)
#     print("")
#
# if IS_VERBOSE:
#     print("Run Whole Process: ")
#     s = f"cat all-logs | python q2_mapper.py -qt {QUERY} | sort | python q2_reducer.py"
#     print(s)
#     print("")

print(f"*********Submit job for query {QUERY}:*********")
h = hasher
s = f"""hadoop jar /usr/lib/hadoop/hadoop-streaming.jar \
-file ./q4_mapper.py \
-file ./q2_reducer.py \
-mapper "q4_mapper.py -s '{START}' -e '{END}'" \
-reducer "q2_reducer.py" \
-input s3://hila-hw4/logs{demo}/ \
-output s3://hila-hw4/output_{h}/
"""
print(s)

print("*********Get files:*********")
s = f"hdfs dfs -get s3://hila-hw4/output_{h}/"
print(s)

Q4_KEYS = [
"2022-09-05:19 , UG",
"2022-09-05:19 , DK",
"2022-09-05:19 , ER",
"2022-09-05:19 , GD",
"2022-09-05:19 , NG",
]

print(f"*********Display the required results ({QUERY}):*********")
for k in Q4_KEYS:
    print(f"cat output_{hasher}/part* | grep '{k}' ")
#
# if IS_VERBOSE:
#     for k in key_map.get(QUERY):
#         s = f"cat all-logs | python q2_mapper.py -qt {QUERY} | sort | python q2_reducer.py | grep '{k}' "
#         print(s)
#
#     print("local debug:")
#     for k in key_map.get(QUERY):
#         s = f"cat logs/*.csv | python src/q2_mapper.py -qt {QUERY} | sort | python src/q2_reducer.py | grep '{k}' "
#         print(s)



