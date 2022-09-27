# Print steps
import datetime

IS_DEMO = False
IS_VERBOSE = False

MACHINE_URL = "hadoop@ec2-3-17-208-49.us-east-2.compute.amazonaws.com"
QUERY = "q1"
REDUCERS=2

hasher = datetime.datetime.now().microsecond
hasher = QUERY + "_" + str(hasher)
# hasher = "q1_12031"
if IS_DEMO:
    demo = "_demo"
else:
    demo = ""

print(f"Commands for {QUERY}:")

print("*********Copy files:*********")
s = f"scp -i hw4.pem hw4/src/q2_mapper.py hw4/src/q2_reducer.py " \
    f"{MACHINE_URL}:~"
print(s)
print("")

if IS_DEMO:
    print("*********Copy Demo file:*********")
    s = f"scp -i hw4.pem hw4/logs_demo/file-input1.csv " \
        f"{MACHINE_URL}:~/logs"
    print(s)
    print("")

    print("*********Copy Demo Out file:*********")
    s = f"scp -i hw4.pem hw4/logs_demo_mapper/out.csv " \
        f"{MACHINE_URL}:~/logs_demo_mapper"
    print(s)
    print("")

if IS_VERBOSE:
    print("*********Copy Whole Log Files*********")
    for i in range(1,5):
        s = f"scp -i hw4.pem hw4/logs/file-input{i}.csv "\
            f"{MACHINE_URL}:~/all-logs"
        print(s)
    print("")

print("*********Connect through ssh:*********")
s = f"ssh -i hw4.pem {MACHINE_URL}"
print(s)
print("")

if IS_DEMO:
    print("Run demo mapper: ")
    s = f"cat logs | python q2_mapper.py -qt {QUERY} | sort"
    print(s)
    print("")

    print("Run demo reducer: ")
    s = "cat logs_demo_mapper | python q2_reducer.py"
    print(s)
    print("")

    print("Run Whole Process: ")
    s = f"cat logs | python q2_mapper.py -qt {QUERY} | sort | python q2_reducer.py"
    print(s)
    print("")

if IS_VERBOSE:
    print("Run Whole Process: ")
    s = f"cat all-logs | python q2_mapper.py -qt {QUERY} | sort | python q2_reducer.py"
    print(s)
    print("")

print(f"*********Submit job for query {QUERY}:*********")
h = hasher
s = f"""hadoop jar /usr/lib/hadoop/hadoop-streaming.jar \
-D mapreduce.job.reduces={REDUCERS} \
-file ./q2_mapper.py \
-file ./q2_reducer.py \
-mapper "q2_mapper.py -qt {QUERY}" \
-reducer "q2_reducer.py" \
-input s3://hila-hw4/logs{demo}/ \
-output s3://hila-hw4/output_{h}/
"""
print(s)

print("*********Get files:*********")
s = f"hdfs dfs -get s3://hila-hw4/output_{h}/"
print(s)

Q1_KEYS = [
    "22-09-06 04",
    "22-09-06 05",
    "22-09-06 06",
    "22-09-06 07",
    "22-09-06 08",
]
Q2_KEYS = [
    "22-09-03 13:http://example.com/?url=124",
    "22-09-03 13:http://example.com/?url=056",
    "22-09-03 13:http://example.com/?url=066",
    "22-09-03 13:http://example.com/?url=167",
    "22-09-03 13:http://example.com/?url=113",
]

Q3_KEYS = [
    "22-09-03 13:http://example.com/?url=124",
    "22-09-03 13:http://example.com/?url=056",
    "22-09-03 13:http://example.com/?url=066",
    "22-09-03 13:http://example.com/?url=167",
    "22-09-03 13:http://example.com/?url=113",

]

key_map = {"q1": Q1_KEYS, "q2": Q2_KEYS, "q3": Q3_KEYS}

print(f"*********Display the required results ({QUERY}):*********")
for k in key_map.get(QUERY):
    print(f"cat output_{hasher}/part* | grep '{k}' ")

if IS_VERBOSE:
    for k in key_map.get(QUERY):
        s = f"cat all-logs | python q2_mapper.py -qt {QUERY} | sort | python q2_reducer.py | grep '{k}' "
        print(s)

    print("local debug:")
    for k in key_map.get(QUERY):
        s = f"cat logs/*.csv | python src/q2_mapper.py -qt {QUERY} | sort | python src/q2_reducer.py | grep '{k}' "
        print(s)



