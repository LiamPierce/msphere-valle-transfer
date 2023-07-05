import sys
import os
import subprocess
import threading
import time
import re

data_version_id = "ds3x3swdvw6xp1v:13o9aod"
lock = threading.RLock()
finished_file_count = 0;

thread_queue = [];
file_count = 0

global_tic = time.perf_counter()

threads = []
thread_count = 1
batch_size=10

def thread(tid):
    global data_version_id
    global thread_queue
    global file_count
    global finished_file_count
    global batch_size

    while True:
        time.sleep(0.2)
        if not len(thread_queue[tid]):
            time.sleep(0.1) 
            continue;

        result = None

        if batch_size > 1:
            data = [thread_queue[tid].pop() for _ in range(min(len(thread_queue[tid]), batch_size))]
            subprocess.call((f"mv -t /valle/transfer/{tid} " + (" ".join([f"/valle/data/{fname}" for fname in data]))).split(" "))

            print("Attempting upload of "+str(len(os.listdir(f"/valle/transfer/{tid}"))))
            result = subprocess.run([
                "gradient",
                "datasets",
                "files",
                "put",
                "--id", 
                data_version_id,
                f"--source-path",f"/valle/transfer/{tid}/",
                "--target-path", "/"
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            uploaded = int(re.search(r'\((\d+)\)', result.stdout.decode().strip()).group(1))

            subprocess.call((f"mv -t /valle/data "+(" ".join([f"/valle/transfer/{tid}/{fname}" for fname in data]))).split(" "))
            
            if uploaded < 10:
                print(f"Failure in thread {tid}: Gradient silently failed to upload batch.")
                continue
        else:
            data = thread_queue[tid].pop()
            result = subprocess.run([
                "gradient",
                "datasets",
                "files",
                "put",
                "--id", 
                data_version_id,
                f"--source-path",f"/valle/data/{data}",
                "--target-path", "/"
            ], stderr=subprocess.PIPE);#stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

        if result.stderr.decode().strip() != "":
            print(f"Failure in thread {tid}:"+result.stderr.decode().strip())
            # There was an error. Put back the data for the next try.
            thread_queue[tid].extend(data)
            continue;

        
        with lock:
            finished_file_count+=batch_size
            print(f"{finished_file_count} of {file_count} in "+str(time.perf_counter() - global_tic)+" seconds. "+str(((time.perf_counter() - global_tic)/finished_file_count*file_count)/3600)+" hours remaining.")



for i in range(0, thread_count):
    thread_queue.insert(i, []);
    
    try:
        os.mkdir(f"/valle/transfer/{i}")
    except OSError: pass
        
    t = threading.Thread(target=thread, args=(i,))
    
    threads.append(t);

subprocess.call("mv /valle/transfer/*/* /valle/data", shell=True) 


total=0
for i in os.listdir("/valle/data"):
    total = total + 1;
    file_count = file_count + 1
    thread_queue[total % thread_count].append(i) 


for i, queue in enumerate(thread_queue):
    print(f"Queue {i} : "+str(len(queue)))  

for thread in threads:
    thread.start()

for thread in threads:
    thread.join()

print("Done!")
#subprocess.call([
#    "gradient",
#    "datasets",
#    "versions",
#    "commit",
#    "--id",
#    data_version_id
#])
