import csv
import email
import sys
import time
from pymongo import MongoClient
from queue import Queue
from threading import Thread

MONGO_DB_NAME = "enron"
MONGO_COLLECTION_NAME = "mails"
MAX_THREADS = 8
csv.field_size_limit(sys.maxsize)

def parser_worker(queue, out_queue):
    while True:
        text = queue.get()
        msg = email.message_from_string(text)
        mail_from = msg.get('From')
        mail_to = msg.get('To')
        mail_message = msg.get_payload()
        o = {"from": mail_from, "to": mail_to, "message": mail_message,}
        out_queue.put(o)
        queue.task_done()

def saver_worker(queue, i, buff):
    while True:
        obj = queue.get()
        buff[i].append(obj)
        queue.task_done()

def insert_worker(queue, db):
    while True:
        list = queue.get()
        print(f"Inserting chunk of: {len(list)} rows")
        db[MONGO_COLLECTION_NAME].insert_many(list)
        queue.task_done()


def init(file_name):
    
    buffer_data = [[] for i in range(MAX_THREADS)]
    client = MongoClient()
    db = client[MONGO_DB_NAME]
    start_time = time.time()
    
    db[MONGO_COLLECTION_NAME].drop()
    
    q = Queue()
    save_q = Queue()
    insert_q = Queue()

    print(f"Set up werker for every thread available ({MAX_THREADS})")
    for i in range(MAX_THREADS):
        t = Thread(target=parser_worker, args=(q, save_q,))
        t.daemon = True
        t.start()

    for i in range(MAX_THREADS):
        t = Thread(target=saver_worker, args=(save_q, i, buffer_data))
        t.daemon = True
        t.start()

    for i in range(MAX_THREADS):
        t = Thread(target=insert_worker, args=(insert_q, db,))
        t.daemon = True
        t.start()

    with open(file_name, "rt") as f:
        reader = csv.reader(f)
        headers = next(reader)
    
        #for i in range(100):
        #    q.put(next(reader)[1])
        print("Putin;) sum work into queue ...")

        for row in reader:
            q.put(row[1])
    
        q.join()
        save_q.join()
        print(f"Elapsed read/transform time: {time.time() - start_time}")
        for l in buffer_data:
            insert_q.put(l)
        insert_q.join()
        print(f"Elapsed total process time {time.time() - start_time}")


if __name__ == '__main__':
    try:
        file_name = sys.argv[1]
    except IndexError:
        print("Usage: main.py <file>")
        sys.exit(1)

    init(file_name)

