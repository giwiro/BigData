import time
from pymongo import MongoClient
from bson import Code
from pprint import pprint

MONGO_DB_NAME = "enron"
MONGO_COLLECTION_NAME = "mails"
client = MongoClient()
db = client[MONGO_DB_NAME]

mapper = Code(
    """
        function map() {
            emit(this.from, this.message.split(' ').length)
        }
        
    """
)

reducer = Code(
    """
        function reducer(k, v) {
            return Array.sum(v)
        }
    """
)

if __name__ == '__main__':
    start_time = time.time()

    print("Executing map_reduce...")
    res = db[MONGO_COLLECTION_NAME].map_reduce(mapper, reducer, "words")
    print("Top three writers (including bots and forwared messages):")
    for i, doc in enumerate(res.find().sort("value", -1).limit(3)):
            pprint(f"{i + 1}. {doc}")
    
    print(f"Elapsed time: {time.time() - start_time}")
