import pymongo

myclient = pymongo.MongoClient("mongodb+srv://cluster0-3cmqe.mongodb.net/?retryWrites=true&w=majority",
 username = 'unique', password = 'unique')
mydb = myclient["test"]
mycol = mydb["emailsvc"]

f = open('./emailsvc.csv', 'r')
for line in f:
    w = line.strip()
    rec = dict()
    rec['email'] = w
    mycol.insert_one(rec)

        
