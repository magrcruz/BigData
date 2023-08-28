import re
import threading
import nltk
from nltk.stem import SnowballStemmer
from multiprocessing import Manager
from collections import defaultdict
import time

class ResourceQueue:
    def __init__(self):
        self.queue = []
        self.lock = threading.Lock()

    def enqueue(self, item):
        with self.lock:
            self.queue.append(item)

    def dequeue(self):
        with self.lock:
            if self.queue:
                return self.queue.pop(0)
            return None
queues =  [ResourceQueue() for _ in range(12)]

threads = []
num_threads = 8

dictionaries = [defaultdict(int) for _ in range(12)] #12 diccionarios

file = open('content/0.5Falcon.txt', 'r')
file_lock = threading.Lock()
batch = 1024*1024

global proccesing_file
proccesing_file = True

nltk.download('punkt')  # Descargar los datos necesarios para el tokenizado
nltk.download('wordnet')  # Descargar los datos necesarios para la lematización
stemmer = SnowballStemmer("english")  # Utilizar el stemmer para el idioma inglés
reTkn = re.compile(r'[a-zA-Z]+', re.UNICODE)

container = [0,1,1,1,2,3,3,3,4,4,4,5,5,6,7,8,8,8,9,10,11,11,11,11,11,11]

def hash(letra):
    return container[ord(letra)-97] #handcrafted

# Función que lee contenido desde el archivo
def read_file():
    #minifile = open("content/temp/"+threading.current_thread().name,"w")
    while True:
        with file_lock:
            text = file.read(batch)
            if not text:
                #file_lock.release()
                break
            #hacer que no corte palabras
            while re.search(r"\w+", text[-1]):
                text += file.read(1)
        words = reTkn.findall(text)
        stemmed_words = [stemmer.stem(word) for word in words if word.isalpha()]  # Solo aplicar a palabras alfabéticas
        
        arrs = [[] for _ in range(12)]
        for w in stemmed_words: arrs[hash(w[0])].append(w)
        for i in range(len(arrs)):
            queues[i].enqueue(arrs[i])
        
def mycount(i1,i2):
    global proccesing_file

    full = True
    while full or proccesing_file:
        full = False
        arr = queues[i1].dequeue()#optimizable por un notify
        if arr!=None:
            full = True
            for w in arr:
                dictionaries[i1][w] +=1
            
        arr = queues[i2].dequeue()#optimizable por un notify
        if arr!=None:
            full = True
            for w in arr:
                dictionaries[i2][w] +=1

def main():

    start_time = time.time()

    threads2 = []

    # Crear los hilos y agregarlos a la lista
    for idx in range(num_threads):
        thread = threading.Thread(target=read_file)
        thread.name = f"Thread-{idx+1}"
        threads.append(thread)
    # Iniciar los hilos
    #for thread in threads:
        thread.start()

    threads2 = [threading.Thread(target=mycount,args=([9,2])),
                threading.Thread(target=mycount,args=([5,3])),
                threading.Thread(target=mycount,args=([6,10])),
                threading.Thread(target=mycount,args=([7,11])),
                threading.Thread(target=mycount,args=([4,1])),
                threading.Thread(target=mycount,args=([0,8]))]
    for idx in range(len(threads2)):
        threads2[idx].start()

    # Esperar a que los hilos terminen
    for thread in threads:
        thread.join()
    print("Termino de procesar el archivo")

    global proccesing_file
    proccesing_file = False

    for thread in threads2:
        thread.join()

    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"Tiempo de ejecución: {elapsed_time} segundos")
    for dict in dictionaries:
        for key, value in sorted(dict.items()):
            print(key, value)

if __name__ == "__main__":
    main()