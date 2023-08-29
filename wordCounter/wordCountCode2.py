import re
import threading
import nltk
from nltk.stem import SnowballStemmer
from multiprocessing import Manager
from collections import defaultdict
import time

num_threads_reader = 8
num_threads_counter = 4

threads_reader = []
threads_counter = []

diccionarios = [defaultdict(int) for _ in range(num_threads_counter)] #num_threads_counter diccionarios

file = open('content/0.5Falcon.txt', 'r')
file_lock = threading.Lock()
batch = 8*1024*1024

global proccesing_file
proccesing_file = True

nltk.download('punkt')  
nltk.download('wordnet')  # Descargar para la lematización
stemmer = SnowballStemmer("english")  # Utilizar el stemmer para el idioma inglés
reTkn = re.compile(r'[a-zA-Z]+', re.UNICODE)

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
queues = [[ResourceQueue() for _ in range(num_threads_reader)] for __ in range(num_threads_counter)]

container = [0,1,1,1,2,3,3,3,4,4,4,5,5,6,7,8,8,8,9,10,11,11,11,11,11,11]

def myhash(letra):
    return container[ord(letra)-97]%num_threads_counter #handcrafted

# Función que lee contenido desde el archivo
def read_file(idx):
    while True:
        with file_lock:
            text = file.read(batch)
            if not text:
                break
            #hacer que no corte palabras
            while re.search(r"\w+", text[-1]):
                text += file.read(1)
        words = reTkn.findall(text)
        stemmed_words = [stemmer.stem(word) for word in words if word.isalpha()]  # Solo aplicar a palabras alfabéticas
        
        arrs = [[] for _ in range(num_threads_counter)]
        for w in stemmed_words: arrs[myhash(w[0])].append(w)
        for i in range(len(arrs)):
            queues[i][idx].enqueue(arrs[i])
        
def myCount(idx):
    global proccesing_file
    has_content = True
    while proccesing_file and has_content:
        has_content = False
        for i in range(num_threads_reader):
            has_content = True
            arr = queues[idx][i].dequeue()
            if arr!=None:
                has_content = True
                for w in arr:
                    diccionarios[idx][w]+=1

def main():

    start_time = time.time()

    # Crear los hilos y agregarlos a la lista
    for idx in range(num_threads_reader):
        thread = threading.Thread(target=read_file, args=(idx,))
        thread.name = f"Thread-{idx+1}"
        threads_reader.append(thread)

    for thread in threads_reader:
        thread.start()

    for idx in range(num_threads_counter):
        thread = threading.Thread(target=myCount, args=(idx,))
        threads_counter.append(thread)
        thread.start()

    # Esperar a que los hilos terminen
    for thread in threads_reader:
        thread.join()
    print("Termino de procesar el archivo")

    global proccesing_file
    proccesing_file = False

    for thread in threads_counter:
        thread.join()

    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"Tiempo de ejecución: {elapsed_time} segundos")
    for dict in diccionarios:
        for key, value in sorted(dict.items()):
            print(key, value)

if __name__ == "__main__":
    main()