import re
import threading
import nltk
from nltk.corpus import words, stopwords
from nltk.stem import SnowballStemmer
from multiprocessing import Manager
from collections import defaultdict
import time

nltk.download('stopwords')
nltk.download('words')
nltk.download('punkt')
nltk.download('wordnet')
nltk.download('omw-1.4')
stop = stopwords.words('english')
stemmer = SnowballStemmer("english")  # Utilizar el stemmer para el idioma inglés
reTkn = re.compile(r'[a-zA-Z]+', re.UNICODE)

inverted_index = {}
batch = 1024*1024
threads = []

#def getMiniFile(name, size):

def read_file(namefile, batch, idx):
    file = open(namefile,'r')
    while True:
        text = file.read(batch)
        if not text:
            break
        #hacer que no corte palabras
        while re.search(r"\w+", text[-1]): text += file.read(1)
        words = reTkn.findall(text)
        stemmed_words = [stemmer.stem(word) for word in words if word not in (stop) and word.isalpha()]  # Solo aplicar a palabras alfabéticas
        
        for word in stemmed_words:
            if word in inverted_index:
                inverted_index[word].append(idx)
            else:
                inverted_index[word] = idx

def main():
    files = [
        ""
    ]

    num_threads = len(files)
    for i in num_threads:
        thread = threading.Thread(target=read_file, args=(files[i],batch,i))
        thread.name = f"Thread-{i}"
        threads.append(thread)

    start_time = time.time()

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"Tiempo de ejecución: {elapsed_time} segundos\n")

    print("Archivos")
    for i in range(len(files)):
        print(i, files)

    print("Indice invertido")
    for key, value in sorted(dict.items()):
        print(key, value)

if __name__ == "__main__":
    main()