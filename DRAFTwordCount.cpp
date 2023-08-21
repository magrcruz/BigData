//DRAFT
//https://www.geeksforgeeks.org/implement-thread-safe-queue-in-c/
#include <map>


// C++ implementation of the above approach
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <queue>

using namespace std;

// Thread-safe queue
template <typename T>
class TSQueue {
private:
    // Underlying queue
    std::queue<T> m_queue;
  
    // mutex for thread synchronization
    std::mutex m_mutex;

    // mutex for file read
    std::mutex m_file;
  
    // Condition variable for signaling
    std::condition_variable m_cond;
  
public:
    // Pushes an element to the queue
    void push(T item)
    {
        // Acquire lock
        std::unique_lock<std::mutex> lock(m_mutex);

        // Add item
        m_queue.push(item);
  
        // Notify one thread that
        // is waiting
        m_cond.notify_one();
    }
  
    // Pops an element off the queue
    T pop()
    {
  
        // acquire lock
        std::unique_lock<std::mutex> lock(m_mutex);
  
        // wait until queue is not empty
        m_cond.wait(lock,
                    [this]() { return !m_queue.empty(); });
  
        // retrieve item
        T item = m_queue.front();
        m_queue.pop();
  
        // return item
        return item;
    }
};

void countFragment(){

}

// Driver code
int main()
{
    TSQueue<int> q;
  
    // Push some data
    q.push(1);
    q.push(2);
    q.push(3);
  
    // Pop some data
    std::cout << q.pop() << std::endl;
    std::cout << q.pop() << std::endl;
    std::cout << q.pop() << std::endl;
  
    //
    


    return 0;
}




#include <stdio.h>
#include <vector>

using namespace std;
//PASOS
FILE *pFile, *start, *end;
pFile = fopen ("myfile.txt","w");
start = pFile;
if (pFile!=NULL){
    fputs ("fopen example",pFile);
    fclose (pFile);
}

//Ver el tamanio del archivo
long long int size;
fseek ( pFile , 0 , SEEK_END );
end = pFile;
size = pFile - start;
rewind (pFile);

//Dividir en bloques, un tamnio aproximado y busca el primer espacio disponible
int stringAproxSize = 100000;

TSQueue<long long int> positions;

void splitFile(){
    positions.push(0);
    while (pFile< end){
        fseek ( pFile , stringAproxSize , SEEK_CUR );
        while (*pFile != '\0' || *pFile != '\n' || *pFile != ' ' || pFile< end) pFile++;
        long long int pos = pFile-start;
        positions.push(pos);
    }
}
splitFile();
//idea de optimizacion que un thread haga esto
//si la cola esta vacia hacer un lock cuando se agregue uno notificar

//por threads procesar 
void countWords(){
    long long int pos = positions.pop();
    //Aqui deberia tener un mutex para que solo este utilice el archivo
    FILE *p = pFile;
    fseek ( p , pos , SEEK_SET );
    string str(p,p+stringAproxSize),word;
    stringstream ss(str);

    std::map<char,long long int> vocab;
    while (getline(ss, word, ' ')){
        vocab
    }

}
