#include <stdlib.h>
#include <pthread.h>
#include <mutex.h>

typedef struct{
    int id;
} run_t ; 


struct engine {
    // type : run_t
    run_t *runs = NULL;

    // type : int
    int n_agents; 

    // type : size_t
    size_t quantity = 0;
    size_t capacity = 0;

    // type : mutex
    pthread_mutex_t mutex; 

    void engine_start(int &n_agents, char &workdir){

    };

    int engine_submit(run_t *p){
        return 0;
    };

    bool engine_cancel(int id){
      return true;
    };

    char* engine_status(int id){

    };

    char* engine_list(){

    };

};


int main(){
    
    // type : run_t
    run_t *runs = NULL;

    // type : int
    int n_agents; 

    // type : size_t
    size_t quantity = 0;
    size_t capacity = 0;

    // type : mutex
    pthread_mutex_t mutex; 

    // process


}