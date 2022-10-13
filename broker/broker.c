#include <stdio.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <unistd.h>
#include <string.h>
#include "map.h"
#include "queue.h"
#include "set.h"
#include <pthread.h>
#include <sys/uio.h>

map * clientes; //MAPA DE CLIENTES - KEY = UUID, VALUE = STRUCT
map * temas; //MAPA DE TEMAS - KEY = NOMBRE, VALUE = STRUCT 
int socketfd;

//PROBANDO

struct cliente { 
    char* UUID;
    int n_sockfd;
    queue *eventos;
    set* temasSubs;
};

struct tema {
    char* nombre;
    set*  clientesSubs;
};

struct evento {
    char* tema;
    void* evento;
    uint32_t tam;
    int refs;
};

struct cabecera { //AUXILIAR PARA EL PASO DE MENSAJES
	int long1;
	int long2;
};

char* removeID;
void remove_sub(void *c, void * v){
    struct tema *t = v;
    set_remove(t->clientesSubs,removeID,NULL);

}

void liberar(void *v){
    free(v);
}

void returnInt(int desc,uint32_t v){    
    uint32_t tmp = htonl(v);
    write(desc, &tmp, sizeof(tmp));
}

void* conexion_fun(void* arg){ // FUNCION DE THREADS, 1 POR CLIENTE/CONEXION
    int* argumento = (int*) arg;
    int desc = *argumento;
    char* UUID = (char*) malloc(1024*sizeof(char)); //LIBERAR CUANDO EL CLIENTE SE DESCONECTE (UUID) - LIBERADO
    int error=1;
    struct cliente *c;

    free(argumento);
   
    
    int fin = 1;
    while(fin){ // BUCLE DE UN CLIENTE
        int tmp;
        int tam1,tam2;

        if (recv(desc, &tmp, sizeof(int), MSG_WAITALL)<=0) 
            break;//0
        int funcion = ntohl(tmp);
        char *atributo1;
        void *atributo2;
        recv(desc, UUID, 32, MSG_WAITALL);
        if(funcion==1){              
            c = malloc(sizeof(struct cliente)); //LIBERAR CUANDO EL CLIENTE SE DESCONECTE
            c->UUID = UUID;
            c->n_sockfd = desc;
            c->eventos = queue_create(0);
            c->temasSubs = set_create(0);       
            map_put(clientes,UUID,c);// METE EL CLIENTE AL MAPA DE CLIENTES
            returnInt(desc,1);        
            continue;
        }
        int ndatos;
        if(funcion == 4 || funcion == 6 || funcion == 11|| funcion == 12|| funcion == 14) 
            ndatos = 2;         
        if(funcion == 2  || funcion == 5 || funcion == 13 )
            ndatos = 4;
        if(funcion == 3)
            ndatos = 5;

        struct cabecera cab;
        if(ndatos>3){
            recv(desc, &cab, sizeof(cab), MSG_WAITALL);//3
            tam1=ntohl(cab.long1);
            atributo1 = (char*) malloc(tam1+1); //LIBERAR CUANDO SE HAYA TRATADO - LIBERADO
            if(ndatos > 4){
                tam2=ntohl(cab.long2);
                atributo2 = (void*) malloc(tam2+1); //LIBERAR CUANDO SE HAYA TRATADO - LIBERADO          
            }
        }
        if(ndatos>3){
            recv(desc, atributo1, tam1, MSG_WAITALL);//4
            atributo1[tam1]='\0';
            if(ndatos>4)
                recv(desc, atributo2, tam2, MSG_WAITALL);//5
        }

        if(map_size(clientes)>0){
            c = NULL;
            c = map_get(clientes, UUID, &error);
            }
        else
            error = -1;
        if(error<0){
            returnInt(desc,-1);
            continue;
        }

        //DEPURACION
        //CLIENTS SIZE
        if(funcion == 11) {
            returnInt(desc,map_size(clientes));
        }
        //TEMAS SIZE
        if(funcion == 12) {
            returnInt(desc,map_size(temas));
        }
        //N SUBS
        if(funcion == 13) {
            struct tema *t;
            t = map_get(temas,atributo1,&error);
            if(error < 0){
                returnInt(desc,-1);
                continue;
            }
            returnInt(desc,set_size(t->clientesSubs));

         }
        //EVENTS
        if(funcion == 14) {
            returnInt(desc,queue_length(c->eventos));
        }
        
        //FUNCIONES PRINCIPALES
        
        //SUSCRIBE
        if(funcion == 2) {
            struct tema *t;
            t = map_get(temas,atributo1,&error);
            if(error < 0){
                returnInt(desc,-1);
                continue;
            }
            int contenido = 0;
            set_iter *i = set_iter_init(c->temasSubs);
            for ( ; set_iter_has_next(i); set_iter_next(i)){
                if(!strcmp((char*)set_iter_value(i),atributo1)){
                    contenido=1;
                    break;
                }
            } set_iter_exit(i);

            if(contenido){
                returnInt(desc,-1);
                continue;
            }

            set_add(c->temasSubs,atributo1); //LIBERAR ATRIBUTO 1 CUANDO SE DESUSCRIBA
            set_add(t->clientesSubs,UUID);   //NO LIBERAR UUID AL SACARLO DEL SET, UUID SE LIBERA AL FINAL DEL CLIENTE
            returnInt(desc,0);
        }
        //PUBLISH
        if(funcion == 3) {
            struct tema *t;
            t = map_get(temas,atributo1,&error);
            if(error < 0){
                returnInt(desc,-1);
                continue;
            }

            struct evento *e;
            e=malloc(sizeof(struct evento)); //LIBERAR CUANDO NO QUEDEN REFERENCIAS - LIBERADO
            e->tema=atributo1; //LIBERAR CUANDO NO QUEDEN REFERENCIAS - LIBERADO
            e->evento=atributo2; //LIBERAR CUANDO NO QUEDEN REFERENCIAS - LIBERADO
            e->tam= tam2;
            e->refs=0;
            set_iter *i = set_iter_init(t->clientesSubs);
             //SACAR TODOS LOS UUID DE LOS QUE ESTAN SUSCRITOS AL TEMA
             for ( ; set_iter_has_next(i); set_iter_next(i)){
                struct cliente *c2; 
                c2 = map_get(clientes, set_iter_value(i), &error);
                e->refs = e->refs + 1;
                queue_push_back(c2->eventos,e);
            }set_iter_exit(i);

            if(e->refs < 1){
                free(e->tema);
                free(e->evento);
                free(e);
            }

            returnInt(desc,0);
         }
        //GET
        if(funcion == 4) {                              
            struct evento *e = queue_pop_front(c->eventos,&error);
            printf(" %s %s %d",e->tema, (char*)e->evento,e->tam);
            if(error<0){            
                char msg1[1024]="vacio";
                char msg2[1024]="vacio";
                int escrito;
                struct cabecera cab2;
                cab2.long1=htonl(strlen(msg1));
                cab2.long2=htonl(strlen(msg2));
                struct iovec iov[3];
                iov[0].iov_base=&cab2;
                iov[0].iov_len=sizeof(cab2);
                iov[1].iov_base=msg1;
                iov[1].iov_len=strlen(msg1);
                iov[2].iov_base=msg2;
                iov[2].iov_len=strlen(msg2);
                if((escrito=writev(desc, iov, 3))<0) {
                    perror("error en writev");
                    continue;
                }
                continue;
            }

            int escrito;
            struct cabecera cab2;
            cab2.long1=htonl(e->tam);
            cab2.long2=htonl(strlen(e->tema));
            struct iovec iov[3];
            iov[0].iov_base=&cab2;
            iov[0].iov_len=sizeof(cab2);
            iov[1].iov_base=e->evento;
            iov[1].iov_len=e->tam;
            iov[2].iov_base=e->tema;
            iov[2].iov_len=strlen(e->tema);
                if((escrito=writev(desc, iov, 3))<0) {
                    perror("error en writev");
                    continue;
                }
            e->refs = e->refs - 1;
            if(e->refs < 1){
                free(e->tema);
                free(e->evento);
                free(e);
            }
         }
        //UNSUB
        if(funcion == 5) {         
            struct tema *t;
            t = map_get(temas,atributo1,&error);
            if(error < 0){
                returnInt(desc,-1);
                continue;
            }

            int contenido = 0;
            int contenido2 = 0;
            set_iter *i = set_iter_init(c->temasSubs);
            for ( ; set_iter_has_next(i); set_iter_next(i)){
                if(!strcmp((char*)set_iter_value(i),atributo1)){
                    set_remove(c->temasSubs,(char*)set_iter_value(i),liberar); //ELIMINO DE SET Y LIBERO MEMORIA
                    contenido = 1;
                    break;
                }
            }set_iter_exit(i);
        
            set_iter *i2 = set_iter_init(t->clientesSubs);
            for ( ; set_iter_has_next(i2); set_iter_next(i2)){
                if(!strcmp((char*)set_iter_value(i2),UUID)){ 
                    set_remove(t->clientesSubs,(char*)set_iter_value(i2),NULL);   //ELIMINO DE SET (LA MEMORIA SE LIBERA CUANDO EL CLIENTE FINALIZA)
                    contenido2 = 1;
                    break;
                }
            }set_iter_exit(i2);

            if(contenido == 0 || contenido2 == 0){
                returnInt(desc,-1);
                continue;
            }

            returnInt(desc,0);
            free(atributo1); 
        }
        //END
        if(funcion == 6) {
            //DAR DE BAJA A LOS CLIENTES EN LOS TEMAS
            removeID = UUID;
            map_visit(temas,remove_sub);
            map_remove_entry(clientes,UUID,NULL);

            //LIBERAR TODO LO CORRESPONDIENTE AL CLIENTE
            free(c->eventos);
            free(c->temasSubs);
            free(c->UUID); //LIBERA EL UUID
            free(c);
            returnInt(desc,0);
            close(desc);
            fin = 0;
        }
        

        //LIBERAR ATRIBUTOS NO NECESARIOS / VACIOS
       
    }
    return 0;
}

int leer_temas(FILE *f){ //LEE TODOS LOS TEMAS DEL FICHERO PASADO COMO SEGUNDO ARGUMENTO
    char* nombreT;
    struct tema *t;
    while(fscanf(f,"%ms",&nombreT) != -1){ //LIBERAR AL FINAL DE LA EJECUCION - NUNCA HAY FIN
        t = malloc(sizeof(struct tema)); //LIBERAR SOLO AL FINAL DE LA EJECUCION - NUNCA HAY FIN
        t->nombre = nombreT;
        t->clientesSubs = set_create(0); //LIBERAR AL FINAL DE LA EJECUCION - NUNCA HAY FIN
        if(map_put(temas,nombreT,t)<0){
        }
    }
    return 0;
}

int main(int argc, char *argv[]){
    if(argc!=3) {
        fprintf(stderr, "Uso: %s puerto fichero_temas\n", argv[0]);
        return 1;
    }
   
    struct sockaddr_in addr;
    int opt = 1;
    int addrlen = sizeof(addr);
       
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    clientes = map_create(key_string, 0);
    temas = map_create(key_string, 0);

    if((socketfd = socket(AF_INET,SOCK_STREAM,0)) == 0){
        perror("ERROR EN EL SOCKET");
        exit(1);
    }

    if(setsockopt(socketfd, SOL_SOCKET,SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
        perror("ERROR");
        exit(1);
    } //PARA PERMITIR VARIAS CONEXIONES CON LA MISMA DIRECCION / PUERTO

    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(atoi(argv[1]));

    if(bind(socketfd, (struct sockaddr*)&addr, sizeof(addr)) < 0){
        perror("ERROR EN EL BIND");
        exit(1);
    } //BIND

    if(listen(socketfd, 5) < 0){
        perror("ERROR EN EL LISTEN");
        exit(1);
    } //LISTEN
        
    FILE *f;
    f = fopen(argv[2],"r");
    if( f == NULL){
        perror("TEMAS");
        exit(1);
    }        
    leer_temas(f);
    
    while(1){ //BUCLE DE SERVICIO
        int *new_socket;
        new_socket = (int*)malloc(sizeof(int));
        if((*new_socket = accept(socketfd, (struct sockaddr*)&addr,(socklen_t*)&addrlen)) < 0) {
            perror("Error al aceptar el cliente");
            exit(1);
        }// ACCEPT

        pthread_t thread;
        pthread_create(&thread,&attr,conexion_fun,new_socket); //SE LE ASIGNA UN THREAD
    }

    return 0;
}
