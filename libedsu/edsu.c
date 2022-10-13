#include <stdio.h>
#include <unistd.h>
#include "edsu.h"
#include "comun.h"
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netdb.h>
#include <sys/uio.h>

struct cabecera {
	int long1;
	int long2;
};

int socketfd = 0;
UUID_t id;

// se ejecuta antes que el main de la aplicación
__attribute__((constructor)) void inicio(void){
    if (begin_clnt()<0) {
        fprintf(stderr, "Error al iniciarse aplicación\n");
        // terminamos con error la aplicación antes de que se inicie
	// en el resto de la biblioteca solo usaremos return
        _exit(1);
    }
}

// se ejecuta después del exit de la aplicación
__attribute__((destructor)) void fin(void){
    if (end_clnt()<0) {
        fprintf(stderr, "Error al terminar la aplicación\n");
        // terminamos con error la aplicación
	// en el resto de la biblioteca solo usaremos return
        _exit(1);
    }
}

int send_msg(int funcion, const char* atributo1, const void* atributo2,uint32_t tam2,int ndatos){    
    int escrito;
    struct cabecera cab;
    if(ndatos>3){
        cab.long1=htonl(strlen(atributo1));
        cab.long2=htonl(tam2);
    }

    int datos=htonl(funcion);
	struct iovec iov[ndatos];
    iov[0].iov_base=&datos;
	iov[0].iov_len=sizeof(datos);
    iov[1].iov_base=id;
    iov[1].iov_len=strlen(id);
    if(ndatos>3){
        iov[2].iov_base=&cab;
	    iov[2].iov_len=sizeof(cab);
	    iov[3].iov_base=(char*)atributo1;
	    iov[3].iov_len=strlen(atributo1);
        if(ndatos>4){
            iov[4].iov_base=(void*)atributo2;
            iov[4].iov_len=tam2;
        }
    }
    if ((escrito=writev(socketfd, iov, ndatos))<0) {
        perror("error en writev");
        return 1;
    }
    return 0;
}

// operaciones que implementan la funcionalidad del proyecto
int begin_clnt(void){
    int port = atoi(getenv("BROKER_PORT"));
    generate_UUID(id);

    struct sockaddr_in addr;
    if ((socketfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        printf("Error al crear el socket\n");
        return -1;
    }
    
    addr.sin_family = AF_INET; // CONEXION NO LOCAL
    addr.sin_port = htons(port);// PUERTO
    struct hostent *host_info;
    host_info=gethostbyname(getenv("BROKER_HOST"));
    addr.sin_addr=*(struct in_addr *)host_info->h_addr; // DIRECCION DEL SERVIDOR
 
    if (connect(socketfd, (struct sockaddr*)&addr, sizeof(addr))< 0) {
        printf("Error al intentar conectarse con el servidor\n");
        return -1;
    }

    
    if(send_msg(1,NULL,NULL,0,2)<0){
        return -1;
    }  
    
    uint32_t tmp;
    read(socketfd, &tmp, sizeof(tmp));

    if(tmp != 1){
        return 1;
    }
    printf("CONEXION ESTABLECIDA \n");
    return 0;    
}






int end_clnt(void){


    if(send_msg(6,NULL,NULL,0,2)<0){
        return -1;
    }  
    
    uint32_t tmp;
    read(socketfd, &tmp, sizeof(tmp));
    return ntohl(tmp);        
}

int subscribe(const char *tema){
    if(send_msg(2,tema,NULL,0,4)<0){
        return -1;
    }  
    
    uint32_t tmp;
    read(socketfd, &tmp, sizeof(tmp));
    return ntohl(tmp);
}

int unsubscribe(const char *tema){
    
    if(send_msg(5,tema,NULL,0,4)<0){
        return -1;
    }  
    
    uint32_t tmp;
    read(socketfd, &tmp, sizeof(tmp));
    return ntohl(tmp);    
}

int publish(const char *tema, const void *evento, uint32_t tam_evento){
    if(send_msg(3,tema,evento,tam_evento,5)<0){
        return -1;
    }  
    
    uint32_t tmp;
    read(socketfd, &tmp, sizeof(tmp));
    return ntohl(tmp);    
}

int get(char **tema, void **evento, uint32_t *tam_evento){
    if(send_msg(4,NULL,NULL,0,2)<0){
        return -1;
    }
    //RESPUESTA DEL SERVIDOR
    struct cabecera cab2;
    recv(socketfd, &cab2, sizeof(cab2), MSG_WAITALL);
    int tam1=ntohl(cab2.long1);
    int tam2=ntohl(cab2.long2); 
    void *eventoRes = malloc(tam1);
    char *temaRes = malloc(tam2+1);
    recv(socketfd, eventoRes, tam1, MSG_WAITALL);
    recv(socketfd, temaRes, tam2, MSG_WAITALL);
    temaRes[tam2]='\0';
    
    if (!strcmp(temaRes,"vacio")) {
        return -1;
    }

    *evento=eventoRes;
    *tam_evento = tam1;
    *tema = temaRes;
   
    return 0;
}

// operaciones que facilitan la depuración y la evaluación
int topics(){ // cuántos temas existen en el sistema
    if(send_msg(12,NULL,NULL,0,2)<0){
        return -1;
    }
    
    uint32_t tmp,size;
    read(socketfd, &tmp, sizeof(tmp));
    size = ntohl(tmp);
    return size;
}

int clients(){ // cuántos clientes existen en el sistema  
    if(send_msg(11,NULL,NULL,0,2)<0){
        return -1;
    }
    uint32_t tmp,size;
    read(socketfd, &tmp, sizeof(tmp));
    size = ntohl(tmp);
    return size;
}

int subscribers(const char *tema){ // cuántos subscriptores tiene este tema
    
    if(send_msg(13,tema,NULL,0,4)<0){
        return -1;
    }
    
    uint32_t tmp,size;
    read(socketfd, &tmp, sizeof(tmp));
    size = ntohl(tmp);
    return size;
}

int events() { // nº eventos pendientes de recoger por este cliente

    if(send_msg(14,NULL,NULL,0,2)<0){
        return -1;
    }
    
    uint32_t tmp,size;
    read(socketfd, &tmp, sizeof(tmp));
    size = ntohl(tmp);
    return size;
}
