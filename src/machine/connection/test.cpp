#include <stdio.h>
#include <iostream>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#define ZMQ_BUILD_DRAFT_API 1
#include "zmq.hpp"
#include <sys/time.h>
int main(){
    zmq::context_t context (1);
    zmq::socket_t socket (context, ZMQ_RADIO);

    std::cout << "Connecting to hello world server…" << std::endl;
    socket.connect ("udp://192.168.1.4:5555");

    for (int request_nbr = 0; request_nbr != 10; request_nbr++)
    {
        zmq::message_t request (5);
        request.set_group("test");
        memcpy (request.data (), "Hello", 5);
        std::cout << "Sending Hello " << request_nbr << "…" << std::endl;
        socket.send (request);

        sleep(1);
    }

    return 0;
}

