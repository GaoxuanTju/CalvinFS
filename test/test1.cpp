#include <stdio.h>
#include <iostream>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#define ZMQ_BUILD_DRAFT_API 1
#include <zmq.hpp>
#include <sys/time.h>
int main(){
    std::cout << zmq_has("draft") << std::endl;

    zmq::context_t context (1);
    zmq::socket_t socket (context, ZMQ_DISH);
    socket.bind ("udp://*:5555");
    socket.join("test");

    while (true)
    {
        zmq::message_t request;
	if (socket.recv (&request, ZMQ_DONTWAIT)) {
	       std::cout << "Received Hello" << std::endl;
	}
    }

    return 0;
}

