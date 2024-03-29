// Author: Alexander Thomson <thomson@cs.yale.edu>
// Author: Kun Ren <kun@cs.yale.edu>

#include <stdlib.h>
#include <map>

#define ZMQ_BUILD_DRAFT_API

#include "machine/cluster_config.h"
#include "machine/connection/connection_zmq.h"
#include <zmq.hpp>
#include "machine/message_buffer.h"
#include "machine/message_handler.h"
#include "common/atomic.h"
#include "common/mutex.h"
#include "common/types.h"
#include "common/utils.h"
#include "proto/header.pb.h"
#include <string>
using std::map;

// Per-process zmq context.
// TODO(agt): Initialize in process-wide init function? Deallocate eventually?
zmq::context_t *context_ = NULL;
int connection_count_ = 0;
Mutex context_lock_;

zmq::context_t *GetZMQContext()
{
  Lock l(&context_lock_);
  if (context_ == NULL)
  {
    context_ = new zmq::context_t(1);
  }
  connection_count_++;
  return context_;
}

ConnectionZMQ::ConnectionZMQ(
    uint64 id,
    const ClusterConfig &config,
    MessageHandler *handler)
{
  id_ = id;
  config_ = config;
  handler_ = handler;
  destructor_called_ = false;

  // Lookup and set host/port.
  MachineInfo machine_info;
  CHECK(config_.lookup_machine(id_, &machine_info)) << id_;
  hostname_ = machine_info.host();
  port_ = machine_info.port();

  // Setup sockets and start main loop running.
  //
  // TODO(agt): This should actually happen using the Machine's ThreadPool!
  pthread_create(&thread_UDP, NULL, ListenerLoop_UDP, reinterpret_cast<void *>(this));
  pthread_create(&thread_, NULL, ListenerLoop, reinterpret_cast<void *>(this));
}

ConnectionZMQ::~ConnectionZMQ()
{
  // Stop the main listener loop.
  destructor_called_ = true;
  pthread_join(thread_, NULL);
  pthread_join(thread_UDP, NULL);

  // Close tcp sockets.
  delete socket_in_;
  delete socket_in_UDP;
  for (map<uint64, zmq::socket_t *>::iterator it = sockets_out_.begin();
       it != sockets_out_.end(); ++it)
  {
    delete it->second;
  }
  for (map<uint64, Mutex *>::iterator it = mutexes_.begin();
       it != mutexes_.end(); ++it)
  {
    delete it->second;
  }
  for (map<uint64, zmq::socket_t *>::iterator it = sockets_out_UDP.begin();
       it != sockets_out_UDP.end(); ++it)
  {
    delete it->second;
  }
  for (map<uint64, Mutex *>::iterator it = mutexes_UDP.begin();
       it != mutexes_UDP.end(); ++it)
  {
    delete it->second;
  }
}

// Helper deletion function called by zmq::~message_t after it is done sending
// in SendMessage() below.
void DeleteMessagePart(void *data, void *hint)
{
  delete reinterpret_cast<MessagePart *>(hint);
}

void ConnectionZMQ::SendMessageExternal(
    Header *header,
    MessageBuffer *message)
{
  LOG(ERROR) << "Is this place called?";
  char endpoint[256];
  snprintf(endpoint, sizeof(endpoint), "tcp://%s:%d",
           header->external_host().c_str(), header->external_port());
  zmq::socket_t temp_socket(*GetZMQContext(), ZMQ_PUSH);
  temp_socket.connect(endpoint);

  // Add header to message.
  message->Append(*header);
  delete header;

  // Send.
  for (uint32 i = 0; i < message->size(); i++)
  {
    // Create message.
    void *data = reinterpret_cast<void *>(const_cast<char *>(
        (*message)[i].data()));
    int size = (*message)[i].size();

    // gaoxuan --这里才是具体发出包的地方
    string s = (*message)[i].data();
    LOG(ERROR) << "this place :" << s;

    //

    MessagePart *part = message->StealPart(i);
    zmq::message_t msg(data, size,
                       DeleteMessagePart,
                       part);

    // Send message. All but the last are sent with ZMQ's SNDMORE flag.
    if (i == message->size() - 1)
    {
      temp_socket.send(msg);
    }
    else
    {
      temp_socket.send(msg, ZMQ_SNDMORE);
    }
  }

  delete message;
}

void ConnectionZMQ::SendMessage_UDP(uint64 recipient, MessageBuffer *message)
{
  // Local messages can be given directly to the handler.
  if (recipient == id_)
  {
    MessagePart *part = message->PopBack();

    Msg *mmsg = new Msg();
    mmsg->ParseFromArray(part->buffer().data(), part->buffer().size());

    delete part;

    Header *header = new Header();
    *header = mmsg->header();
    MessageBuffer *message1;
    if (mmsg->type() == 3)
    {
      message1 = new MessageBuffer(mmsg->startappproto());
    }
    else if (mmsg->type() == 2)
    {
      message1 = new MessageBuffer(mmsg->action_batch());
    }
    else if (mmsg->type() == 1)
    {
      message1 = new MessageBuffer(mmsg->action());
    }
    else
    {
      message1 = new MessageBuffer();
    }
    handler_->HandleMessage(header, message1);
    return;
  }

  Lock l(mutexes_UDP[recipient]);

  // Create message.
  void *data = reinterpret_cast<void *>(const_cast<char *>(
      (*message)[0].data()));

  // LOG(ERROR) << "erwerwe";

  int size = (*message)[0].size();


  MessagePart *part = message->StealPart(0);
  zmq::message_t msg(data, size,
                     DeleteMessagePart,
                     part);
  msg.set_group("test1");
  // Send message. All but the last are sent with ZMQ's SNDMORE flag.
  sockets_out_UDP[recipient]->send(msg);

  delete message;
}

void ConnectionZMQ::SendMessage(uint64 recipient, MessageBuffer *message)
{
  // Local messages can be given directly to the handler.
  if (recipient == id_)
  {
    MessagePart *part = message->PopBack();
    Header *header = new Header();
    header->ParseFromArray(part->buffer().data(), part->buffer().size());
    delete part;

    handler_->HandleMessage(header, message);
    return;
  }

  Lock l(mutexes_[recipient]);

  for (uint32 i = 0; i < message->size(); i++)
  {
    // Create message.
    void *data = reinterpret_cast<void *>(const_cast<char *>(
        (*message)[i].data()));

    int size = (*message)[i].size();

    MessagePart *part = message->StealPart(i);
    zmq::message_t msg(data, size,
                       DeleteMessagePart,
                       part);

    // Send message. All but the last are sent with ZMQ's SNDMORE flag.
    if (i == message->size() - 1)
    {

      // gaoxuan --如果要改包头，只能在这里改，可以通过判断字符串里有没有那几种操作类型来决定要不要改

      //
      sockets_out_[recipient]->send(msg);
    }
    else
    {
      sockets_out_[recipient]->send(msg, ZMQ_SNDMORE);
    }
  }
  delete message;
}

void ConnectionZMQ::Init_UDP()
{
  // Bind port for incoming socket.
  char endpoint[256];
  snprintf(endpoint, sizeof(endpoint), "udp://*:5678");

  socket_in_UDP = new zmq::socket_t(*GetZMQContext(), ZMQ_DISH);
  socket_in_UDP->bind(endpoint);
  socket_in_UDP->join("test1");
  // Initialize mutexes.
  for (map<uint64, MachineInfo>::const_iterator it =
           config_.machines().begin();
       it != config_.machines().end(); ++it)
  {
    mutexes_UDP[it->second.id()] = new Mutex();
  }

  // Wait a bit for other nodes to bind sockets before connecting to them.
  Spin(2);

  // Connect to remote outgoing sockets.
  for (map<uint64, MachineInfo>::const_iterator it =
           config_.machines().begin();
       it != config_.machines().end(); ++it)
  {
    if (it->second.id() != id_)
    { // Only connect to remote nodes.
      snprintf(endpoint, sizeof(endpoint), "udp://%s:5678",
               it->second.host().c_str());
      sockets_out_UDP[it->second.id()] =
          new zmq::socket_t(*GetZMQContext(), ZMQ_RADIO);
      sockets_out_UDP[it->second.id()]->connect(endpoint);
    }
  }
}

void ConnectionZMQ::Init() {
  // Bind port for incoming socket.
  char endpoint[256];
  snprintf(endpoint, sizeof(endpoint), "tcp://*:%d", port_);
  socket_in_ = new zmq::socket_t(*GetZMQContext(), ZMQ_PULL);
  socket_in_->bind(endpoint);

  // Initialize mutexes.
  for (map<uint64, MachineInfo>::const_iterator it =
          config_.machines().begin();
       it != config_.machines().end(); ++it) {
      mutexes_[it->second.id()] = new Mutex();
  }

  // Wait a bit for other nodes to bind sockets before connecting to them.
  Spin(2);

  // Connect to remote outgoing sockets.
  for (map<uint64, MachineInfo>::const_iterator it =
          config_.machines().begin();
       it != config_.machines().end(); ++it) {
    if (it->second.id() != id_) {  // Only connect to remote nodes.
      snprintf(endpoint, sizeof(endpoint), "tcp://%s:%d",
               it->second.host().c_str(), it->second.port());
      sockets_out_[it->second.id()] =
          new zmq::socket_t(*GetZMQContext(), ZMQ_PUSH);
      sockets_out_[it->second.id()]->connect(endpoint);
    }
  }
}

void *ConnectionZMQ::ListenerLoop_UDP(void *arg)
{
  ConnectionZMQ *connection = reinterpret_cast<ConnectionZMQ *>(arg);
  connection->Init_UDP();

  zmq::message_t *msg_part = new zmq::message_t();

  while (!connection->destructor_called_UDP)
  {
    // Get the next message. (Non-blocking.)
    // LOG(ERROR)<<"before dontwait";
    
    if (connection->socket_in_UDP->recv(msg_part, ZMQ_DONTWAIT))
    {
      // if (connection->socket_in_->recv(msg_part, ZMQ_DONTWAIT)) {
      // See if that was the final message part for this message.
      // Final message part. Decode as header.
      
      Msg *msg1 = new Msg();

      msg1->ParseFromArray(msg_part->data(), msg_part->size());

      delete msg_part;

      // Pass decoded header and message to the handler.

      Header *header = new Header();
      *header = msg1->header();
      if(header->type()==Header::DATA)
      {
        msg1->set_type(1);
      }
      if(header->type()==Header::RPC)
      {
      }
      MessageBuffer *message1;
      if (msg1->type() == 3)
      {
        message1 = new MessageBuffer(msg1->startappproto());
      }
      else if (msg1->type() == 2)
      {
        message1 = new MessageBuffer(msg1->action_batch());
      }
      else if (msg1->type() == 1)
      {

        message1 = new MessageBuffer(msg1->action());
      }
      else
      {
        message1 = new MessageBuffer();
      }

      connection->handler_->HandleMessage(header, message1);

      // Get a new empty msg_part ready for next part received.
      msg_part = new zmq::message_t();
    }
  }

  // Delete unused message/part.

  delete msg_part;
  return NULL;
}

void* ConnectionZMQ::ListenerLoop(void* arg) {
  ConnectionZMQ* connection = reinterpret_cast<ConnectionZMQ*>(arg);
  connection->Init();

  zmq::message_t* msg_part = new zmq::message_t();
  MessageBuffer* message = new MessageBuffer();
  while (!connection->destructor_called_) {
    // Get the next message. (Non-blocking.)
    if (connection->socket_in_->recv(msg_part, ZMQ_DONTWAIT)) {
      // See if that was the final message part for this message.
      int more;
      size_t moresize = sizeof(more);
      connection->socket_in_->getsockopt(ZMQ_RCVMORE, &more, &moresize);
      if (!more) {
        // Final message part. Decode as header.
        Header* header = new Header();
        header->ParseFromArray(msg_part->data(), msg_part->size());
        delete msg_part;

        // Pass decoded header and message to the handler.
        connection->handler_->HandleMessage(header, message);

        // Get a new empty message ready for the next message received.
        message = new MessageBuffer();
      } else {
        // More parts remain for this message. Just add this one to the output
        // message.
        message->Append(msg_part);
      }
      // Get a new empty msg_part ready for next part received.
      msg_part = new zmq::message_t();
    }
  }

  // Delete unused message/part.
  delete message;
  delete msg_part;
  return NULL;
}
