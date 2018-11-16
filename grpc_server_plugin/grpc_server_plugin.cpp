/**
 *  @file
 *  @copyright defined in eos/LICENSE.txt
 */
#include <eosio/grpc_server_plugin/grpc_server_plugin.hpp>
#include <eosio/chain/eosio_contract.hpp>
#include <eosio/chain/config.hpp>
#include <eosio/chain/exceptions.hpp>
#include <eosio/chain/transaction.hpp>
#include <eosio/chain/types.hpp>

#include <fc/io/json.hpp>
#include <fc/utf8.hpp>
#include <fc/variant.hpp>

#include <boost/algorithm/string.hpp>
#include <boost/chrono.hpp>
#include <boost/signals2/connection.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>

#include <queue>
#include <eosio/chain/genesis_state.hpp>
#include <grpcpp/grpcpp.h>
#include "eosio_grpc_server.grpc.pb.h"

namespace fc { class variant; }

namespace eosio {

using chain::account_name;
using chain::action_name;
using chain::block_id_type;
using chain::permission_name;
using chain::transaction;
using chain::signed_transaction;
using chain::signed_block;
using chain::transaction_id_type;
using chain::packed_transaction;

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using eosio_grpc_server::EosRequest;
using eosio_grpc_server::EosReply;
using eosio_grpc_server::Eos_Service;

static appbase::abstract_plugin& _grpc_server_plugin = app().register_plugin<grpc_server_plugin>();


class grpc_server_plugin_impl final : public Eos_Service::Service {
public:
   ~grpc_server_plugin_impl();
   std::string server_address = std::string("");
   void init();
   void runServer();
   boost::thread server_thread;
   Status rpc_sendaction(ServerContext* context, const EosRequest* request,
        EosReply* reply) override;
};

Status grpc_server_plugin_impl::rpc_sendaction(ServerContext* context, const EosRequest* request,
                EosReply* reply){
    std::string prefix("GetAction:");
    reply->set_message(prefix +request->action()+ "\r\n" +request->json());
    return Status::OK;
}

void grpc_server_plugin_impl::runServer()
{
   ServerBuilder builder;
   builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
   builder.RegisterService(this);
   std::unique_ptr<Server> server(builder.BuildAndStart());
   server->Wait();
}

void grpc_server_plugin_impl::init()
{
   server_thread = boost::thread([this] { runServer(); });
}

grpc_server_plugin_impl::~grpc_server_plugin_impl()
{
      server_thread.interrupt();
}
////////////
// grpc_server_plugin
////////////

grpc_server_plugin::grpc_server_plugin()
:my(new grpc_server_plugin_impl)
{
}

grpc_server_plugin::~grpc_server_plugin()
{
}

void grpc_server_plugin::set_program_options(options_description& cli, options_description& cfg)
{
   cfg.add_options()
         ("grpc-server-address", bpo::value<std::string>(),
         "grpc-server-address string.grcp server bind ip and port. Example:0.0.0.0:21005")
         ;
}

void grpc_server_plugin::plugin_initialize(const variables_map& options)
{
   try {
     
         if( options.count( "grpc-server-address" )) {
            my->server_address = options.at( "grpc-server-address" ).as<std::string>();
            b_need_start = true;
         }
         if(!b_need_start)
         {
               return ;
         }
         my->init();
      
   } FC_LOG_AND_RETHROW()
}

void grpc_server_plugin::plugin_startup()
{
}

void grpc_server_plugin::plugin_shutdown()
{
   my.reset();
}






} // namespace eosio
