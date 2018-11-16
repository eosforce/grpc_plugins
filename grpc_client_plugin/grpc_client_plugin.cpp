/**
 *  @file
 *  @copyright defined in eos/LICENSE.txt
 */
#include <eosio/grpc_client_plugin/grpc_client_plugin.hpp>
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
#include "eosio_grpc_client.grpc.pb.h"

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

using grpc::Status;
using grpc::Channel;
using grpc::ClientContext;
using eosio_grpc_client::EosRequest;
using eosio_grpc_client::EosReply;
using eosio_grpc_client::Eos_Service;

static appbase::abstract_plugin& _grpc_client_plugin = app().register_plugin<grpc_client_plugin>();

class grpc_stub
{
public:
  grpc_stub(std::shared_ptr<Channel> channel)
      : stub_(Eos_Service::NewStub(channel)) {}
  std::string PutRequest(std::string action,std::string json);
  ~grpc_stub(){}
private:
  std::unique_ptr<Eos_Service::Stub> stub_;
};

class grpc_client_plugin_impl {
public:
   grpc_client_plugin_impl(){}
   ~grpc_client_plugin_impl();
   std::string client_address = std::string("");
   void init();
   boost::thread client_thread;
   void consume_blocks();

   fc::optional<boost::signals2::scoped_connection> accepted_block_connection;
   fc::optional<boost::signals2::scoped_connection> irreversible_block_connection;
   fc::optional<boost::signals2::scoped_connection> accepted_transaction_connection;
   fc::optional<boost::signals2::scoped_connection> applied_transaction_connection;

   void accepted_block( const chain::block_state_ptr& );
   void applied_irreversible_block(const chain::block_state_ptr&);
   void accepted_transaction(const chain::transaction_metadata_ptr&);
   void applied_transaction(const chain::transaction_trace_ptr&);
   void process_accepted_transaction(const chain::transaction_metadata_ptr&);
   //void _process_accepted_transaction(const chain::transaction_metadata_ptr&);
   void process_applied_transaction(const chain::transaction_trace_ptr&);
   //void _process_applied_transaction(const chain::transaction_trace_ptr&);
   void process_accepted_block( const chain::block_state_ptr& );
   //void _process_accepted_block( const chain::block_state_ptr& );
   void process_irreversible_block(const chain::block_state_ptr&);
   //void _process_irreversible_block(const chain::block_state_ptr&);
   template<typename Queue, typename Entry> void queue(Queue& queue, const Entry& e);

   std::deque<chain::transaction_metadata_ptr> transaction_metadata_queue;
   std::deque<chain::transaction_metadata_ptr> transaction_metadata_process_queue;
   std::deque<chain::transaction_trace_ptr> transaction_trace_queue;
   std::deque<chain::transaction_trace_ptr> transaction_trace_process_queue;
   std::deque<chain::block_state_ptr> block_state_queue;
   std::deque<chain::block_state_ptr> block_state_process_queue;
   std::deque<chain::block_state_ptr> irreversible_block_state_queue;
   std::deque<chain::block_state_ptr> irreversible_block_state_process_queue;

   std::atomic_bool done{false};
   std::atomic_bool startup{true};
   boost::mutex mtx;
   boost::condition_variable condition;
   size_t max_queue_size = 512;
   int queue_sleep_time = 0;
private:
   std::unique_ptr<grpc_stub> _grpc_stub;
   
};

std::string grpc_stub::PutRequest(std::string action,std::string json)
{
  try{
    EosRequest request;
    request.set_action(action);
    request.set_json(json);
    EosReply reply;
    ClientContext context;
    Status status = stub_->rpc_sendaction(&context, request, &reply);
    if (status.ok()) {
      return reply.message();
    } else {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }catch(std::exception& e)
  {
     elog( "Exception on grpc_stub PutRequest: ${e}", ("e", e.what()));
  }
}

template<typename Queue, typename Entry>
void grpc_client_plugin_impl::queue( Queue& queue, const Entry& e ) {
   boost::mutex::scoped_lock lock( mtx );
   auto queue_size = queue.size();
   if( queue_size > max_queue_size ) {
      lock.unlock();
      condition.notify_one();
      queue_sleep_time += 10;
      if( queue_sleep_time > 1000 )
         wlog("queue size: ${q}", ("q", queue_size));
      boost::this_thread::sleep_for( boost::chrono::milliseconds( queue_sleep_time ));
      lock.lock();
   } else {
      queue_sleep_time -= 10;
      if( queue_sleep_time < 0 ) queue_sleep_time = 0;
   }
   queue.emplace_back( e );
   lock.unlock();
   condition.notify_one();
}

void grpc_client_plugin_impl::accepted_transaction( const chain::transaction_metadata_ptr& t ) {
   try {
         queue( transaction_metadata_queue, t );
   } catch (fc::exception& e) {
      elog("FC Exception while accepted_transaction ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while accepted_transaction ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while accepted_transaction");
   }
}

void grpc_client_plugin_impl::applied_transaction( const chain::transaction_trace_ptr& t ) {
   try {
      // always queue since account information always gathered
      queue( transaction_trace_queue, t );
   } catch (fc::exception& e) {
      elog("FC Exception while applied_transaction ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while applied_transaction ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while applied_transaction");
   }
}

void grpc_client_plugin_impl::applied_irreversible_block( const chain::block_state_ptr& bs ) {
   try {
         queue( irreversible_block_state_queue, bs );
   } catch (fc::exception& e) {
      elog("FC Exception while applied_irreversible_block ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while applied_irreversible_block ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while applied_irreversible_block");
   }
}

void grpc_client_plugin_impl::accepted_block( const chain::block_state_ptr& bs ) {
   try {
         queue( block_state_queue, bs );
   } catch (fc::exception& e) {
      elog("FC Exception while accepted_block ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while accepted_block ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while accepted_block");
   }
}

void grpc_client_plugin_impl::process_accepted_transaction( const chain::transaction_metadata_ptr& t ) {
   try {
       const auto& trx = t->trx;
       auto json = fc::json::to_string( trx );
       std::string action = std::string("process_accepted_transaction");
      auto reply = _grpc_stub->PutRequest(action,json);
   } catch (fc::exception& e) {
      elog("FC Exception while processing accepted transaction metadata: ${e}", ("e", e.to_detail_string()));
   } catch (std::exception& e) {
      elog("STD Exception while processing accepted tranasction metadata: ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while processing accepted transaction metadata");
   }
}

void grpc_client_plugin_impl::process_applied_transaction( const chain::transaction_trace_ptr& t ) {
   try {
      // always call since we need to capture setabi on accounts even if not storing transaction traces
      // auto json = fc::json::to_string(t.trace).c_str();
      // std::string action = std::string("process_applied_transaction");
      // auto reply = _grpc_stub->PutRequest(action,json);
   } catch (fc::exception& e) {
      elog("FC Exception while processing applied transaction trace: ${e}", ("e", e.to_detail_string()));
   } catch (std::exception& e) {
      elog("STD Exception while processing applied transaction trace: ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while processing applied transaction trace");
   }
}

void grpc_client_plugin_impl::process_irreversible_block(const chain::block_state_ptr& bs) {
  try {
        //_process_irreversible_block( bs );
  } catch (fc::exception& e) {
     elog("FC Exception while processing irreversible block: ${e}", ("e", e.to_detail_string()));
  } catch (std::exception& e) {
     elog("STD Exception while processing irreversible block: ${e}", ("e", e.what()));
  } catch (...) {
     elog("Unknown exception while processing irreversible block");
  }
}

void grpc_client_plugin_impl::process_accepted_block( const chain::block_state_ptr& bs ) {
   try {
         //_process_accepted_block( bs );
   } catch (fc::exception& e) {
      elog("FC Exception while processing accepted block trace ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while processing accepted block trace ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while processing accepted block trace");
   }
}

void grpc_client_plugin_impl::consume_blocks() {
   try {
      while (true) {
         boost::mutex::scoped_lock lock(mtx);
         while ( transaction_metadata_queue.empty() &&
                 transaction_trace_queue.empty() &&
                 block_state_queue.empty() &&
                 irreversible_block_state_queue.empty() &&
                 !done ) {
            condition.wait(lock);
         }

         // capture for processing
         size_t transaction_metadata_size = transaction_metadata_queue.size();
         if (transaction_metadata_size > 0) {
            transaction_metadata_process_queue = move(transaction_metadata_queue);
            transaction_metadata_queue.clear();
         }
         size_t transaction_trace_size = transaction_trace_queue.size();
         if (transaction_trace_size > 0) {
            transaction_trace_process_queue = move(transaction_trace_queue);
            transaction_trace_queue.clear();
         }
         size_t block_state_size = block_state_queue.size();
         if (block_state_size > 0) {
            block_state_process_queue = move(block_state_queue);
            block_state_queue.clear();
         }
         size_t irreversible_block_size = irreversible_block_state_queue.size();
         if (irreversible_block_size > 0) {
            irreversible_block_state_process_queue = move(irreversible_block_state_queue);
            irreversible_block_state_queue.clear();
         }

         lock.unlock();

         if (done) {
            ilog("draining queue, size: ${q}", ("q", transaction_metadata_size + transaction_trace_size + block_state_size + irreversible_block_size));
         }

         // process applied transactions
         while (!transaction_trace_process_queue.empty()) {
            const auto& t = transaction_trace_process_queue.front();
            process_applied_transaction(t);
            transaction_trace_process_queue.pop_front();
         }

         //process accepted transactions
         while (!transaction_metadata_process_queue.empty()) {
            const auto& t = transaction_metadata_process_queue.front();
            process_accepted_transaction(t);
            transaction_metadata_process_queue.pop_front();
         }

         // process blocks
         while (!block_state_process_queue.empty()) {
            const auto& bs = block_state_process_queue.front();
            process_accepted_block( bs );
            block_state_process_queue.pop_front();
         }

         // process irreversible blocks
         while (!irreversible_block_state_process_queue.empty()) {
            const auto& bs = irreversible_block_state_process_queue.front();
            process_irreversible_block(bs);
            irreversible_block_state_process_queue.pop_front();
         }

         if( transaction_metadata_size == 0 &&
             transaction_trace_size == 0 &&
             block_state_size == 0 &&
             irreversible_block_size == 0 &&
             done ) {
            break;
         }
      }
      ilog("grpc_client consume thread shutdown gracefully");
   } catch (fc::exception& e) {
      elog("FC Exception while consuming block ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while consuming block ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while consuming block");
   }
}

void grpc_client_plugin_impl::init()
{
   try {
      _grpc_stub.reset(new grpc_stub(grpc::CreateChannel(
            client_address, grpc::InsecureChannelCredentials())));
      _grpc_stub->PutRequest(std::string("init"),std::string("init--json"));
      client_thread = boost::thread([this] { consume_blocks(); });
   } catch(...) {
         elog( "grpc_client unknown exception, init failed, line ${line_nun}", ( "line_num", __LINE__ ));
      }
   startup = false;
}



grpc_client_plugin_impl::~grpc_client_plugin_impl()
{
   if(!startup){
      try {
         ilog( "grpc shutdown in process please be patient this can take a few minutes" );
         done = true;
         condition.notify_one();
         client_thread.join();
      } catch( std::exception& e ) {
         elog( "Exception on mongo_db_plugin shutdown of consume thread: ${e}", ("e", e.what()));
      }
   }
}
////////////
// grpc_client_plugin
////////////

grpc_client_plugin::grpc_client_plugin()
:my(new grpc_client_plugin_impl)
{
}

grpc_client_plugin::~grpc_client_plugin()
{
}

void grpc_client_plugin::set_program_options(options_description& cli, options_description& cfg)
{
   cfg.add_options()
         ("grpc-client-address", bpo::value<std::string>(),
         "grpc-client-address string.grcp server bind ip and port. Example:127.0.0.1:21005")
         ;
}

void grpc_client_plugin::plugin_initialize(const variables_map& options)
{
   try {
         if( options.count( "grpc-client-address" )) {
            my->client_address = options.at( "grpc-client-address" ).as<std::string>();
            //b_need_start = true;

// hook up to signals on controller
         chain_plugin* chain_plug = app().find_plugin<chain_plugin>();
         EOS_ASSERT( chain_plug, chain::missing_chain_plugin_exception, ""  );
         auto& chain = chain_plug->chain();
         //my->chain_id.emplace( chain.get_chain_id());

         my->accepted_block_connection.emplace( chain.accepted_block.connect( [&]( const chain::block_state_ptr& bs ) {
            my->accepted_block( bs );
         } ));
         my->irreversible_block_connection.emplace(
               chain.irreversible_block.connect( [&]( const chain::block_state_ptr& bs ) {
                  my->applied_irreversible_block( bs );
               } ));
         my->accepted_transaction_connection.emplace(
               chain.accepted_transaction.connect( [&]( const chain::transaction_metadata_ptr& t ) {
                  my->accepted_transaction( t );
               } ));
         my->applied_transaction_connection.emplace(
               chain.applied_transaction.connect( [&]( const chain::transaction_trace_ptr& t ) {
                  my->applied_transaction( t );
               } ));

            my->init();
         } 
         
              
   } FC_LOG_AND_RETHROW()
}

void grpc_client_plugin::plugin_startup()
{
}

void grpc_client_plugin::plugin_shutdown()
{
   my.reset();
}






} // namespace eosio
