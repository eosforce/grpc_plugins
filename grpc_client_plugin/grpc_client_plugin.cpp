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
#include "transfer.grpc.pb.h"
#include "transaction.grpc.pb.h"
#include "block.grpc.pb.h"

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

using force_transfer::grpc_transfer;
using force_transfer::TransferRequest;
using force_transfer::TransferReply;

using force_transaction::grpc_transaction;
using force_transaction::TransactionRequest;
using force_transaction::TransactionReply;

using force_block::grpc_block;
using force_block::BlockTransRequest;
using force_block::BlockRequest;
using force_block::BlockReply;


static appbase::abstract_plugin& _grpc_client_plugin = app().register_plugin<grpc_client_plugin>();

class grpc_stub
{
public:
  grpc_stub(std::shared_ptr<Channel> channel)
      : stub_(Eos_Service::NewStub(channel)),
      transfer_stub_(grpc_transfer::NewStub(channel)),
      transaction_stub_(grpc_transaction::NewStub(channel)),
      block_stub_(grpc_block::NewStub(channel)) {}
  std::string PutRequest(std::string action,std::string json);
  std::string PutTransferRequest(std::string from,std::string to,std::string amount,std::string memo,std::string trx_id);
  std::string PutTransactionRequest(int blocknum,std::string trxjson,std::string trx_id);
  std::string PutBlockRequest(int blocknum,vector<BlockTransRequest> &blockTrans);
  ~grpc_stub(){}
private:
  std::unique_ptr<Eos_Service::Stub> stub_;
  std::unique_ptr<grpc_transfer::Stub> transfer_stub_;
  std::unique_ptr<grpc_transaction::Stub> transaction_stub_;
  std::unique_ptr<grpc_block::Stub> block_stub_;
};

class grpc_client_plugin_impl {
public:
   grpc_client_plugin_impl(){}
   ~grpc_client_plugin_impl();
   std::string client_address = std::string("");
   void init();
   boost::thread client_thread;
   void consume_blocks();
   
   void insert_default_abi();

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
   void _process_irreversible_block(const chain::block_state_ptr&);
   template<typename Queue, typename Entry> void queue(Queue& queue, const Entry& e);

   optional<abi_serializer> get_abi_serializer( account_name n );
   template<typename T> fc::variant to_variant_with_abi( const T& obj );

   void purge_abi_cache();

   fc::microseconds abi_serializer_max_time;
   size_t abi_cache_size = 0;
   struct by_account;
   struct by_last_access;

   struct abi_cache {
      account_name                     account;
      fc::time_point                   last_accessed;
      fc::optional<abi_serializer>     serializer;
   };

   typedef boost::multi_index_container<abi_cache,
         indexed_by<
               ordered_unique< tag<by_account>,  member<abi_cache,account_name,&abi_cache::account> >,
               ordered_non_unique< tag<by_last_access>,  member<abi_cache,fc::time_point,&abi_cache::last_accessed> >
         >
   > abi_cache_index_t;

   abi_cache_index_t abi_cache_index;

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

std::string grpc_stub::PutTransferRequest(std::string from,std::string to,std::string amount,std::string memo,std::string trx_id)
{
   try{
    TransferRequest request;
    request.set_from(from);
    request.set_to(to);
    request.set_amount(amount);
    request.set_memo(memo);
    request.set_trxid(trx_id);

    TransferReply reply;
    ClientContext context;
    Status status = transfer_stub_->rpc_sendaction(&context, request, &reply);
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

std::string grpc_stub::PutTransactionRequest(int blocknum,std::string trxjson,std::string trx_id)
{
   try{
    TransactionRequest request;
    request.set_blocknum(blocknum);
    request.set_trx(trxjson);
    request.set_trxid(trx_id);
    

    TransactionReply reply;
    ClientContext context;
    Status status = transaction_stub_->rpc_sendaction(&context, request, &reply);
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

std::string grpc_stub::PutBlockRequest(int blocknum,vector<BlockTransRequest> &blockTrans)
{
   try{
    BlockRequest request;
    request.set_blocknum(blocknum);
    int nsize = blockTrans.size();
    for (int i=0;i!=nsize;++i)
    {
       BlockTransRequest *blockTranstemp = request.add_trans();
       *blockTranstemp = blockTrans[i];
    }
    

    BlockReply reply;
    ClientContext context;
    Status status = block_stub_->rpc_sendaction(&context, request, &reply);
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
    //  auto reply = _grpc_stub->PutRequest(action,json);
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
        _process_irreversible_block( bs );
  } catch (fc::exception& e) {
     elog("FC Exception while processing irreversible block: ${e}", ("e", e.to_detail_string()));
  } catch (std::exception& e) {
     elog("STD Exception while processing irreversible block: ${e}", ("e", e.what()));
  } catch (...) {
     elog("Unknown exception while processing irreversible block");
  }
}

void grpc_client_plugin_impl::_process_irreversible_block(const chain::block_state_ptr& bs) {
      const auto block_num = bs->block->block_num();
      bool transactions_in_block = false;
      vector<BlockTransRequest> blockTans;
      bool HasTransaction = false;
      for( const auto& receipt : bs->block->transactions ) {
         string trx_id_str;


         // bool executed = receipt->status == chain::transaction_receipt_header::executed;
         // if (!executed) {
         //    continue ;
         // }
         if( receipt.trx.contains<packed_transaction>() ) {
            const auto& pt = receipt.trx.get<packed_transaction>();
            // get id via get_raw_transaction() as packed_transaction.id() mutates internal transaction state
            const auto& raw = pt.get_raw_transaction();
            const auto& trx = fc::raw::unpack<transaction>( raw );

            const auto& id = trx.id();
            trx_id_str = id.str();

            auto v = to_variant_with_abi( trx );
            string trx_json = fc::json::to_string( v );
            //将transaction的信息发过去  block的信息额外再添加
           // auto reply = _grpc_stub->PutTransactionRequest(block_num,trx_json,trx_id_str);

            BlockTransRequest tempBlockTrans;
            tempBlockTrans.set_trx(trx_json);
            tempBlockTrans.set_trxid(trx_id_str);
            blockTans.push_back(tempBlockTrans);
            HasTransaction = true;
           
         } else {
            const auto& id = receipt.trx.get<transaction_id_type>();
            trx_id_str = id.str();
         }

      }
      if (HasTransaction)
         auto reply = _grpc_stub->PutBlockRequest(block_num,blockTans);


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
      //insert_default_abi();
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

void grpc_client_plugin_impl::purge_abi_cache() {
   if( abi_cache_index.size() < abi_cache_size ) return;

   // remove the oldest (smallest) last accessed
   auto& idx = abi_cache_index.get<by_last_access>();
   auto itr = idx.begin();
   if( itr != idx.end() ) {
      idx.erase( itr );
   }
}

optional<abi_serializer> grpc_client_plugin_impl::get_abi_serializer( account_name n ) {
   if( n.good()) {
      try {

         auto itr = abi_cache_index.find( n );
         if( itr != abi_cache_index.end() ) {
            abi_cache_index.modify( itr, []( auto& entry ) {
               entry.last_accessed = fc::time_point::now();
            });

            return itr->serializer;
         }

      } FC_CAPTURE_AND_LOG((n))
   }
   return optional<abi_serializer>();
}

template<typename T>
fc::variant grpc_client_plugin_impl::to_variant_with_abi( const T& obj ) {
   fc::variant pretty_output;
   abi_serializer::to_variant( obj, pretty_output,
                               [&]( account_name n ) { return get_abi_serializer( n ); },
                               abi_serializer_max_time );
   return pretty_output;
}

void grpc_client_plugin_impl::insert_default_abi()
{
   //eosio.token
   {
      auto abiPath = app().config_dir() / "eosio.token" += ".abi";
      FC_ASSERT( fc::exists( abiPath ), "no abi file found ");
      auto abijson = fc::json::from_file(abiPath).as<abi_def>();
      auto abi = fc::raw::pack(abijson);
      abi_def abi_def = fc::raw::unpack<chain::abi_def>( abi );
     // const string json_str = fc::json::to_string( abi_def );
     purge_abi_cache(); // make room if necessary
     abi_cache entry;
     entry.account = N(eosio.token);
     entry.last_accessed = fc::time_point::now();
     abi_serializer abis;
     abis.set_abi( abi_def, abi_serializer_max_time );
     entry.serializer.emplace( std::move( abis ) );
     abi_cache_index.insert( entry );
   }

   //eosio
   {
      auto abiPath = app().config_dir() / "System01" += ".abi";
      FC_ASSERT( fc::exists( abiPath ), "no abi file found ");
      auto abijson = fc::json::from_file(abiPath).as<abi_def>();
      auto abi = fc::raw::pack(abijson);
      abi_def abi_def = fc::raw::unpack<chain::abi_def>( abi );
     // const string json_str = fc::json::to_string( abi_def );
     purge_abi_cache(); // make room if necessary
     abi_cache entry;
     entry.account = N(eosio);
     entry.last_accessed = fc::time_point::now();
     abi_serializer abis;
     abis.set_abi( abi_def, abi_serializer_max_time );
     entry.serializer.emplace( std::move( abis ) );
     abi_cache_index.insert( entry );
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
         ("grpc-abi-cache-size", bpo::value<uint32_t>()->default_value(2048),
          "The maximum size of the abi cache for serializing data.")
         ;
}

void grpc_client_plugin::plugin_initialize(const variables_map& options)
{
   try {
         if( options.count( "grpc-client-address" )) {
            my->client_address = options.at( "grpc-client-address" ).as<std::string>();
            //b_need_start = true;

         if( options.count( "grpc-abi-cache-size" )) {
            my->abi_cache_size = options.at( "grpc-abi-cache-size" ).as<uint32_t>();
            EOS_ASSERT( my->abi_cache_size > 0, chain::plugin_config_exception, "mongodb-abi-cache-size > 0 required" );
         }
         my->abi_serializer_max_time = app().get_plugin<chain_plugin>().get_abi_serializer_max_time();

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
