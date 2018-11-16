/**
 *  @file
 *  @copyright defined in eos/LICENSE.txt
 */
#pragma once

#include <eosio/chain_plugin/chain_plugin.hpp>
#include <appbase/application.hpp>
#include <memory>

namespace eosio {

using grpc_server_plugin_impl_ptr = std::shared_ptr<class grpc_server_plugin_impl>;

/**
 *   See data dictionary (DB Schema Definition - EOS API) for description of MongoDB schema.
 *
 *   If cmake -DBUILD_grpc_server_plugin=true  not specified then this plugin not compiled/included.
 */
class grpc_server_plugin : public plugin<grpc_server_plugin> {
public:
   APPBASE_PLUGIN_REQUIRES((chain_plugin))

   grpc_server_plugin();
   virtual ~grpc_server_plugin();

   virtual void set_program_options(options_description& cli, options_description& cfg) override;

   void plugin_initialize(const variables_map& options);
   void plugin_startup();
   void plugin_shutdown();

private:
   grpc_server_plugin_impl_ptr my;
   bool b_need_start = false;
};

}

