cmake_minimum_required(VERSION 2.8)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
#set(protobuf_MODULE_COMPATIBLE TRUE)
#find_package(protobuf CONFIG REQUIRED)
#message(STATUS "Using protobuf ${protobuf_VERSION}")
#set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
#set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
#find_package(grpc CONFIG REQUIRED)
#message(STATUS "Using gRPC ${gRPC_VERSION}")
#set(_GRPC_GRPCPP_UNSECURE grpc::grpc++_unsecure)
#set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:grpc::grpc_cpp_plugin>)


  #add_subdirectory(../../../../merge_eos_eosforce/grpc/grpc ${CMAKE_CURRENT_BINARY_DIR}/grpc EXCLUDE_FROM_ALL)
  #message(STATUS "client Using gRPC via add_subdirectory.")

  set(_PROTOBUF_LIBPROTOBUF libprotobuf)
  set(_PROTOBUF_PROTOC $<TARGET_FILE:protoc>)
  set(_GRPC_GRPCPP_UNSECURE grpc++_unsecure)
  set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:grpc_cpp_plugin>)




get_filename_component(hw_proto "./include/protos/eosio_grpc_client.proto" ABSOLUTE)
get_filename_component(hw_proto_path "${hw_proto}" PATH)
set(hw_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/eosio_grpc_client.pb.cc")
set(hw_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/eosio_grpc_client.pb.h")
set(hw_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/eosio_grpc_client.grpc.pb.cc")
set(hw_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/eosio_grpc_client.grpc.pb.h")
add_custom_command(
      OUTPUT "${hw_proto_srcs}" "${hw_proto_hdrs}" "${hw_grpc_srcs}" "${hw_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${hw_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${hw_proto}"
      DEPENDS "${hw_proto}")

#include_directories("${CMAKE_CURRENT_BINARY_DIR}")

get_filename_component(hw_proto "./include/protos/transfer.proto" ABSOLUTE)
get_filename_component(hw_proto_path "${hw_proto}" PATH)
set(hw_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/transfer.pb.cc")
set(hw_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/transfer.pb.h")
set(hw_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/transfer.grpc.pb.cc")
set(hw_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/transfer.grpc.pb.h")
add_custom_command(
      OUTPUT "${hw_proto_srcs}" "${hw_proto_hdrs}" "${hw_grpc_srcs}" "${hw_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${hw_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${hw_proto}"
      DEPENDS "${hw_proto}")

get_filename_component(hw_proto "./include/protos/transaction.proto" ABSOLUTE)
get_filename_component(hw_proto_path "${hw_proto}" PATH)
set(hw_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/transaction.pb.cc")
set(hw_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/transaction.pb.h")
set(hw_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/transaction.grpc.pb.cc")
set(hw_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/transaction.grpc.pb.h")
add_custom_command(
      OUTPUT "${hw_proto_srcs}" "${hw_proto_hdrs}" "${hw_grpc_srcs}" "${hw_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${hw_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${hw_proto}"
      DEPENDS "${hw_proto}")

get_filename_component(hw_proto "./include/protos/block.proto" ABSOLUTE)
get_filename_component(hw_proto_path "${hw_proto}" PATH)
set(hw_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/block.pb.cc")
set(hw_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/block.pb.h")
set(hw_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/block.grpc.pb.cc")
set(hw_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/block.grpc.pb.h")
add_custom_command(
      OUTPUT "${hw_proto_srcs}" "${hw_proto_hdrs}" "${hw_grpc_srcs}" "${hw_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${hw_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${hw_proto}"
      DEPENDS "${hw_proto}")

include_directories("${CMAKE_CURRENT_BINARY_DIR}")


file(GLOB HEADERS "include/eosio/grpc_plugin/*.hpp")
include_directories("/usr/local/include/grpcpp")
include_directories("/usr/local/include/google/protobuf")
LINK_LIBRARIES("/usr/local/lib/libgrpc++.so" "/usr/local/lib/libgrpc++.so.1")
add_library( grpc_client_plugin
             grpc_client_plugin.cpp
             eosio_grpc_client.grpc.pb.cc
             eosio_grpc_client.pb.cc
             transfer.grpc.pb.cc
             transfer.pb.cc
             transaction.grpc.pb.cc
             transaction.pb.cc
             block.grpc.pb.cc
             block.pb.cc
             ${HEADERS} )

target_link_libraries( grpc_client_plugin appbase chain_plugin eosio_chain fc ${_GRPC_GRPCPP_UNSECURE} ${_PROTOBUF_LIBPROTOBUF})
target_include_directories( grpc_client_plugin PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include" )


