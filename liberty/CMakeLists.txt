cmake_minimum_required (VERSION 2.6)
project (concord)
set(VERSION 0.1.0)

link_directories (
  ${PROJECT_SOURCE_DIR}/third_party/lib
  /usr/local/lib
  /usr/lib/x86_64-linux-gnu
  )

include_directories (
  SYSTEM
  ${PROJECT_SOURCE_DIR}/third_party/include
  src
  )

set (BASE_CPP_CLIENT_LIBRARIES
  :libbolt_cpp_client.so
  :libglog.so.0
  :libthriftnb-0.9.2.so
  :libthrift-0.9.2.so
  :libthriftz-0.9.2.so
  :libglog.so.0.0.0
  :libgflags.so.2.1.0
  :libpthread.so
  :libdouble-conversion.so.1.0
  :libboost_system.so.1.55.0
  :libevent.a
  :libglog.so.0
  :libstdc++.so.6
  :libssl.so.1.0.0
  :libcrypto.so.1.0.0
  :libgflags.so.2
  :libunwind.so.8
  :libpthread.so.0
  :libz.so.1.2.8
  )

set(BASE_FLAGS
  "-fPIC"
  "-fuse-ld=gold"
  "-Wall"
  "-Werror"
  "-Wextra"
  "-Wformat"
  "-pedantic"
  "-Wmissing-braces"
  "-Wparentheses"
  "-Wpointer-arith"
  "-Wformat-security"
  "-Wunused"
  "-Wno-unused-parameter"
  "-Wcast-align"
  "-Wmissing-field-initializers"
  "-O2"
  )

set (CXX_FLAG_LIST
  ${BASE_FLAGS}
  "-std=c++1y"
  "-ftemplate-depth-256"
  "-Wnon-virtual-dtor"
  "-Woverloaded-virtual"
  "-Wsign-promo"
  "-Wreorder"
  "-Wdelete-non-virtual-dtor"
  )

function (join VALUES SEP OUTPUT)
  string (REPLACE ";" "${SEP}" _TMP_STR "${VALUES}")
  set (${OUTPUT} "${_TMP_STR}" PARENT_SCOPE)
endfunction ()




set (CMAKE_CXX_COMPILER g++-5)
set (CMAKE_C_COMPILER gcc-5)
set(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} -v" )
join ("${CXX_FLAG_LIST}" " " CMAKE_CXX_FLAGS)
join ("${BASE_FLAGS}" " " CMAKE_C_FLAGS)



add_executable (
  log_counter
  ${PROJECT_SOURCE_DIR}/src/log_parser.cc
  ${PROJECT_SOURCE_DIR}/src/log_counter.cc
  )
target_link_libraries (
  log_counter
  ${BASE_CPP_CLIENT_LIBRARIES}
  libre2.a
  )


add_executable (
  unique
  ${PROJECT_SOURCE_DIR}/src/unique/unique.cc
  )
target_link_libraries (
  unique
  ${BASE_CPP_CLIENT_LIBRARIES}
  sasl2
  bloom
  re2.a
  rdkafka.a
  rdkafka++.a
  )

add_executable (
  kafka_fill
  ${PROJECT_SOURCE_DIR}/src/kafka/fill.cc
  )
target_link_libraries (
  kafka_fill
  ${BASE_CPP_CLIENT_LIBRARIES}
  sasl2
  :librdkafka.a
  :librdkafka++.a
  )
