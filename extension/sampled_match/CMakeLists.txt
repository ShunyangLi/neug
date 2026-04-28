# Find required packages for FaSTest
find_package(Boost REQUIRED)
find_package(GSL REQUIRED)

# Collect FaSTest source files (header-only library with some cpp files)
file(GLOB_RECURSE FASTEST_LIB_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/include/fastest_lib/src/**/*.cpp")

# Collect sampled_match extension source files
file(GLOB SAMPLED_MATCH_EXTENSION_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

# Combine all sources
set(SAMPLED_MATCH_EXTENSION_OBJECT_FILES
    ${SAMPLED_MATCH_EXTENSION_SOURCES}
    ${FASTEST_LIB_SOURCES})

# Use the extension build system (should be called before add_extension_test)
build_extension_lib("sampled_match")

# Set include directories
target_include_directories(neug_sampled_match_extension PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include/fastest_lib/src
    ${CMAKE_CURRENT_BINARY_DIR}
    ${Boost_INCLUDE_DIRS}
    ${GSL_INCLUDE_DIRS})

# Include rapidjson for JSON parsing
target_include_directories(neug_sampled_match_extension PRIVATE SYSTEM
    ${CMAKE_SOURCE_DIR}/third_party/rapidjson/include)

# Link libraries
# protobuf::libprotobuf is added explicitly because src/CMakeLists.txt:35
# links protobuf as PRIVATE on the neug target — its INTERFACE_LINK_LIBRARIES
# does not propagate to downstream extension consumers, even though many neug
# PUBLIC headers (neug_db.h, connection.h, query_result.h, …) transitively
# include generated *.pb.h files. Linking it here ensures the extension can
# resolve protobuf symbols at link/load time independently of how libneug
# happens to expose (or not expose) them.
target_link_libraries(neug_sampled_match_extension PRIVATE
    ${GSL_LIBRARIES}
    Boost::boost
    neug
    protobuf::libprotobuf
)

# Build test by compiling extension sources directly (no link to neug_sampled_match_extension)
if(BUILD_TEST)
    add_extension_test(NAME sampled_match
        TEST_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/tests/sampled_match_test.cpp
        EXTRA_INCLUDE_DIRS
            ${CMAKE_CURRENT_SOURCE_DIR}/include/fastest_lib/src
            ${CMAKE_SOURCE_DIR}/third_party/rapidjson/include
        EXTRA_LINK_LIBS
            GSL::gsl
            GSL::gslcblas
            Boost::boost
    )
    # The project bundles googletest 1.13 via FetchContent and links
    # libgtest.a built from those sources. On Homebrew Macs the root
    # CMakeLists adds /opt/homebrew/include project-wide (via
    # include_directories(SYSTEM ${OPENSSL_INCLUDE_DIR}) -> SYSTEM list),
    # which contains a newer gtest with a different MakeAndRegisterTestInfo
    # signature. The two SYSTEM dirs share -isystem priority and homebrew
    # is listed first, so its header silently wins -> undefined symbols at
    # link time. Force the bundled gtest headers to be searched first by
    # adding them as a non-SYSTEM PRIVATE include: -I always beats -isystem
    # in clang's search order.
    if(TARGET sampled_match_extension_test)
        target_include_directories(sampled_match_extension_test BEFORE PRIVATE
            ${CMAKE_BINARY_DIR}/_deps/googletest-src/googletest/include
            ${CMAKE_BINARY_DIR}/_deps/googletest-src/googlemock/include)
        # The test calls LOAD sampled_match; at runtime, which dlopens the
        # libsampled_match.neug_extension produced by build_extension_lib().
        # add_extension_test does not wire this build-order dependency, so
        # add it explicitly — otherwise `make sampled_match_extension_test`
        # produces the binary without the .so it expects to find.
        add_dependencies(sampled_match_extension_test neug_sampled_match_extension)
    endif()
endif()
