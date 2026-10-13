cmake_minimum_required(VERSION 3.16)
project(agentclient_spdiag_bridge LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(SPDIAG_BRIDGE_STATIC_SDK "Link exported SpDiag::spdiag_static instead of SpDiag::spdiag_lib" OFF)

# Production builds always require the installed, real SpDiag SDK.
# native/tests/fake_sdk is never part of this target's include path.
find_package(SpDiag CONFIG REQUIRED)

add_library(spdiag_bridge SHARED spdiag_bridge.cpp)
target_include_directories(spdiag_bridge PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")
set_target_properties(spdiag_bridge PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    CXX_VISIBILITY_PRESET hidden
    VISIBILITY_INLINES_HIDDEN YES)

if(SPDIAG_BRIDGE_STATIC_SDK)
    target_link_libraries(spdiag_bridge PRIVATE SpDiag::spdiag_static)
else()
    target_link_libraries(spdiag_bridge PRIVATE SpDiag::spdiag_lib)
endif()

install(TARGETS spdiag_bridge LIBRARY DESTINATION lib)
install(FILES spdiag_bridge.h DESTINATION include)
