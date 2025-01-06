cmake_minimum_required(VERSION 3.15...3.26)
project(prototype)

find_package(Arrow REQUIRED)
find_package(Python 3.8 COMPONENTS Interpreter Development.Module REQUIRED)
find_package(nanobind CONFIG REQUIRED)

# Put the compiled libraries in our prototype directory
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/prototype)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/prototype)

# Create our Python module
nanobind_add_module(
    prototype_cpp
    prototype/prototype_cpp.cpp
)

# Link against Arrow's shared library
target_link_libraries(prototype_cpp PRIVATE Arrow::arrow_shared)

# For formal installations
install(TARGETS prototype_cpp
        LIBRARY DESTINATION prototype
        RUNTIME DESTINATION prototype)
