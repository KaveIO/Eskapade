cmake_minimum_required(VERSION 3.0 FATAL_ERROR)

project(esroofit
        VERSION 1.0.0
        LANGUAGES CXX
        )

# Set CMAKE_MODULE_PATH so that cmake knows where to find our custom cmake modules with helpers/utilities.
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

# Set the C++ standard we're using.
set(CMAKE_CXX_STANDARD 11)
# Set the library output locations
if(NOT CMAKE_LIBRARY_OUTPUT_DIRECTORY)
    set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib/${PROJECT_NAME})
    set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib/${PROJECT_NAME})
    set(CMAKE_INSTALL_LIBDIR           ${CMAKE_INSTALL_PREFIX}/lib/${PROJECT_NAME})
else()
    set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib/${PROJECT_NAME})
    set(CMAKE_INSTALL_LIBDIR           ${CMAKE_BINARY_DIR}/lib/${PROJECT_NAME})
endif()

# Set CXX compiler flags.
set(ESROOFIT_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -O2 -Wmissing-field-initializers -Wall -Wpedantic -Wextra")
# Set to TRUE to turn ROOT configuration debugging.
set(ROOT_CONFIG_DEBUG FALSE)
# The version of ROOT that we require when using Linux.
set(REQUIRED_ROOT_VERSION_LINUX 6.10.04)
# The version of ROOT that we require when using macOS.
set(REQUIRED_ROOT_VERSION_MACOS 6.10.08)
# The set of options that ROOT should have been compiled with.
set(ROOT_OPTIONS cxx11 fftw3 mathmore python3 roofit soversion thread tmva)
# The set of components that should be present in ROOT.
set(ROOT_COMPONENTS Minuit2 PyROOT RooFitCore RooFit RooStats TMVA)

# Include cmake/Common.cmake.
include(Common)

# Now configure everything.
info("********************************************************************")
info("Configuring build for ${PROJECT_NAME}-${PROJECT_VERSION}")
info()
info("${PROJECT_NAME} CXX flags: ${ESROOFIT_CXX_FLAGS}")
info()
info("Searching for ROOT Data Analysis Framework:")

# Let's try to find ROOT and the necessary ROOT components.
if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    find_package(ROOT ${REQUIRED_ROOT_VERSION_MACOS} EXACT REQUIRED COMPONENTS ${ROOT_COMPONENTS})
else()
    find_package(ROOT ${REQUIRED_ROOT_VERSION_LINUX} EXACT REQUIRED COMPONENTS ${ROOT_COMPONENTS})
endif()

if(ROOT_FOUND)
    info("Found ROOT Data Analysis Framework ${HAPPY} ")
    info("    ROOT Version: ${ROOT_VERSION}")
    info("    ROOT CMake helpers/utilities path: ${ROOT_USE_FILE}")
    info("    ROOT C flags: ${ROOT_C_FLAGS}")
    info("    ROOT CXX flags: ${ROOT_CXX_FLAGS}")
    info("    ROOT compile definitions: ${ROOT_DEFINITIONS}")
    info("    ROOT linker flags: ${ROOT_EXE_LINKER_FLAGS}")
    info("    ROOT include directories: ${ROOT_INCLUDE_DIRS}")
    info("    ROOT libraries:")
    foreach(root_lib ${ROOT_LIBRARIES})
        info("        ${root_lib}")
    endforeach()

    # Check that root was compiled with the right set of options.
    foreach(root_option ${ROOT_OPTIONS})
        if(ROOT_${root_option}_FOUND)
            info("    ROOT is compiled with option \"-D${root_option}=ON\" ${HAPPY}")
        else()
            fatal_info("ROOT is not compiled with option \"-D${root_option}=ON\"!")
            fatal_info("Please reconfigure and recompile root with the following CMake flags:")
            fatal_info("    -Dfftw3=ON -Dmathmore=ON -Dminuit2=ON -Droofit=ON -Dtmva=ON -Dsoversion=ON -Dthread=ON -Dpython3=ON -DPYTHON_EXECUTABLE=path_to_python_exe -DPYTHON_INCLUDE_DIR=path_to_python_include -DPYTHON_LIBRARY=path_to_python_lib")
            fatal_info("You may need to install additional third party libraries, see \"https:://root.cern.ch/building-root\" for more info on building ROOT.")
            fatal_abort("Aborting!")
        endif()
    endforeach()

    # Where are the required ROOT components located?
    foreach(root_component ${ROOT_COMPONENTS})
        if(ROOT_${root_component}_LIBRARY)
            info("    ROOT \"${root_component}\" component located at: ${ROOT_${root_component}_LIBRARY} ${HAPPY}")
        else()
            fatal_info("Missing component \"${root_component}\"")
            fatal_abort("Aborting!")
        endif()
    endforeach()

else()
    fatal_info("   Missing ROOT Data Analysis Framework!")
    fatal_abort(Aborting!)

endif()

# Include ROOT's CMake helpers/utilities.
include(${ROOT_USE_FILE})

info()

# Update CXX flags with ROOT CXX flags.
info("Updating ${PROJECT_NAME} CXX flags with ROOT CXX flags")
set(CMAKE_CXX_FLAGS "${ESROOFIT_CXX_FLAGS} ${ROOT_CXX_FLAGS}")
info("Updated ${PROJECT_NAME} CXX flags:  ${CMAKE_CXX_FLAGS}")

set(ESROOFIT_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include)
info("${PROJECT_NAME} include directories: ${ESROOFIT_INCLUDE_DIR}")
set(ESROOFIT_INCLUDE_FILES
        ${ESROOFIT_INCLUDE_DIR}/esroofit/ABCDUtils.h
        ${ESROOFIT_INCLUDE_DIR}/esroofit/DataUtils.h
        ${ESROOFIT_INCLUDE_DIR}/esroofit/DrawUtils.h
        ${ESROOFIT_INCLUDE_DIR}/esroofit/RhhNDKeysPdf.h
        ${ESROOFIT_INCLUDE_DIR}/esroofit/RooABCDHistPdf.h
        ${ESROOFIT_INCLUDE_DIR}/esroofit/RooComplementCoef.h
        ${ESROOFIT_INCLUDE_DIR}/esroofit/RooExpandedFitResult.h
        ${ESROOFIT_INCLUDE_DIR}/esroofit/RooNonCentralBinning.h
        ${ESROOFIT_INCLUDE_DIR}/esroofit/RooParamHistPdf.h
        ${ESROOFIT_INCLUDE_DIR}/esroofit/RooTruncExponential.h
        ${ESROOFIT_INCLUDE_DIR}/esroofit/RooWeibull.h
        ${ESROOFIT_INCLUDE_DIR}/esroofit/Statistics.h
        ${ESROOFIT_INCLUDE_DIR}/esroofit/TMsgLogger.h
        )

# file(RELATIVE_PATH test ${CMAKE_CURRENT_SOURCE_DIR}  ${ESROOFIT_INCLUDE_DIR}/esroofit/ABCDUtils.h)

set(ESROOFIT_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src)
info("${PROJECT_NAME} src directories: ${ESROOFIT_SRC_DIR}")
set(ESROOFIT_CXX_FILES
        ${ESROOFIT_SRC_DIR}/ABCDUtils.cxx
        ${ESROOFIT_SRC_DIR}/DataUtils.cxx
        ${ESROOFIT_SRC_DIR}/DrawUtils.cxx
        ${ESROOFIT_SRC_DIR}/RhhNDKeysPdf.cxx
        ${ESROOFIT_SRC_DIR}/RooABCDHistPdf.cxx
        ${ESROOFIT_SRC_DIR}/RooComplementCoef.cxx
        ${ESROOFIT_SRC_DIR}/RooExpandedFitResult.cxx
        ${ESROOFIT_SRC_DIR}/RooNonCentralBinning.cxx
        ${ESROOFIT_SRC_DIR}/RooParamHistPdf.cxx
        ${ESROOFIT_SRC_DIR}/RooTruncExponential.cxx
        ${ESROOFIT_SRC_DIR}/RooWeibull.cxx
        ${ESROOFIT_SRC_DIR}/Statistics.cxx
        ${ESROOFIT_SRC_DIR}/TMsgLogger.cxx
        )

set(ESROOFIT_DICT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/dict)
info("${PROJECT_NAME} dict directories: ${ESROOFIT_DICT_DIR}")
set(ESROOFIT_DICT_FILE
        ${ESROOFIT_DICT_DIR}/esroofit/LinkDef.h
        )

include_directories(${ESROOFIT_INCLUDE_DIR} ${ESROOFIT_DICT_DIR})


set(ESROOFIT_SOURCE_FILES
        ${ESROOFIT_CXX_FILES}
        ${ESROOFIT_INCLUDE_FILES}
        ${ESROOFIT_DICT_FILE}
        )

# Generate ROOT dictionary.
# This will generate G__esroofit.cxx that we need to add to the library.
# We want ROOT to use relative paths instead of absolute paths,
# so that it can pick up the headers from ROOT_INCLUDE_PATH environment
# variable.
ROOT_GLOB_HEADERS(ESROOFIT_HEADERS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}/include ${CMAKE_CURRENT_SOURCE_DIR}/include/esroofit/*.h)
ROOT_GENERATE_DICTIONARY(G__esroofit ${ESROOFIT_HEADERS} MODULE ${project_name} LINKDEF ${ESROOFIT_DICT_FILE}
        OPTIONS -noIncludePaths)

# Now put it the source together.
add_library(${PROJECT_NAME} SHARED ${ESROOFIT_SOURCE_FILES} G__esroofit.cxx)

# Set target properties.
# This will set:
#    - The library version.
set_target_properties(${PROJECT_NAME}
        PROPERTIES
        VERSION ${PROJECT_VERSION})

# Add custom target to copy headers relative to the lib output directory
# as part of the build process.
# The generated ROOT/cling dictionary wants/needs them.
add_custom_command(
        TARGET ${PROJECT_NAME} POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_directory
        ${ESROOFIT_INCLUDE_DIR}/esroofit
        ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/esroofit)

# Link 'em.
target_link_libraries(${PROJECT_NAME} ${ROOT_LIBRARIES})

info()
info("Run \"cmake --build . -- -jN\", where N is the number of cores to use, to build ${PROJECT_NAME}")
info("The libraries should be in ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}")
info("********************************************************************")
