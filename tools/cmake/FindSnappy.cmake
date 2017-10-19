#.rst:
# FindSnappy
# --------
# Finds the libsnappy library
#
# This will will define the following variables::
#
# SNAPPY_FOUND - system has libsnappy
# SNAPPY_INCLUDE_DIRS - the libsnappy include directory
# SNAPPY_LIBRARIES - the libsnappy libraries
#
# and the following imported targets::
#
#   SNAPPY::SNAPPY   - The libsnappy library

if(PKG_CONFIG_FOUND)
  pkg_check_modules(PC_SNAPPY snappy QUIET)
endif()

find_path(SNAPPY_INCLUDE_DIR snappy.h
        PATHS ${PC_SNAPPY_INCLUDEDIR})
find_library(SNAPPY_LIBRARY snappy
        PATHS ${PC_SNAPPY_LIBRARY})
set(SNAPPY_VERSION ${PC_SNAPPY_VERSION})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(SNAPPY
        REQUIRED_VARS SNAPPY_LIBRARY SNAPPY_INCLUDE_DIR
        VERSION_VAR SNAPPY_VERSION)

if(SNAPPY_FOUND)
  set(SNAPPY_LIBRARIES ${SNAPPY_LIBRARY})
  set(SNAPPY_INCLUDE_DIRS ${SNAPPY_INCLUDE_DIR})

  if(NOT TARGET SNAPPY::SNAPPY)
    add_library(SNAPPY::SNAPPY UNKNOWN IMPORTED)
    set_target_properties(SNAPPY::SNAPPY PROPERTIES
            IMPORTED_LOCATION "${SNAPPY_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${SNAPPY_INCLUDE_DIR}")
  endif()
endif()

mark_as_advanced(SNAPPY_INCLUDE_DIR SNAPPY_LIBRARY)
