#.rst:
# FindSWIPL
# --------
# Finds the libuuid library
#
# This will will define the following variables::
#
# SWIPL_FOUND - system has libuuid
# SWIPL_INCLUDE_DIRS - the libuuid include directory
# SWIPL_LIBRARIES - the libuuid libraries
#
# and the following imported targets::
#
#   SWIPL::SWIPL   - The swi-prolog library

INCLUDE(FindPkgConfig)

if(PKG_CONFIG_FOUND)
  pkg_check_modules(PC_SWIPL swipl QUIET)
endif()

find_path(SWIPL_INCLUDE_DIR SWI-cpp.h PATHS ${PC_SWIPL_INCLUDE_DIRS})
find_library(SWIPL_LIBRARY swipl PATHS ${PC_SWIPL_LIBRARY})

set(SWIPL_VERSION ${PC_SWIPL_VERSION})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(SWIPL
                                  REQUIRED_VARS SWIPL_LIBRARY SWIPL_INCLUDE_DIR
                                  VERSION_VAR SWIPL_VERSION)

if(SWIPL_FOUND)
  set(SWIPL_LIBRARIES ${SWIPL_LIBRARY})
  set(SWIPL_INCLUDE_DIRS ${SWIPL_INCLUDE_DIR})

  if(NOT TARGET SWIPL::SWIPL)
    add_library(SWIPL::SWIPL UNKNOWN IMPORTED)
    set_target_properties(SWIPL::SWIPL PROPERTIES
                                     IMPORTED_LOCATION "${SWIPL_LIBRARY}"
                                     INTERFACE_INCLUDE_DIRECTORIES "${SWIPL_INCLUDE_DIR}")
  endif()
endif()

mark_as_advanced(SWIPL_INCLUDE_DIR SWIPL_LIBRARY)
