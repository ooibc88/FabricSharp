#----------------------------------------------------------------
# Generated CMake target import file for configuration "Release".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "gflags::gflags_shared" for configuration "Release"
set_property(TARGET gflags::gflags_shared APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(gflags::gflags_shared PROPERTIES
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libgflags.so.2.2.2"
  IMPORTED_SONAME_RELEASE "libgflags.so.2.2"
  )

list(APPEND _IMPORT_CHECK_TARGETS gflags::gflags_shared )
list(APPEND _IMPORT_CHECK_FILES_FOR_gflags::gflags_shared "${_IMPORT_PREFIX}/lib/libgflags.so.2.2.2" )

# Import target "gflags::gflags_nothreads_shared" for configuration "Release"
set_property(TARGET gflags::gflags_nothreads_shared APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(gflags::gflags_nothreads_shared PROPERTIES
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libgflags_nothreads.so.2.2.2"
  IMPORTED_SONAME_RELEASE "libgflags_nothreads.so.2.2"
  )

list(APPEND _IMPORT_CHECK_TARGETS gflags::gflags_nothreads_shared )
list(APPEND _IMPORT_CHECK_FILES_FOR_gflags::gflags_nothreads_shared "${_IMPORT_PREFIX}/lib/libgflags_nothreads.so.2.2.2" )

# Import target "gflags::gflags_static" for configuration "Release"
set_property(TARGET gflags::gflags_static APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(gflags::gflags_static PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "CXX"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libgflags.a"
  )

list(APPEND _IMPORT_CHECK_TARGETS gflags::gflags_static )
list(APPEND _IMPORT_CHECK_FILES_FOR_gflags::gflags_static "${_IMPORT_PREFIX}/lib/libgflags.a" )

# Import target "gflags::gflags_nothreads_static" for configuration "Release"
set_property(TARGET gflags::gflags_nothreads_static APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(gflags::gflags_nothreads_static PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "CXX"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libgflags_nothreads.a"
  )

list(APPEND _IMPORT_CHECK_TARGETS gflags::gflags_nothreads_static )
list(APPEND _IMPORT_CHECK_FILES_FOR_gflags::gflags_nothreads_static "${_IMPORT_PREFIX}/lib/libgflags_nothreads.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
