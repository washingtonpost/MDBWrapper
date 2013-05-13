## This is the shared library to be built
lib_LTLIBRARIES = libmodmdbwrapper.la

## Define the source file for the module
libmodmdbwrapper_la_SOURCES = mod_mdbwrapper.c

## Define that an include directory is required.
INCLUDES = -I$(apache_dir)/include -I$(mongoc_dir)/src -I$(jsonc_dir) -I$(apr_dir) 

## Add compiler flag for 64 bit
AM_CFLAGS = -Im4 -std=gnu99 
