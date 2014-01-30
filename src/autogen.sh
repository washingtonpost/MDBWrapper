#!/bin/sh

autoreconf -fiv
touch NEWS README AUTHORS ChangeLog
./configure $@
