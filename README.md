# MDBWrapper

Apache2 MDBWrapper module

## Install

### Automatically

```bash
sudo apxs -ian mdbwrapper binaries/[your system]/libmodmdbwrapper.la
```


## To build

First, you need the following requirements (versions listed are what the binaries in this repo are currently compiled with):

* apache2 (http://httpd.apache.org/ [tested/in prod with v2.2.x])
* json-c (https://github.com/json-c/json-c [v0.11])
* mongo-c (https://github.com/mongodb/mongo-c-driver/tree/v0.7.1 [v0.7.1])

Configure

```bash
configure and make the above 3 libs.

cd src
./autogen.sh --with-apache=/path/to/apache2 --with-jsonc=/path/to/json-c --with-mongoc=/path/to/mongo-c

make

```

`make` creates a `libmodmdbwrapper.la` file. Follow the install instructions above to install automatically.


# LICENSE

MIT