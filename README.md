# MDBWrapper

Apache2 MDBWrapper module

## To install automatically

```bash
apxs binaries/[your system]/libmodmdbwrapper.la
```


## To build

First, you need the following requirements:

* apache2 (http://httpd.apache.org/)
* json-c (https://github.com/json-c/json-c)
* mongo-c (https://github.com/mongodb/mongo-c-driver)


Configure

```bash
cd src
./configure --with-apache=/path/to/apache2 --with-jsonc=/path/to/json-c --with-mongoc=/path/to/mongo-c
```

Make

```bash
make
```

Make creates a libmodmdbwrapper.la file. Follow the install instructions above to install automatically.


# LICENSE

MIT