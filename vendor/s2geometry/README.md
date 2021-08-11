#
# Temporary build instructions for s2 in a Kart context
#

    $ brew install googletest glog gflags swig

* make sure homebrew's s2geometry is not installed!

    $ brew ls s2geometry
    Error: No such keg: /usr/local/Cellar/s2geometry

    $ source venv/bin/activate
    $ cd vendor/s2geometry/src
    $ mkdir build
    $ cd build

* avoid system libssl
* force a particular python
* set rpath on macos for `_pywraps2.so` so it finds `libs2.dylib`

    $ cmake \
        -DCMAKE_CXX_FLAGS=-I/usr/local/opt/googletest/include \
        -DCMAKE_INSTALL_RPATH=@loader_path/../.. \
        -DCMAKE_INSTALL_PREFIX=../../../../venv/ \
        -DPython3_FIND_STRATEGY=LOCATION \
        -DPython3_ROOT_DIR=../../../../venv/ \
        -DOPENSSL_ROOT_DIR=/usr/local/opt/openssl@1.1/ \
        -DOPENSSL_INCLUDE_DIR=/usr/local/opt/openssl@1.1/include \
        -DWITH_GFLAGS=ON \
        -DWITH_GLOG=ON \
        -DWITH_PYTHON=ON \
        ..

    $ make
    $ make install

* check it works

    $ python
    >>> import pywraps2 as s2
    >>> s2.S2CellId.FromToken('487').ToLatLng().ToStringInDegrees()
    52.658913,-3.228648
