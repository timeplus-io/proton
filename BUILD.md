# Build From Source

## Ubuntu Linux - x86

### Build with docker container

```sh
./docker/packager/packager --package-type binary --docker-image-version clang-16 --proton-build --output-dir `pwd`/build_output
```

### Bare metal build

#### Install toolchain

- clang-16 /clang++-16 or above
- cmake 3.20 or above
- ninja

```sh
apt install git cmake ccache python3 ninja-build wget apt-transport-https apt-utils ca-certificates dnsutils gnupg iputils-ping lsb-release gpg curl
```

install llvm-16 compiler

```sh
bash -c "$(wget -O - https://apt.llvm.org/llvm.sh)"
chmod +x llvm.sh
sudo ./llvm.sh 16
```

#### Build

```
$ git clone --recurse-submodules git@github.com:timeplus-io/proton.git
$ cd proton
$ mkdir -p build && cd build && cmake ..
$ ninja
```

## Redhat Linux - x86

### Build with docker container

### Bare metal build

#### Install toolchain

- clang-16 /clang++-16 or above
- cmake 3.20 or above
- ninja

#### Build

```
$ git clone --recurse-submodules git@github.com:timeplus-io/proton.git
$ cd proton
$ mkdir -p build && cd build && cmake ..
$ ninja
```

## Ubuntu Linux - ARM

### Build with docker container

### Bare metal build

#### Install toolchain

- clang-16 /clang++-16 or above
- cmake 3.20 or above
- ninja

#### Build

```
$ git clone --recurse-submodules git@github.com:timeplus-io/proton.git
$ cd proton
$ mkdir -p build && cd build && cmake ..
$ ninja
```

## Redhat Linux - ARM

### Build with docker container

### Bare metal build

#### Install toolchain

- clang-16 /clang++-16 or above
- cmake 3.20 or above
- ninja

#### Build

```
$ git clone --recurse-submodules git@github.com:timeplus-io/proton.git
$ cd proton
$ mkdir -p build && cd build && cmake ..
$ ninja
```

## MacOS - Intel Architecture

### Build with docker container

### Bare metal build

#### Install toolchain

We don't support build Proton by using Apple Clang. Please use `brew install llvm` to install
clang-16 / clang++-16.


- clang-16 /clang++-16 or above
- cmake 3.20 or above
- ninja

#### Build

```
$ git clone --recurse-submodules git@github.com:timeplus-io/proton.git
$ cd proton
$ mkdir -p build && cd build && cmake ..
$ ninja
```

## MacOS - Apple Silicon

### Build with docker container

### Bare metal build

#### Install toolchain

We don't support build Proton by using Apple Clang. Please use `brew install llvm` to install
clang-16 / clang++-16.


- clang-16 /clang++-16 or above
- cmake 3.20 or above
- ninja

#### Build

```
$ git clone --recurse-submodules git@github.com:timeplus-io/proton.git
$ cd proton
$ mkdir -p build && cd build && cmake ..
$ ninja
```



# Run proton binary locally

Enter Proton binary folder and run Proton server

```
$ cd proton/build
$ ./programs/proton server --config ../programs/server/config.yaml
```

In another console, run Proton client

```
$ cd proton/build
$ ./programs/proton client
```

Then follow [Timeplus user documents](https://docs.timeplus.com) to create stream, insert data and run queries.
