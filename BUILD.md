# Build From Source

## Ubuntu Linux - x86

### Build with docker container

```sh
./docker/packager/packager --package-type binary --docker-image-version clang-16 --proton-build --enable_proton_local --output-dir `pwd`/build_output
```

### Bare metal build

#### Install toolchain

- clang-16 /clang++-16 or above
- cmake 3.20 or above
- ninja

```sh
apt install git cmake ccache python3 ninja-build wget apt-transport-https apt-utils ca-certificates dnsutils gnupg iputils-ping lsb-release gpg curl software-properties-common
```

install llvm-16 compiler

```sh
wget https://apt.llvm.org/llvm.sh
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


#### Step 1: Verify Xcode Version for macOS

Ensure you have the correct version of Xcode installed:
- Version 14.3.1
- Or any version newer than 15.1.Beta.1.

To check your Xcode version, run the following command:
```shell
/Applications/Xcode.app/Contents/Developer/usr/bin/xcodebuild -version
```

You should receive an output similar to:
```plaintext
Xcode 14.3.1
Build version 14E300c
```

Or, for newer versions:
```plaintext
Xcode 15.1
Build version 15C5042i
```


Below's a concise version of the Xcode installation steps:



##### Install Xcode Beta 15.1 Beta 2

1. **Download Xcode Beta:**
   Download Xcode 15.1 Beta 2 from the official apple xcode link or choose another version from [xcodereleases.com](https://xcodereleases.com/).

2. **Install Xcode Beta:**
   Extract the XIP file by double-clicking it and then drag the `Xcode-beta` app into the `/Applications` folder.

3. **Select Xcode Beta Version:**
   In the terminal, run:
   ```shell
   sudo xcode-select -s /Applications/Xcode-beta.app/Contents/Developer
   ```

4. **Verify Installation:**
   Check the version to ensure it's correctly installed:
   ```shell
   /usr/bin/xcodebuild -version
   ```
   You should see:
   ```plaintext
   Xcode 15.1
   Build version 15C5042i
   ```

#### Step 2: Install Dependencies with Homebrew

Proton build is not supported with Apple Clang. Use Homebrew to install LLVM version 16 instead:
```shell
brew install llvm@16
```

First, if you haven't installed Homebrew yet, follow the instructions at [https://brew.sh/](https://brew.sh/).

Next, install the required dependencies using the following commands:
```shell
brew update
brew install ccache cmake ninja libtool gettext llvm@16 gcc binutils grep findutils libiconv
```

#### Step 3: Build Proton Manually

Set up the environment variables required for building Proton:
```shell
export PATH=$(brew --prefix llvm@16)/bin:$PATH
export CC=$(brew --prefix llvm@16)/bin/clang
export CXX=$(brew --prefix llvm@16)/bin/clang++
```

Clone the Proton repository and initiate the build process:
```shell
git clone --recurse-submodules git@github.com:timeplus-io/proton.git
cd proton
mkdir -p build && cd build
cmake ..
ninja
```


# Run proton binary locally

Enter Proton binary folder and run Proton server

You can either launch the server with a self-modified(especially the path) YAML file or start it without specifying the config file (This way will store the data in current path).

```
$ cd proton/build
$ ./programs/proton server --config-file ../programs/server/config.yaml
$ ./programs/proton server start
```

In another console, run Proton client

```
$ cd proton/build
$ ./programs/proton client
```

Then follow [Timeplus user documents](https://docs.timeplus.com) to create stream, insert data and run queries.
