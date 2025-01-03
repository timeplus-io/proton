## About the `generated` folder ##

In order to build pulsar-client-cpp, it requires `protoc` to generate the C++ code for `PulsarApi.proto`, which just unnecessarily complicates the build tool chain (for us). And also, the bigger problem is that, currently, `proton` still uses a out-of-date protobuf library, thus it requires an old version of `protoc` to generate the code. So we simply just pre-generated the source code from the Protobuf file and hosted the code directly.
