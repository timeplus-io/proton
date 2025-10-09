# for better compatibility with SCL spec file
%global pkg_name mongo-cxx-driver

Name:           mongo-cxx-driver
Version:        3.6.5
Release:        1%{?dist}
Summary:        A C++ Driver for MongoDB
License:        ASL 2.0
URL:            https://github.com/mongodb/mongo-cxx-driver/wiki
Source0:        https://github.com/mongodb/%{pkg_name}/archive/%{name}-r%{version}.tar.gz


Patch2:         mongo-cxx-driver-3.3.1_paths.patch
Patch3:         mongo-cxx-driver-catch-update.patch
BuildRequires:  boost-devel >= 1.49
BuildRequires:  openssl-devel
BuildRequires:  cmake
BuildRequires:  cyrus-sasl-devel
BuildRequires:  libbson-devel
BuildRequires:  mongo-c-driver-devel
BuildRequires:  snappy-devel
BuildRequires:  gcc-c++
BuildRequires:  libzstd-devel
BuildRequires:  cmake(mongocrypt)

Provides: libmongodb = 2.6.0-%{release}
Provides: libmongodb%{?_isa} = 2.6.0-%{release}
Obsoletes: libmongodb <= 2.4.9-8
Provides: bundled(catch) = 2.13.5

%description
This package provides the shared library for the MongoDB C++ Driver.


%package devel
Summary:        MongoDB header files
Requires:       %{name}%{?_isa} = %{version}-%{release}

Provides: libmongodb-devel = 2.6.0-%{release}
Provides: libmongodb-devel%{?_isa} = 2.6.0-%{release}
Obsoletes: libmongodb-devel <= 2.4.9-8

Provides:       mongodb-devel = 2.6.0-%{release}
Obsoletes:      mongodb-devel < 2.4

%description devel
This package provides the header files for MongoDB C++ driver.

%package bsoncxx
Summary:        C++ library for working with BSON
Requires:       %{pkg_name}%{?_isa} = %{version}-%{release}


%description bsoncxx
This package provides the shared library for working with BSON.


%package bsoncxx-devel
Summary:        C++ header files for library for working with BSON
Requires:       %{pkg_name}-bsoncxx%{?_isa} = %{version}-%{release}


%description bsoncxx-devel
This package provides the C++ header files for library for working with BSON.


%prep
%setup -q -n %{name}-r%{version}

%patch2 -p1 -b .paths
%patch3 -p1 -b .catchupdate


%build

export CFLAGS="$CFLAGS $RPM_OPT_FLAGS"
export LDFLAGS="$LDFLAGS $RPM_LD_FLAGS"

%cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DBSONCXX_POLY_USE_BOOST=1 \
    .

%cmake_build


%install
%cmake_install
rm -r %{buildroot}%{_datadir}/%{name}
rm %{buildroot}%{_libdir}/cmake/mongocxx-%{version}/*.cmake
rm %{buildroot}%{_libdir}/cmake/bsoncxx-%{version}/*.cmake

%files
%doc README.md
%license LICENSE
%license THIRD-PARTY-NOTICES
%{_libdir}/libmongocxx.so.*

%files devel
%{_includedir}/mongocxx/
%{_libdir}/libmongocxx.so
%{_libdir}/pkgconfig/libmongocxx.pc
%{_libdir}/cmake/*mongocxx*

%files bsoncxx
%{_libdir}/libbsoncxx.so.*

%files bsoncxx-devel
%{_includedir}/bsoncxx
%{_libdir}/libbsoncxx.so
%{_libdir}/pkgconfig/libbsoncxx.pc
%{_libdir}/cmake/*bsoncxx*

%changelog
* Mon Aug 09 2021 Honza Horak <hhorak@redhat.com> - 3.6.5-1
- Update to 3.6.5
- Update bundled catch to 2.13.5

* Thu Jul 22 2021 Fedora Release Engineering <releng@fedoraproject.org> - 3.4.1-6
- Rebuilt for https://fedoraproject.org/wiki/Fedora_35_Mass_Rebuild

* Tue Jan 26 2021 Fedora Release Engineering <releng@fedoraproject.org> - 3.4.1-5
- Rebuilt for https://fedoraproject.org/wiki/Fedora_34_Mass_Rebuild

* Tue Aug 11 2020 Honza Horak <hhorak@redhat.com> - 3.4.1-4
- Fix FTBFS caused by cmake changes

* Sat Aug 01 2020 Fedora Release Engineering <releng@fedoraproject.org> - 3.4.1-3
- Second attempt - Rebuilt for
  https://fedoraproject.org/wiki/Fedora_33_Mass_Rebuild

* Tue Jul 28 2020 Fedora Release Engineering <releng@fedoraproject.org> - 3.4.1-2
- Rebuilt for https://fedoraproject.org/wiki/Fedora_33_Mass_Rebuild

* Mon Mar 09 2020 Honza Horak <hhorak@redhat.com> - 3.4.1-1
- Update to 3.4.1
- Add libzstd-devel and mongocrypt as BR

* Wed Jan 29 2020 Fedora Release Engineering <releng@fedoraproject.org> - 3.4.0-2
- Rebuilt for https://fedoraproject.org/wiki/Fedora_32_Mass_Rebuild

* Mon Sep 09 2019 Honza Horak <hhorak@redhat.com> - 3.4.0-1
- Rebase to 3.4.0

* Thu Jul 25 2019 Fedora Release Engineering <releng@fedoraproject.org> - 3.3.1-6
- Rebuilt for https://fedoraproject.org/wiki/Fedora_31_Mass_Rebuild

* Fri Feb 01 2019 Patrik Novotný <panovotn@redhat.com> - 3.3.1-5
- Disable testing, as it requires MongoDB

* Fri Feb 01 2019 Fedora Release Engineering <releng@fedoraproject.org> - 3.3.1-4
- Rebuilt for https://fedoraproject.org/wiki/Fedora_30_Mass_Rebuild

* Fri Jan 25 2019 Jonathan Wakely <jwakely@redhat.com> - 3.3.1-3
- Rebuilt for Boost 1.69

* Wed Jan 23 2019 Björn Esser <besser82@fedoraproject.org> - 3.3.1-2
- Append curdir to CMake invokation. (#1668512)

* Fri Oct 19 2018 Matej Mužila <mmuzila@redhat.com> - 3.3.1-1
- Upgrade to version 3.3.1

* Thu Sep 13 2018 Matej Mužila <mmuzila@redhat.com> - 1.1.2-15
- Add gcc-c++ as a build dependency. Resolves #1604873

* Fri Jul 13 2018 Fedora Release Engineering <releng@fedoraproject.org> - 1.1.2-14
- Rebuilt for https://fedoraproject.org/wiki/Fedora_29_Mass_Rebuild

* Fri Feb 09 2018 Igor Gnatenko <ignatenkobrain@fedoraproject.org> - 1.1.2-13
- Escape macros in %%changelog

* Thu Feb 08 2018 Fedora Release Engineering <releng@fedoraproject.org> - 1.1.2-12
- Rebuilt for https://fedoraproject.org/wiki/Fedora_28_Mass_Rebuild

* Tue Jan 23 2018 Jonathan Wakely <jwakely@redhat.com> - 1.1.2-11
- Rebuilt for Boost 1.66

* Thu Aug 03 2017 Fedora Release Engineering <releng@fedoraproject.org> - 1.1.2-10
- Rebuilt for https://fedoraproject.org/wiki/Fedora_27_Binutils_Mass_Rebuild

* Wed Jul 26 2017 Fedora Release Engineering <releng@fedoraproject.org> - 1.1.2-9
- Rebuilt for https://fedoraproject.org/wiki/Fedora_27_Mass_Rebuild

* Wed Jul 19 2017 Jonathan Wakely <jwakely@redhat.com> - 1.1.2-8
- Rebuilt for s390x binutils bug

* Mon Jul 03 2017 Jonathan Wakely <jwakely@redhat.com> - 1.1.2-7
- Rebuilt for Boost 1.64

* Mon May 15 2017 Fedora Release Engineering <rel-eng@lists.fedoraproject.org> - 1.1.2-6
- Rebuilt for https://fedoraproject.org/wiki/Fedora_26_27_Mass_Rebuild

* Tue Feb 28 2017 Marek Skalický <mskalick@redhat.com> - 1.1.2-5
- Temporary disable optimizations (some tests are failing with it)

* Fri Feb 10 2017 Fedora Release Engineering <releng@fedoraproject.org> - 1.1.2-4
- Rebuilt for https://fedoraproject.org/wiki/Fedora_26_Mass_Rebuild

* Sat Nov 19 2016 Peter Robinson <pbrobinson@fedoraproject.org> 1.1.2-3
- Remove ExclusiveArch. While a MongoDB instance is little endian only, this is a client
- Build with openssl 1.0

* Tue Aug 02 2016 Marek Skalický <mskalick@redhat.com> - 1.1.2-2
- Enabled sasl support
- Unit tests added in check section

* Wed Jun 22 2016 Marek Skalicky <mskalick@redhat.com> - 1.1.2-1
- Upgrade to version 1.1.2

* Tue May 17 2016 Jonathan Wakely <jwakely@redhat.com> - 1.1.0-4
- Rebuilt for linker errors in boost (#1331983)

* Thu Feb 04 2016 Fedora Release Engineering <releng@fedoraproject.org> - 1.1.0-3
- Rebuilt for https://fedoraproject.org/wiki/Fedora_24_Mass_Rebuild

* Mon Jan 18 2016 Jonathan Wakely <jwakely@redhat.com> - 1.1.0-2
- Rebuilt for Boost 1.60

* Thu Dec 10 2015 Marek Skalicky <mskalick@redhat.com> - 1.1.0-1
- Upgrade to version 1.1.0

* Fri Nov 20 2015 Marek Skalicky <mskalick@redhat.com> - 1.0.7-1
- Upgrade to version 1.0.7

* Thu Oct 22 2015 Tim Niemueller <tim@niemueller.de> - 1.0.6-1
- Upgrade to version 1.0.6
- Add --c++11 flag

* Thu Aug 27 2015 Jonathan Wakely <jwakely@redhat.com> - 1.0.5-2
- Rebuilt for Boost 1.59

* Wed Aug 19 2015 Marek Skalicky <mskalick@redhat.com> - 1.0.5-1
- Upgrade to version 1.0.5

* Mon Aug 17 2015 Marek Skalicky <mskalick@redhat.com> - 1.0.4-1
- Upgrade to version 1.0.4

* Wed Jul 29 2015 Fedora Release Engineering <rel-eng@lists.fedoraproject.org> - 1.0.2-4
- Rebuilt for https://fedoraproject.org/wiki/Changes/F23Boost159

* Wed Jul 22 2015 David Tardon <dtardon@redhat.com> - 1.0.2-3
- rebuild for Boost 1.58

* Wed Jun 17 2015 Fedora Release Engineering <rel-eng@lists.fedoraproject.org> - 1.0.2-2
- Rebuilt for https://fedoraproject.org/wiki/Fedora_23_Mass_Rebuild

* Tue May 26 2015 Marek Skalicky <mskalick@redhat.com> - 1.0.2-1
- Upgrade to version 1.0.2

* Tue Apr 14 2015 Marek Skalicky <mskalick@redhat.com> - 1.0.1-1
- Upgrade to version 1.0.1

* Tue Feb 10 2015 Marek Skalicky <mskalick@redhat.com> - 1.0.0-3
- Disabled -Werror (dont't build with gcc 5.0)

* Wed Feb 04 2015 Petr Machata <pmachata@redhat.com> - 1.0.0-2
- Bump for rebuild.

* Thu Jan 29 2015 Marek Skalicky <mskalick@redhat.com> - 1.0.0-1
- Upgrade to stable version 1.0.0

* Tue Jan 27 2015 Petr Machata <pmachata@redhat.com> - 1.0.0-0.8.rc3
- Rebuild for boost 1.57.0

* Fri Jan 02 2015 Marek Skalicky <mskalick@redhat.com> - 1.0.0-0.7.rc3
- Upgrade to rc3

* Tue Nov 18 2014 Marek Skalický <mskalick@redhat.com> - 1.0.0-0.6.rc2
- Upgrade to rc2
- Changed scons target to build only driver

* Mon Oct 27 2014 Marek Skalický <mskalick@redhat.com> - 1.0.0-0.5.rc1
- Upgrade to rc1
- Added mongo-cxx-driver-devel requires (openssl-devel, boost-devel)

* Sat Oct 25 2014 Peter Robinson <pbrobinson@fedoraproject.org> 1.0.0-0.4.rc1
- Don't reset the Release until 1.0.0 GA

* Fri Oct 24 2014 Marek Skalický <mskalick@redhat.com> - 1.0.0-0.1.rc1
- Upgrade to rc1

* Thu Oct 9 2014 Marek Skalický <mskalick@redhat.com> - 1.0.0-0.3.rc0
- Added Provides: mongodb-devel = 2.6.0-1 provided by libmongo-devel

* Thu Oct 9 2014 Marek Skalický <mskalick@redhat.com> - 1.0.0-0.2.rc0
- Added Provides: libmongodb%%{?_isa} packages

* Tue Sep 30 2014 Marek Skalický <mskalick@redhat.com> - 1.0.0-0.1.rc0
- initial port
