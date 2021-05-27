%define __spec_install_post %{nil}
%define __os_install_post %{_dbpath}/brp-compress
%define debug_package %{nil}
%define _servicedir /usr/lib/systemd/system
%define _version %{getenv:PACKAGE_VERSION}
%define _release %{getenv:PACKAGE_RELEASE}
%define _name %{getenv:PACKAGE_NAME}
%define _bindir /opt/stackable/%{_name}
%define _description %{getenv:PACKAGE_DESCRIPTION}

Name: %{_name}
Summary: %{_description}
Version: %{_version}
Release: %{_release}%{?dist}
License: OSL 3.0
Group: Applications/System
Source0: %{name}-%{version}.tar.gz

BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

%description
%{summary}

%prep
%setup -q

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}
cp -a * %{buildroot}

%post
systemctl daemon-reload

%preun
if [ $1 == 0 ]; then #uninstall
  systemctl unmask %{name}.service
  systemctl stop %{name}.service
  systemctl disable %{name}.service
fi

%postun
if [ $1 == 0 ]; then #uninstall
  systemctl daemon-reload
  systemctl reset-failed
fi

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
%{_bindir}/*
%{_servicedir}/%{name}.service
