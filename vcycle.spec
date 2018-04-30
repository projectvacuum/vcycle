Name: vcycle
Version: %(echo ${VCYCLE_VERSION:-0.0})
Release: 1
BuildArch: noarch
Summary: Vcycle daemon and tools
License: BSD
Group: System Environment/Daemons
Source: vcycle.tgz
URL: http://www.gridpp.ac.uk/vac/
Vendor: GridPP
Packager: Andrew McNab <Andrew.McNab@cern.ch>
Requires: httpd,mod_ssl,python-pycurl,m2crypto,python-requests,openssl

%description
VM lifecycle manager daemon for OpenStack, EC2 etc

%package azure
Summary: Azure plugin for Vcycle
Requires: vcycle

%description azure
Azure plugin for Vcycle, contributed by CERN/IT

%package occi
Summary: OCCI plugin for Vcycle
Requires: vcycle

%description occi
OCCI plugin for Vcycle, contributed by CERN/IT

%package dbce
Summary: DBCE plugin for Vcycle
Requires: vcycle

%description dbce
DBCE plugin for Vcycle, contributed by CERN/IT

%package creamce
Summary: CREAM plugin for vcycle
Requires: vcycle

%description creamce
CREAM plugin for vcycle

%package google
Summary: Google plugin for vcycle
Requires: vcycle

%description google
Google plugin for vcycle

%prep

%setup -n vcycle

%build

%install
make install

%preun
if [ "$1" = "0" ] ; then
  # if uninstallation rather than upgrade then stop
  service vcycled stop
fi

%post
service vcycled status
if [ $? = 0 ] ; then
  # if already running then restart with new version
  service vcycled restart
fi

%files
/usr/sbin/*
/usr/share/doc/vcycle-%{version}
%{python_sitelib}/vcycle/__init__.py*
%{python_sitelib}/vcycle/core/__init__.py*
%{python_sitelib}/vcycle/core/file.py*
%{python_sitelib}/vcycle/core/shared.py*
%{python_sitelib}/vcycle/core/vacutils.py*
%{python_sitelib}/vcycle/plugins/__init__.py*
%{python_sitelib}/vcycle/plugins/openstack/__init__.py*
%{python_sitelib}/vcycle/plugins/openstack/openstack_api.py*
%{python_sitelib}/vcycle/plugins/openstack/image_api.py*
%{python_sitelib}/vcycle/plugins/ec2_api.py*
/var/lib/vcycle
/etc/rc.d/init.d/vcycled
/etc/logrotate.d/vcycled
/etc/vcycle.d
/usr/share/man/man5/*
/usr/share/man/man8/*

%post azure
pip install azure-servicemanagement-legacy

%files azure
%{python_sitelib}/vcycle/plugins/azure_api.py*

%files occi
%{python_sitelib}/vcycle/plugins/occi_api.py*

%files dbce
%{python_sitelib}/vcycle/plugins/dbce_api.py*

%files creamce
%{python_sitelib}/vcycle/plugins/creamce_api.py*

%files google
%{python_sitelib}/vcycle/plugins/google_api.py*
