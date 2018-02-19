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
%{python_sitelib}/vcycle/shared.py*
%{python_sitelib}/vcycle/vacutils.py*
%{python_sitelib}/vcycle/openstack/openstack_api.py*
%{python_sitelib}/vcycle/ec2_api.py*
%{python_sitelib}/vcycle/openstack/*.py*
/var/lib/vcycle
/etc/rc.d/init.d/vcycled
/etc/logrotate.d/vcycled
/etc/vcycle.d
/usr/share/man/man5/*
/usr/share/man/man8/*

%post azure
pip install azure-servicemanagement-legacy

%files azure
%{python_sitelib}/vcycle/azure_api.py*

%files occi
%{python_sitelib}/vcycle/occi_api.py*

%files dbce
%{python_sitelib}/vcycle/dbce_api.py*
