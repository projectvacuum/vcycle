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
VM lifecycle manager daemon for OpenStack etc

%package azure
Summary: Azure plugin for Vcycle
Requires: vcycle

%description azure
Azure plugin for Vcycle

%package dbce
Summary: DBCE plugin for Vcycle
Requires: vcycle

%description dbce
DBCE plugin for Vcycle

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
/usr/sbin
/usr/share/doc/vcycle-%{version}
/usr/lib64/python2.6/site-packages/vcycle/__init__.py*
/usr/lib64/python2.6/site-packages/vcycle/shared.py*
/usr/lib64/python2.6/site-packages/vcycle/vacutils.py*
/usr/lib64/python2.6/site-packages/vcycle/openstack_api.py*
/usr/lib64/python2.6/site-packages/vcycle/occi_api.py*
/var/lib/vcycle
/etc/rc.d/init.d/vcycled
/etc/logrotate.d/vcycled
/etc/vcycle.d
/usr/share/man/man5/*
/usr/share/man/man8/*

%post azure
pip install azure-servicemanagement-legacy

%files azure
/usr/lib64/python2.6/site-packages/vcycle/azure_api.py* 

%files dbce
/usr/lib64/python2.6/site-packages/vcycle/dbce_api.py*
