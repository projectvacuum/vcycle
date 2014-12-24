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
Requires: httpd,mod_ssl,python-novaclient,python-pycurl,m2crypto

%description
VM lifecycle manager daemon for OpenStack etc

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
/usr/lib64/python2.6/site-packages/vcycle
/var/lib/vcycle
/etc/rc.d/init.d/vcycled
/etc/logrotate.d/vcycled
/etc/vcycle.d
