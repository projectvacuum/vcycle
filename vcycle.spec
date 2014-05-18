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
Requires: httpd,mod_ssl,python-novaclient

%description
VM lifecycle manager daemon for OpenStack etc

%prep

%setup -n vcycle

%build

%install
make install
mkdir -p $RPM_BUILD_ROOT/usr/sbin

# we are rpm so we can put files in /usr/sbin etc too
cp -f $RPM_BUILD_ROOT/var/lib/vcycle/bin/vcycle $RPM_BUILD_ROOT/usr/sbin

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
/usr/sbin/vcycle
/var/lib/vcycle/bin
/var/lib/vcycle/doc
/var/lib/vcycle/tmp
/var/lib/vcycle/user_data
/var/lib/vcycle/machines
/var/lib/vcycle/machineoutputs
/etc/rc.d/init.d/vcycled
/etc/logrotate.d/vcycled
