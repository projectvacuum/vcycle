#
#  Andrew McNab, University of Manchester.
#  Copyright (c) 2013-6. All rights reserved.
#
#  Redistribution and use in source and binary forms, with or
#  without modification, are permitted provided that the following
#  conditions are met:
#
#    o Redistributions of source code must retain the above
#      copyright notice, this list of conditions and the following
#      disclaimer.
#    o Redistributions in binary form must reproduce the above
#      copyright notice, this list of conditions and the following
#      disclaimer in the documentation and/or other materials
#      provided with the distribution.
#
#  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
#  CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
#  INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
#  MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
#  DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS
#  BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
#  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
#  TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
#  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
#  ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
#  OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
#  OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
#  POSSIBILITY OF SUCH DAMAGE.
#
#  Contacts: Andrew.McNab@cern.ch  http://www.gridpp.ac.uk/vcycle/
#

include VERSION

OPENSTACK_PLUGINS := $(wildcard plugins/openstack/*.py) 

PLUGIN_FILES:= $(wildcard plugins/*.py) $(OPENSTACK_PLUGINS)

CORE_FILES := $(wildcard core/*.py)

INSTALL_FILES = $(PLUGIN_FILES) $(CORE_FILES) \
                vcycled __init__.py example.vcycle.conf vcycle-cgi \
                vcycle.httpd.conf vcycle.httpd.inc vcycled.init \
                vcycled.logrotate admin-guide.html VERSION CHANGES \
                vcycle.conf.5 vcycled.8

TGZ_FILES=$(INSTALL_FILES) Makefile vcycle.spec

GNUTAR ?= tar

PYTHONDIR := $(shell python -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")

vcycle.tgz: $(TGZ_FILES)
	mkdir -p TEMPDIR/vcycle TEMPDIR/vcycle/plugins \
                TEMPDIR/vcycle/plugins/openstack TEMPDIR/vcycle/core
	for i in $(TGZ_FILES) ; do cp $$i TEMPDIR/vcycle/$$i ; done
	cd TEMPDIR ; $(GNUTAR) zcvf ../vcycle.tgz --owner=root --group=root vcycle
	rm -R TEMPDIR

install: $(INSTALL_FILES)
	mkdir -p $(RPM_BUILD_ROOT)/usr/sbin \
	         $(RPM_BUILD_ROOT)$(PYTHONDIR)/vcycle \
	         $(RPM_BUILD_ROOT)$(PYTHONDIR)/vcycle/core \
	         $(RPM_BUILD_ROOT)$(PYTHONDIR)/vcycle/plugins \
	         $(RPM_BUILD_ROOT)$(PYTHONDIR)/vcycle/plugins/openstack \
 	         $(RPM_BUILD_ROOT)/usr/share/doc/vcycle-$(VERSION) \
 	         $(RPM_BUILD_ROOT)/usr/share/man/man5 \
                 $(RPM_BUILD_ROOT)/usr/share/man/man8 \
 	         $(RPM_BUILD_ROOT)/var/lib/vcycle/tmp \
 	         $(RPM_BUILD_ROOT)/var/lib/vcycle/imagecache \
	         $(RPM_BUILD_ROOT)/var/lib/vcycle/apel-archive \
	         $(RPM_BUILD_ROOT)/var/lib/vcycle/apel-outgoing \
	         $(RPM_BUILD_ROOT)/var/lib/vcycle/spaces/vcycle01.example.com/example/files \
	         $(RPM_BUILD_ROOT)/var/lib/vcycle/www \
	         $(RPM_BUILD_ROOT)/var/lib/vcycle/joboutputs \
	         $(RPM_BUILD_ROOT)/var/lib/vcycle/machines \
	         $(RPM_BUILD_ROOT)/etc/rc.d/init.d \
	         $(RPM_BUILD_ROOT)/etc/logrotate.d \
	         $(RPM_BUILD_ROOT)/etc/vcycle.d
	cp vcycled vcycle-cgi \
	   $(RPM_BUILD_ROOT)/usr/sbin
	cp $(CORE_FILES) $(RPM_BUILD_ROOT)$(PYTHONDIR)/vcycle/core
	cp __init__.py $(RPM_BUILD_ROOT)$(PYTHONDIR)/vcycle
	echo $(OPENSTACK_PLUGINS)
	for i in $(PLUGIN_FILES) ; do cp $$i $(RPM_BUILD_ROOT)$(PYTHONDIR)/vcycle/$$i ; done
	cp VERSION CHANGES vcycle.httpd.conf vcycle.httpd.inc \
	   example.vcycle.conf vcycle.conf.5 vcycled.8 \
	   admin-guide.html \
	   $(RPM_BUILD_ROOT)/usr/share/doc/vcycle-$(VERSION)
	cp VERSION \
	   $(RPM_BUILD_ROOT)/var/lib/vcycle
	cp vcycled.init \
	   $(RPM_BUILD_ROOT)/etc/rc.d/init.d/vcycled
	cp vcycled.logrotate \
	   $(RPM_BUILD_ROOT)/etc/logrotate.d/vcycled
	cp vcycle.conf.5 \
	   $(RPM_BUILD_ROOT)/usr/share/man/man5
	cp vcycled.8 \
	   $(RPM_BUILD_ROOT)/usr/share/man/man8

rpm: vcycle.tgz
	rm -Rf RPMTMP
	mkdir -p RPMTMP/SOURCES RPMTMP/SPECS RPMTMP/BUILD \
         RPMTMP/SRPMS RPMTMP/RPMS/noarch RPMTMP/BUILDROOT
	cp -f vcycle.tgz RPMTMP/SOURCES
	export VCYCLE_VERSION=$(VERSION) ; rpmbuild -ba \
	  --define "_topdir $(shell pwd)/RPMTMP" \
	  --buildroot $(shell pwd)/RPMTMP/BUILDROOT vcycle.spec

