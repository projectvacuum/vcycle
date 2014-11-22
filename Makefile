#
#  Andrew McNab, University of Manchester.
#  Copyright (c) 2013-4. All rights reserved.
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
#  Contacts: Andrew.McNab@cern.ch  http://www.gridpp.ac.uk/vac/
#

include VERSION

INSTALL_FILES=vcycled vcycle VCYCLE.py vcycle-cgi vcycled.init \
          vcycled.logrotate VERSION CHANGES vcycle-httpd.conf
          
TGZ_FILES=$(INSTALL_FILES) Makefile vcycle.spec

GNUTAR ?= tar
vcycle.tgz: $(TGZ_FILES)
	mkdir -p TEMPDIR/vcycle
	cp $(TGZ_FILES) TEMPDIR/vcycle
	cd TEMPDIR ; $(GNUTAR) zcvf ../vcycle.tgz --owner=root --group=root vcycle
	rm -R TEMPDIR

install: $(INSTALL_FILES)
	mkdir -p $(RPM_BUILD_ROOT)/var/lib/vcycle/bin \
	         $(RPM_BUILD_ROOT)/var/lib/vcycle/doc \
	         $(RPM_BUILD_ROOT)/var/lib/vcycle/tmp \
	         $(RPM_BUILD_ROOT)/var/lib/vcycle/vmtypes \
	         $(RPM_BUILD_ROOT)/var/lib/vcycle/machineoutputs \
	         $(RPM_BUILD_ROOT)/var/lib/vcycle/machines \
	         $(RPM_BUILD_ROOT)/etc/rc.d/init.d \
	         $(RPM_BUILD_ROOT)/etc/logrotate.d
	cp vcycled vcycle VCYCLE.py vcycle-cgi \
	   $(RPM_BUILD_ROOT)/var/lib/vcycle/bin
	cp VERSION CHANGES vcycle-httpd.conf \
	   $(RPM_BUILD_ROOT)/var/lib/vcycle/doc
	cp vcycled.init \
	   $(RPM_BUILD_ROOT)/etc/rc.d/init.d/vcycled
	cp vcycled.logrotate \
	   $(RPM_BUILD_ROOT)/etc/logrotate.d/vcycled
	
rpm: vcycle.tgz
	rm -Rf RPMTMP
	mkdir -p RPMTMP/SOURCES RPMTMP/SPECS RPMTMP/BUILD \
         RPMTMP/SRPMS RPMTMP/RPMS/noarch RPMTMP/BUILDROOT
	cp -f vcycle.tgz RPMTMP/SOURCES
	export VCYCLE_VERSION=$(VERSION) ; rpmbuild -ba \
	  --define "_topdir $(shell pwd)/RPMTMP" \
	  --buildroot $(shell pwd)/RPMTMP/BUILDROOT vcycle.spec
