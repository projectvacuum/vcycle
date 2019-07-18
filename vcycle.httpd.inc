#
#  Apache httpd.conf include file for Vcycle
#
#  Andrew McNab, University of Manchester.
#  Copyright (c) 2013-9. All rights reserved.
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
#  Contacts: Andrew.McNab@cern.ch  https://www.gridpp.ac.uk/vcycle/

#
#  Apache httpd.conf include file for Vcycle
#
#  This file should be copied to /etc/httpd/includes/vcycle.httpd.inc
#  where it will be loaded by the common Vac Project httpd.conf
#  The hostnames and perhaps portn numbers in this file must be replaced
#  with the correct values for your Vcycle service.
#
#  Place vcycle.cert.pem and vcycle.key.pem in /etc/grid-security
#  and install the CA files in /etc/grid-security/certificates
#
#  If necessary, you can do this with:
#
#  cat >/etc/yum.repos.d/eugridpma.repo <<EOF
#  [eugridpma]
#  name=EUGridPMA
#  baseurl=http://dist.eugridpma.info/distribution/igtf/current/
#  gpgcheck=1
#  gpgkey=https://dist.eugridpma.info/distribution/igtf/current/GPG-KEY-EUGridPMA-RPM-3
#  EOF
#  yum -y install ca_policy_eugridpma
#
#  Things to check if you get no successful requests:
#  - iptables is disabled or allows incoming TCP on ports 80 and 443
#  - SE linux is diabled (echo 0 >/selinux/enforce) or configured to allow
#    httpd to access the paths in this file

<VirtualHost *:80>
ServerName vm21.tier2.hep.manchester.ac.uk
DocumentRoot "/var/lib/vcycle/shared/spaces"

CustomLog logs/http--vcycle.log combined

RedirectMatch ^/machines/([^/]*)/([^/]*)/machinefeatures$ /machines/$1/$2/machinefeatures/
RedirectMatch ^/machines/([^/]*)/([^/]*)/jobfeatures$ /machines/$1/$2/jobfeatures/

AliasMatch    ^/machines/([^/]*)/([^/]*)/machinefeatures/([^/]*) /var/lib/vcycle/shared/spaces/$1/current/$2/machinefeatures/$3
AliasMatch    ^/machines/([^/]*)/([^/]*)/jobfeatures/([^/]*) /var/lib/vcycle/shared/spaces/$1/current/$2/jobfeatures/$3
AliasMatch    ^/heartbeatlists/([^/]*)/([^/]*) /var/lib/vcycle/shared/spaces/$1/heartbeatlists/$2
Alias          /blank404error /dev/null

<DirectoryMatch "^/var/lib/vcycle/shared/spaces/[^/]*/current/[^/]*/machinefeatures/|^/var/lib/vcycle/shared/spaces/[^/]*/current/[^/]*/jobfeatures/|^/var/lib/vcycle/shared/spaces/[^/]*/heartbeatlists/">
 ErrorDocument 404 /blank404error
 Options +Indexes
</DirectoryMatch>
</VirtualHost>

<VirtualHost vm21.tier2.hep.manchester.ac.uk:443>
ServerName vm21.tier2.hep.manchester.ac.uk
DocumentRoot "/var/lib/vcycle/shared/spaces"

SSLEngine               on
SSLCertificateFile      /etc/grid-security/vcycle.cert.pem
SSLCertificateKeyFile   /etc/grid-security/vcycle.key.pem
SSLCACertificatePath    /etc/grid-security/certificates
#SSLCARevocationPath    YOUR CRL DIRECTORY WOULD GO HERE
SSLVerifyClient         optional
SSLVerifyDepth          10
SSLOptions              +StdEnvVars

CustomLog logs/https-vcycle.log combined

ScriptAlias /vcycle-cgi /usr/sbin/vcycle-cgi
Script PUT /vcycle-cgi

RedirectMatch ^/machines/([^/]*)/([^/]*)/machinefeatures$ /machines/$1/$2/machinefeatures/
RedirectMatch ^/machines/([^/]*)/([^/]*)/jobfeatures$ /machines/$1/$2/jobfeatures/

AliasMatch    ^/machines/([^/]*)/([^/]*)/machinefeatures/([^/]*) /var/lib/vcycle/shared/spaces/$1/current/$2/machinefeatures/$3
AliasMatch    ^/machines/([^/]*)/([^/]*)/jobfeatures/([^/]*) /var/lib/vcycle/shared/spaces/$1/current/$2/jobfeatures/$3
AliasMatch    ^/heartbeatlists/([^/]*)/([^/]*) /var/lib/vcycle/shared/spaces/$1/heartbeatlists/$2
Alias          /blank404error /dev/null

<DirectoryMatch "^/var/lib/vcycle/shared/spaces/[^/]*/current/[^/]*/machinefeatures/|^/var/lib/vcycle/shared/spaces/[^/]*/current/[^/]*/jobfeatures/|^/var/lib/vcycle/shared/spaces/[^/]*/heartbeatlists/">
 ErrorDocument 404 /blank404error
 Options +Indexes
</DirectoryMatch>
</VirtualHost>
