import json
import pycurl
import os
import StringIO
from abc import ABCMeta, abstractmethod
from six import add_metaclass

import vcycle.vacutils

# TODO: Figure a way to have this in one place
class OpenstackError(Exception):
  pass

@add_metaclass(ABCMeta)
class GlanceBase(object):
  """ Base class for glance related functions

  v1 and v2 api classes derive from this
  """
  def __init__(self, token, imageURL):
    self.token = token
    self.imageURL = imageURL
    self.curl = pycurl.Curl()

  @abstractmethod
  def uploadImage(self):
    raise NotImplementedError(__name__)

  @abstractmethod
  def getImageDetails(self):
    raise NotImplementedError(__name__)

class GlanceV2(GlanceBase):
  """ Class to interact with Glance v2 API """

  def __init__(self, token, imageURL):
    super(GlanceV2, self).__init__(token, imageURL)
    vcycle.vacutils.logLine('Using Glance v2 api')

  def uploadImage(self, imageFile, imageName, imageLastModified,
                  verbose = False):
    """ Upload an image using Glance v2 API """
    imageID = self._createImage(imageFile, imageName, imageLastModified,
                                verbose)
    self._uploadImageData(imageFile, imageID, verbose)
    vcycle.vacutils.logLine('Uploaded image file to Glance')
    return imageID

  def _createImage(self, imageFile, imageName,
                   imageLastModified, verbose = False):
    """ Request image space """

    # set cert path
    if os.path.isdir('/etc/grid-security/certificates'):
      self.curl.setopt(pycurl.CAPATH, '/etc/grid-security/certificates')

    # Create image
    self.curl.setopt(pycurl.CUSTOMREQUEST, 'POST')
    self.curl.setopt(pycurl.URL, self.imageURL + '/v2/images')
    self.curl.setopt(pycurl.USERAGENT, 'Vcycle ' + vcycle.shared.vcycleVersion)
    self.curl.setopt(pycurl.TIMEOUT, 30)
    self.curl.setopt(pycurl.FOLLOWLOCATION, False)
    self.curl.setopt(pycurl.SSL_VERIFYPEER, 1)
    self.curl.setopt(pycurl.SSL_VERIFYHOST, 2)
    self.curl.setopt(pycurl.HTTPHEADER, ['X-Auth-Token: ' + self.token])

    # data to send
    disk_format = 'iso' if imageName.endswith('.iso') else 'raw'
    jsonRequest = {
        "name": imageName,
        "disk_format": disk_format,
        "container_format": "bare",
        "visibility": "private",
        "tags": [
          "last_modified: " + str(imageLastModified),
          "architecture: x86_64"
          ]
        }
    self.curl.setopt(pycurl.POSTFIELDS, json.dumps(jsonRequest))

    # set verbose option
    if verbose:
      self.curl.setopt(pycurl.VERBOSE, 2)
    else:
      self.curl.setopt(pycurl.VERBOSE, 0)

    # output buffer
    outputBuffer = StringIO.StringIO()
    self.curl.setopt(pycurl.WRITEFUNCTION, outputBuffer.write)

    self.curl.perform()

    if self.curl.getinfo(pycurl.RESPONSE_CODE) / 100 != 2:
      raise Exception('Image upload returns HTTP error code '
          + str(self.curl.getinfo(pycurl.RESPONSE_CODE)))

    try:
      imageID = json.loads(outputBuffer.getvalue())['id']
    except:
      raise Exception('Image upload does not return imageID')

    return imageID

  def _uploadImageData(self, imageFile, imageID, verbose):
    """ Upload image data """
    # Upload data
    imageFileURL = self.imageURL + '/v2/images/' + imageID + '/file'
    self.curl.setopt(pycurl.URL, str(imageFileURL))

    try:
      f = open(str(imageFile), 'r')
    except Exception as e:
      raise OpenstackError('Failed to open file ' + imageFile + '(' + str(e)
                            + ')')
    self.curl.setopt(pycurl.UPLOAD, True)
    self.curl.setopt(pycurl.READFUNCTION, f.read)

    self.curl.setopt(pycurl.USERAGENT, 'Vcycle ' + vcycle.shared.vcycleVersion)
    self.curl.setopt(pycurl.TIMEOUT, 30)
    self.curl.setopt(pycurl.FOLLOWLOCATION, False)
    self.curl.setopt(pycurl.SSL_VERIFYPEER, 1)
    self.curl.setopt(pycurl.SSL_VERIFYHOST, 2)
    self.curl.setopt(pycurl.CUSTOMREQUEST, 'PUT')
    self.curl.setopt(pycurl.HTTPHEADER, [
      'X-Auth-Token: ' + self.token,
      'Content-Type: application/octet-stream'])

    outputBuffer = StringIO.StringIO()
    self.curl.setopt(pycurl.WRITEFUNCTION, outputBuffer.write)

    try:
      self.curl.perform()
    except Exception as e:
      raise OpenstackError('Failed uploading image (' + str(e) + ')')

    if self.curl.getinfo(pycurl.RESPONSE_CODE) / 100 != 2:
      raise OpenstackError('Image upload returns HTTP error code '
          + str(self.curl.getinfo(pycurl.RESPONSE_CODE)))

    return imageID

  def getImageDetails(self):
    """ Get the existing images details """
    self.curl.setopt(pycurl.URL, self.imageURL + '/v2/images')
    self.curl.setopt(pycurl.USERAGENT, 'Vcycle ' + vcycle.shared.vcycleVersion)
    self.curl.setopt(pycurl.TIMEOUT, 30)
    self.curl.setopt(pycurl.FOLLOWLOCATION, False)
    self.curl.setopt(pycurl.SSL_VERIFYPEER, 1)
    self.curl.setopt(pycurl.SSL_VERIFYHOST, 2)
    self.curl.setopt(pycurl.CUSTOMREQUEST, 'GET')

    self.curl.setopt(pycurl.HTTPHEADER, ['X-Auth-Token: ' + self.token])

    outputBuffer = StringIO.StringIO()
    self.curl.setopt(pycurl.WRITEFUNCTION, outputBuffer.write)

    headersBuffer = StringIO.StringIO()
    self.curl.setopt(pycurl.HEADERFUNCTION, headersBuffer.write)

    try:
      self.curl.perform()
    except Exception as e:
      raise OpenstackError('Failed to get image details (' + str(e) + ')')

    response = json.loads(outputBuffer.getvalue())

    return {
        'response' : response,
        'status' : self.curl.getinfo(pycurl.RESPONSE_CODE)
        }

  def updateShutdownTimeout(self, osShutdownTimeout):
    """ Update os_shutdown_timeout property for images """
    response = self.getImageDetails()['response']
    for image in response['images']:
      if (osShutdownTimeout is not None and
          image['os_shutdown_timeout'] is None):
        self._addOsShutdownTimeout(image['id'], osShutdownTimeout)
      elif (osShutdownTimeout is None and
          image['os_shutdown_timeout'] is not None):
        self._delOsShutdownTimeout(image['id'])
      elif (osShutdownTimeout is not None and
          image['os_shutdown_timeout'] is not None and
          osShutdownTimeout != image['os_shutdown_timeout']):
        self._repOsShutdownTimeout(image['id'], osShutdownTimeout)

  def _addOsShutdownTimeout(self, imageID, osShutdownTimeout):
    self._patchOsShutdownTimeout(imageID, 'add', osShutdownTimeout)

  def _delOsShutdownTimeout(self, imageID):
    self._patchOsShutdownTimeout(imageID, 'remove')

  def _repOsShutdownTimeout(self, imageID, osShutdownTimeout):
    self._patchOsShutdownTimeout(imageID, 'replace', osShutdownTimeout)

  def _patchOsShutdownTimeout(self, imageID, op, value = None):
    """ patch image value """

    # set cert path
    if os.path.isdir('/etc/grid-security/certificates'):
      self.curl.setopt(pycurl.CAPATH, '/etc/grid-security/certificates')

    vcycle.vacutils.logLine('{} {} {}'.format(imageID, op, value))
    self.curl.setopt(pycurl.CUSTOMREQUEST, 'PATCH')
    self.curl.setopt(pycurl.URL, str(self.imageURL + '/v2/images/' + imageID))
    self.curl.setopt(pycurl.USERAGENT, 'Vcycle ' + vcycle.shared.vcycleVersion)
    self.curl.setopt(pycurl.TIMEOUT, 30)
    self.curl.setopt(pycurl.FOLLOWLOCATION, False)
    self.curl.setopt(pycurl.SSL_VERIFYPEER, 1)
    self.curl.setopt(pycurl.SSL_VERIFYHOST, 2)

    self.curl.setopt(pycurl.HTTPHEADER, [
      'X-Auth-Token: ' + self.token,
      'Content-Type: application/openstack-images-v2.1-json-patch'])

    jsonRequest = {
        "op": op,
        "path": "/os_shutdown_timeout"
        }
    if op in ['add', 'replace'] and value is not None:
      jsonRequest["value"] = str(value)
    self.curl.setopt(pycurl.POSTFIELDS, "[" + json.dumps(jsonRequest) + "]")

    outputBuffer = StringIO.StringIO()
    self.curl.setopt(pycurl.WRITEFUNCTION, outputBuffer.write)

    try:
      self.curl.perform()
    except Exception as e:
      vcycle.vacutils.logLine('Failed to get replace image porperties ('
          + str(e) + ')')

class GlanceV1(GlanceBase):
  """ Class to interact with Glance v1 API """

  def __init__(self, token, imageURL):
    super(GlanceV1, self).__init__(token, imageURL)
    vcycle.vacutils.logLine('Using Glance v1 api')

  def uploadImage(self, imageFile, imageName, imageLastModified,
                  verbose = False):
    """ Upload an image using Glance v1 API """
    try:
      f = open(imageFile, 'r')
    except Exception as e:
      raise OpenstackError('Failed to open image file ' + imageName + ' (' + str(e) + ')')

    self.curl.setopt(pycurl.READFUNCTION, f.read)
    self.curl.setopt(pycurl.UPLOAD, True)
    self.curl.setopt(pycurl.CUSTOMREQUEST, 'POST')
    self.curl.setopt(pycurl.URL, self.imageURL + '/v1/images')
    self.curl.setopt(pycurl.USERAGENT, 'Vcycle ' + vcycle.shared.vcycleVersion)
    self.curl.setopt(pycurl.TIMEOUT, 30)
    self.curl.setopt(pycurl.FOLLOWLOCATION, False)
    self.curl.setopt(pycurl.SSL_VERIFYPEER, 1)
    self.curl.setopt(pycurl.SSL_VERIFYHOST, 2)

    self.curl.setopt(pycurl.HTTPHEADER,
        [ 'x-image-meta-disk_format: ' + ('iso' if imageName.endswith('.iso') else 'raw'),
          # ^^^ 'raw' for hdd; 'iso' for iso
          'Content-Type: application/octet-stream',
          'Accept: application/json',
          'Transfer-Encoding: chunked',
          'x-image-meta-container_format: bare',
          'x-image-meta-is_public: False',
          'x-image-meta-name: ' + imageName,
          'x-image-meta-property-architecture: x86_64',
          'x-image-meta-property-last-modified: ' + str(imageLastModified),
          'X-Auth-Token: ' + self.token
          ])

    outputBuffer = StringIO.StringIO()
    self.curl.setopt(pycurl.WRITEFUNCTION, outputBuffer.write)

    if verbose:
      self.curl.setopt(pycurl.VERBOSE, 2)
    else:
      self.curl.setopt(pycurl.VERBOSE, 0)

    if os.path.isdir('/etc/grid-security/certificates'):
      self.curl.setopt(pycurl.CAPATH, '/etc/grid-security/certificates')

    try:
      self.curl.perform()
    except Exception as e:
      raise OpenstackError('Failed uploading image (' + str(e) + ')')

    # Any 2xx code is OK; otherwise raise an exception
    if self.curl.getinfo(pycurl.RESPONSE_CODE) / 100 != 2:
      raise OpenstackError('Image upload returns HTTP error code ' + str(self.curl.getinfo(pycurl.RESPONSE_CODE)))

    try:
      response = json.loads(outputBuffer.getvalue())
    except Exception as e:
      raise OpenstackError('JSON decoding of HTTP(S) response fails (' + str(e) + ')')

    try:
      vcycle.vacutils.logLine('Uploaded new image ' + imageName + ' with ID ' + str(response['image']['id']))
      return str(response['image']['id'])
    except:
      raise OpenstackError('Failed to upload image file for ' + imageName + ' (' + str(e) + ')')

  def getImageDetails(self):
    """ Get image details using glance v1 API """

    self.curl.setopt(pycurl.URL, self.imageURL + '/v1/images')
    self.curl.setopt(pycurl.USERAGENT, 'Vcycle ' + vcycle.shared.vcycleVersion)
    self.curl.setopt(pycurl.TIMEOUT, 30)
    self.curl.setopt(pycurl.FOLLOWLOCATION, False)
    self.curl.setopt(pycurl.SSL_VERIFYPEER, 1)
    self.curl.setopt(pycurl.SSL_VERIFYHOST, 2)
    self.curl.setopt(pycurl.CUSTOMREQUEST, 'GET')

    self.curl.setopt(pycurl.HTTPHEADER, ['X-Auth-Token: ' + self.token])

    outputBuffer = StringIO.StringIO()
    self.curl.setopt(pycurl.WRITEFUNCTION, outputBuffer.write)

    headersBuffer = StringIO.StringIO()
    self.curl.setopt(pycurl.HEADERFUNCTION, headersBuffer.write)

    try:
      self.curl.perform()
    except Exception as e:
      raise OpenstackError('Failed to get image details (' + str(e) + ')')

    response = json.loads(outputBuffer.getvalue())

    return {
        'response' : response,
        'status' : self.curl.getinfo(pycurl.RESPONSE_CODE)
        }
