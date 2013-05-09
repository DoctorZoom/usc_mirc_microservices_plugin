from plugin_manager import IslandoraListenerPlugin
import ConfigParser

from converter import FFMpeg
import os
from lxml import etree
import requests

class usc_mirc_microservices_plugin(IslandoraListenerPlugin):
    def initialize(self, config_parser):
        IslandoraListenerPlugin.initialize(self, config_parser)

        try:
          self.islandora_url = config_parser.get('MIRC', 'url')
          self.islandora_create_access_endpoint = self.islandora_url + '/' + config_parser.get('MIRC', 'create_access_endpoint')
          self.islandora_username = config_parser.get('MIRC', 'username')
          self.islandora_password = config_parser.get('MIRC', 'password')
          self.stream_output_path = config_parser.get('MIRC', 'output_path')
          if not os.path.isdir(self.stream_output_path):
              self.logger.error('Transcode output path does not exist, or is not a directory!')
              return False
          bug_name = config_parser.get('MIRC', 'bug_name')
          self.bug_path = os.path.realpath(os.path.join(os.path.dirname(__file__), bug_name))
          if not os.path.isfile(self.bug_path):
              self.logger.error('File at bug path does not exist!')
              return False
        except ConfigParser.Error:
          self.logger.exception('Failed to read values from config.')
          return False

        self.f = FFMpeg()

        return True

    def fedoraMessage(self, message, obj, client):
        if 'usc:mezzanineCModel' in message['content_models'] and message['method'] == 'ingest' and 'PBCORE' in obj:
           data = {
             'parent': obj.pid
           }
           # Get the mezz path from the PBCore.
           # /pb:pbcoreInstantiationDocument/pb:instantiationIdentifier[@source="filename"]
           pbcore = etree.fromstring(obj['PBCORE'].getContent().read())
           path = pbcore.xpath('/pb:pbcoreInstantiationDocument/pb:instantiationIdentifier[@source="filename"]', namespaces={
             'pb': 'http://www.pbcore.org/PBCore/PBCoreNamespace.html'
           })
           if len(path) > 0:
               path = path[0].text
           else:
               self.logger.warning('Missing path in PBCore.')
           # Throw the mezz path at the access copy function, and create a child as an access copy.
           data['video_path'] = self.produceVideoAccessCopy(path)
           # Throw the mezz path at the thumbnail function, and store the thumbnail somewhere.
           data['thumbnail_path'] = self.produceThumbnail(path)
           # Throw the paths to the access copy and thumbnail at Islandora.
           if not self.requests_session:
             self.requests_session = requests.Session()

           r = self.requests_session.post(self.islandora_create_access_endpoint, data=data)
           if r.code == requests.codes.unauthorized:
               # first attempt failed due to missing creds... Let's try to authenticate, and try again.
               r = self.requests_session.post(self.islandora_url + '/user/login', data={
                 'username': self.islandora_username,
                 'password': self.islandora_password,
                 'form_id': 'user_login'
               }, headers={'content-type': 'application/x-www-form-urlencoded'})

               r = self.requests_session.post(self.islandora_create_access_endpoint, data=data)

           os.remove(data['thumbnail_path'])

           if r.code == requests.codes.created:
               self.logger.info('Islandora created new access variant.')
           else:
               self.logger.warning('Islandora failed to create the new access variant.')
    '''
    Create an access copy with FFMpeg, and return the path to where it is.

    @return
      A string, indicating where the access copy was created.
    '''
    def produceVideoAccessCopy(self, filename):
        basename = os.path.basename(filename)
        base, ext = os.path.splitext(basename)
        output_name = os.path.join(self.stream_output_path, base + '_Acc.m4v')
        info = self.f.probe(filename)
        conv = self.f.convert(filename, output_name, opts=[
            '-i', self.bug_path,
            '-filter_complex' , '[0:v]yadif[0:-1];[1:v]scale=width=%s:height=%s[1:-1];[0:-1][1:-1]overlay[out]' % (info.video.width, info.video.height),
            '-map','[out]',
            '-map', '0:a:0',
            '-c:v', 'libx264',
            '-pix_fmt','yuv420p',
            '-x264opts', 'bitrate=800',
            '-s','480x360' ,
            '-strict','-2','-c:a','aac',
            '-movflags', 'faststart'
        ], timeout=False)

        for timecode in conv:
            pass

        return output_name

    '''
    Create a thumbnail with FFMpeg.

    @return
      A file object
    '''
    def produceThumbnail(self, filename):
        basename = os.path.basename(filename)
        base, ext = os.path.splitext(basename)
        output_name = os.path.join(self.stream_output_path, base + '_thumbnail.png')

        self.f.thumbnail(filename, 20, output_name, '350x260')

        return output_name
