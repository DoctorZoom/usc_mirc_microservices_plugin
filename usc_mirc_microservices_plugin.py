from plugin_manager import IslandoraListenerPlugin
import ConfigParser

from converter import FFMpeg
import os
from lxml import etree
import requests

class usc_mirc_microservices_plugin(IslandoraListenerPlugin):
    def initialize(self, config_parser):
        super(IslandoraListenerPlugin, self).initialize(config_parser)

        try:
          self.islandora_url = config_parser.get('Islandora', 'url')
          self.islandora_create_access_endpoint = self.islandora_url + '/' + config_parser.get('Islandora', 'create_access_endpoint')
          self.islandora_username = config_parser.get('Islandora', 'username')
          self.islandora_password = config_parser.get('Islandora', 'password')
          self.stream_url_base = config_parser.get('Custom', 'streaming_url_base')
          self.stream_output_path = config_parser.get('Custom', 'output_path')
          bug_name = config_parser.get('Custom', 'bug_name')
          self.bug_path = os.path.realpath(bug_name)
          self.mezzanine_content_model
        except ConfigParser.Error:
          self.logger.exception('Failed to read values from config.')
          return False

        self.f = FFMpeg()

        return True

    def fedoraMessage(self, message, obj, client):
        if 'usc:mezzanineCModel' in message['content_models'] and message['method'] == 'ingest' and 'PBCORE' in object:
           data = {
             'parent': object.pid
           }
           # Get the mezz path from the PBCore.
           # /pb:pbcoreInstantiationDocument/pb:instantiationIdentifier[@source="filename"]
           path = 'asdf'
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
        conv = self.f.convert(filename, output_name, [
            '-i', self.bug_path,
            '-filter_complex' , '[0:v]yadif[0:-1];[0:-1][1:v]overlay[out]',
            '-map','[out]',
            '-map', '0:a:0',
            '-c:v', 'libx264',
            '-pix_fmt','yuv420p',
            '-x264opts', 'bitrate=800',
            '-s','480x360' ,
            '-strict','-2','-c:a','aac'
        ])

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
