
from islandoraUtils.fedoraLib import update_datastream
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
        self.requests_session = requests.Session()

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
           r = self.requests_session.post(self.islandora_create_access_endpoint, data=data)
           if r.status_code == requests.codes.forbidden:
               # first attempt might fail due to an expired session... Let's try to authenticate, and try again.
               r = self.requests_session.post(self.islandora_url + '/user/login', data={
                 'name': self.islandora_username,
                 'pass': self.islandora_password,
                 'form_id': 'user_login',
               }, headers={'content-type': 'application/x-www-form-urlencoded'})

               r = self.requests_session.post(self.islandora_create_access_endpoint, data=data)

           update_datastream(obj, 'TN', data['thumbnail_path'], label='Thumbnail', mimeType='image/png')

           os.remove(data['thumbnail_path'])

           if r.status_code == requests.codes.created:
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
        #   For mezzanines that are not 1920 pixels wide deinterlace if necessary and scale
        if info.video.video_width != 1920:
            conv = self.f.convert(filename, output_name, opts=[
                '-i', self.bug_path,
                '-filter_complex', 'yadif,scale=-2:360,overlay=10:main_h-overlay_h-10',
                '-c:v', 'libx264', '-pix_fmt', 'yuv420p', '-b:v', '786k',
                '-c:a', 'libvo_aacenc',
                '-movflags', 'faststart'
                ],timeout=False)
        #  For mezzanines that are 1920 pixels wide, crop to remove pillar box and scale
        else:
            conv = self.f.convert(filename, output_name, opts=[
                '-i', self.bug_path,
                '-filter_complex', 'crop=1440:in_h:240:0,scale=-2:360,overlay=10:main_h-overlay_h-10',
                '-c:v', 'libx264', '-pix_fmt', 'yuv420p', '-b:v', '786k',
                '-c:a', 'libvo_aacenc',
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
       #    Again test for width
       if info.video.video_width != 1920:
            thumb = self.f.convert(filename, output_name, opts=[
                '-vf', 'yadif,scale=480:360,thumbnail=80','-frames:v', '1'
            ])
       else:
            thumb = self.f.convert(filename, output_name, opts=[
                '-vf', 'crop=1440:in_h:240:0,scale=480:360,thumbnail=80',
                '-frames:v', '1'
            ])

       for timecode in thumb:
           pass

       return output_name
