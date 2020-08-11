#!/usr/bin/env python3
# Implements a Google pubsub v1 push listener, see:
# https://cloud.google.com/pubsub/docs/push
#
# In order to work, grok-pull must be running as a daemon service with
# the "socket" option enabled in the configuration.
#
# The pubsub message should contain two attributes:
# {
#   "message": {
#     "attributes": {
#       "proj": "projname",
#       "repo": "/path/to/repo.git"
#     }
#   }
# }
#
# "proj" value should map to a "$proj.conf" file in /etc/grokmirror
#        (you can override that default via the GROKMIRROR_CONFIG_DIR env var).
# "repo" value should match a repo defined in the manifest file as understood
#        by the running grok-pull daemon (it will ignore anything else)
#
# Any other attributes or the "data" field are ignored.

import falcon
import json
import os
import socket
import re

from configparser import ConfigParser, ExtendedInterpolation

# Some sanity defaults
MAX_PROJ_LEN = 32
MAX_REPO_LEN = 1024

# noinspection PyBroadException
class PubsubListener(object):

    def on_get(self, req, resp):
        resp.status = falcon.HTTP_200
        resp.body = "We don't serve GETs here\n"

    def on_post(self, req, resp):
        if not req.content_length:
            resp.status = falcon.HTTP_500
            resp.body = 'Payload required\n'
            return

        try:
            doc = json.load(req.stream)
        except:
            resp.status = falcon.HTTP_500
            resp.body = 'Failed to parse payload as json\n'
            return
         
        try:
            proj = doc['message']['attributes']['proj']
            repo = doc['message']['attributes']['repo']
        except (KeyError, TypeError):
            resp.status = falcon.HTTP_500
            resp.body = 'Not a pubsub v1 payload\n'
            return

        if len(proj) > MAX_PROJ_LEN or len(repo) > MAX_REPO_LEN:
            resp.status = falcon.HTTP_500
            resp.body = 'Repo or project value too long\n'
            return

        # Proj shouldn't contain slashes or whitespace
        if re.search(r'[\s/]', proj):
            resp.status = falcon.HTTP_500
            resp.body = 'Invalid characters in project name\n'
            return

        # Repo shouldn't contain whitespace
        if re.search(r'\s', proj):
            resp.status = falcon.HTTP_500
            resp.body = 'Invalid characters in repo name\n'
            return

        confdir = os.environ.get('GROKMIRROR_CONFIG_DIR', '/etc/grokmirror')
        cfgfile = os.path.join(confdir, '{}.conf'.format(proj))
        if not os.access(cfgfile, os.R_OK):
            resp.status = falcon.HTTP_500
            resp.body = 'Invalid project name\n'
            return
        config = ConfigParser(interpolation=ExtendedInterpolation())
        config.read(cfgfile)
        if 'pull' not in config or not config['pull'].get('socket'):
            resp.status = falcon.HTTP_500
            resp.body = 'Invalid project configuration (no socket defined)\n'
            return
        sockfile = config['pull'].get('socket')
        if not os.access(sockfile, os.W_OK):
            resp.status = falcon.HTTP_500
            resp.body = 'Invalid project configuration (socket does not exist or is not writable)\n'
            return

        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                client.connect(sockfile)
                client.send(repo.encode())
        except:
            resp.status = falcon.HTTP_500
            resp.body = 'Unable to communicate with the socket\n'
            return

        resp.status = falcon.HTTP_204


app = falcon.API()
pl = PubsubListener()
app.add_route('/pubsub_v1', pl)
