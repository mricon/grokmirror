#!/bin/bash
# This is executed by grok-pull if manifest_command is defined.
# You should install the other file as one of your commands in local-code
# and enable it in .gitolite.rc
PRIMARY=$(gitolite mirror list master gitolite-admin)
STATEFILE="$(gitolite query-rc GL_ADMIN_BASE)/.${PRIMARY}.manifest.lastupd"
GL_COMMAND=get-grok-manifest

if [[ -s $STATEFILE ]]; then
    LASTUPD=$(cat $STATEFILE)
fi
ssh $PRIMARY $GL_COMMAND $LASTUPD
ECODE=$?

if [[ $ECODE == 0 ]]; then
    date +'%s' > $STATEFILE
fi
exit $ECODE
