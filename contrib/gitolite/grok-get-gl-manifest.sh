#!/bin/bash
# This is executed by grok-pull if manifest_command is defined.
# You should install the other file as one of your commands in local-code
# and enable it in .gitolite.rc
PRIMARY=$(gitolite mirror list master gitolite-admin)
STATEFILE="$(gitolite query-rc GL_ADMIN_BASE)/.${PRIMARY}.manifest.lastupd"
GL_COMMAND=get-grok-manifest

if [[ -s $STATEFILE ]] && [[ $1 != '--force' ]]; then
    LASTUPD=$(cat $STATEFILE)
fi
NOWSTAMP=$(date +'%s')

ssh $PRIMARY $GL_COMMAND $LASTUPD
ECODE=$?

if [[ $ECODE == 0 ]]; then
    echo $NOWSTAMP > $STATEFILE
fi
exit $ECODE
