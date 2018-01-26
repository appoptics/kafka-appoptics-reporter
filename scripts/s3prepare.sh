#!/bin/bash

set -e

# This script creates a deploy/ directory under which subdirectories
# mapped to our s3 librato-jar layout are also created.

# Figure out which branch this is on by finding where the commit
# is contained.
GIT_BRANCH=`git branch -r --contains HEAD | sed -e 's@^[ ]*origin/@@g' -e 's@/@-@g' | tail -n 1`
# Handle tagged checkouts
if [ -z "$GIT_BRANCH" ]; then
	GIT_BRANCH="tags"
fi

if [ -z "$PROJECT_NAME" ]; then
    echo "PROJECT_NAME must be set"
    exit 1
fi

GIT_VERSION=`git describe`

for FILE in `ls target/$PROJECT_NAME*.jar`; do
	BASE=`basename $FILE`
	DEPLOY_DIR="deploy/$PROJECT_NAME/$GIT_BRANCH/$GIT_VERSION/"
	mkdir -p $DEPLOY_DIR
	CP_CMD="cp $FILE $DEPLOY_DIR$BASE"
	echo $CP_CMD
	$CP_CMD
	if [ $? -ne 0 ]; then
		exit 1
	fi
done

exit 0
