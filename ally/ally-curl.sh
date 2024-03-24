#!/bin/sh

# You'll need two things to run this script:
# - the copy of the JSON Ally loads from bank-statements, which has
#   statement dates and IDs (URL something like
#   https://secure.ally.com/acs/v1/bank-statements?year=2019&docType=STATEMENTS)
# - a curl command with all the requisite cookies embedded, for
#   downloading statements

# To get the JSON, you can find the request, open the response tab, click in
# the JSON and choose "Copy All".

# To get the curl command, find some statement request by loading an Ally
# statement and using the webdev tools.  Use "Copy as curl" to copy the
# request.

# Run this script, with the curl command as args and passing in the contents of
# the bank-statements JSON on stdin

# It'll look something like `./ally-curl.sh curl https://secure.ally.com/... ...`

# Note that copied cURLs expire fairly quickly; if you're doing multiple years,
# you probably want to download the various bank-statements.json *first* and
# then run it in a loop. For that, try:

# for year in 2018 2019 2022 2023 2024; do  < bank-statements.$year.json ./ally-curl.sh curl ....; done

shift 2; # remove curl and the URL

jq '.statements[]|[" ",.uploadDate,.documentId, ""]|join(" ")' | while read quote time id eq; do
    echo time=$time id=$id
    curl -o $time.pdf https://secure.ally.com/acs/v1/bank-statements/$id "$@"
done
