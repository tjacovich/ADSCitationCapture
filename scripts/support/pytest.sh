#!/bin/bash

USER=root
GROUP=root

cd /app
if [ -e alembic.ini ]; then
    if [ "${DISABLE_ALEMBIC_UPGRADE}" != True ] && [ "${DISABLE_ALEMBIC_UPGRADE}" != true ]; then
        su -s /bin/bash -c "mkdir -p alembic/versions/" -g ${GROUP} ${USER}
        su -s /bin/bash -c "alembic -c ./alembic.ini upgrade head" -g ${GROUP} ${USER}
    fi
fi

py.test

echo "For interactive access, run in a diferent terminal:"
echo "  docker exec -it pytest_citation_capture_pipeline bash"
echo "Press CTRL+c to stop"
tail -f /dev/null
