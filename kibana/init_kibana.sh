#!/bin/sh
set -e

echo "🔥 INIT KIBANA START"

echo "📁 Contenuto root:"
ls -l /

echo "📁 Contenuto /:"
ls -l /dashboards.ndjson || true

echo "⏳ Attendo Kibana API..."
until curl -s http://kibana:5601/api/status | grep -q '"overall":{"level":"available"'; do
  echo "💤 Kibana non pronta..."
  sleep 5
done

echo "✅ Kibana pronta"

echo "📊 Import dashboards..."
curl -f -X POST "http://kibana:5601/api/saved_objects/_import?overwrite=true" \
  -H "kbn-xsrf: true" \
  -H "Content-Type: multipart/form-data" \
  -F "file=@/dashboards.ndjson"

echo "✅ DASHBOARD IMPORTATA"
echo "🏁 INIT KIBANA END"
