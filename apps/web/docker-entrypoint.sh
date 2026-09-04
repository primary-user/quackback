#!/bin/sh
set -e

echo "========================================"
echo "  Quackback starting..."
echo "========================================"

# Migrations: skipped in K8s where a pre-upgrade Helm hook Job runs them
# before pods roll. Set SKIP_MIGRATIONS=true to opt out of the on-start
# migration step. Default behavior matches `docker run` ergonomics.
if [ "$SKIP_MIGRATIONS" = "true" ]; then
  echo ""
  echo "SKIP_MIGRATIONS=true — skipping startup migration (handled out-of-band)"
else
  echo ""
  echo "Running database migrations..."
  bun /app/migrate.mjs
  echo "Migrations complete."
fi

# Optionally seed the database
if [ "$SEED_DATABASE" = "true" ]; then
  echo ""
  echo "Seeding database..."
  bun /app/seed.mjs
  echo "Seeding complete."
fi

# Start the application
echo ""
echo "Starting Quackback server on port ${PORT:-3000}..."
echo "========================================"
# --smol trades a little throughput for a much smaller resident heap: Bun runs
# the GC more eagerly and grows the heap in smaller steps instead of holding
# whatever high-water mark it once reached.
#
# That trade is correct for a self-hosted comment server. The workload is a
# handful of requests an hour, so the throughput given up is invisible, while
# the memory held is billed by the gigabyte-month around the clock whether or
# not anybody is reading.
#
# Set BUN_SMOL=false to opt out on an instance with real traffic.
if [ "$BUN_SMOL" = "false" ]; then
  exec bun .output/server/index.mjs
else
  exec bun --smol .output/server/index.mjs
fi
