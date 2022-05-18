#!/bin/bash
rm -rf dbcompare_pkg
mkdir dbcompare_pkg
cp -r README.md target/lib etc/dbcompare.conf scripts/dbcompare.sh target/dbcompare-1.0-SNAPSHOT.jar dbcompare_pkg/
mv dbcompare_pkg/dbcompare-1.0-SNAPSHOT.jar dbcompare_pkg/dbcompare-1.0.jar