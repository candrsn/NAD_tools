#!/bin/bash


export TMPDIR="$HOME/tmp"

compare() {
    echo "

    ATTACH DATABASE 'nad_r2.db' as r2;
    ATTACH DATABASE 'nad_r3.db' as r3;
    ATTACH DATABASE 'nad_r4.db' as r4;
    ATTACH DATABASE 'nad_r5.db' as r5;
    ATTACH DATABASE 'nad_r6.db' as r6;

.mode column
SELECT rr6.state, 
     rr6.recs AS r6_recs, rr5.recs as r5_recs, rr4.recs as r4_recs, rr3.recs as r3_recs, rr2.recs as r2_recs,
    rr6.last_update, rr5.last_update, rr4.last_update, rr3.last_update, rr2.last_update
FROM
r6.nad_stats as rr6 LEFT OUTER JOIN
r5.nad_stats as rr5 ON (rr5.state = rr6.state) LEFT OUTER JOIN
r4.nad_stats as rr4 ON (rr4.state = rr6.state) LEFT OUTER JOIN
r3.nad_stats as rr3 ON (rr3.state = rr6.state) LEFT OUTER JOIN
r2.nad_stats as rr2 ON (rr2.state = rr6.state)

;

    " | sqlite3 
}

build_rel_stats() {
    r="$1"
    echo "
-- DROP TABLE IF EXISTS nad_stats;
CREATE TABLE IF NOT EXISTS nad_stats (recs INTEGER, state CHAR(2), last_update TEXT);
INSERT INTO nad_stats (recs, state, last_update)
    SELECT count(*) as cnt, state, max(lastupdate) as last_update from nad_data GROUP BY 2;

SELECT COUNT(*) FROM nad_stats;
    " | sqlite3 nad_r${r}.db
}

build_stats() {
    for itm in 3 4 5 6; do
        echo "build stats for rel $r"
        build_rel_stats $itm
    done
}

build_rel_geo() {
    r="$1"
    echo "
.load mod_spatialite
SELECT initspatialmetadata('WGS-84');

CREATE TABLE IF NOT EXISTS nad_geo (oid INTEGER);
SELECT addgeometrycolumn('nad_geo', 'geom', 4326, 'POINT', 2);

-- convert from TEXT to Float
INSERT INTO nad_geo (oid, geom)
SELECT oid, makepoint(0+longitude, 0+latitude, 4326) FROM nad_data;

SELECT st_astext(geom) FROM nad_geo limit 6;

SELECT createspatialindex('nad_geo', 'geom');

SELECT COUNT(*) FROM nad_geo;
    " | sqlite3 nad_r${r}.db

}


