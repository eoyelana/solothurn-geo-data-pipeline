-- Table Definition ----------------------------------------------
DROP TABLE IF EXISTS building CASCADE;

CREATE TABLE building
(
    id               BIGSERIAL PRIMARY KEY,
    egid             character varying(20),
    geo_polygon      geometry(MultiPolygon, 4326),
    geo_center       geometry(Point, 4326),
    create_timestamp timestamp without time zone DEFAULT now(),
    building_type    text,
    area             integer
);

-- Indices -------------------------------------------------------

CREATE INDEX building_geo_polygon_idx ON building USING GIST (geo_polygon);
CREATE INDEX building_geo_center_idx ON building USING GIST (geo_center);