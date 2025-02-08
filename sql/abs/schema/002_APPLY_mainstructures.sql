CREATE TABLE IF NOT EXISTS abs.state (
    state_code VARCHAR(1) PRIMARY KEY,
    state_name VARCHAR(100),
    geometry GEOMETRY(MultiPolygon, 7844)
);

-- Greater Capitial City Statitical(?) Area
CREATE TABLE IF NOT EXISTS abs.gccsa (
    gcc_code VARCHAR(5) PRIMARY KEY,
    gcc_name VARCHAR(100),
    state_code VARCHAR(1),
    geometry GEOMETRY(MultiPolygon, 7844),
    FOREIGN KEY (state_code) REFERENCES abs.state(state_code)
);

CREATE TABLE IF NOT EXISTS abs.sa4 (
    sa4_code VARCHAR(3) PRIMARY KEY,
    sa4_name VARCHAR(100),
    gcc_code VARCHAR(5),
    state_code VARCHAR(1),
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(MultiPolygon, 7844),
    FOREIGN KEY (gcc_code) REFERENCES abs.gccsa(gcc_code),
    FOREIGN KEY (state_code) REFERENCES abs.state(state_code)
);

CREATE TABLE IF NOT EXISTS abs.sa3 (
    sa3_code VARCHAR(5) PRIMARY KEY,
    sa3_name VARCHAR(100),
    sa4_code VARCHAR(3),
    gcc_code VARCHAR(5),
    state_code VARCHAR(1),
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(MultiPolygon, 7844),
    FOREIGN KEY (sa4_code) REFERENCES abs.sa4(sa4_code),
    FOREIGN KEY (gcc_code) REFERENCES abs.gccsa(gcc_code),
    FOREIGN KEY (state_code) REFERENCES abs.state(state_code)
);

CREATE TABLE IF NOT EXISTS abs.sa2 (
    sa2_code VARCHAR(9) PRIMARY KEY,
    sa2_name VARCHAR(100),
    sa3_code VARCHAR(5),
    sa4_code VARCHAR(3),
    gcc_code VARCHAR(5),
    state_code VARCHAR(1),
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(MultiPolygon, 7844),
    FOREIGN KEY (sa3_code) REFERENCES abs.sa3(sa3_code),
    FOREIGN KEY (sa4_code) REFERENCES abs.sa4(sa4_code),
    FOREIGN KEY (gcc_code) REFERENCES abs.gccsa(gcc_code),
    FOREIGN KEY (state_code) REFERENCES abs.state(state_code)
);

CREATE TABLE IF NOT EXISTS abs.sa1 (
    sa1_code VARCHAR(11) PRIMARY KEY,
    sa2_code VARCHAR(9),
    sa3_code VARCHAR(5),
    sa4_code VARCHAR(3),
    gcc_code VARCHAR(5),
    state_code VARCHAR(1),
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(MultiPolygon, 7844),
    FOREIGN KEY (sa2_code) REFERENCES abs.sa2(sa2_code),
    FOREIGN KEY (sa3_code) REFERENCES abs.sa3(sa3_code),
    FOREIGN KEY (sa4_code) REFERENCES abs.sa4(sa4_code),
    FOREIGN KEY (gcc_code) REFERENCES abs.gccsa(gcc_code),
    FOREIGN KEY (state_code) REFERENCES abs.state(state_code)
);

CREATE TABLE IF NOT EXISTS abs.meshblock (
    mb_code VARCHAR(15) PRIMARY KEY,
    mb_cat VARCHAR(50),
    sa1_code VARCHAR(11),
    sa2_code VARCHAR(9),
    sa3_code VARCHAR(5),
    sa4_code VARCHAR(3),
    gcc_code VARCHAR(5),
    state_code VARCHAR(1),
    area_sqkm DOUBLE PRECISION,
    geometry GEOMETRY(Polygon, 7844),
    FOREIGN KEY (sa1_code) REFERENCES abs.sa1(sa1_code),
    FOREIGN KEY (sa2_code) REFERENCES abs.sa2(sa2_code),
    FOREIGN KEY (sa3_code) REFERENCES abs.sa3(sa3_code),
    FOREIGN KEY (sa4_code) REFERENCES abs.sa4(sa4_code),
    FOREIGN KEY (gcc_code) REFERENCES abs.gccsa(gcc_code),
    FOREIGN KEY (state_code) REFERENCES abs.state(state_code)
);

CREATE INDEX IF NOT EXISTS idx_sa1_code ON abs.meshblock (sa1_code);
CREATE INDEX IF NOT EXISTS idx_sa2_code ON abs.meshblock (sa2_code);
CREATE INDEX IF NOT EXISTS idx_sa3_code ON abs.meshblock (sa3_code);
CREATE INDEX IF NOT EXISTS idx_sa4_code ON abs.meshblock (sa4_code);
CREATE INDEX IF NOT EXISTS idx_gcc_code ON abs.meshblock (gcc_code);
CREATE INDEX IF NOT EXISTS idx_state_code ON abs.meshblock (state_code);
CREATE INDEX IF NOT EXISTS idx_geometry ON abs.meshblock USING GIST (geometry)
