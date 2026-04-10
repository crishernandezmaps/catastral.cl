-- Indexes for catastro_actual (created AFTER bulk load)
CREATE UNIQUE INDEX IF NOT EXISTS idx_actual_rol ON catastro_actual (comuna, manzana, predio);
CREATE INDEX IF NOT EXISTS idx_actual_comuna ON catastro_actual (comuna);
CREATE INDEX IF NOT EXISTS idx_actual_destino ON catastro_actual (dc_cod_destino);
CREATE INDEX IF NOT EXISTS idx_actual_sup ON catastro_actual (dc_sup_terreno) WHERE dc_sup_terreno IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_actual_avaluo ON catastro_actual (rc_avaluo_total) WHERE rc_avaluo_total IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_actual_direccion ON catastro_actual USING gin (rc_direccion gin_trgm_ops);

ANALYZE catastro_actual;
ANALYZE catastro_historico;
ANALYZE comunas_lookup;

-- Spatial index for coordinate-based search (PostGIS)
CREATE INDEX IF NOT EXISTS idx_actual_coords ON catastro_actual USING GIST (
  geography(ST_SetSRID(ST_MakePoint(lon, lat), 4326))
) WHERE lat IS NOT NULL AND lon IS NOT NULL;
