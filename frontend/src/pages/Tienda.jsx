import { useState, useEffect, useMemo } from 'react';
import { Search, FileSpreadsheet, Database, Download, Loader2, Info } from 'lucide-react';
import { useAuth } from '../context/AuthContext';
import { getSecureDownloadLinks } from '../services/api';
import ShareToDownload from '../components/ShareToDownload';
import comunasData from '../data/comunas.json';
import './CommuneList.css';
import './CommuneListAdditions.css';

const CommuneList = () => {
  const [searchTerm, setSearchTerm] = useState('');
  const { hasShared, isAdmin } = useAuth();
  const canDownload = hasShared || isAdmin;

  const [downloadLinks, setDownloadLinks] = useState({});
  const [loadingLinks, setLoadingLinks] = useState({});

  // Sorting & pagination
  const [sortConfig, setSortConfig] = useState({ key: null, direction: 'asc' });
  const [activeFilter, setActiveFilter] = useState(null);
  const [currentPage, setCurrentPage] = useState(1);
  const itemsPerPage = 15;

  useEffect(() => {
    setCurrentPage(1);
  }, [searchTerm, activeFilter, sortConfig]);

  const handleSort = (key) => {
    let direction = 'asc';
    if (sortConfig.key === key && sortConfig.direction === 'asc') direction = 'desc';
    setSortConfig({ key, direction });
  };

  // Region filter
  const regions = useMemo(() => [...new Set(comunasData.map(c => c.region))].sort(), []);

  const handleFilterClick = (region) => {
    setActiveFilter(activeFilter === region ? null : region);
  };

  const handleDownload = async (comunaId) => {
    if (downloadLinks[comunaId]) return; // already loaded
    setLoadingLinks(prev => ({ ...prev, [comunaId]: true }));
    try {
      const data = await getSecureDownloadLinks(comunaId);
      if (data.success && data.links) {
        setDownloadLinks(prev => ({ ...prev, [comunaId]: data.links }));
      }
    } catch (err) {
      console.error('Error fetching links:', err);
    } finally {
      setLoadingLinks(prev => ({ ...prev, [comunaId]: false }));
    }
  };

  // Filtering and sorting
  const sortedComunas = [...comunasData].sort((a, b) => {
    if (!sortConfig.key) return 0;
    let aValue, bValue;
    aValue = a[sortConfig.key] ?? -1;
    bValue = b[sortConfig.key] ?? -1;
    if (aValue < bValue) return sortConfig.direction === 'asc' ? -1 : 1;
    if (aValue > bValue) return sortConfig.direction === 'asc' ? 1 : -1;
    return 0;
  });

  const filteredComunas = sortedComunas.filter((comuna) => {
    const matchesSearch = comuna.nombre.toLowerCase().includes(searchTerm.toLowerCase()) ||
                          comuna.region.toLowerCase().includes(searchTerm.toLowerCase()) ||
                          comuna.id.includes(searchTerm);
    const matchesFilter = activeFilter ? comuna.region === activeFilter : true;
    return matchesSearch && matchesFilter;
  });

  const totalPages = Math.ceil(filteredComunas.length / itemsPerPage);
  const paginatedComunas = filteredComunas.slice((currentPage - 1) * itemsPerPage, currentPage * itemsPerPage);

  return (
    <div className="commune-list-page">
      <div className="container commune-container">
        <header className="page-header">
          <h1>Datos Catastrales por Comuna</h1>
          <p>{comunasData.length} comunas con poligonos vectorizados, datos SII y series historicas. Descarga en CSV y GeoPackage.</p>
        </header>

        {/* Share gate — shown if user hasn't shared yet */}
        {!canDownload && (
          <div style={{ marginBottom: 24 }}>
            <ShareToDownload />
          </div>
        )}

        <div className="controls-bar">
          <div className="search-and-filters">
            <div className="search-wrapper">
              <Search className="search-icon" size={18} />
              <input
                type="text"
                placeholder="Busca por comuna o region..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="search-input"
              />
            </div>

            <div className="quick-filters" style={{ flexWrap: 'wrap' }}>
              <span className="filter-label" style={{ fontSize: '0.85rem', color: 'var(--color-text-muted)', display: 'flex', alignItems: 'center' }}>Region:</span>
              {regions.slice(0, 8).map(r => (
                <button
                  key={r}
                  className={`filter-btn ${activeFilter === r ? 'active' : ''}`}
                  onClick={() => handleFilterClick(r)}
                  style={{ fontSize: '0.75rem' }}
                >
                  {r}
                </button>
              ))}
            </div>
          </div>
          <div className="stats-indicator">
            <span className="text-accent">{filteredComunas.length}</span> comunas
          </div>
        </div>

        <div className="list-wrapper">
          <div className="list-header" style={{ gridTemplateColumns: '70px 1.2fr 1fr 70px 80px 1.4fr' }}>
            <div className="col-codigo sortable" onClick={() => handleSort('id')}>
              CUT <span className={`sort-indicator ${sortConfig.key === 'id' ? 'active' : ''}`}>{sortConfig.key === 'id' ? (sortConfig.direction === 'asc' ? '↑' : '↓') : '↕'}</span>
            </div>
            <div className="col-comuna sortable" onClick={() => handleSort('nombre')}>
              Comuna <span className={`sort-indicator ${sortConfig.key === 'nombre' ? 'active' : ''}`}>{sortConfig.key === 'nombre' ? (sortConfig.direction === 'asc' ? '↑' : '↓') : '↕'}</span>
            </div>
            <div className="col-region sortable" onClick={() => handleSort('region')}>
              Region <span className={`sort-indicator ${sortConfig.key === 'region' ? 'active' : ''}`}>{sortConfig.key === 'region' ? (sortConfig.direction === 'asc' ? '↑' : '↓') : '↕'}</span>
            </div>
            <div className="stat-hdr sortable" onClick={() => handleSort('cobertura_pct')} style={{ position: 'relative' }}>
              <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4 }}>
                C <span className={`sort-indicator ${sortConfig.key === 'cobertura_pct' ? 'active' : ''}`}>{sortConfig.key === 'cobertura_pct' ? (sortConfig.direction === 'asc' ? '↑' : '↓') : '↕'}</span>
                <span className="cobertura-info-trigger" onClick={e => e.stopPropagation()} style={{ cursor: 'help', display: 'inline-flex' }}>
                  <Info size={13} color="var(--color-text-muted)" />
                  <span className="cobertura-info-tooltip">Cobertura: porcentaje de predios con geometria poligonal vectorizada. Los predios sin cobertura corresponden a unidades al interior de edificios (que comparten el poligono del lote), predios rurales sin levantamiento cartografico, o roles registrados en el SII sin cartografia asociada.</span>
                </span>
              </span>
            </div>
            <div className="stat-hdr sortable" onClick={() => handleSort('gpkg_mb')} data-tip="Peso total de los archivos descargables">
              Peso <span className={`sort-indicator ${sortConfig.key === 'gpkg_mb' ? 'active' : ''}`}>{sortConfig.key === 'gpkg_mb' ? (sortConfig.direction === 'asc' ? '↑' : '↓') : '↕'}</span>
            </div>
            <div className="col-acciones"></div>
          </div>

          <div className="list-body">
            {paginatedComunas.length > 0 ? (
              paginatedComunas.map((comuna) => {
                const links = downloadLinks[comuna.id];
                const isLoadingLinks = loadingLinks[comuna.id];
                const mutedStyle = { fontSize: '0.8rem', color: 'var(--color-text-muted)', textAlign: 'right' };

                return (
                  <div key={comuna.id} className="row-huincha" style={{ gridTemplateColumns: '70px 1.2fr 1fr 70px 80px 1.4fr' }}>
                    <div className="col-codigo row-code">{comuna.id}</div>
                    <div className="col-comuna row-name">{comuna.nombre}</div>
                    <div className="col-region row-region">{comuna.region}</div>
                    <div style={mutedStyle}>{comuna.cobertura_pct != null ? `${comuna.cobertura_pct}%` : '—'}</div>
                    <div style={mutedStyle}>{(() => { const total = (comuna.gpkg_mb || 0) + (comuna.csv_mb || 0) + (comuna.csv_raw_mb || 0); return total >= 1024 ? `${(total / 1024).toFixed(1)} GB` : `${Math.round(total)} MB`; })()}</div>
                    <div className="col-acciones row-actions">
                      {canDownload ? (
                        links ? (
                          <div className="download-options inline-downloads row-downloads">
                            {links.csv_raw?.map((l, i) => <a key={`raw${i}`} href={l.url} target="_blank" rel="noopener noreferrer" className="action-btn csv-btn"><FileSpreadsheet size={16} /><span>SII</span></a>)}
                            {links.csv?.map((l, i) => <a key={`csv${i}`} href={l.url} target="_blank" rel="noopener noreferrer" className="action-btn csv-btn"><FileSpreadsheet size={16} /><span>CSV</span></a>)}
                            {links.gpkg?.map((l, i) => <a key={`gpkg${i}`} href={l.url} target="_blank" rel="noopener noreferrer" className="action-btn shp-btn"><Database size={16} /><span>GPKG</span></a>)}
                          </div>
                        ) : (
                          <button
                            className="btn cart-toggle-btn cart-add-available"
                            onClick={() => handleDownload(comuna.id)}
                            disabled={isLoadingLinks}
                          >
                            {isLoadingLinks ? (
                              <><Loader2 size={16} style={{ animation: 'spin 1s linear infinite' }} /> Generando...</>
                            ) : (
                              <><Download size={16} /> Descargar</>
                            )}
                          </button>
                        )
                      ) : (
                        <span style={{ fontSize: '0.8rem', color: 'var(--color-text-muted)' }}>
                          Comparte en LinkedIn para desbloquear
                        </span>
                      )}
                    </div>
                  </div>
                );
              })
            ) : (
              <div className="empty-state">
                No se encontraron comunas que coincidan con tu busqueda.
              </div>
            )}
          </div>

          {totalPages > 1 && (
            <div className="pagination-controls">
              <button className="btn btn-secondary pagination-btn" onClick={() => setCurrentPage(p => Math.max(1, p - 1))} disabled={currentPage === 1}>Anterior</button>
              <span className="pagination-info">Pagina {currentPage} de {totalPages}</span>
              <button className="btn btn-secondary pagination-btn" onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))} disabled={currentPage === totalPages}>Siguiente</button>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default CommuneList;
