import React, { useState, useEffect, useMemo } from 'react';
import { Package, LogOut, AlertCircle, FileText, Database, MapPin, Info, X, Globe, Share2 } from 'lucide-react';
import { useNavigate, Link } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { getMyPurchases, getSecureDownloadLinks, getAvailability, getMetadataContent } from '../services/api';
import comunasData from '../data/comunas.json';
import './MyData.css';

const MyData = () => {
  const [purchases, setPurchases] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [errorMSG, setErrorMSG] = useState(null);
  const [downloadLinks, setDownloadLinks] = useState({});
  const [metaModal, setMetaModal] = useState({ open: false, data: null, loading: false, comunaName: '' });
  const [domainAccess, setDomainAccess] = useState(null);
  const [allAvailable, setAllAvailable] = useState([]);
  const navigate = useNavigate();
  const { user, logout, openLoginModal, hasShared } = useAuth();

  // Clear cart on successful payment redirect
  useEffect(() => {
    if (new URLSearchParams(window.location.search).get('status') === 'success') {
      localStorage.removeItem('tremen_cart');
    }
  }, []);

  useEffect(() => {
    if (!user) {
      setIsLoading(false);
      openLoginModal();
      navigate('/');
      return;
    }
    fetchPurchases();
  }, [user]);

  const fetchPurchases = async () => {
    setIsLoading(true);
    try {
      const data = await getMyPurchases();
      setPurchases(data.purchases || []);

      // If user has shared on LinkedIn or has domain access, fetch all available communes
      if (data.domainAccess || hasShared) {
        if (data.domainAccess) setDomainAccess(data.domainAccess);
        try {
          const avail = await getAvailability();
          setAllAvailable(avail.available || []);
        } catch (err) {
          console.error('Failed to fetch availability:', err);
        }
      }
    } catch (err) {
      if (err.status === 401) {
        logout();
        navigate('/');
        return;
      }
      setErrorMSG(err.message);
    } finally {
      setIsLoading(false);
    }
  };

  const loadDownloadLinks = async (externalRef) => {
    if (downloadLinks[externalRef]) return;

    try {
      const data = await getSecureDownloadLinks(externalRef);
      if (data.success && data.links) {
        setDownloadLinks(prev => ({ ...prev, [externalRef]: data.links }));
      }
    } catch (err) {
      console.error("Link generation failed", err);
    }
  };

  // Only pre-load links for individual purchases (not all 300+ domain communes)
  useEffect(() => {
    if (purchases.length > 0) {
      purchases.forEach(p => {
        loadDownloadLinks(p.external_reference);
      });
    }
  }, [purchases]);

  // Merge: individual purchases + all available (from domain access)
  const displayItems = useMemo(() => {
    const hasFullAccess = domainAccess || hasShared;
    if (!hasFullAccess || allAvailable.length === 0) {
      return purchases.map(p => ({ ...p, source: 'purchase' }));
    }

    const purchasedIds = new Set(purchases.map(p => p.external_reference));
    const accessSource = hasShared ? 'share' : 'domain';
    const extraItems = allAvailable
      .filter(id => !purchasedIds.has(id))
      .map(id => ({
        id: `${accessSource}_${id}`,
        external_reference: id,
        created_at: null,
        expires_at: domainAccess?.expiresAt || null,
        source: accessSource,
      }));

    return [
      ...purchases.map(p => ({ ...p, source: 'purchase' })),
      ...extraItems,
    ];
  }, [purchases, domainAccess, hasShared, allAvailable]);

  const handleLogout = () => {
    logout();
    navigate('/');
  };

  const getComunaName = (refId) => {
    const match = comunasData.find(c => c.id === refId);
    return match ? match.nombre : refId;
  };

  const openMetadata = async (comunaId, comunaName) => {
    setMetaModal({ open: true, data: null, loading: true, comunaName });
    try {
      const json = await getMetadataContent(comunaId);
      setMetaModal({ open: true, data: json, loading: false, comunaName });
    } catch {
      setMetaModal({ open: true, data: { error: 'No se pudo cargar el metadata' }, loading: false, comunaName });
    }
  };

  const closeMetadata = () => setMetaModal({ open: false, data: null, loading: false, comunaName: '' });

  const formatSize = (bytes) => {
    if (!bytes) return null;
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  };

  const formatDate = (dateStr) => {
    const d = new Date(dateStr);
    if (isNaN(d.getTime()) || d.getFullYear() < 2000) return '—';
    return d.toLocaleDateString('es-CL');
  };

  // On-demand download: load links when clicking a domain-access item
  const handleRequestDownload = (externalRef) => {
    loadDownloadLinks(externalRef);
  };

  /* ── Not logged in ── */
  if (!user) {
    return (
      <div className="md-page">
        <section className="md-hero">
          <div className="container md-hero-inner">
            <span className="md-label">Mis Compras</span>
            <h1 className="md-title">Mis Datos Desbloqueados</h1>
          </div>
        </section>
        <section className="md-content">
          <div className="container md-narrow">
            <div className="md-login-prompt">
              <Package size={48} className="md-empty-icon" />
              <h2>Inicia Sesion</h2>
              <p>
                Para visualizar y descargar tus datos espaciales adquiridos,
                ingresa con tu correo usando el boton superior.
              </p>
              <button className="btn btn-primary btn-lg" onClick={openLoginModal}>
                Abrir ventana de acceso
              </button>
            </div>
          </div>
        </section>
      </div>
    );
  }

  /* ── Main view ── */
  return (
    <div className="md-page">

      {/* Hero */}
      <section className="md-hero">
        <div className="container md-hero-inner">
          <span className="md-label">Mis Compras</span>
          <h1 className="md-title">Mis Datos Desbloqueados</h1>
          <p className="md-subtitle">
            Descarga tus archivos geoespaciales en formatos CSV, GeoPackage y GeoJSON directamente desde AWS S3.
          </p>
          <div className="md-user-badge">
            <span className="md-user-email">{user.email}</span>
            <button onClick={handleLogout} className="md-logout-btn" title="Cerrar Sesion">
              <LogOut size={16} />
            </button>
          </div>
        </div>
      </section>

      {/* Content */}
      <section className="md-content">
        <div className="container md-narrow">

          {isLoading ? (
            <div className="md-loading">Cargando base de datos...</div>
          ) : errorMSG ? (
            <div className="md-alert">
              <AlertCircle size={18} />
              <span>{errorMSG}</span>
            </div>
          ) : displayItems.length === 0 ? (
            <div className="md-empty">
              <Package size={48} className="md-empty-icon" />
              <h3>No tienes datos comprados aun</h3>
              <p>Tus compras se vincularan automaticamente a este correo.</p>
              <Link to="/comunas" className="btn btn-primary">Ir al Catalogo</Link>
            </div>
          ) : (
            <>
              {/* Access banners */}
              {hasShared && (
                <div className="md-domain-banner">
                  <Share2 size={16} />
                  <span>
                    Acceso completo desbloqueado via <strong>LinkedIn</strong> — Permanente
                  </span>
                </div>
              )}
              {domainAccess && (
                <div className="md-domain-banner">
                  <Globe size={16} />
                  <span>
                    Acceso corporativo activo para <strong>@{domainAccess.domain}</strong>
                    {domainAccess.expiresAt ? ` — Vigente hasta ${formatDate(domainAccess.expiresAt)}` : ' — Permanente'}
                  </span>
                </div>
              )}

              {/* Compact inbox list */}
              <div className="md-inbox">
                <div className="md-inbox-header">
                  <span className="md-inbox-title">Datasets disponibles</span>
                  <span className="md-inbox-count">{displayItems.length}</span>
                </div>

                {displayItems.map(item => {
                  const links = downloadLinks[item.external_reference];
                  const isDomain = item.source === 'domain' || item.source === 'share';

                  return (
                    <div key={item.id} className="md-row">
                      <span className="md-row-name">{getComunaName(item.external_reference)}</span>
                      <span className="md-row-id">{item.external_reference}</span>
                      <span className="md-row-tag">
                        {isDomain ? (
                          <span className="md-domain-tag">{item.source === 'share' ? 'LinkedIn' : 'Corporativo'}</span>
                        ) : (
                          <span className="md-row-date">{formatDate(item.created_at)}</span>
                        )}
                      </span>
                      <span className="md-row-spacer" />
                      <span className="md-row-actions">
                        {links ? (
                          links.bundle ? (
                            <a href={links.bundle} target="_blank" rel="noopener noreferrer" className="md-dl bundle-btn">
                              <Package size={12} /> Bundle
                            </a>
                          ) : (
                            <>
                              <a href={links.csv?.url || links.csv} target="_blank" rel="noopener noreferrer" className="md-dl csv-btn">
                                <FileText size={12} /> CSV
                                {links.csv?.size && <span className="md-dl-size">{formatSize(links.csv.size)}</span>}
                              </a>
                              {links.gpkg && (
                                <a href={links.gpkg?.url || links.gpkg} target="_blank" rel="noopener noreferrer" className="md-dl shp-btn">
                                  <Database size={12} /> GPKG
                                  {links.gpkg?.size && <span className="md-dl-size">{formatSize(links.gpkg.size)}</span>}
                                </a>
                              )}
                              {links.geojson && (
                                <a href={links.geojson?.url || links.geojson} target="_blank" rel="noopener noreferrer" className="md-dl json-btn">
                                  <MapPin size={12} /> GeoJSON
                                  {links.geojson?.size && <span className="md-dl-size">{formatSize(links.geojson.size)}</span>}
                                </a>
                              )}
                              <button onClick={() => openMetadata(item.external_reference, getComunaName(item.external_reference))} className="md-dl metadata-btn">
                                <Info size={12} /> Meta
                              </button>
                            </>
                          )
                        ) : isDomain ? (
                          <button onClick={() => handleRequestDownload(item.external_reference)} className="md-dl csv-btn">
                            Generar Enlaces
                          </button>
                        ) : (
                          <span className="md-generating">Firmando S3...</span>
                        )}
                      </span>
                    </div>
                  );
                })}
              </div>
            </>
          )}

        </div>
      </section>

      {/* Metadata Modal */}
      {metaModal.open && (
        <div className="meta-modal-overlay" onClick={closeMetadata}>
          <div className="meta-modal" onClick={e => e.stopPropagation()}>
            <div className="meta-modal-header">
              <h3><Info size={18} /> Metadata — {metaModal.comunaName}</h3>
              <button onClick={closeMetadata} className="meta-modal-close"><X size={20} /></button>
            </div>
            <div className="meta-modal-body">
              {metaModal.loading ? (
                <p className="meta-loading">Cargando metadata...</p>
              ) : (
                <pre className="meta-json">{JSON.stringify(metaModal.data, null, 2)}</pre>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default MyData;
