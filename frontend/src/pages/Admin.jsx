import React, { useState, useEffect, useMemo } from 'react';
import { Package, Trash2, Key, Mail, Calendar, CreditCard, Gift, Globe, Users, Share2, ExternalLink, Download } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { getAdminGrants, getAdminPurchases, getAdminUsers, deleteAdminUser, createGrant, revokeGrant, getAdminDomainGrants, createDomainGrant, revokeDomainGrant, getAdminShares, revokeShare } from '../services/api';
import comunasData from '../data/comunas.json';
import './Admin.css';

const TABS = [
  { id: 'shares',    label: 'LinkedIn Shares', icon: Share2 },
  { id: 'grants',    label: 'Accesos',   icon: Key },
  { id: 'domains',   label: 'Dominios',  icon: Globe },
  { id: 'purchases', label: 'Compras Flow', icon: CreditCard },
  { id: 'users',     label: 'Usuarios',  icon: Users },
];

const Admin = () => {
  const [activeTab, setActiveTab] = useState('shares');
  const [grants, setGrants] = useState([]);
  const [purchases, setPurchases] = useState([]);
  const [users, setUsers] = useState([]);
  const [shares, setShares] = useState([]);
  const [shareStats, setShareStats] = useState({ total_shares: 0, total_downloads: 0 });
  const [isLoading, setIsLoading] = useState(true);
  const [domainGrants, setDomainGrants] = useState([]);
  const [errorMSG, setErrorMSG] = useState('');
  const [successMSG, setSuccessMSG] = useState('');
  const navigate = useNavigate();
  const { user, isAdmin, openLoginModal } = useAuth();

  // Form State
  const [formEmail, setFormEmail] = useState('');
  const [formBundle, setFormBundle] = useState('');
  const [formDuration, setFormDuration] = useState('7');

  // Domain Form State
  const [formDomain, setFormDomain] = useState('');
  const [formDomainDuration, setFormDomainDuration] = useState('always');

  useEffect(() => {
    if (!user) {
      openLoginModal();
      return;
    }
    if (!isAdmin) {
      navigate('/');
      return;
    }
    fetchAdminData();
  }, [user, isAdmin, navigate, openLoginModal]);

  const fetchAdminData = async () => {
    setIsLoading(true);
    try {
      const [grantsData, purchasesData, domainGrantsData, usersData, sharesData] = await Promise.all([
        getAdminGrants(),
        getAdminPurchases(),
        getAdminDomainGrants(),
        getAdminUsers(),
        getAdminShares(),
      ]);
      setGrants(grantsData.grants);
      setPurchases(purchasesData.purchases);
      setDomainGrants(domainGrantsData.domainGrants);
      setUsers(usersData.users);
      setShares(sharesData.shares || []);
      setShareStats(sharesData.stats || { total_shares: 0, total_downloads: 0 });
    } catch {
      setErrorMSG('Error de conexion con el servidor.');
    } finally {
      setIsLoading(false);
    }
  };

  const handleRevokeShare = async (shareId) => {
    if (!confirm('Revocar este share? El usuario perdera su acceso.')) return;
    try {
      await revokeShare(shareId);
      fetchAdminData();
    } catch (err) {
      setErrorMSG(err.data?.error || 'Error al revocar share');
    }
  };

  const handleGrant = async (e) => {
    e.preventDefault();
    setErrorMSG('');
    setSuccessMSG('');
    if (!formEmail || !formBundle) {
      setErrorMSG('Correo y paquete son obligatorios.');
      return;
    }
    try {
      await createGrant(formEmail, formBundle, formDuration);
      setSuccessMSG(`Acceso otorgado exitosamente a ${formEmail}`);
      setFormEmail('');
      setFormBundle('');
      fetchAdminData();
    } catch (err) {
      setErrorMSG(err.data?.error || 'Error al procesar el acceso.');
    }
  };

  const handleRevoke = async (grantId) => {
    if (!window.confirm('Seguro que deseas revocar este acceso manual?')) return;
    try {
      await revokeGrant(grantId);
      setSuccessMSG('Acceso revocado correctamente.');
      fetchAdminData();
    } catch (err) {
      setErrorMSG(err.data?.error || 'No se pudo revocar.');
    }
  };

  const handleDomainGrant = async (e) => {
    e.preventDefault();
    setErrorMSG('');
    setSuccessMSG('');
    if (!formDomain) {
      setErrorMSG('Dominio es obligatorio.');
      return;
    }
    try {
      await createDomainGrant(formDomain, formDomainDuration);
      setSuccessMSG(`Dominio @${formDomain.replace(/^@/, '')} autorizado exitosamente`);
      setFormDomain('');
      fetchAdminData();
    } catch (err) {
      setErrorMSG(err.data?.error || 'Error al autorizar el dominio.');
    }
  };

  const handleDeleteUser = async (userId, email) => {
    if (!window.confirm(`Seguro que deseas eliminar al usuario ${email}? Se eliminaran tambien sus compras y accesos.`)) return;
    try {
      await deleteAdminUser(userId);
      setSuccessMSG(`Usuario ${email} eliminado correctamente.`);
      fetchAdminData();
    } catch (err) {
      setErrorMSG(err.data?.error || 'No se pudo eliminar el usuario.');
    }
  };

  const handleRevokeDomain = async (grantId) => {
    if (!window.confirm('Seguro que deseas revocar este acceso de dominio?')) return;
    try {
      await revokeDomainGrant(grantId);
      setSuccessMSG('Acceso de dominio revocado correctamente.');
      fetchAdminData();
    } catch (err) {
      setErrorMSG(err.data?.error || 'No se pudo revocar.');
    }
  };

  const baseComunas = useMemo(
    () => comunasData.filter(c => c.tier !== 'BUNDLE').sort((a, b) => a.nombre.localeCompare(b.nombre)),
    []
  );
  const bundlesList = useMemo(
    () => comunasData.filter(c => c.tier === 'BUNDLE'),
    []
  );

  const formatDate = (dateStr) => {
    const d = new Date(dateStr);
    if (isNaN(d.getTime()) || d.getFullYear() < 2000) return '—';
    return d.toLocaleDateString('es-CL');
  };

  const totalRevenue = useMemo(
    () => purchases.reduce((sum, p) => sum + (p.amount || 0), 0),
    [purchases]
  );

  if (!isAdmin) return null;

  return (
    <div className="admin-page">

      {/* Hero */}
      <section className="admin-hero">
        <div className="container admin-hero-inner">
          <span className="admin-label">Panel Interno</span>
          <h1 className="admin-title">Administracion</h1>
          <p className="admin-subtitle">
            Gestion de accesos manuales, regalos corporativos y transacciones Flow.
          </p>
        </div>
      </section>

      {/* Content */}
      <section className="admin-content">
        <div className="container admin-narrow">

          {/* Stats */}
          {!isLoading && (
            <div className="admin-stats">
              <div className="admin-stat">
                <div className="admin-stat-value">{purchases.length}</div>
                <div className="admin-stat-label">Compras Flow</div>
              </div>
              <div className="admin-stat">
                <div className="admin-stat-value">{grants.length}</div>
                <div className="admin-stat-label">Accesos Manuales</div>
              </div>
              <div className="admin-stat">
                <div className="admin-stat-value">
                  {totalRevenue > 0 ? `$${Math.round(totalRevenue / 1000)}k` : '$0'}
                </div>
                <div className="admin-stat-label">Ingresos</div>
              </div>
              <div className="admin-stat">
                <div className="admin-stat-value">{domainGrants.length}</div>
                <div className="admin-stat-label">Dominios</div>
              </div>
              <div className="admin-stat">
                <div className="admin-stat-value">{users.length}</div>
                <div className="admin-stat-label">Usuarios</div>
              </div>
            </div>
          )}

          {/* Alerts */}
          {errorMSG && <div className="admin-alert error">{errorMSG}</div>}
          {successMSG && <div className="admin-alert success">{successMSG}</div>}

          {/* Tab bar */}
          <div className="admin-tabs">
            {TABS.map(tab => {
              const Icon = tab.icon;
              return (
                <button
                  key={tab.id}
                  className={`admin-tab${activeTab === tab.id ? ' active' : ''}`}
                  onClick={() => setActiveTab(tab.id)}
                >
                  <Icon size={15} />
                  <span>{tab.label}</span>
                </button>
              );
            })}
          </div>

          {/* Tab panels */}
          <div className="admin-panel">

            {/* ── Tab: LinkedIn Shares ── */}
            {activeTab === 'shares' && (
              <>
                <div className="admin-card">
                  <div className="admin-card-header">
                    <div className="admin-card-icon accent"><Share2 size={18} /></div>
                    <h2 className="admin-card-title">Estadisticas</h2>
                  </div>
                  <div className="admin-card-body">
                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 16 }}>
                      <div>
                        <div style={{ fontSize: '0.7rem', color: 'var(--color-text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Total shares</div>
                        <div style={{ fontSize: '1.8rem', fontWeight: 700, color: '#000000' }}>{shareStats.total_shares}</div>
                      </div>
                      <div>
                        <div style={{ fontSize: '0.7rem', color: 'var(--color-text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Total descargas</div>
                        <div style={{ fontSize: '1.8rem', fontWeight: 700 }}>{shareStats.total_downloads}</div>
                      </div>
                      <div>
                        <div style={{ fontSize: '0.7rem', color: 'var(--color-text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Descargas / share</div>
                        <div style={{ fontSize: '1.8rem', fontWeight: 700 }}>
                          {shareStats.total_shares > 0 ? (shareStats.total_downloads / shareStats.total_shares).toFixed(1) : '0'}
                        </div>
                      </div>
                    </div>
                  </div>
                </div>

                <div className="admin-card" style={{ marginTop: 16 }}>
                  <div className="admin-card-header">
                    <div className="admin-card-icon"><Share2 size={18} /></div>
                    <h2 className="admin-card-title">Posts de LinkedIn ({shares.length})</h2>
                  </div>
                  <div className="admin-card-body">
                    {isLoading ? (
                      <p style={{ color: 'var(--color-text-muted)', textAlign: 'center', padding: 20 }}>Cargando...</p>
                    ) : shares.length === 0 ? (
                      <p style={{ color: 'var(--color-text-muted)', textAlign: 'center', padding: 20 }}>
                        Nadie ha compartido todavia
                      </p>
                    ) : (
                      <table className="admin-table">
                        <thead>
                          <tr>
                            <th>Usuario LinkedIn</th>
                            <th>URL del Post</th>
                            <th><Download size={12} style={{ display: 'inline', verticalAlign: 'middle' }} /> Descargas</th>
                            <th>Fecha</th>
                            <th>Acciones</th>
                          </tr>
                        </thead>
                        <tbody>
                          {shares.map(s => (
                            <tr key={s.id}>
                              <td>
                                {s.linkedin_username ? (
                                  <a
                                    href={`https://www.linkedin.com/in/${s.linkedin_username}`}
                                    target="_blank" rel="noopener noreferrer"
                                    style={{ color: '#000000', textDecoration: 'none' }}
                                  >
                                    @{s.linkedin_username}
                                  </a>
                                ) : (
                                  <span style={{ color: 'var(--color-text-muted)', fontStyle: 'italic' }}>sin usuario</span>
                                )}
                              </td>
                              <td>
                                <a
                                  href={s.post_url}
                                  target="_blank" rel="noopener noreferrer"
                                  style={{ color: 'var(--color-text-secondary)', fontSize: '0.75rem', display: 'inline-flex', alignItems: 'center', gap: 4 }}
                                >
                                  {s.post_url.length > 60 ? s.post_url.slice(0, 60) + '...' : s.post_url}
                                  <ExternalLink size={10} />
                                </a>
                              </td>
                              <td><strong>{s.downloads_count}</strong></td>
                              <td style={{ fontSize: '0.8rem', color: 'var(--color-text-muted)' }}>
                                {s.created_at ? new Date(s.created_at).toLocaleDateString('es-CL') : '—'}
                              </td>
                              <td>
                                <button
                                  className="btn btn-danger btn-sm"
                                  onClick={() => handleRevokeShare(s.id)}
                                  title="Revocar acceso"
                                >
                                  <Trash2 size={12} />
                                </button>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    )}
                  </div>
                </div>
              </>
            )}

            {/* ── Tab: Accesos (form + table) ── */}
            {activeTab === 'grants' && (
              <>
                <div className="admin-card">
                  <div className="admin-card-header">
                    <div className="admin-card-icon accent"><Key size={18} /></div>
                    <h2 className="admin-card-title">Otorgar Nuevo Acceso</h2>
                  </div>
                  <div className="admin-card-body">
                    <form onSubmit={handleGrant} className="admin-form">
                      <div className="form-group">
                        <label className="form-label"><Mail size={14} /> Correo Electronico</label>
                        <input
                          type="email"
                          placeholder="cliente@empresa.com"
                          value={formEmail}
                          onChange={(e) => setFormEmail(e.target.value)}
                          required
                        />
                      </div>
                      <div className="form-group">
                        <label className="form-label"><Package size={14} /> Paquete o Comuna</label>
                        <select value={formBundle} onChange={(e) => setFormBundle(e.target.value)} required>
                          <option value="">Seleccione un paquete de datos...</option>
                          <optgroup label="Bundles Especiales">
                            {bundlesList.map(b => (
                              <option key={b.id} value={b.id}>{b.nombre}</option>
                            ))}
                          </optgroup>
                          <optgroup label="Comunas Individuales">
                            {baseComunas.map(c => (
                              <option key={c.id} value={c.id}>{c.nombre} (Tier {c.tier})</option>
                            ))}
                          </optgroup>
                        </select>
                      </div>
                      <div className="form-row">
                        <div className="form-group">
                          <label className="form-label"><Calendar size={14} /> Tiempo de Acceso</label>
                          <select value={formDuration} onChange={(e) => setFormDuration(e.target.value)}>
                            <option value="1">24 Horas (Piloto rapido)</option>
                            <option value="7">7 Dias (Trial estandar)</option>
                            <option value="30">30 Dias (Mensual)</option>
                            <option value="always">Permanente (Sin expiracion)</option>
                          </select>
                        </div>
                        <div className="form-group" style={{ justifyContent: 'flex-end' }}>
                          <button type="submit" className="btn btn-primary btn-lg admin-submit">
                            Emitir Acceso
                          </button>
                        </div>
                      </div>
                    </form>
                  </div>
                </div>

                <div className="admin-card admin-table-card">
                  <div className="admin-card-header">
                    <div className="admin-card-icon purple"><Gift size={18} /></div>
                    <h2 className="admin-card-title">Accesos Manuales Otorgados</h2>
                  </div>
                  <div className="admin-card-body">
                    {isLoading ? (
                      <p className="admin-empty">Cargando base de datos...</p>
                    ) : grants.length === 0 ? (
                      <p className="admin-empty">No hay accesos manuales vigentes actualmente.</p>
                    ) : (
                      <div className="admin-table-wrap">
                        <table className="admin-table">
                          <thead>
                            <tr>
                              <th>Email</th>
                              <th>Paquete</th>
                              <th>Vigencia</th>
                              <th>Accion</th>
                            </tr>
                          </thead>
                          <tbody>
                            {grants.map(grant => {
                              const isExpired = grant.expires_at && new Date(grant.expires_at) < new Date();
                              return (
                                <tr key={grant.id} className={isExpired ? 'expired-row' : ''}>
                                  <td className="cell-email">{grant.email}</td>
                                  <td className="cell-mono">{grant.external_reference}</td>
                                  <td className="cell-date">
                                    {grant.expires_at ? formatDate(grant.expires_at) : 'Permanente'}
                                    {isExpired && <span className="status-badge expired">Expirado</span>}
                                  </td>
                                  <td>
                                    <button onClick={() => handleRevoke(grant.id)} className="btn-revoke" title="Revocar Acceso">
                                      <Trash2 size={14} /> Revocar
                                    </button>
                                  </td>
                                </tr>
                              );
                            })}
                          </tbody>
                        </table>
                      </div>
                    )}
                  </div>
                </div>
              </>
            )}

            {/* ── Tab: Dominios (form + table) ── */}
            {activeTab === 'domains' && (
              <>
                <div className="admin-card">
                  <div className="admin-card-header">
                    <div className="admin-card-icon green"><Globe size={18} /></div>
                    <h2 className="admin-card-title">Autorizar Dominio Completo</h2>
                  </div>
                  <div className="admin-card-body">
                    <form onSubmit={handleDomainGrant} className="admin-form">
                      <div className="form-row">
                        <div className="form-group">
                          <label className="form-label"><Globe size={14} /> Dominio</label>
                          <input
                            type="text"
                            placeholder="cchc.cl"
                            value={formDomain}
                            onChange={(e) => setFormDomain(e.target.value)}
                            required
                          />
                        </div>
                        <div className="form-group">
                          <label className="form-label"><Calendar size={14} /> Tiempo de Acceso</label>
                          <select value={formDomainDuration} onChange={(e) => setFormDomainDuration(e.target.value)}>
                            <option value="7">7 Dias</option>
                            <option value="30">30 Dias</option>
                            <option value="90">90 Dias</option>
                            <option value="365">1 Ano</option>
                            <option value="always">Permanente (Sin expiracion)</option>
                          </select>
                        </div>
                      </div>
                      <button type="submit" className="btn btn-primary btn-lg admin-submit">
                        Autorizar Dominio
                      </button>
                    </form>
                  </div>
                </div>

                <div className="admin-card admin-table-card">
                  <div className="admin-card-header">
                    <div className="admin-card-icon green"><Globe size={18} /></div>
                    <h2 className="admin-card-title">Dominios Autorizados</h2>
                  </div>
                  <div className="admin-card-body">
                    {isLoading ? (
                      <p className="admin-empty">Cargando base de datos...</p>
                    ) : domainGrants.length === 0 ? (
                      <p className="admin-empty">No hay dominios autorizados actualmente.</p>
                    ) : (
                      <div className="admin-table-wrap">
                        <table className="admin-table">
                          <thead>
                            <tr>
                              <th>Dominio</th>
                              <th>Creado por</th>
                              <th>Vigencia</th>
                              <th>Accion</th>
                            </tr>
                          </thead>
                          <tbody>
                            {domainGrants.map(dg => {
                              const isExpired = dg.expires_at && new Date(dg.expires_at) < new Date();
                              return (
                                <tr key={dg.id} className={isExpired ? 'expired-row' : ''}>
                                  <td className="cell-email">@{dg.domain}</td>
                                  <td className="cell-mono">{dg.created_by}</td>
                                  <td className="cell-date">
                                    {dg.expires_at ? formatDate(dg.expires_at) : 'Permanente'}
                                    {isExpired && <span className="status-badge expired">Expirado</span>}
                                  </td>
                                  <td>
                                    <button onClick={() => handleRevokeDomain(dg.id)} className="btn-revoke" title="Revocar Dominio">
                                      <Trash2 size={14} /> Revocar
                                    </button>
                                  </td>
                                </tr>
                              );
                            })}
                          </tbody>
                        </table>
                      </div>
                    )}
                  </div>
                </div>
              </>
            )}

            {/* ── Tab: Compras Flow ── */}
            {activeTab === 'purchases' && (
              <div className="admin-card admin-table-card">
                <div className="admin-card-header">
                  <div className="admin-card-icon blue"><CreditCard size={18} /></div>
                  <h2 className="admin-card-title">Compras Exitosas (Flow)</h2>
                </div>
                <div className="admin-card-body">
                  {isLoading ? (
                    <p className="admin-empty">Cargando base de datos...</p>
                  ) : purchases.length === 0 ? (
                    <p className="admin-empty">No hay compras registradas actualmente.</p>
                  ) : (
                    <div className="admin-table-wrap">
                      <table className="admin-table">
                        <thead>
                          <tr>
                            <th>Email</th>
                            <th>Paquete</th>
                            <th>Fecha</th>
                            <th>Transaccion</th>
                            <th>Monto</th>
                            <th>RUT</th>
                          </tr>
                        </thead>
                        <tbody>
                          {purchases.map(purchase => (
                            <tr key={purchase.id}>
                              <td className="cell-email">{purchase.email}</td>
                              <td className="cell-mono">{purchase.external_reference}</td>
                              <td>{formatDate(purchase.created_at)}</td>
                              <td className="cell-mono">{purchase.preference_id || '—'}</td>
                              <td className="cell-amount">
                                {purchase.amount != null ? `$${purchase.amount.toLocaleString('es-CL')}` : '—'}
                              </td>
                              <td className="cell-mono">{purchase.payer_rut || '—'}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                </div>
              </div>
            )}

            {/* ── Tab: Usuarios ── */}
            {activeTab === 'users' && (
              <div className="admin-card admin-table-card">
                <div className="admin-card-header">
                  <div className="admin-card-icon cyan"><Users size={18} /></div>
                  <h2 className="admin-card-title">Usuarios Registrados</h2>
                </div>
                <div className="admin-card-body">
                  {isLoading ? (
                    <p className="admin-empty">Cargando base de datos...</p>
                  ) : users.length === 0 ? (
                    <p className="admin-empty">No hay usuarios registrados actualmente.</p>
                  ) : (
                    <div className="admin-table-wrap">
                      <table className="admin-table">
                        <thead>
                          <tr>
                            <th>Email</th>
                            <th>Registro</th>
                            <th>Compras</th>
                            <th>Accesos</th>
                            <th>Accion</th>
                          </tr>
                        </thead>
                        <tbody>
                          {users.map(u => (
                            <tr key={u.id}>
                              <td className="cell-email">{u.email}</td>
                              <td>{formatDate(u.created_at)}</td>
                              <td className="cell-amount">{u.purchase_count}</td>
                              <td className="cell-amount">{u.grant_count}</td>
                              <td>
                                <button onClick={() => handleDeleteUser(u.id, u.email)} className="btn-revoke" title="Eliminar Usuario">
                                  <Trash2 size={14} /> Eliminar
                                </button>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                </div>
              </div>
            )}

          </div>
        </div>
      </section>
    </div>
  );
};

export default Admin;
