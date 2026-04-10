import React from 'react';
import { Database, Map, Shapes, Link2, Wrench, Download, Code, Server, Terminal, ExternalLink, Share2 } from 'lucide-react';
import { Link } from 'react-router-dom';
import './AccesoLibre.css';

const AccesoLibre = () => {
  return (
    <div className="al-page">

      {/* Hero */}
      <section className="al-hero">
        <div className="container al-hero-inner">
          <span className="al-label">Open Methodology</span>
          <h1 className="al-title">Metodologia de Extraccion</h1>
          <p className="al-subtitle">
            Transparentamos nuestro pipeline tecnologico paso a paso.
            Transformando datos alfanumericos publicos en un modelo geoespacial estructurado, lote a lote.
          </p>
        </div>
      </section>

      {/* Intro */}
      <section className="section">
        <div className="container al-narrow">
          <div className="al-intro">
            <p>
              Tremen Spatial Data Science cree en el acceso abierto a la informacion publica.
              El Servicio de Impuestos Internos (SII) provee la base de datos de avaluos y una visualizacion en su mapa tributario.
              Sin embargo, no existen descargas directas vectoriales para su uso en software SIG (QGIS, ArcGIS).
            </p>
            <p>
              Si cuentas con las habilidades tecnicas de programacion y bases de datos, aqui te explicamos la metodologia exacta (Fase 0 a Fase 4)
              que utilizamos en nuestra arquitectura Cloud para construir la cartografia predial a nivel nacional, para que puedas replicarlo por tu cuenta.
            </p>
          </div>

          <div className="al-disclosure">
            <strong>Declaracion de Transparencia</strong>
            <span>
              Tremen bajo ningun punto de vista cobra por la venta de datos publicos.
              El valor de nuestro catalogo refleja exclusivamente las cientos de horas de ingenieria, los costos de infraestructura Cloud (VPS y S3),
              y el diseno de algoritmos paralelos necesarios para empaquetar, sanear y entregar esta informacion en formatos listos para usar.
            </span>
          </div>
        </div>
      </section>

      {/* Timeline */}
      <section className="section section-alt">
        <div className="container al-narrow">
          <div className="al-timeline">

            {/* Fase 0 */}
            <article className="tl-item">
              <div className="tl-marker m0"><Database size={20} /></div>
              <div className="tl-card">
                <span className="tl-phase">Fase 0</span>
                <h2>Obtencion de Datos Alfanumericos API</h2>
                <p>
                  Todo comienza con el <strong>Listado de Roles de Avaluo</strong> semestral en formato <code>.txt</code> de ancho fijo.
                  Este archivo contiene la matriz base de comunas, manzanas y numeros de predio de todo el pais.
                </p>
                <ul className="tl-list">
                  <li><strong>Division paralela:</strong> Un script divide el archivo original de casi 10 millones de lineas en archivos pequenos por comuna.</li>
                  <li><strong>Consumo de API SII:</strong> 144 workers en paralelo a traves de 3 servidores VPS para consultar la API publica <code>getPredioNacional</code> del SII por cada rol.</li>
                  <li><strong>Enriquecimiento:</strong> 8 capas de datos simultaneamente: coordenadas, avaluo total/exento, superficie terreno/construida, destino y muestras del Observatorio de Suelo.</li>
                </ul>
                <div className="tl-tech">
                  <span className="tl-badge"><Terminal size={12} /> Bash</span>
                  <span className="tl-badge"><Code size={12} /> Python (urllib3)</span>
                  <span className="tl-badge"><Server size={12} /> VPS Cluster</span>
                </div>
              </div>
            </article>

            {/* Fase 1 */}
            <article className="tl-item">
              <div className="tl-marker m1"><Map size={20} /></div>
              <div className="tl-card">
                <span className="tl-phase">Fase 1</span>
                <h2>Descarga de Cuadriculas Raster (WMS)</h2>
                <p>
                  Una vez obtenidos los datos tabulares, necesitamos las geometrias. El SII expone un mapa mediante un servicio WMS, pero no entrega poligonos, sino imagenes cuadradas (tiles).
                </p>
                <ul className="tl-list">
                  <li><strong>Limites BCN:</strong> Cruzamos el requerimiento con los shapefiles oficiales de limites comunales de la Biblioteca del Congreso Nacional.</li>
                  <li><strong>Calculo de Tiles:</strong> Un algoritmo calcula cuales cuadriculas intersectan con la comuna en zoom Z=19 (~0.3 metros por pixel).</li>
                  <li><strong>GeoTIFF por Cluster:</strong> Los fragmentos adyacentes se descargan masivamente y se fusionan mediante Rasterio, formando una gigantesca imagen GeoTIFF por cada fragmento urbano.</li>
                </ul>
                <div className="tl-tech">
                  <span className="tl-badge"><Code size={12} /> Rasterio</span>
                  <span className="tl-badge"><Code size={12} /> GeoPandas</span>
                </div>
              </div>
            </article>

            {/* Fase 2 */}
            <article className="tl-item">
              <div className="tl-marker m2"><Shapes size={20} /></div>
              <div className="tl-card">
                <span className="tl-phase">Fase 2</span>
                <h2>Vectorizacion (Imagen a Poligono)</h2>
                <p>
                  Las imagenes GeoTIFF deben transformarse en figuras geometricas reales. Al inspeccionar el Raster WMS, las lineas prediales poseen un color rojo constante (Valor Digital = 182).
                </p>
                <ul className="tl-list">
                  <li><strong>Polygonize:</strong> <code>gdal_polygonize</code> busca continuidades de pixeles adyacentes con R=182, transformando fronteras de pixeles en anillos poligonales.</li>
                  <li><strong>Filtrado:</strong> La salida bruta produce millones de poligonos de ruido. Se filtra para conservar unicamente las formas cerradas y limpias, guardandolas en GeoPackage (<code>.gpkg</code>).</li>
                </ul>
                <div className="tl-tip">
                  Tip alternativo: Puedes cargar el GeoTIFF directamente en <strong>QGIS</strong> y utilizar la herramienta nativa <em>Raster &gt; Conversion &gt; Poligonizar</em> que funciona excepcionalmente bien para este proposito.
                </div>
                <div className="tl-tech">
                  <span className="tl-badge"><Code size={12} /> GDAL (gdal-bin)</span>
                  <span className="tl-badge"><Code size={12} /> Python-GDAL</span>
                </div>
              </div>
            </article>

            {/* Fase 3 */}
            <article className="tl-item">
              <div className="tl-marker m3"><Link2 size={20} /></div>
              <div className="tl-card">
                <span className="tl-phase">Fase 3</span>
                <h2>Cruce Espacial (Spatial Join / Rol Base)</h2>
                <p>
                  Tenemos los datos tabulares con un punto lat/lon (Fase 0) y los poligonos dibujados (Fase 2). Debemos unirlos mediante un Join Espacial.
                </p>
                <ul className="tl-list">
                  <li><strong>Punto-en-Poligono:</strong> Todo punto que caiga dentro de un cuadrado geometrico hereda automaticamente su forma. Un lote simple (1 casa) = 1 predio coincidente.</li>
                  <li><strong>Resolucion de Rol Base (Condominios):</strong> Cuando encontramos multiples puntos dentro del mismo poligono (un edificio de 100 departamentos), se realiza un scaneo rapido con la API buscando el rol especial 90XXX hasta descubrir el poligono matriz envolvente.</li>
                </ul>
                <div className="tl-tech">
                  <span className="tl-badge"><Code size={12} /> GeoPandas (sjoin)</span>
                  <span className="tl-badge"><Code size={12} /> Shapely</span>
                </div>
              </div>
            </article>

            {/* Fase 4 */}
            <article className="tl-item">
              <div className="tl-marker m4"><Wrench size={20} /></div>
              <div className="tl-card">
                <span className="tl-phase">Fase 4</span>
                <h2>Correccion Heuristica Final</h2>
                <p>
                  La API publica puede tener ~2% de desviacion por topologia inconsistente. Para llegar a los niveles de completitud exigidos en el ambito profesional, se realiza un pase de rescate.
                </p>
                <ul className="tl-list">
                  <li><strong>Simulacion de Clics:</strong> A los poligonos huerfanos que no reportaron roles validos en la Fase 3, se les extrae su centroide (el centro absoluto de la forma).</li>
                  <li><strong>getFeatureInfo:</strong> Nuestro pipeline realiza consultas web simulando que un usuario humano hizo clic exactamente en esa coordenada del mapa tributario, capturando el rol resultante. Esto empuja la precision del sistema a un ~99% registral.</li>
                </ul>
                <div className="tl-tech">
                  <span className="tl-badge"><Code size={12} /> WMS GetFeatureInfo</span>
                  <span className="tl-badge"><Code size={12} /> Scripts Heuristicos</span>
                </div>
              </div>
            </article>

          </div>
        </div>
      </section>

      {/* CTA */}
      <section className="section al-cta">
        <div className="container al-narrow">
          <div className="al-cta-card">
            <Download size={28} className="al-cta-icon" />
            <h3>La alternativa lista para usar</h3>
            <p>
              Replicar esta arquitectura Cloud toma incontables horas de ingenieria y orquestacion.
              Si solo necesitas consultar un rol especifico, usa la herramienta oficial gratuita.
              Si requieres geometria comunal masiva, ahorra tiempo adquiriendo la cartografia lista para QGIS.
            </p>
            <div className="al-cta-actions">
              <a href="https://www4.sii.cl/mapasui/internet/#/contenido/index.html" target="_blank" rel="noopener noreferrer" className="btn btn-outline btn-lg">
                <ExternalLink size={18} /> Mapas SII
              </a>
              <Link to="/comunas" className="btn btn-primary btn-lg">
                <Share2 size={18} /> Ver Catalogo
              </Link>
            </div>
          </div>
        </div>
      </section>

    </div>
  );
};

export default AccesoLibre;
