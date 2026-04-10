import { Link } from 'react-router-dom'

export default function Metodologia() {
  const sectionStyle = { marginBottom: 48 }
  const h2Style = { fontSize: '1.8rem', marginBottom: 12 }
  const h3Style = { fontSize: '1.2rem', marginBottom: 8, marginTop: 32 }
  const pStyle = { color: '#555', fontSize: '0.9rem', lineHeight: 1.8, marginBottom: 12 }
  const tableStyle = { width: '100%', fontSize: '0.82rem', marginBottom: 16 }
  const thStyle = { textAlign: 'left', padding: '8px 12px', borderBottom: '2px solid #000', fontWeight: 600, fontSize: '0.75rem', textTransform: 'uppercase', letterSpacing: '0.05em' }
  const tdStyle = { padding: '6px 12px', borderBottom: '1px solid var(--color-border)', verticalAlign: 'top' }
  const codeStyle = { fontFamily: 'monospace', fontSize: '0.82rem', background: '#f0f0f0', padding: '1px 5px', borderRadius: 3 }

  return (
    <div className="container" style={{ maxWidth: 800, padding: '48px 24px 80px' }}>
      <p style={{ fontSize: '0.75rem', color: '#999', textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: 12 }}>
        Documentacion
      </p>
      <h1 style={{ fontSize: '2.5rem', marginBottom: 8 }}>Metodologia</h1>
      <p style={{ color: '#777', fontSize: '0.95rem', marginBottom: 48 }}>
        Como se construye el dataset catastral mas completo de Chile, y por que es necesario.
      </p>

      {/* ── Context ── */}
      <section style={sectionStyle}>
        <h2 style={h2Style}>Contexto</h2>
        <p style={pStyle}>
          El Servicio de Impuestos Internos (SII) administra el catastro fiscal de los 9.5 millones de predios
          urbanos y rurales de Chile. Estos registros incluyen avaluos fiscales actualizados semestralmente,
          valor comercial de suelo por metro cuadrado, superficie de terreno y construccion, destino predial,
          coordenadas geograficas y la geometria vectorial de cada lote.
        </p>
        <p style={pStyle}>
          Es, en terminos de cobertura y actualizacion, uno de los conjuntos de datos territoriales mas completos
          de America Latina. Y es formalmente publico: la Ley 20.285 de Transparencia garantiza el derecho a
          consultarlo.
        </p>
        <p style={pStyle}>
          Sin embargo, en la practica es inutilizable para analisis masivo. El problema no es legal — es tecnico.
          Los datos se publican en tres formatos que, combinados, impiden su uso analitico directo.
        </p>
      </section>

      {/* ── Three barriers ── */}
      <section style={sectionStyle}>
        <h2 style={h2Style}>Las tres barreras de acceso</h2>

        <h3 style={h3Style}>Barrera 1: el archivo de ancho fijo</h3>
        <p style={pStyle}>
          El SII publica semestralmente el <em>Rol Semestral de Contribuciones de Bienes Raices</em>
          (<code style={codeStyle}>BRTMPNACROL</code>), un archivo de texto plano de 3 GB sin cabeceras ni
          delimitadores, con campos de longitud fija definidos por posicion de caracter y codificacion
          <code style={codeStyle}>latin-1</code>. Este formato, disenado para sistemas mainframe de los anos
          ochenta, no puede ser abierto con ninguna herramienta de analisis convencional (Excel, Google Sheets,
          Power BI) sin un proceso previo de parseo especifico.
        </p>

        <h3 style={h3Style}>Barrera 2: la API sin capacidad de consulta masiva</h3>
        <p style={pStyle}>
          El SII expone la API <code style={codeStyle}>getPredioNacional</code> que devuelve datos completos de
          un predio dado su codigo de comuna, manzana y predio. Esta API esta disenada para consultas unitarias.
          No existe un endpoint que permita obtener todos los predios de una comuna. Para construir el dataset
          completo se requieren 9.5 millones de llamadas individuales. Adicionalmente, el servidor bloquea IPs
          que realizan consultas en volumen.
        </p>

        <h3 style={h3Style}>Barrera 3: datos espaciales atrapados en imagenes</h3>
        <p style={pStyle}>
          El visor cartografico del SII muestra los poligonos de cada predio en un mapa interactivo. Pero solo
          expone esta geometria a traves de un servicio WMS que entrega imagenes PNG renderizadas. El servicio
          no ofrece los poligonos como datos vectoriales descargables. Cualquiera puede <em>ver</em> un predio
          en el visor; nadie puede <em>analizar</em> todos los predios sin superar barreras tecnicas significativas.
        </p>

        <h3 style={h3Style}>El costo economico</h3>
        <p style={pStyle}>
          Los intermediarios que comercializan datasets catastrales estructurados cobran entre USD 2,000 y
          USD 6,000 por dataset comunal. El costo agregado de esta inaccesibilidad se estima en mas de
          USD 2 millones anuales en transferencias hacia intermediarios cuyo principal valor agregado es
          superar las barreras tecnicas de formato.
        </p>
      </section>

      {/* ── Pipeline ── */}
      <section style={sectionStyle}>
        <h2 style={h2Style}>El pipeline</h2>
        <p style={pStyle}>
          Para superar las tres barreras, se diseno un pipeline de 5 fases que combina parseo de formatos
          heredados, extraccion distribuida con 30 tuneles VPN, vectorizacion raster con GDAL, y un spatial
          join que asigna un poligono a cada predio.
        </p>

        <table style={tableStyle}>
          <thead>
            <tr>
              <th style={thStyle}>Fase</th>
              <th style={thStyle}>Descripcion</th>
              <th style={thStyle}>Output</th>
              <th style={thStyle}>Tiempo</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td style={tdStyle}><strong>Fase 0</strong></td>
              <td style={tdStyle}>Extraccion masiva de datos tabulares. Parseo del TXT de ancho fijo + 9.5M llamadas a la API del SII via 30 tuneles WireGuard en paralelo + coordenadas BCN.</td>
              <td style={tdStyle}>CSV por comuna con ~90 columnas: avaluos, superficies, destinos, coordenadas, areas homogeneas, observatorio de suelo urbano.</td>
              <td style={tdStyle}>~15 dias</td>
            </tr>
            <tr>
              <td style={tdStyle}><strong>Fase 1</strong></td>
              <td style={tdStyle}>Descarga cartografica WMS. Tiles PNG del mapa catastral a zoom 19 (~0.3 m/pixel), 30 workers en paralelo con work stealing entre tuneles.</td>
              <td style={tdStyle}>GeoTIFF RGBA por comuna (EPSG:3857).</td>
              <td style={tdStyle}>~10 dias</td>
            </tr>
            <tr>
              <td style={tdStyle}><strong>Fase 2</strong></td>
              <td style={tdStyle}>Vectorizacion raster. Polygonize con GDAL Band 1, filtrado por DN, relleno de hoyos de texto WMS, filtrado por compactness.</td>
              <td style={tdStyle}>GeoPackage con poligonos prediales vectorizados.</td>
              <td style={tdStyle}>~2 dias</td>
            </tr>
            <tr>
              <td style={tdStyle}><strong>Fase 3</strong></td>
              <td style={tdStyle}>Join espacial. Asigna un poligono a cada predio usando 6 metodos en cascada: point-in-polygon, nearest 10m, herencia por coordenada, manzana neighbor, getFeatureInfo SII, fallback.</td>
              <td style={tdStyle}>CSV + GPKG por comuna con datos tabulares y geometria.</td>
              <td style={tdStyle}>~3 horas</td>
            </tr>
            <tr>
              <td style={tdStyle}><strong>Fase 4</strong></td>
              <td style={tdStyle}>Enriquecimiento con catastro semestral. Agrega 24 columnas de edificacion/construccion del TXT (pisos, materiales, calidades, bienes comunes, padre).</td>
              <td style={tdStyle}>CSV (112 cols) + GPKG + CSV raw por comuna. Producto final.</td>
              <td style={tdStyle}>~50 min</td>
            </tr>
          </tbody>
        </table>
      </section>

      {/* ── Three layers ── */}
      <section style={sectionStyle}>
        <h2 style={h2Style}>Las tres capas de datos</h2>
        <p style={pStyle}>
          Para cada una de las 343 comunas, se producen tres archivos complementarios:
        </p>

        <h3 style={h3Style}>GeoPackage (.gpkg)</h3>
        <p style={pStyle}>
          Contiene todos los poligonos vectorizados del territorio comunal en EPSG:4326, con los atributos
          tabulares donde hay match. Es el archivo principal para analisis geoespacial en QGIS, PostGIS, Python
          (GeoPandas), R, o cualquier herramienta SIG. Cada feature tiene la geometria del lote y las variables
          del predio: avaluo, superficie, destino, coordenadas, area homogenea, valor comercial de suelo, y las
          columnas de construccion del catastro semestral (pisos, materiales, calidades).
        </p>

        <h3 style={h3Style}>CSV procesado (.csv)</h3>
        <p style={pStyle}>
          Contiene todas las filas: predios con poligono, predios sin poligono (pero con lat/lon y datos
          tabulares), y poligonos huerfanos sin datos tabulares. Es el archivo mas completo: ~112 columnas
          incluyendo datos de la API SII, el catastro semestral, areas homogeneas y observatorio de suelo
          urbano 2025. Sin geometria (por ser CSV), pero con coordenadas lat/lon donde existen.
        </p>

        <h3 style={h3Style}>CSV crudo (_raw.csv)</h3>
        <p style={pStyle}>
          Slice directo del catastro semestral del SII para esa comuna: las 39 columnas originales del archivo
          <code style={codeStyle}>BRTMPNACROL</code> ya parseadas y en formato tabular limpio. Incluye datos
          que no estan en las otras capas, como la direccion catastral original, serie del rol, indicador de
          aseo, y cuota trimestral de contribuciones. Util para quien necesita los datos del SII en su forma
          mas pura, sin transformaciones.
        </p>
      </section>

      {/* ── Key variables ── */}
      <section style={sectionStyle}>
        <h2 style={h2Style}>Variables principales</h2>
        <table style={tableStyle}>
          <thead>
            <tr>
              <th style={thStyle}>Variable</th>
              <th style={thStyle}>Fuente</th>
              <th style={thStyle}>Descripcion</th>
            </tr>
          </thead>
          <tbody>
            {[
              ['comuna, manzana, predio', 'SII', 'Identificacion unica del predio (rol catastral)'],
              ['lat, lon', 'API SII + BCN', 'Coordenadas WGS84'],
              ['valorTotal', 'API SII', 'Avaluo fiscal total (CLP)'],
              ['supTerreno', 'API SII', 'Superficie del terreno (m2)'],
              ['supConsMt2', 'API SII', 'Superficie construida (m2)'],
              ['valorComercial_clp_m2', 'API SII', 'Valor comercial de suelo por m2 (CLP)'],
              ['destinoDescripcion', 'API SII', 'Destino del predio (Habitacional, Comercial, etc.)'],
              ['ah, ah_valorUnitario', 'API SII', 'Area homogenea y valor unitario de suelo'],
              ['txt_direccion', 'TXT SII', 'Direccion segun rol de contribuciones'],
              ['rc_avaluo_total', 'TXT SII', 'Avaluo fiscal total segun rol (CLP)'],
              ['dc_contribucion_semestral', 'TXT SII', 'Contribucion semestral (CLP)'],
              ['pisos_max', 'TXT SII', 'Numero maximo de pisos construidos'],
              ['materiales', 'TXT SII', 'Codigos de materialidad (A=Acero, B=Hormigon, C=Albanileria, E=Madera)'],
              ['calidades', 'TXT SII', 'Calidad constructiva (1=Superior a 5=Inferior)'],
              ['dc_bc1_comuna/manzana/predio', 'TXT SII', 'Bien comun del edificio (identifica el lote padre)'],
              ['pol_area_m2', 'Pipeline', 'Area del poligono vectorizado (m2)'],
              ['geometry', 'Pipeline', 'Poligono predial vectorizado (EPSG:4326, ~30 cm precision)'],
            ].map(([v, src, desc], i) => (
              <tr key={i}>
                <td style={tdStyle}><code style={codeStyle}>{v}</code></td>
                <td style={tdStyle}>{src}</td>
                <td style={tdStyle}>{desc}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      {/* ── Coverage ── */}
      <section style={sectionStyle}>
        <h2 style={h2Style}>Cobertura</h2>
        <table style={tableStyle}>
          <thead>
            <tr>
              <th style={thStyle}>Metrica</th>
              <th style={thStyle}>Valor</th>
            </tr>
          </thead>
          <tbody>
            {[
              ['Total filas CSV', '11,306,273'],
              ['Total poligonos (GPKG)', '9,128,582'],
              ['Predios con datos + poligono', '7,292,537'],
              ['Poligonos huerfanos (sin datos tabulares)', '1,836,045'],
              ['Predios catastro-only (sin poligono)', '2,177,687'],
              ['Predios con coordenadas lat/lon', '7,083,741'],
              ['Comunas cubiertas', '343 de 346'],
              ['Precision geometrica', '~30 cm (zoom 19 WMS)'],
              ['Periodo de datos', 'Segundo semestre 2025'],
            ].map(([k, v], i) => (
              <tr key={i}>
                <td style={tdStyle}>{k}</td>
                <td style={{ ...tdStyle, fontVariantNumeric: 'tabular-nums', fontWeight: 500 }}>{v}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      {/* ── Impact ── */}
      <section style={sectionStyle}>
        <h2 style={h2Style}>Impacto publico</h2>
        <p style={pStyle}>
          Este proyecto nace de la tesis <em>"Datos publicos como catalizador de la industria: el caso de los
          datos catastrales del Servicio de Impuestos Internos en Chile"</em>, desarrollada en el Magister en
          Analitica de Negocios de la Universidad de Chile y MIT Sloan School of Management (2026).
        </p>
        <p style={pStyle}>
          La tesis documenta que la brecha entre disponibilidad formal y acceso efectivo a los datos catastrales
          genera un costo estimado de mas de USD 2 millones anuales para el mercado chileno, concentra la
          capacidad analitica en los actores mas grandes, y deja al resto operando con informacion incompleta
          o de segunda mano.
        </p>
        <p style={pStyle}>
          Catastral.cl elimina esa barrera. Cualquier actor — brokers inmobiliarios, investigadores, municipios,
          periodistas, emprendedores — puede descargar el dataset completo de cualquier comuna de Chile, sin
          costo, sin registro, y en formatos analiticos estandar (CSV, GeoPackage).
        </p>
        <p style={pStyle}>
          Los casos de uso documentados en la tesis incluyen analisis de plusvalia y valor de suelo, prospeccion
          de predios por destino y superficie, identificacion de suelo subutilizado, due diligence con datos
          objetivos, e inteligencia de localizacion para estaciones de servicio.
        </p>
      </section>

      {/* ── Legal ── */}
      <section style={sectionStyle}>
        <h2 style={h2Style}>Marco legal</h2>
        <p style={pStyle}>
          Los datos catastrales del SII son informacion publica por naturaleza: no contienen datos personales
          protegidos, sino registros administrativos del patrimonio inmobiliario del pais. La Ley 20.285 de
          Transparencia y Acceso a la Informacion Publica consagra el derecho de cualquier persona a solicitar
          y recibir informacion de los organos del Estado. El SII cumple formalmente con el mandato de
          publicacion al publicar semestralmente el Rol de Contribuciones de Bienes Raices. Este proyecto
          estructura y redistribuye esos datos publicos.
        </p>
      </section>

      {/* ── CTA ── */}
      <div style={{ textAlign: 'center', padding: '32px 0', borderTop: '1px solid var(--color-border)' }}>
        <Link to="/tienda" className="btn-primary" style={{ display: 'inline-flex', alignItems: 'center', gap: 8 }}>
          Ir a la Tienda
        </Link>
      </div>
    </div>
  )
}
