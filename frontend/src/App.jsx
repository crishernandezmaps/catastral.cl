import { Routes, Route } from 'react-router-dom'
import Header from './components/Header'
import Footer from './components/Footer'
import Home from './pages/Home'
import Descargas from './pages/Descargas'
import Tienda from './pages/Tienda'
import Metodologia from './pages/Metodologia'
import MisCompras from './pages/MisCompras'
import Admin from './pages/Admin'
import LoginModal from './components/LoginModal'

export default function App() {
  return (
    <>
      <Header />
      <LoginModal />
      <main style={{ flex: 1 }}>
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/descargas" element={<Descargas />} />
          <Route path="/tienda" element={<Tienda />} />
          <Route path="/comunas" element={<Tienda />} />
          <Route path="/metodologia" element={<Metodologia />} />
          <Route path="/mis-compras" element={<MisCompras />} />
          <Route path="/admin" element={<Admin />} />
        </Routes>
      </main>
      <Footer />
    </>
  )
}
