export default function HeroAnimation() {
  return (
    <div className="hero-anim">
      <div className="hero-anim-inner">
        <svg viewBox="0 0 1200 700" preserveAspectRatio="xMidYMid slice" className="hero-svg" xmlns="http://www.w3.org/2000/svg">
          <defs>
            <linearGradient id="accent-fade" x1="0" y1="0" x2="1" y2="1">
              <stop offset="0%" stopColor="#000000" stopOpacity="0.08" />
              <stop offset="100%" stopColor="#0ea5e9" stopOpacity="0.03" />
            </linearGradient>
            <linearGradient id="scan-grad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="transparent" />
              <stop offset="45%" stopColor="rgba(14,165,233,0.0)" />
              <stop offset="49%" stopColor="rgba(14,165,233,0.05)" />
              <stop offset="50%" stopColor="rgba(14,165,233,0.10)" />
              <stop offset="51%" stopColor="rgba(14,165,233,0.05)" />
              <stop offset="55%" stopColor="rgba(14,165,233,0.0)" />
              <stop offset="100%" stopColor="transparent" />
            </linearGradient>
          </defs>
          <g className="scene-group">
            {/* Grid */}
            <g className="grid-lines" fill="none">
              {[...Array(12)].map((_, i) => <line key={`h${i}`} x1={0} y1={i*60+10} x2={1200} y2={i*60+10} stroke="rgba(14,165,233,0.06)" strokeWidth="0.5" />)}
              {[...Array(16)].map((_, i) => <line key={`v${i}`} x1={i*80} y1={0} x2={i*80} y2={700} stroke="rgba(14,165,233,0.06)" strokeWidth="0.5" />)}
            </g>
            {/* Scan line */}
            <rect className="scan-line" x="0" y="-700" width="1200" height="1400" fill="url(#scan-grad)" />
            {/* Blocks */}
            <g className="blocks" strokeWidth="1.2" fill="none">
              <polygon points="100,100 270,60 370,150 200,190" />
              <polygon points="120,220 290,180 390,270 220,310" />
              <polygon points="800,80 970,40 1070,130 900,170" />
              <polygon points="820,200 990,160 1090,250 920,290" />
              <polygon points="80,400 250,360 350,450 180,490" />
              <polygon points="100,520 270,480 370,570 200,610" />
              <polygon points="810,410 980,370 1080,460 910,500" />
              <polygon points="830,530 1000,490 1100,580 930,620" />
            </g>
            {/* Parcels */}
            <g className="parcels" stroke="#000000" strokeWidth="0.8" fill="url(#accent-fade)">
              <polygon points="100,100 185,80 235,125 150,145" className="p p1" />
              <polygon points="185,80 270,60 320,105 235,125" className="p p2" />
              <polygon points="150,145 235,125 285,170 200,190" className="p p3" />
              <polygon points="235,125 320,105 370,150 285,170" className="p p4" />
              <polygon points="120,220 205,200 255,245 170,265" className="p p5" />
              <polygon points="205,200 290,180 340,225 255,245" className="p p6" />
              <polygon points="800,80 885,60 935,105 850,125" className="p p7" />
              <polygon points="885,60 970,40 1020,85 935,105" className="p p8" />
              <polygon points="850,125 935,105 985,150 900,170" className="p p9" />
              <polygon points="935,105 1020,85 1070,130 985,150" className="p p10" />
              <polygon points="80,400 165,380 215,425 130,445" className="p p11" />
              <polygon points="165,380 250,360 300,405 215,425" className="p p12" />
              <polygon points="130,445 215,425 265,470 180,490" className="p p13" />
              <polygon points="810,410 895,390 945,435 860,455" className="p p14" />
              <polygon points="895,390 980,370 1030,415 945,435" className="p p15" />
              <polygon points="860,455 945,435 995,480 910,500" className="p p16" />
            </g>
            {/* Data dots */}
            <g className="data-dots">
              <circle cx="190" cy="140" r="1.5" className="dot d1" />
              <circle cx="310" cy="250" r="1.5" className="dot d2" />
              <circle cx="920" cy="120" r="1.5" className="dot d3" />
              <circle cx="1020" cy="220" r="1.5" className="dot d4" />
              <circle cx="170" cy="440" r="1.5" className="dot d5" />
              <circle cx="260" cy="550" r="1.5" className="dot d6" />
              <circle cx="900" cy="460" r="1.5" className="dot d7" />
              <circle cx="1000" cy="560" r="1.5" className="dot d8" />
            </g>
            {/* Pings */}
            <g className="ping ping-1"><circle cx="235" cy="125" r="3" fill="#000000" opacity="0.6" /><circle cx="235" cy="125" r="10" fill="none" stroke="#000000" strokeWidth="0.8" className="ping-ring" /></g>
            <g className="ping ping-2"><circle cx="935" cy="105" r="3" fill="#000000" opacity="0.6" /><circle cx="935" cy="105" r="10" fill="none" stroke="#000000" strokeWidth="0.8" className="ping-ring" /></g>
            <g className="ping ping-3"><circle cx="215" cy="425" r="3" fill="#000000" opacity="0.6" /><circle cx="215" cy="425" r="10" fill="none" stroke="#000000" strokeWidth="0.8" className="ping-ring" /></g>
            <g className="ping ping-4"><circle cx="945" cy="435" r="3" fill="#000000" opacity="0.6" /><circle cx="945" cy="435" r="10" fill="none" stroke="#000000" strokeWidth="0.8" className="ping-ring" /></g>
          </g>
        </svg>
      </div>
      <div className="hero-vignette" />
    </div>
  )
}
