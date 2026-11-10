import { useFeed } from "./feed";
import { CrashPanel, DepthChart, Footer, Header, Ladder, OrderForm, ProofPanel, StatsPanel, Tape } from "./components";

const symbol = new URLSearchParams(location.search).get("symbol") || "PIT";

export function App() {
  const { book, mode, downSince } = useFeed(symbol);
  return (
    <div className="app">
      <Header book={book} mode={mode} />
      {mode === "replay" && (
        <div className="banner" role="status">
          The engine is asleep (free-tier hosts spin down when idle) and is waking up
          {downSince ? `, ${Math.max(1, Math.round((Date.now() - downSince) / 1000))} s so far` : ""}. This is a <b>recorded session</b> produced by the same engine; the live feed takes over automatically.
        </div>
      )}
      <main className="grid">
        <Ladder book={book} />
        <div className="col">
          <DepthChart book={book} />
          <Tape book={book} />
        </div>
        <div className="col">
          <OrderForm book={book} mode={mode} />
          <CrashPanel book={book} mode={mode} />
        </div>
        <div className="wide"><StatsPanel mode={mode} symbol={symbol} /></div>
        <div className="wide"><ProofPanel /></div>
      </main>
      <Footer />
    </div>
  );
}
