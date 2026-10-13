import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import useBaseUrl from '@docusaurus/useBaseUrl';
import Layout from '@theme/Layout';
import CodeBlock from '@theme/CodeBlock';
import Heading from '@theme/Heading';
import InstallCommand from '@site/src/components/InstallCommand';
import styles from './index.module.css';

// Output of `diapason run --rate 5000 --warmup 2s --duration 10s` against
// `diapason server --capacity 20000` (see the "Your first run" guide).
const TERMINAL = `$ diapason run --url http://127.0.0.1:8080/ --rate 5000 \\
      --warmup 2s --duration 10s --threads 4 --connections 100
requests: scheduled 60350 = completed 60350 + failed 0
          + not sent 0 + in flight 0 -> balanced
phase measure (measured) [2.0s, 12.0s): scheduled 50317
  achieved 5031.7/s by intended time, 5031.3/s by completion
  ms                  p50      p90      p99    p99.9
  corrected         1.187    1.335    1.460    1.561
  server            1.181    1.325    1.444    1.532
  connection wait   0.003    0.007    0.018    0.056
  generator lag     0.000    0.002    0.062    0.130
fidelity: ok
exit 0: ok`;

const stats = [
  {value: '200 000', unit: 'req/s', text: 'HTTP/1.1 keep-alive from one machine, every fidelity check passing'},
  {value: '1.4 – 2.9', unit: 'µs', text: 'generator lag at p99 on Linux at that rate'},
  {value: '0', unit: 'lost', text: 'every scheduled request ends in a counted bucket, checked at each run'},
  {value: '0.1', unit: 'µs', text: 'start-time alignment of two agents, one with its clock 500 ms ahead'},
];

const features = [
  {
    title: 'Open-loop schedules',
    text: 'Constant, Poisson, ramps, steps, piecewise profiles and replay files, in named phases, split across workers so that the aggregate stays smooth.',
    icon: 'M3 12h4l3-8 4 16 3-8h4',
  },
  {
    title: 'A client that reports on itself',
    text: 'Generator lag, connection wait, send-rate ratio, burstiness, admission, CPU starvation: each a check with a threshold and an exit code.',
    icon: 'M9 12l2 2 4-4M12 3l8 4v5c0 5-3.5 8-8 9-4.5-1-8-4-8-9V7z',
  },
  {
    title: 'HTTP/1.1, HTTP/2, TLS',
    text: 'Keep-alive and pipelining, HTTP/2 streams with a limit, rustls with SNI and client certificates, a bounded pool or connections on demand.',
    icon: 'M4 6h16M4 12h16M4 18h10',
  },
  {
    title: 'Realistic requests',
    text: 'Templates with Zipf, uniform, normal and choice variables, CSV or JSON-lines data files, weighted scenarios with the realised mix.',
    icon: 'M8 4H5v16h3M16 4h3v16h-3M10 9h4M10 15h4',
  },
  {
    title: 'Results for other tools',
    text: 'A versioned JSON summary, HdrHistogram interval logs, per-request traces in CSV or a compact binary format, live OpenMetrics.',
    icon: 'M4 20V10M10 20V4M16 20v-7M22 20H2',
  },
  {
    title: 'Several client nodes',
    text: 'A coordinator and agents with clock-offset measurement: the same requests as one node, with merged histograms and checks.',
    icon: 'M5 12a2 2 0 1 0 0-.01M19 6a2 2 0 1 0 0-.01M19 18a2 2 0 1 0 0-.01M7 11l10-4M7 13l10 4',
  },
];

function Icon({d}: {d: string}): ReactNode {
  return (
    <svg className={styles.icon} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <path d={d} />
    </svg>
  );
}

export default function Home(): ReactNode {
  return (
    <Layout
      title="Open-loop load generator"
      description="diapason: an open-loop load generator whose measurements can be trusted">
      <header className={styles.hero}>
        <div className={`container ${styles.heroGrid}`}>
          <div className={styles.heroText}>
            <p className={styles.eyebrow}>Open-source load generator · Rust</p>
            <Heading as="h1" className={styles.title}>
              Load tests whose numbers you can trust
            </Heading>
            <p className={styles.lead}>
              diapason sends requests on a schedule that does not wait for the
              server, accounts for every one of them, separates its own delays
              from the server's, and tells you when the client itself was the
              bottleneck.
            </p>
            <div className={styles.install}>
              <InstallCommand />
            </div>
            <div className={styles.buttons}>
              <Link className="button button--primary button--lg" to="/docs/guides/first-run">
                Your first run
              </Link>
              <Link className={`button button--lg ${styles.ghost}`} to="/docs/intro">
                Documentation
              </Link>
            </div>
          </div>
          <div className={styles.terminal} aria-label="diapason output">
            <div className={styles.terminalBar}>
              <span />
              <span />
              <span />
            </div>
            <pre className={styles.terminalBody}>{TERMINAL}</pre>
          </div>
        </div>
      </header>

      <section className={styles.stats}>
        <div className="container">
          <div className={styles.statsGrid}>
            {stats.map((s) => (
              <div key={s.text} className={styles.stat}>
                <div className={styles.statValue}>
                  {s.value} <span>{s.unit}</span>
                </div>
                <div className={styles.statText}>{s.text}</div>
              </div>
            ))}
          </div>
          <p className={styles.statNote}>
            Measured on one 22-CPU Linux machine; see{' '}
            <Link to="/docs/design-measurements">Design measurements</Link>.
          </p>
        </div>
      </section>

      <main>
        <section className={styles.section}>
          <div className="container">
            <Heading as="h2" className={styles.sectionTitle}>Four delays per request</Heading>
            <p className={styles.sectionLead}>
              Each request has an intended send time from the schedule. Corrected
              latency runs from that time to the last byte of the response, so a
              late client or a full pool cannot hide a slow server.
            </p>
            <img className={styles.diagram} src={useBaseUrl('/img/delays.svg')} alt="Generator lag, connection wait and server time add up to corrected latency" />
          </div>
        </section>

        <section className={`${styles.section} ${styles.alt}`}>
          <div className={`container ${styles.split}`}>
            <div>
              <Heading as="h2" className={styles.sectionTitle}>Why open loop matters</Heading>
              <p>
                Same server, same 1-second freeze, same rate of about 1260
                requests per second. A closed-loop client waits for each
                response before sending the next request, so only the few
                requests in flight see the freeze: its p99 stays at 5 ms. An
                open-loop client keeps sending, and the p99 shows what users
                saw.
              </p>
              <Link to="/docs/guides/coordinated-omission">Reproduce it in two commands →</Link>
            </div>
            <table className={styles.compare}>
              <thead>
                <tr>
                  <th></th>
                  <th>p50</th>
                  <th>p90</th>
                  <th>p99</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td>closed loop, 2 connections</td>
                  <td>0.97 ms</td>
                  <td>2.6 ms</td>
                  <td className={styles.good}>5.0 ms</td>
                </tr>
                <tr>
                  <td>open loop (diapason)</td>
                  <td>1.21 ms</td>
                  <td>622 ms</td>
                  <td className={styles.bad}>1150 ms</td>
                </tr>
              </tbody>
            </table>
          </div>
        </section>

        <section className={styles.section}>
          <div className="container">
            <Heading as="h2" className={styles.sectionTitle}>What it does</Heading>
            <div className={styles.features}>
              {features.map((f) => (
                <div key={f.title} className={styles.card}>
                  <Icon d={f.icon} />
                  <Heading as="h3">{f.title}</Heading>
                  <p>{f.text}</p>
                </div>
              ))}
            </div>
          </div>
        </section>

        <section className={`${styles.section} ${styles.alt}`}>
          <div className="container">
            <Heading as="h2" className={styles.sectionTitle}>Try it in a minute</Heading>
            <p className={styles.sectionLead}>
              diapason ships with a reference server, so you can try every
              feature on your own machine.
            </p>
            <div className={styles.steps}>
              <div>
                <span className={styles.stepNo}>1</span>
                <p>Install the binary</p>
                <InstallCommand tone="plain" />
              </div>
              <div>
                <span className={styles.stepNo}>2</span>
                <p>Start a server that handles 20 000 req/s</p>
                <CodeBlock language="bash">{'diapason server --capacity 20000 &'}</CodeBlock>
              </div>
              <div>
                <span className={styles.stepNo}>3</span>
                <p>Send 5000 req/s for 10 seconds</p>
                <CodeBlock language="bash">
                  {'diapason run --url http://127.0.0.1:8080/ \\\n  --rate 5000 --warmup 2s --duration 10s'}
                </CodeBlock>
              </div>
            </div>
            <div className={styles.center}>
              <Link className="button button--primary button--lg" to="/docs/guides/first-run">
                Read the guides
              </Link>
            </div>
          </div>
        </section>
      </main>
    </Layout>
  );
}
