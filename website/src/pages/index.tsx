import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import Layout from '@theme/Layout';
import CodeBlock from '@theme/CodeBlock';
import Heading from '@theme/Heading';
import styles from './index.module.css';

const points = [
  {
    title: 'Open loop, every request counted',
    text: 'Requests follow a schedule computed independently of the responses. At the end of every run, scheduled = completed + failed + not sent + in flight, or the run exits with code 4.',
  },
  {
    title: 'The client reports on itself',
    text: 'Generator lag and connection wait are recorded apart from server time. Fidelity checks say whether the client kept to its schedule and whether its own limits shaped the load.',
  },
  {
    title: 'Measurements for other tools',
    text: 'A versioned JSON summary, HdrHistogram interval logs, a per-request trace in CSV or binary columnar format, and a live OpenMetrics endpoint. HTTP/1.1, HTTP/2, TLS, and several client nodes.',
  },
];

export default function Home(): ReactNode {
  return (
    <Layout
      title="Open-loop load generator"
      description="diapason: an open-loop load generator whose measurements can be trusted">
      <header className={styles.hero}>
        <div className="container">
          <Heading as="h1">diapason</Heading>
          <p className="hero__subtitle">
            An open-loop load generator whose measurements can be trusted
          </p>
          <div className={styles.install}>
            <CodeBlock language="bash">
              curl -fsSL https://raw.githubusercontent.com/menraromial/diapason/main/install.sh | sh
            </CodeBlock>
          </div>
          <div className={styles.buttons}>
            <Link className="button button--primary button--lg" to="/docs/intro">
              Read the documentation
            </Link>
            <Link className="button button--secondary button--lg" to="/docs/quick-start">
              Quick start
            </Link>
          </div>
        </div>
      </header>
      <main className={`container ${styles.points}`}>
        <div className="row">
          {points.map((p) => (
            <div key={p.title} className="col col--4">
              <Heading as="h3">{p.title}</Heading>
              <p>{p.text}</p>
            </div>
          ))}
        </div>
      </main>
    </Layout>
  );
}
