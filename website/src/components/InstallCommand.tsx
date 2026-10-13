import {useState, type ReactNode} from 'react';
import styles from './InstallCommand.module.css';

export const INSTALL =
  'curl -fsSL https://raw.githubusercontent.com/menraromial/diapason/main/install.sh | sh';

/** The install one-liner, on a dark background, with a copy button. */
export default function InstallCommand({tone = 'dark'}: {tone?: 'dark' | 'plain'}): ReactNode {
  const [copied, setCopied] = useState(false);
  const copy = () => {
    navigator.clipboard?.writeText(INSTALL).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  };
  return (
    <div className={`${styles.box} ${tone === 'plain' ? styles.plain : ''}`}>
      <code className={styles.cmd}>
        <span className={styles.prompt}>$</span> {INSTALL}
      </code>
      <button type="button" className={styles.copy} onClick={copy} aria-label="Copy the command">
        {copied ? 'Copied' : 'Copy'}
      </button>
    </div>
  );
}
